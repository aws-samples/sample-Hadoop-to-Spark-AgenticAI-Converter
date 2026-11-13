"""Stage 4: Generate conversion plans via LLM with schema validation.

The LLM reads the JobIR and produces a ConversionPlan JSON.
A deterministic validator checks the plan against the required schema
before accepting it. Falls back to deterministic planning if LLM output
fails validation after max retries.
"""
import json
import logging
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from converter.config_loader import ConverterConfig
from converter.operator_map import OPERATOR_MAP, detect_script_type_from_command

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Required ConversionPlan schema
# ---------------------------------------------------------------------------
REQUIRED_PLAN_FIELDS = {
    "chunk_id": str,
    "dag_config": dict,
    "tasks": list,
    "total_tasks": int,
    "source_job_count": int,
    "dependency_graph": dict,
}

REQUIRED_TASK_FIELDS = {
    "task_id": str,
    "source_job_id": str,
    "operator": str,
    "dependencies": list,
}

VALID_OPERATORS = {
    "emr_serverless", "emr_add_steps", "bash", "python",
    "dummy", "external_task_sensor", "trigger_dag_run",
    # Also accept class names the LLM might use
    "EmrServerlessStartJobOperator", "BashOperator", "EmptyOperator",
    "DummyOperator", "ExternalTaskSensor", "TriggerDagRunOperator",
    "PythonOperator", "EmrAddStepsOperator",
}

# Normalise LLM operator class names → canonical operator type strings
OPERATOR_NORMALISE = {
    "EmrServerlessStartJobOperator": "emr_serverless",
    "EmrAddStepsOperator": "emr_add_steps",
    "BashOperator": "bash",
    "PythonOperator": "python",
    "EmptyOperator": "dummy",
    "DummyOperator": "dummy",
    "ExternalTaskSensor": "external_task_sensor",
    "TriggerDagRunOperator": "trigger_dag_run",
}


# ---------------------------------------------------------------------------
# Public entry point — called by MCP server
# ---------------------------------------------------------------------------

def build_plan_prompt(
    chunk: Dict,
    spark_sql_mapping_file: Path,
    cfg: ConverterConfig,
    output_dir: Path,
) -> str:
    """Build the LLM prompt for conversion planning.

    The MCP server calls this, sends the prompt to the LLM, then calls
    validate_and_save_plan() on the LLM's response.
    """
    jobs = chunk["jobs"]
    chunk_id = chunk["chunk_id"]
    folder = chunk["folder"]

    spark_sql_mapping = _load_spark_sql_mapping(spark_sql_mapping_file)
    operator_map_summary = _build_operator_map_summary()

    from converter.prompts.prompt_loader import load_prompt
    template = load_prompt("generate_conversion_plan")

    jobs_json = json.dumps(jobs, indent=2)
    mapping_json = json.dumps(spark_sql_mapping, indent=2)
    plan_file = output_dir / "plans" / f"{chunk_id}_plan.json"
    plan_file.parent.mkdir(parents=True, exist_ok=True)

    prompt = (template
              .replace("{chunk_id}", chunk_id)
              .replace("{folder}", folder)
              .replace("{job_count}", str(len(jobs)))
              .replace("{jobs_json}", jobs_json)
              .replace("{operator_map}", operator_map_summary)
              .replace("{spark_sql_mapping}", mapping_json)
              .replace("{dag_defaults}", json.dumps(cfg.dag_defaults, indent=2))
              .replace("{var_emr_app_id}", cfg.var_emr_app_id)
              .replace("{var_emr_exec_role_arn}", cfg.var_emr_exec_role_arn)
              .replace("{var_scripts_bucket}", cfg.var_scripts_bucket)
              .replace("{var_logs_bucket}", cfg.var_logs_bucket)
              .replace("{output_file}", str(plan_file))
              )

    return prompt


def validate_and_save_plan(
    llm_output: str,
    chunk: Dict,
    spark_sql_mapping_file: Path,
    cfg: ConverterConfig,
    output_dir: Path,
) -> Optional[Dict]:
    """Validate LLM-produced ConversionPlan JSON and save to disk.

    Falls back to deterministic planning if LLM output fails validation.

    Returns the plan dict on success, None on failure.
    """
    chunk_id = chunk["chunk_id"]
    plans_dir = output_dir / "plans"
    plans_dir.mkdir(parents=True, exist_ok=True)
    plan_file = plans_dir / f"{chunk_id}_plan.json"

    # Try to extract and validate JSON from LLM output
    json_blocks = _extract_json_blocks(llm_output)

    for block in json_blocks:
        try:
            plan = json.loads(block)
        except json.JSONDecodeError:
            continue

        if not isinstance(plan, dict):
            continue

        # Normalise operator names the LLM may have used
        _normalise_plan_operators(plan)

        errors = _validate_plan(plan, chunk)
        if not errors:
            # Post-process: schedule + OR trigger rules — deterministic enforcement
            _apply_schedule(plan, output_dir, chunk_id)
            _apply_or_trigger_rules(plan, output_dir, chunk_id)

            plan_file.write_text(json.dumps(plan, indent=2), encoding="utf-8")
            logger.info("✓ LLM plan validated: chunk='%s' (%d tasks) → %s",
                        chunk_id, plan.get("total_tasks", 0), plan_file.name)
            return plan

            plan_file.write_text(json.dumps(plan, indent=2), encoding="utf-8")
            logger.info("✓ LLM plan validated: chunk='%s' (%d tasks) → %s",
                        chunk_id, plan.get("total_tasks", 0), plan_file.name)
            return plan
        else:
            logger.warning("LLM plan validation errors for '%s': %s", chunk_id, errors)

    # Fallback to deterministic planning
    logger.warning("LLM plan failed validation for '%s' — using deterministic fallback", chunk_id)
    plan = _deterministic_fallback_plan(chunk, spark_sql_mapping_file, cfg)
    plan_file.write_text(json.dumps(plan, indent=2), encoding="utf-8")
    logger.info("✓ Fallback plan saved: chunk='%s' (%d tasks)", chunk_id, plan["total_tasks"])
    return plan


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------

def _validate_plan(plan: Dict, chunk: Dict) -> List[str]:
    """Return list of validation errors. Empty list = valid."""
    errors = []

    for field, expected_type in REQUIRED_PLAN_FIELDS.items():
        if field not in plan:
            errors.append(f"Missing required field: {field}")
        elif not isinstance(plan[field], expected_type):
            errors.append(f"Field '{field}' wrong type: expected {expected_type.__name__}")

    if errors:
        return errors

    # dag_config must have dag_id
    dag_config = plan.get("dag_config", {})
    if not dag_config.get("dag_id"):
        errors.append("dag_config.dag_id is missing or empty")

    # tasks validation
    tasks = plan.get("tasks", [])
    if not tasks:
        errors.append("tasks list is empty")
        return errors

    task_ids_seen = set()
    for i, task in enumerate(tasks):
        if not isinstance(task, dict):
            errors.append(f"Task {i} is not a dict")
            continue
        for field, expected_type in REQUIRED_TASK_FIELDS.items():
            if field not in task:
                errors.append(f"Task {i} missing field: {field}")
            elif not isinstance(task[field], expected_type):
                errors.append(f"Task {i} field '{field}' wrong type")

        op = task.get("operator", "")
        if op not in VALID_OPERATORS:
            errors.append(f"Task {i} has invalid operator: '{op}'")

        # ExternalTaskSensor must NOT appear as a task — it's rendered from external_dependencies
        if op in ("external_task_sensor", "ExternalTaskSensor"):
            errors.append(
                f"Task {i} ('{task.get('task_id')}') uses ExternalTaskSensor as a task. "
                "Cross-folder dependencies must be in external_dependencies[], not tasks[]. "
                "The DAG builder renders sensors automatically."
            )

        tid = task.get("task_id", "")
        if tid in task_ids_seen:
            errors.append(f"Duplicate task_id: '{tid}'")
        task_ids_seen.add(tid)

    # 1:1 mapping check
    expected_count = chunk.get("job_count", 0)
    actual_count = len(tasks)
    if expected_count > 0 and actual_count != expected_count:
        errors.append(f"Task count mismatch: expected {expected_count}, got {actual_count}")

    # dependency_graph keys must match task_ids
    dep_graph = plan.get("dependency_graph", {})
    for node in dep_graph:
        if node not in task_ids_seen:
            errors.append(f"dependency_graph references unknown task_id: '{node}'")

    return errors


def _normalise_plan_operators(plan: Dict):
    """Normalise operator class names to canonical type strings in-place."""
    for task in plan.get("tasks", []):
        if isinstance(task, dict):
            op = task.get("operator", "")
            if op in OPERATOR_NORMALISE:
                task["operator"] = OPERATOR_NORMALISE[op]
            # Also normalise operator_type field if present
            op_type = task.get("operator_type", "")
            if op_type in OPERATOR_NORMALISE:
                task["operator_type"] = OPERATOR_NORMALISE[op_type]
                if not task.get("operator"):
                    task["operator"] = OPERATOR_NORMALISE[op_type]


# ---------------------------------------------------------------------------
# Deterministic fallback plan builder
# ---------------------------------------------------------------------------

def _deterministic_fallback_plan(
    chunk: Dict,
    spark_sql_mapping_file: Path,
    cfg: ConverterConfig,
) -> Dict:
    """Build a ConversionPlan deterministically from JobIR + OPERATOR_MAP."""
    spark_sql_mapping = _load_spark_sql_mapping(spark_sql_mapping_file)
    chunk_id = chunk["chunk_id"]
    folder = chunk["folder"]
    jobs: List[Dict] = chunk["jobs"]
    dag_id = _safe_dag_id(folder)

    tasks: List[Dict] = []
    airflow_variables: Dict[str, Dict] = {}
    dependency_graph: Dict[str, List[str]] = {}

    for job in jobs:
        task = _build_task_deterministic(job, spark_sql_mapping, cfg)
        tasks.append(task)

        for var_name, var_value in job.get("variables", {}).items():
            if var_name.startswith("AWS-"):
                continue
            if var_name not in airflow_variables:
                airflow_variables[var_name] = {
                    "value": "__PLACEHOLDER__",
                    "description": f"Variable {var_name} from job {job['job_id']}",
                    "source": job["job_id"],
                    "original_value": var_value,
                }

        dep_task_ids = [_safe_task_id(d) for d in job.get("dependencies", [])]
        dependency_graph[task["task_id"]] = dep_task_ids

    execution_order = _topological_sort(dependency_graph)

    # Derive schedule from folder metadata if available
    schedule = _derive_schedule(chunk)

    return {
        "chunk_id": chunk_id,
        "folder": folder,
        "source_type": "control_m",
        "dag_config": {
            "dag_id": dag_id,
            "schedule": schedule,
            "start_date": "2024-01-01",
            "catchup": False,
            "tags": ["control_m_migration"],
            "default_args": cfg.dag_defaults,
        },
        "tasks": tasks,
        "total_tasks": len(tasks),
        "source_job_count": len(jobs),
        "dependency_graph": dependency_graph,
        "execution_order": execution_order,
        "airflow_variables": airflow_variables,
        "job_to_task_mapping": {
            job["job_id"]: _safe_task_id(job["job_id"]) for job in jobs
        },
        "conversion_summary": {
            "total_source_jobs": len(jobs),
            "total_target_tasks": len(tasks),
            "operator_breakdown": _count_operators(tasks),
        },
    }


def _apply_or_trigger_rules(plan: Dict, output_dir: Path, chunk_id: str):
    """Set trigger_rule=one_success on tasks that have OR conditions in the source."""
    job_ir_file = output_dir / "job_ir" / f"{chunk_id}.json"
    if not job_ir_file.exists():
        return
    try:
        with open(job_ir_file, encoding="utf-8") as f:
            job_ir = json.load(f)
        or_jobs = {
            j["job_id"]
            for j in job_ir.get("jobs", [])
            if (
                j.get("metadata", {}).get("dependency_logic", "").upper() == "OR"
                or j.get("metadata", {}).get("and_or", "").upper() == "O"
            )
        }
        if or_jobs:
            task_map = {t["source_job_id"]: t for t in plan.get("tasks", [])}
            for job_id in or_jobs:
                if job_id in task_map and not task_map[job_id].get("trigger_rule"):
                    task_map[job_id]["trigger_rule"] = "one_success"
                    logger.info("  ✓ trigger_rule=one_success applied: %s", job_id)
    except (OSError, json.JSONDecodeError):
        pass


def _apply_schedule(plan: Dict, output_dir: Path, chunk_id: str):
    """Fill schedule from source XML attributes if LLM left it null."""
    if plan.get("dag_config", {}).get("schedule"):
        return  # already set by LLM
    job_ir_file = output_dir / "job_ir" / f"{chunk_id}.json"
    if not job_ir_file.exists():
        return
    try:
        with open(job_ir_file, encoding="utf-8") as f:
            job_ir = json.load(f)
        derived = _derive_schedule(job_ir)
        if derived:
            plan.setdefault("dag_config", {})["schedule"] = derived
            sched_obj = job_ir.get("schedule") or {}
            plan["dag_config"]["schedule_notes"] = (
                f"Schedule derived from source XML: "
                f"DAYS={sched_obj.get('days')}, TIMEFROM={sched_obj.get('timefrom')} → {derived}"
            )
            logger.info("  ✓ Schedule derived: %s → %s", chunk_id, derived)
    except (OSError, json.JSONDecodeError):
        pass


def _derive_schedule(chunk: Dict) -> Optional[str]:
    """Derive Airflow cron from Control-M DAYS/TIMEFROM in JobIR schedule object."""
    # Primary: read from JobIR schedule object (populated by both LLM and deterministic parser)
    sched_obj = chunk.get("schedule") or {}
    if isinstance(sched_obj, dict):
        days = str(sched_obj.get("days") or "")
        timefrom = str(sched_obj.get("timefrom") or "")
    else:
        days, timefrom = "", ""

    # Fallback: folder metadata (used by deterministic fallback plan)
    if not days and not timefrom:
        metadata = chunk.get("metadata") or chunk.get("folder_metadata") or {}
        days = str(metadata.get("DAYS") or metadata.get("days") or "")
        timefrom = str(metadata.get("TIMEFROM") or metadata.get("timefrom") or "")

    if not days and not timefrom:
        return None

    # Parse time: HHMM → hour, minute
    hour, minute = 0, 0
    if timefrom and len(timefrom) >= 4:
        try:
            hour = int(timefrom[:2])
            minute = int(timefrom[2:4])
        except ValueError:
            pass
    elif timefrom and len(timefrom) == 3:
        try:
            hour = int(timefrom[0])
            minute = int(timefrom[1:3])
        except ValueError:
            pass

    # Parse days
    days_clean = days.strip().upper()
    if not days_clean or days_clean in ("ALL", "1-7", "1234567"):
        return f"{minute} {hour} * * *"
    if days_clean == "12345":
        return f"{minute} {hour} * * 1-5"
    if days_clean == "1":
        return f"{minute} {hour} 1 * *"
    if days_clean in ("67", "6,7"):
        return f"{minute} {hour} * * 6,7"
    # Numeric weekday list e.g. "135" → "1,3,5"
    if days_clean.isdigit() and len(days_clean) <= 7:
        day_list = ",".join(days_clean)
        return f"{minute} {hour} * * {day_list}"

    return None


def _build_task_deterministic(
    job: Dict,
    spark_sql_mapping: Dict[str, str],
    cfg: ConverterConfig,
) -> Dict:
    job_id = job["job_id"]
    job_type = job.get("job_type", "command")
    script_path = job.get("script_path")
    script_type = job.get("script_type")
    command = job.get("command")

    if not script_type and command:
        script_type = detect_script_type_from_command(command)

    operator_type, confidence = _lookup_operator(job_type, script_type)
    source_script = script_path or ""
    target_script = spark_sql_mapping.get(source_script, "")
    script_rel = _derive_script_rel_path(source_script, target_script)
    upstream_task_ids = [_safe_task_id(d) for d in job.get("dependencies", [])]

    task: Dict = {
        "task_id": _safe_task_id(job_id),
        "source_job_id": job_id,
        "source_job_name": job.get("job_name", job_id),
        "source_job_type": job_type,
        "description": job.get("description", ""),
        "operator": operator_type,
        "script_path": script_rel,
        "source_script": source_script,
        "target_script": target_script,
        "script_type": script_type,
        "dependencies": upstream_task_ids,
        "external_dependencies": [],
        "confidence_score": confidence,
    }

    if operator_type == "bash":
        if not command and script_path:
            # Script job (MEMLIB/MEMNAME) without CMDLINE: run the script itself
            interpreter = {"sh": "bash", "bash": "bash", "ksh": "ksh"}.get(script_type or "", "bash")
            command = f"{interpreter} {script_path}"
        task["bash_command"] = command or ""   # made MWAA-ready by the DAG builder

    # Cross-folder dependencies -> ExternalTaskSensor (rendered by the DAG builder)
    for ext in job.get("metadata", {}).get("external_dependencies", []) or []:
        if isinstance(ext, dict) and ext.get("source_folder") and ext.get("source_job_id"):
            task["external_dependencies"].append({
                "external_dag_id": _safe_dag_id(ext["source_folder"]),
                "external_task_id": _safe_task_id(ext["source_job_id"]),
            })
        else:
            logger.warning("Job %s: cross-folder dependency %r has no source folder; "
                           "no sensor generated", job_id, ext)

    if job_type == "sub_flow":
        sub_flow = (job.get("variables") or {}).get("SUB_FLOW_NAME")
        if sub_flow:
            task["trigger_dag_id"] = _safe_dag_id(sub_flow)

    # Preserve OR dependency logic from source XML metadata. The parser
    # records this as metadata["and_or"] == "O" (see parser.py); also accept
    # "dependency_logic" == "OR" for forward/LLM-plan compatibility. Mirrors
    # the check in _apply_or_trigger_rules(), which only runs for LLM-produced
    # plans -- this deterministic fallback path needs the same check inline.
    job_metadata = job.get("metadata", {})
    dep_logic = job_metadata.get("dependency_logic", "")
    and_or = job_metadata.get("and_or", "")
    if dep_logic.upper() == "OR" or and_or.upper() == "O":
        task["trigger_rule"] = "one_success"

    return task


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_spark_sql_mapping(mapping_file: Path) -> Dict[str, str]:
    if not mapping_file.exists():
        logger.warning("spark_sql_mapping.json not found at %s", mapping_file)
        return {}
    try:
        with open(mapping_file, encoding="utf-8") as f:
            raw = json.load(f)
        mapping = {}
        for entry in raw.get("mappings", []):
            src = entry.get("source_script", "")
            tgt = entry.get("target_script", "")
            if src and tgt:
                mapping[src] = tgt
        return mapping
    except (json.JSONDecodeError, OSError) as e:
        logger.error("Could not load spark_sql_mapping.json: %s", e)
        return {}


def _build_operator_map_summary() -> str:
    """Build a human-readable summary of the OPERATOR_MAP for the prompt."""
    lines = []
    for (source, job_type, script_type), (op, conf) in OPERATOR_MAP.items():
        script_str = script_type or "any"
        lines.append(f"  ({source}, {job_type}, {script_str}) → {op.value} [confidence={conf}]")
    return "\n".join(sorted(lines))


def _lookup_operator(job_type: str, script_type: Optional[str]) -> Tuple[str, float]:
    from converter.models import OperatorType
    key = ("control_m", job_type, script_type)
    if key in OPERATOR_MAP:
        op, conf = OPERATOR_MAP[key]
        return op.value, conf
    key_no_script = ("control_m", job_type, None)
    if key_no_script in OPERATOR_MAP:
        op, conf = OPERATOR_MAP[key_no_script]
        return op.value, conf
    if job_type == "dummy":
        return OperatorType.DUMMY.value, 1.0
    if job_type == "aws_lambda":
        return OperatorType.EMR_SERVERLESS.value, 0.80
    return OperatorType.BASH.value, 0.50


def _derive_script_rel_path(source_script: str, target_script: str) -> str:
    if target_script.startswith("s3://"):
        parts = target_script.split("/", 3)
        return parts[3] if len(parts) > 3 else Path(target_script).name
    if target_script:
        return target_script
    if source_script:
        stem = Path(source_script).stem
        return f"spark_sql/{stem}.sql"
    return "spark_sql/unknown.sql"


def _extract_json_blocks(text: str) -> List[str]:
    blocks = []
    fence_pattern = re.compile(r'```(?:json)?\s*(\{[\s\S]*?\}|\[[\s\S]*?\])\s*```', re.DOTALL)
    for m in fence_pattern.finditer(text):
        blocks.append(m.group(1).strip())
    if blocks:
        return blocks
    for start_char, end_char in [('{', '}'), ('[', ']')]:
        depth = 0
        start_idx = None
        for i, ch in enumerate(text):
            if ch == start_char:
                if depth == 0:
                    start_idx = i
                depth += 1
            elif ch == end_char:
                depth -= 1
                if depth == 0 and start_idx is not None:
                    blocks.append(text[start_idx:i+1])
                    start_idx = None
    return blocks


def _safe_task_id(name: str) -> str:
    return re.sub(r'[^a-z0-9_]', '_', name.lower()).strip('_') or 'task'


def _safe_dag_id(name: str) -> str:
    return re.sub(r'[^a-z0-9_]', '_', name.lower()).strip('_') or 'dag'


def _count_operators(tasks: List[Dict]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for t in tasks:
        op = t.get("operator", "unknown")
        counts[op] = counts.get(op, 0) + 1
    return counts


def _topological_sort(graph: Dict[str, List[str]]) -> List[str]:
    in_degree = {node: 0 for node in graph}
    for node, deps in graph.items():
        for dep in deps:
            if dep in graph:
                in_degree[node] = in_degree.get(node, 0) + 1

    queue = [n for n, d in in_degree.items() if d == 0]
    order = []
    while queue:
        node = queue.pop(0)
        order.append(node)
        for candidate, deps in graph.items():
            if node in deps:
                in_degree[candidate] -= 1
                if in_degree[candidate] == 0:
                    queue.append(candidate)

    for node in graph:
        if node not in order:
            order.append(node)

    return order

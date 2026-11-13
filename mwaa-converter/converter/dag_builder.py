"""
Deterministic DAG builder — renders Jinja2 templates from a ConversionPlan JSON.

No LLM involvement. Every line of the generated DAG comes from a template.
This guarantees:
  - Correct Airflow 2.8+ syntax every time
  - Variable.get() with default_var on every call (DagBag loads cleanly)
  - 1:1 job-to-task mapping enforced by the builder, not the LLM
  - Consistent output across runs
"""
import json
import re
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Any

from jinja2 import Environment, FileSystemLoader, StrictUndefined

logger = logging.getLogger(__name__)

# Template directory
TEMPLATE_DIR = Path(__file__).parent / "templates"

# Operator → template file mapping
OPERATOR_TEMPLATE_MAP = {
    "emr_serverless":       "emr_serverless_task.j2",
    "EmrServerlessStartJobOperator": "emr_serverless_task.j2",
    "emr_add_steps":        "emr_add_steps_task.j2",
    "EmrAddStepsOperator":  "emr_add_steps_task.j2",
    "bash":                 "bash_task.j2",
    "BashOperator":         "bash_task.j2",
    "python":               "python_task.j2",
    "PythonOperator":       "python_task.j2",
    "dummy":                "dummy_task.j2",
    "EmptyOperator":        "dummy_task.j2",
    "DummyOperator":        "dummy_task.j2",
    "external_task_sensor": "external_task_sensor.j2",
    "ExternalTaskSensor":   "external_task_sensor.j2",
    "trigger_dag_run":      "trigger_dag_run.j2",
    "TriggerDagRunOperator":"trigger_dag_run.j2",
}

# Operator → import statement
OPERATOR_IMPORTS = {
    "emr_serverless_task.j2":    "from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator",
    "emr_add_steps_task.j2":     "from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator",
    "bash_task.j2":              "from airflow.operators.bash import BashOperator",
    "python_task.j2":            "from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator",
    "dummy_task.j2":             "from airflow.operators.empty import EmptyOperator",
    "external_task_sensor.j2":   "from airflow.sensors.external_task import ExternalTaskSensor",
    "trigger_dag_run.j2":        "from airflow.operators.trigger_dagrun import TriggerDagRunOperator",
}


# Control-M order-date system variables -> Airflow template macros (logical date).
# Longest names first so %%$ODATE is replaced before %%ODATE.
_CTM_DATE_VARS = (
    ("%%$ODATE", "{{ ds_nodash }}"),                                   # yyyymmdd
    ("%%ODATE", "{{ ds_nodash[2:] }}"),                                 # yymmdd
    ("%%$OYEAR", "{{ logical_date.strftime('%Y') }}"),                  # yyyy
    ("%%OYEAR", "{{ logical_date.strftime('%y') }}"),                   # yy
    ("%%OMONTH", "{{ logical_date.strftime('%m') }}"),                  # mm
    ("%%ODAY", "{{ logical_date.strftime('%d') }}"),                    # dd
)


# Runner that executes a Spark SQL file on EMR (see runtime/run_spark_sql.py).
# EMR Serverless accepts only a JAR or Python entry point, and spark-submit
# cannot run a .sql file, so SQL tasks start this script with --sql <file>.
SQL_RUNNER_NAME = "run_spark_sql.py"
SQL_RUNNER_REL_PATH = f"scripts/{SQL_RUNNER_NAME}"
SQL_RUNNER_SOURCE = Path(__file__).parent / "runtime" / SQL_RUNNER_NAME

# Job variables that configure the conversion itself, not the job at run time
_NON_RUNTIME_VAR_SUFFIXES = ("SCRIPT_PATH", "EMR_CLUSTER_ID", "SUB_FLOW_NAME",
                             "SERVICE_TYPE", "LAMBDA_NAME")
_VAR_KEY_RE = re.compile(r"^[A-Za-z0-9_.-]+$")


def _job_variables(plan_vars: Dict, source_job_id: str, task_id: str) -> Dict[str, str]:
    """Runtime variables of one job: {name the job sees: Airflow Variable key}.

    Taken from the plan's airflow_variables entries whose source is this job.
    A key prefixed with the task id (RC_SCORE_CREDIT_S3_OUTPUT) is exposed under
    its original name (S3_OUTPUT).
    """
    prefix = task_id.upper() + "_"
    out: Dict[str, str] = {}
    for key, meta in (plan_vars or {}).items():
        if not isinstance(meta, dict) or meta.get("source") != source_job_id:
            continue
        if not _VAR_KEY_RE.match(key) or key.upper().endswith(_NON_RUNTIME_VAR_SUFFIXES):
            continue
        name = key[len(prefix):] if key.upper().startswith(prefix) else key
        name = re.sub(r"[^A-Za-z0-9_]", "_", name)
        out.setdefault(name, key)
    return out


def _job_cluster_var(plan_vars: Dict, source_job_id: str, default: str) -> str:
    """Airflow Variable holding this job's EMR cluster id (job-specific if the plan has one)."""
    for key, meta in (plan_vars or {}).items():
        if (isinstance(meta, dict) and meta.get("source") == source_job_id
                and key.upper().endswith("EMR_CLUSTER_ID") and _VAR_KEY_RE.match(key)):
            return key
    return default


def _spark_entry_point(script_path: str, var_scripts_bucket: str,
                       job_vars: Dict[str, str]) -> tuple:
    """(entry point path under SCRIPTS_BUCKET, argument expressions) for an EMR Spark task.

    .py scripts run directly. SQL scripts (.sql/.hql) run through the SQL runner,
    with the SQL file and each job variable (--var NAME=value) as arguments.
    Arguments are Python source expressions for the template.
    """
    if script_path.endswith(".py"):
        return script_path, []
    args = [
        '"--sql"',
        f"f\"{{Variable.get('{var_scripts_bucket}', default_var='__PLACEHOLDER__')}}/{script_path}\"",
    ]
    for name, key in job_vars.items():
        args.append('"--var"')
        args.append(f"\"{name}={{{{ var.value.get('{key}', '') }}}}\"")
    return SQL_RUNNER_REL_PATH, args


_SCRIPT_IN_CMD_RE = re.compile(r"(?<![\w{}/.])(/[^\s\"']+\.(?:sh|ksh|bash))(?=\s|$)")


def _bash_command_for_mwaa(command: str) -> str:
    """Point a shell job at its MWAA location and make it safe for BashOperator.

    On MWAA the on-premises script path does not exist; scripts are deployed with
    the DAGs under dags/scripts/, so /opt/x/y.sh becomes {{ dag.folder }}/scripts/y.sh.
    A command ending in .sh/.bash gets a trailing space, because BashOperator
    would otherwise treat it as a template file name.
    """
    command = _translate_ctm_date_vars(command or "")
    m = _SCRIPT_IN_CMD_RE.search(command)
    if m:
        command = (command[:m.start(1)] + "{{ dag.folder }}/scripts/" + Path(m.group(1)).name
                   + command[m.end(1):])
    if command.rstrip().endswith((".sh", ".bash")):
        command = command.rstrip() + " "
    return command


def _translate_ctm_date_vars(command: str) -> str:
    """Replace Control-M order-date variables in a command with Airflow macros."""
    for ctm, macro in _CTM_DATE_VARS:
        command = command.replace(ctm, macro)
    return command


def _safe_task_id(name: str) -> str:
    """Convert any string to a valid Airflow task_id."""
    return re.sub(r'[^a-z0-9_]', '_', name.lower()).strip('_') or 'task'


def _safe_dag_id(name: str) -> str:
    """Convert any string to a valid Airflow dag_id."""
    return re.sub(r'[^a-z0-9_]', '_', name.lower()).strip('_') or 'dag'


def _extract_script_path(task: Dict, spark_sql_mapping: Dict) -> tuple:
    """
    Return (script_path_for_template, source_script, target_script).

    The plan produced by planner.py always stores these at the top level
    of the task dict:
        task["source_script"]  — original path from Control-M XML
        task["target_script"]  — resolved S3 path from spark_sql_mapping
        task["script_path"]    — relative path already derived by planner

    We honour those directly and only fall back to spark_sql_mapping
    re-lookup when target_script is missing (e.g. LLM-generated plans).
    """
    # Primary: planner.py always sets these at top level
    source = task.get("source_script") or task.get("script_path") or ""
    target = task.get("target_script") or ""

    # Secondary: re-lookup in mapping if target is missing
    if not target and source and source in spark_sql_mapping:
        target = spark_sql_mapping[source]

    # script_path: planner already derived this; use it if present
    script_path = task.get("script_path") or ""

    # If script_path is an S3 URI (LLM fallback plan), strip the bucket prefix
    if script_path.startswith("s3://"):
        parts = script_path.split("/", 3)
        script_path = parts[3] if len(parts) > 3 else Path(script_path).name

    # Derive from target if script_path is still empty
    if not script_path:
        if target.startswith("s3://"):
            parts = target.split("/", 3)
            script_path = parts[3] if len(parts) > 3 else Path(target).name
        elif target:
            script_path = target
        elif source:
            stem = Path(source).stem
            script_path = f"spark_sql/{stem}.sql"
        else:
            script_path = "spark_sql/unknown.sql"

    return script_path, source, target


def build_dag(plan_file: Path, output_file: Path,
              spark_sql_mapping: Optional[Dict] = None,
              cfg=None) -> Path:
    """
    Read a ConversionPlan JSON and render a complete MWAA DAG Python file.

    Args:
        plan_file:          Path to *_plan.json
        output_file:        Where to write the .py DAG file
        spark_sql_mapping:  Dict mapping source script paths to S3 targets
        cfg:                ConverterConfig instance (uses defaults if None)

    Returns:
        Path to the written DAG file
    """
    spark_sql_mapping = spark_sql_mapping or {}

    # Load config defaults if not provided
    if cfg is None:
        from converter.config_loader import load_config, ConverterConfig
        cfg = ConverterConfig(load_config())

    # Variable names from config
    VAR_EMR_APP_ID        = cfg.var_emr_app_id
    VAR_EMR_EXEC_ROLE_ARN = cfg.var_emr_exec_role_arn
    VAR_SCRIPTS_BUCKET    = cfg.var_scripts_bucket
    VAR_LOGS_BUCKET       = cfg.var_logs_bucket
    VAR_EMR_CLUSTER_ID    = cfg.var_emr_cluster_id
    SPARK_SUBMIT_PARAMS   = cfg.spark_submit_parameters
    CFG_DAG_DEFAULTS      = cfg.dag_defaults

    with open(plan_file, encoding="utf-8") as f:
        plan = json.load(f)

    # autoescape is intentionally OFF: these templates generate Python source
    # (Airflow DAGs), not HTML. HTML-escaping would turn quotes, '&&', '<' and '>'
    # in generated code into entities (&#34; &amp; ...) and produce invalid Python.
    # Generated DAGs must be reviewed before deployment (see SECURITY.md).
    env = Environment(  # nosec B701
        loader=FileSystemLoader(str(TEMPLATE_DIR)),
        undefined=StrictUndefined,
        autoescape=False,
        trim_blocks=True,
        lstrip_blocks=True,
    )

    chunk_id = plan.get("chunk_id", plan_file.stem.replace("_plan", ""))

    # ── DAG id ────────────────────────────────────────────────────────────
    dag_id = _safe_dag_id(
        plan.get("dag_config", {}).get("dag_id")
        or plan.get("dag_id")
        or chunk_id
    )

    # ── Schedule ──────────────────────────────────────────────────────────
    raw_schedule = (
        plan.get("dag_config", {}).get("schedule_interval")
        or plan.get("dag_config", {}).get("schedule")
        or plan.get("schedule_interval")
        or None
    )
    if raw_schedule is None:
        schedule = "None"
    elif isinstance(raw_schedule, str):
        schedule = f'"{raw_schedule}"'
    else:
        schedule = str(raw_schedule)

    # ── Start date ────────────────────────────────────────────────────────
    start_date_raw = (
        plan.get("dag_config", {}).get("start_date")
        or plan.get("start_date")
        or "2024-01-01"
    )
    try:
        if isinstance(start_date_raw, str):
            sd = datetime.strptime(start_date_raw[:10], "%Y-%m-%d")
        else:
            sd = datetime(2024, 1, 1)
    except (ValueError, TypeError):
        sd = datetime(2024, 1, 1)

    # ── Default args — plan overrides config, config overrides hardcoded defaults ──
    dag_cfg = plan.get("dag_config", {})
    default_args_raw = dag_cfg.get("default_args", {})
    default_args = {
        "owner":               default_args_raw.get("owner",               CFG_DAG_DEFAULTS["owner"]),
        "depends_on_past":     default_args_raw.get("depends_on_past",     CFG_DAG_DEFAULTS["depends_on_past"]),
        "email_on_failure":    default_args_raw.get("email_on_failure",    CFG_DAG_DEFAULTS["email_on_failure"]),
        "email_on_retry":      default_args_raw.get("email_on_retry",      CFG_DAG_DEFAULTS["email_on_retry"]),
        "retries":             default_args_raw.get("retries",             CFG_DAG_DEFAULTS["retries"]),
        "retry_delay_minutes": default_args_raw.get("retry_delay_minutes", CFG_DAG_DEFAULTS["retry_delay_minutes"]),
    }

    # ── Tags ──────────────────────────────────────────────────────────────
    raw_tags = dag_cfg.get("tags") or plan.get("tags") or []
    tags = repr(list(raw_tags)) if raw_tags else '["control_m_migration"]'

    # ── Description ───────────────────────────────────────────────────────
    description = (
        dag_cfg.get("description")
        or plan.get("description")
        or f"Converted from Control-M folder: {chunk_id}"
    )

    # ── Tasks ─────────────────────────────────────────────────────────────
    tasks_raw = plan.get("tasks", [])
    external_sensors_raw = plan.get("external_sensors", [])

    rendered_tasks = []
    used_templates = set()
    task_ids_seen = set()

    # dep_lines/seen_deps initialised here so sensor wiring inside the task
    # loop (external_dependencies rendering) can append to them immediately
    dep_lines: list = []
    seen_deps: set = set()

    plan_vars = plan.get("airflow_variables", {}) or {}
    uses_sql_runner = False

    # Render external sensors first (they are upstream of real tasks)
    for sensor in external_sensors_raw:
        task_id = _safe_task_id(
            sensor.get("sensor_task_id") or sensor.get("task_id") or "wait"
        )
        if task_id in task_ids_seen:
            continue
        task_ids_seen.add(task_id)

        tmpl = env.get_template("external_task_sensor.j2")
        rendered = tmpl.render(
            task_id=task_id,
            external_dag_id=sensor.get("external_dag_id", "unknown_dag"),
            external_task_id=_safe_task_id(
                sensor.get("external_task_id", "unknown_task")
            ),
            timeout=sensor.get("timeout", 3600),
            poke_interval=sensor.get("poke_interval", 60),
        )
        used_templates.add("external_task_sensor.j2")
        rendered_tasks.append({
            "task_id": task_id,
            "source_job_id": sensor.get("depends_on_job_id", task_id),
            "operator_class": "ExternalTaskSensor",
            "rendered": rendered,
        })

    # Render job tasks
    for task in tasks_raw:
        task_id = _safe_task_id(
            task.get("task_id") or task.get("source_job_id") or "task"
        )
        if task_id in task_ids_seen:
            continue
        task_ids_seen.add(task_id)

        source_job_id = task.get("source_job_id") or task.get("source_job_name") or task_id
        source_job_type = task.get("source_job_type") or task.get("job_type") or "command"
        description_text = task.get("description") or ""

        # Determine operator
        operator_raw = (
            task.get("operator")
            or task.get("operator_class")
            or task.get("operator_type")
            or "emr_serverless"
        )
        # LLM may return operator as dict like {"type": "emr_serverless", ...}
        if isinstance(operator_raw, dict):
            operator_raw = (
                operator_raw.get("type")
                or operator_raw.get("class")
                or operator_raw.get("operator_type")
                or "emr_serverless"
            )
        template_name = OPERATOR_TEMPLATE_MAP.get(operator_raw)
        if not template_name:
            # Fallback: infer from script type
            script_type = task.get("script_type") or ""
            if script_type in ("sh", "ksh", "bash"):
                template_name = "bash_task.j2"
            elif source_job_type in ("dummy",) or "dummy" in operator_raw.lower() or "empty" in operator_raw.lower():
                template_name = "dummy_task.j2"
            else:
                template_name = "emr_serverless_task.j2"

        used_templates.add(template_name)
        tmpl = env.get_template(template_name)

        # Build template context based on operator type
        ctx: Dict[str, Any] = {
            "task_id": task_id,
            "source_job_id": source_job_id,
            "source_job_type": source_job_type,
            "description": description_text,
            "trigger_rule": task.get("trigger_rule") or None,
        }

        if template_name in ("emr_serverless_task.j2", "python_task.j2"):
            script_path, source_script, target_script = _extract_script_path(
                task, spark_sql_mapping
            )
            entry_point_path, entry_point_args = _spark_entry_point(
                script_path, VAR_SCRIPTS_BUCKET,
                _job_variables(plan_vars, source_job_id, task_id))
            if entry_point_path == SQL_RUNNER_REL_PATH:
                uses_sql_runner = True
            ctx.update({
                "script_path": script_path,
                "entry_point_path": entry_point_path,
                "entry_point_args": entry_point_args,
                "source_script": source_script or "-",
                "target_script": target_script or "-",
                "aws_conn_id": None,
                "var_emr_app_id":        VAR_EMR_APP_ID,
                "var_emr_exec_role_arn": VAR_EMR_EXEC_ROLE_ARN,
                "var_scripts_bucket":    VAR_SCRIPTS_BUCKET,
                "var_logs_bucket":       VAR_LOGS_BUCKET,
                "spark_submit_parameters": SPARK_SUBMIT_PARAMS,
            })

        elif template_name == "bash_task.j2":
            cmd = (
                task.get("bash_command")
                or task.get("command")
                or task.get("operator_config", {}).get("bash_command")
                or ""
            )
            env_vars = _job_variables(plan_vars, source_job_id, task_id)
            for var in task.get("variables", []) or []:
                if isinstance(var, dict):
                    name = var.get("name", "").lstrip("%")
                    if name and _VAR_KEY_RE.match(name):
                        env_vars.setdefault(name, name)
            ctx.update({
                "source_command": cmd.replace("\n", " "),
                "bash_command": _bash_command_for_mwaa(cmd),
                "env_vars": env_vars or None,
            })

        elif template_name == "external_task_sensor.j2":
            ext_deps = task.get("depends_on_external", [])
            dep = ext_deps[0] if ext_deps else {}
            cfg = task.get("config", {})
            ctx.update({
                "external_dag_id": (
                    dep.get("external_dag_id")
                    or cfg.get("external_dag_id")
                    or "unknown_dag"
                ),
                "external_task_id": _safe_task_id(
                    dep.get("external_task_id")
                    or cfg.get("external_task_id")
                    or "unknown_task"
                ),
                "timeout": dep.get("timeout") or cfg.get("timeout", 3600),
                "poke_interval": dep.get("poke_interval") or cfg.get("poke_interval", 60),
            })

        elif template_name == "trigger_dag_run.j2":
            # Resolve trigger_dag_id: explicit field → variables SUB_FLOW_NAME → conversion_notes
            raw_trigger_id = (
                task.get("trigger_dag_id")
                or task.get("operator_config", {}).get("trigger_dag_id")
            )
            # Try variables dict for SUB_FLOW_NAME
            if not raw_trigger_id or raw_trigger_id == "unknown_dag":
                vars_dict = task.get("variables") or {}
                if isinstance(vars_dict, dict):
                    raw_trigger_id = (
                        vars_dict.get("SUB_FLOW_NAME")
                        or vars_dict.get("%%SUB_FLOW_NAME")
                        or raw_trigger_id
                    )
            # Try conversion_notes — look for a snake_case dag_id after "will be" or similar
            if not raw_trigger_id or raw_trigger_id == "unknown_dag":
                notes = task.get("conversion_notes") or ""
                if isinstance(notes, list):
                    notes = " ".join(notes)
                # First try: explicit "will be <dag_id>" pattern
                m = re.search(r'will\s+be\s+([a-z][a-z0-9]*(?:_[a-z0-9]+)+)', notes)
                if not m:
                    # Second try: find all snake_case identifiers, skip known field names
                    SKIP = {"trigger_dag_id", "dag_id", "task_id", "source_job_id",
                            "conversion_notes", "bash_command", "emr_serverless",
                            "emr_add_steps", "external_task_sensor"}
                    candidates = re.findall(r'\b([a-z][a-z0-9]*(?:_[a-z0-9]+){2,})\b', notes)
                    candidates = [c for c in candidates if c not in SKIP]
                    m = type('m', (), {'group': lambda self, n: candidates[0]})() if candidates else None
                if m:
                    raw_trigger_id = m.group(1)
            ctx.update({
                "trigger_dag_id": _safe_dag_id(raw_trigger_id or "unknown_dag"),
                "wait_for_completion": True,
            })

        elif template_name == "emr_add_steps_task.j2":
            script_path, source_script, target_script = _extract_script_path(
                task, spark_sql_mapping
            )
            entry_point_path, entry_point_args = _spark_entry_point(
                script_path, VAR_SCRIPTS_BUCKET,
                _job_variables(plan_vars, source_job_id, task_id))
            if entry_point_path == SQL_RUNNER_REL_PATH:
                uses_sql_runner = True
            ctx.update({
                "script_path": script_path,
                "entry_point_path": entry_point_path,
                "entry_point_args": entry_point_args,
                "source_script": source_script or "-",
                "target_script": target_script or "-",
                "aws_conn_id": None,
                "var_emr_cluster_id": _job_cluster_var(plan_vars, source_job_id, VAR_EMR_CLUSTER_ID),
                "var_scripts_bucket": VAR_SCRIPTS_BUCKET,
            })

        rendered = tmpl.render(**ctx)
        rendered_tasks.append({
            "task_id": task_id,
            "source_job_id": source_job_id,
            "operator_class": operator_raw,
            "rendered": rendered,
        })

        # Render ExternalTaskSensors for cross-folder dependencies (ODATE=STAT)
        for ext_dep in task.get("external_dependencies", []):
            ext_dag_id = _safe_dag_id(
                ext_dep.get("external_dag_id") or ext_dep.get("source_folder") or "unknown_dag"
            )
            ext_task_id = _safe_task_id(
                ext_dep.get("external_task_id") or ext_dep.get("source_job_id") or "unknown_task"
            )
            sensor_task_id = f"wait_{ext_dag_id}_{ext_task_id}"
            if sensor_task_id in task_ids_seen:
                continue
            task_ids_seen.add(sensor_task_id)
            sensor_tmpl = env.get_template("external_task_sensor.j2")
            sensor_rendered = sensor_tmpl.render(
                task_id=sensor_task_id,
                external_dag_id=ext_dag_id,
                external_task_id=ext_task_id,
                timeout=ext_dep.get("timeout", 3600),
                poke_interval=ext_dep.get("poke_interval", 60),
            )
            used_templates.add("external_task_sensor.j2")
            rendered_tasks.append({
                "task_id": sensor_task_id,
                "source_job_id": f"external:{ext_dag_id}.{ext_task_id}",
                "operator_class": "ExternalTaskSensor",
                "rendered": sensor_rendered,
            })
            # Wire sensor → this task
            dep = f"{sensor_task_id} >> {task_id}"
            if dep not in seen_deps:
                seen_deps.add(dep)
                dep_lines.append(dep)

    # ── Dependencies ──────────────────────────────────────────────────────

    # Internal dependencies from tasks
    for task in tasks_raw:
        task_id = _safe_task_id(
            task.get("task_id") or task.get("source_job_id") or "task"
        )
        upstreams = (
            task.get("dependencies")
            or task.get("upstream_task_ids")
            or task.get("upstream_tasks")
            or task.get("depends_on_internal")
            or []
        )
        # LLM may return dependencies as dict like {"internal": [...], "external": [...]}
        if isinstance(upstreams, dict):
            upstreams = upstreams.get("internal") or upstreams.get("upstream") or []
        for up in upstreams:
            if isinstance(up, dict):
                up = up.get("task_id") or up.get("from") or ""
            up_id = _safe_task_id(str(up))
            if up_id and up_id in task_ids_seen:
                dep = f"{up_id} >> {task_id}"
                if dep not in seen_deps:
                    seen_deps.add(dep)
                    dep_lines.append(dep)

    # Sensor → task dependencies
    for sensor in external_sensors_raw:
        sensor_id = _safe_task_id(
            sensor.get("sensor_task_id") or sensor.get("task_id") or "wait"
        )
        downstream = _safe_task_id(
            sensor.get("downstream_task") or sensor.get("depends_on_job_id") or ""
        )
        if sensor_id and downstream and downstream in task_ids_seen:
            dep = f"{sensor_id} >> {downstream}"
            if dep not in seen_deps:
                seen_deps.add(dep)
                dep_lines.append(dep)

    # Execution order fallback — if no deps found, use execution_order list
    if not dep_lines:
        order = plan.get("execution_order") or plan.get("dependency_chain", [])
        if isinstance(order, str):
            # "a >> b >> c" format
            parts = [_safe_task_id(p.strip()) for p in order.split(">>")]
            for i in range(len(parts) - 1):
                if parts[i] in task_ids_seen and parts[i+1] in task_ids_seen:
                    dep = f"{parts[i]} >> {parts[i+1]}"
                    if dep not in seen_deps:
                        seen_deps.add(dep)
                        dep_lines.append(dep)
        elif isinstance(order, list):
            clean = [_safe_task_id(str(x)) for x in order if x]
            for i in range(len(clean) - 1):
                if clean[i] in task_ids_seen and clean[i+1] in task_ids_seen:
                    dep = f"{clean[i]} >> {clean[i+1]}"
                    if dep not in seen_deps:
                        seen_deps.add(dep)
                        dep_lines.append(dep)

    # ── Imports ───────────────────────────────────────────────────────────
    imports = sorted({OPERATOR_IMPORTS[t] for t in used_templates if t in OPERATOR_IMPORTS})

    # ── Render base template ──────────────────────────────────────────────
    base_tmpl = env.get_template("dag_base.j2")
    dag_code = base_tmpl.render(
        dag_id=dag_id,
        dag_description=description,
        source_type=plan.get("source_type", "control_m"),
        chunk_id=chunk_id,
        generation_timestamp=datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        default_args=default_args,
        schedule=schedule,
        start_year=sd.year,
        start_month=sd.month,
        start_day=sd.day,
        tags=tags,
        operator_imports=imports,
        tasks=rendered_tasks,
        dependencies=dep_lines,
        has_external_sensors="external_task_sensor.j2" in used_templates,
    )

    # Ship the SQL runner next to the DAGs (upload to <SCRIPTS_BUCKET>/scripts/)
    if uses_sql_runner and SQL_RUNNER_SOURCE.exists():
        runner_dst = output_file.parent.parent / "scripts" / SQL_RUNNER_NAME
        runner_dst.parent.mkdir(parents=True, exist_ok=True)
        runner_dst.write_text(SQL_RUNNER_SOURCE.read_text(encoding="utf-8"), encoding="utf-8")

    # ── Write output ──────────────────────────────────────────────────────
    output_file.parent.mkdir(parents=True, exist_ok=True)
    output_file.write_text(dag_code)
    logger.info(f"  Built DAG: {output_file.name}  ({len(rendered_tasks)} tasks, {len(dep_lines)} deps)")  # pylint: disable=logging-fstring-interpolation
    return output_file

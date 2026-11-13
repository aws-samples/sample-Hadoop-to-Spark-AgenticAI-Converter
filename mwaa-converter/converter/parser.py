"""Stage 1: Parse Control-M XML source files into JobIR JSON via LLM.

The LLM reads the raw XML and produces structured JobIR JSON.
A deterministic XML pre-reader extracts the raw content and validates
the LLM output against the required schema before accepting it.

Each SMART_FOLDER in the XML becomes one JobIR file.
"""
import json
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

# defusedxml blocks XML entity expansion ("billion laughs"), external entities
# (XXE) and external DTD retrieval. Control-M exports are local files, but they
# are still treated as untrusted input.
import defusedxml.ElementTree as ET
from defusedxml import DefusedXmlException

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Required JobIR schema — every job the LLM produces must conform to this
# ---------------------------------------------------------------------------
REQUIRED_JOB_FIELDS = {
    "job_id": str,
    "job_name": str,
    "job_type": str,       # command | script | dummy | aws_lambda | sub_flow
    "dependencies": list,  # list of upstream job_id strings
    "variables": dict,
    "metadata": dict,
}

VALID_JOB_TYPES = {"command", "script", "dummy", "aws_lambda", "emr_add_steps", "sub_flow", "unknown"}
VALID_SCRIPT_TYPES = {"hql", "sql", "py", "sh", "ksh", "bash", None}


# ---------------------------------------------------------------------------
# Public entry point — called by MCP server
# ---------------------------------------------------------------------------

def build_parse_prompt(xml_file: Path, output_dir: Path) -> str:
    """Read the XML file and return a fully-formed LLM prompt for parsing.

    The MCP server calls this, sends the prompt to the LLM, then calls
    validate_and_save_job_ir() on the LLM's response.
    """
    xml_content = xml_file.read_text(encoding="utf-8", errors="replace")
    job_ir_dir = output_dir / "job_ir"
    job_ir_dir.mkdir(parents=True, exist_ok=True)

    # Pre-scan to tell the LLM how many folders/jobs to expect
    folder_summary = _prescan_xml(xml_content)

    from converter.prompts.prompt_loader import load_prompt
    template = load_prompt("parse_control_m_xml")

    prompt = template.replace("{job_definition}", xml_content)
    prompt += f"""

PRE-SCAN SUMMARY (use this to verify your output is complete):
{folder_summary}

OUTPUT INSTRUCTIONS:
- Save one JobIR JSON file per SMART_FOLDER to the paths listed above
- Each file must be valid JSON matching the schema exactly
- After saving all files, confirm with: "JobIR saved: <folder_name> → <file_path>"

Output directory for JobIR files: {job_ir_dir}
"""
    return prompt


def validate_and_save_job_ir(
    llm_output: str,
    output_dir: Path,
    xml_file: Path,
) -> List[Path]:
    """Validate LLM-produced JobIR JSON and save to disk.

    Falls back to deterministic XML parsing if LLM output fails validation
    after extracting any valid JSON blocks found.

    Returns list of written JobIR file paths.
    """
    job_ir_dir = output_dir / "job_ir"
    job_ir_dir.mkdir(parents=True, exist_ok=True)

    written: List[Path] = []

    # Try to extract JSON from LLM output
    json_blocks = _extract_json_blocks(llm_output)

    for block in json_blocks:
        try:
            data = json.loads(block)
        except json.JSONDecodeError:
            continue

        # Handle both single-folder {folder, jobs} and array of jobs
        if isinstance(data, dict) and "jobs" in data:
            folder = data.get("folder", "default")
            jobs = data["jobs"]
            if _validate_jobs(jobs, folder):
                out_file = job_ir_dir / f"{_safe_filename(folder)}.json"
                out_file.write_text(json.dumps(data, indent=2), encoding="utf-8")
                written.append(out_file)
                logger.info("✓ LLM JobIR validated: folder='%s' (%d jobs) → %s", folder, len(jobs), out_file.name)

        elif isinstance(data, list) and data:
            # LLM returned a flat list of jobs — group by folder
            by_folder: Dict[str, List] = {}
            for job in data:
                if isinstance(job, dict):
                    folder = job.get("folder", "default")
                    by_folder.setdefault(folder, []).append(job)

            for folder, jobs in by_folder.items():
                if _validate_jobs(jobs, folder):
                    ir = {"folder": folder, "jobs": jobs}
                    out_file = job_ir_dir / f"{_safe_filename(folder)}.json"
                    out_file.write_text(json.dumps(ir, indent=2), encoding="utf-8")
                    written.append(out_file)
                    logger.info("✓ LLM JobIR validated: folder='%s' (%d jobs) → %s", folder, len(jobs), out_file.name)

    if written:
        return written

    # LLM output didn't produce valid JobIR — fall back to deterministic parser
    logger.warning("LLM output failed schema validation — falling back to deterministic XML parser")
    return _deterministic_fallback(xml_file, job_ir_dir)


# ---------------------------------------------------------------------------
# Schema validation
# ---------------------------------------------------------------------------

def _validate_jobs(jobs: list, folder: str) -> bool:
    """Validate a list of job dicts against the required schema."""
    if not jobs:
        logger.warning("Empty jobs list for folder '%s'", folder)
        return False

    for i, job in enumerate(jobs):
        if not isinstance(job, dict):
            logger.warning("Job %d in folder '%s' is not a dict", i, folder)
            return False

        # Required fields
        for field, expected_type in REQUIRED_JOB_FIELDS.items():
            if field not in job:
                logger.warning("Job '%s' missing required field '%s'", job.get("job_id", i), field)
                return False
            if not isinstance(job[field], expected_type):
                logger.warning("Job '%s' field '%s' has wrong type: expected %s got %s",
                               job.get("job_id", i), field, expected_type.__name__, type(job[field]).__name__)
                return False

        # job_type must be valid
        if job.get("job_type") not in VALID_JOB_TYPES:
            logger.warning("Job '%s' has invalid job_type: '%s'", job.get("job_id"), job.get("job_type"))
            return False

        # dependencies must be list of strings
        for dep in job.get("dependencies", []):
            if not isinstance(dep, str):
                logger.warning("Job '%s' dependency is not a string: %s", job.get("job_id"), dep)
                return False

    return True


# ---------------------------------------------------------------------------
# JSON extraction from LLM output
# ---------------------------------------------------------------------------

def _extract_json_blocks(text: str) -> List[str]:
    """Extract all JSON objects/arrays from LLM output text."""
    blocks = []

    # Try code fences first: ```json ... ```
    fence_pattern = re.compile(r'```(?:json)?\s*(\{[\s\S]*?\}|\[[\s\S]*?\])\s*```', re.DOTALL)
    for m in fence_pattern.finditer(text):
        blocks.append(m.group(1).strip())

    if blocks:
        return blocks

    # Try bare JSON objects/arrays
    # Find the outermost { } or [ ] blocks
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


# ---------------------------------------------------------------------------
# Deterministic fallback — used when LLM output fails validation
# ---------------------------------------------------------------------------

def _deterministic_fallback(xml_file: Path, job_ir_dir: Path) -> List[Path]:
    """Parse XML deterministically as a last resort."""
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()
        written = []

        # job name -> folder name, for every folder in the file, so a condition
        # set by a job in another folder becomes an external dependency.
        job_folders: Dict[str, str] = {}
        for sf in root.iter("SMART_FOLDER"):
            sf_name = sf.get("FOLDER_NAME") or sf.get("JOBNAME") or "default"
            for je in sf.findall("JOB"):
                if je.get("JOBNAME"):
                    job_folders[je.get("JOBNAME")] = sf_name

        for smart_folder in root.iter("SMART_FOLDER"):
            folder_name = (
                smart_folder.get("FOLDER_NAME")
                or smart_folder.get("JOBNAME")
                or "default"
            )
            application = smart_folder.get("APPLICATION", "")
            sub_application = smart_folder.get("SUB_APPLICATION", "")

            # Capture folder-level schedule attributes
            days = smart_folder.get("DAYS") or None
            timefrom = smart_folder.get("TIMEFROM") or None
            calendar_name = None
            cal_elem = smart_folder.find("RULE_BASED_CALENDAR")
            if cal_elem is not None:
                calendar_name = cal_elem.get("NAME")

            jobs = []
            for job_elem in smart_folder.findall("JOB"):
                job = _parse_job_element_deterministic(
                    job_elem, folder_name, application, sub_application
                )
                jobs.append(job)
            _split_external_dependencies(jobs, folder_name, job_folders)

            if jobs:
                ir = {
                    "folder": folder_name,
                    "application": application,
                    "sub_application": sub_application,
                    "schedule": {
                        "days": days,
                        "timefrom": timefrom,
                        "calendar": calendar_name,
                    },
                    "jobs": jobs,
                }
                out_file = job_ir_dir / f"{_safe_filename(folder_name)}.json"
                out_file.write_text(json.dumps(ir, indent=2), encoding="utf-8")
                written.append(out_file)
                logger.info("✓ Fallback parsed: folder='%s' (%d jobs)", folder_name, len(jobs))

        return written

    except ET.ParseError as e:
        logger.error("Deterministic fallback XML parse error: %s", e)
        return []
    except DefusedXmlException as e:
        logger.error("Refusing to parse %s — XML contains forbidden constructs "
                     "(entities / external references): %s", xml_file, e)
        return []


def _split_external_dependencies(jobs: List[Dict], folder_name: str,
                                  job_folders: Dict[str, str]) -> None:
    """Move dependencies on jobs from other folders into metadata.external_dependencies.

    Each entry is {"source_folder": <folder>, "source_job_id": <job>}; the planner turns
    it into an ExternalTaskSensor. Upstream jobs found in no folder are kept as
    external with source_folder=None and logged.
    """
    local = {j["job_id"] for j in jobs}
    for job in jobs:
        internal, external = [], []
        for dep in job.get("dependencies", []):
            if dep in local:
                internal.append(dep)
                continue
            src_folder = job_folders.get(dep)
            if src_folder is None:
                logger.warning("Job %s depends on %s, which is in no folder of this file",
                               job["job_id"], dep)
            external.append({"source_folder": src_folder, "source_job_id": dep})
        job["dependencies"] = internal
        if external:
            job.setdefault("metadata", {})["external_dependencies"] = external


def _parse_job_element_deterministic(
    job_elem: Any,  # xml.etree.ElementTree.Element (returned by defusedxml)
    folder_name: str,
    folder_application: str,
    folder_sub_application: str,
) -> Dict:
    """Extract all fields from a <JOB> element."""
    job_name = job_elem.get("JOBNAME", "")
    task_type = job_elem.get("TASKTYPE", "Job")
    appl_type = job_elem.get("APPL_TYPE", "OS")
    description = job_elem.get("DESCRIPTION", "")
    run_as = job_elem.get("RUN_AS", "")
    node_id = job_elem.get("NODEID", "")
    memname = job_elem.get("MEMNAME", "")
    memlib = job_elem.get("MEMLIB", "")
    cmdline = job_elem.get("CMDLINE", "")
    application = job_elem.get("APPLICATION", "") or folder_application
    sub_application = job_elem.get("SUB_APPLICATION", "") or folder_sub_application

    variables: Dict[str, str] = {}
    for var in job_elem.findall("VARIABLE"):
        name = var.get("NAME", "").lstrip("%")
        value = var.get("VALUE", "")
        if name:
            variables[name] = value

    script_path = (
        variables.get("SCRIPT_PATH")
        or (_build_script_path(memlib, memname) if memname else None)
        or (_extract_script_from_cmdline(cmdline) if cmdline else None)
    )
    script_type = _detect_script_type(script_path) if script_path else None
    job_type = _map_job_type(task_type, appl_type, script_type, variables)

    dependencies: List[str] = []
    has_or_condition = False
    for incond in job_elem.findall("INCOND"):
        cond_name = incond.get("NAME", "")
        dep = _extract_dependency_from_condition(cond_name, job_name)
        if dep:
            dependencies.append(dep)
        if incond.get("AND_OR", "A").upper() == "O":
            has_or_condition = True

    metadata: Dict = {
        "task_type": task_type,
        "appl_type": appl_type,
        "node_id": node_id,
    }
    if has_or_condition:
        metadata["and_or"] = "O"

    return {
        "job_id": job_name,
        "job_name": job_name,
        "job_type": job_type,
        "description": description,
        "command": cmdline or None,
        "script_path": script_path,
        "script_type": script_type,
        "folder": folder_name,
        "application": application,
        "sub_application": sub_application,
        "run_as": run_as,
        "dependencies": dependencies,
        "variables": variables,
        "metadata": metadata,
    }


# ---------------------------------------------------------------------------
# XML pre-scanner — gives LLM a summary to verify completeness
# ---------------------------------------------------------------------------

def _prescan_xml(xml_content: str) -> str:
    """Quick scan to count folders and jobs — used in the prompt."""
    try:
        root = ET.fromstring(xml_content)
        lines = []
        for sf in root.iter("SMART_FOLDER"):
            folder = sf.get("FOLDER_NAME") or sf.get("JOBNAME") or "unknown"
            jobs = sf.findall("JOB")
            lines.append(f"  SMART_FOLDER '{folder}': {len(jobs)} JOB elements")
        return "\n".join(lines) if lines else "  (no SMART_FOLDERs found)"
    except (ET.ParseError, DefusedXmlException):
        return "  (could not pre-scan XML)"


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _map_job_type(task_type: str, appl_type: str, script_type: Optional[str],
                  variables: Optional[Dict] = None) -> str:
    tt = task_type.lower()
    at = appl_type.lower()
    if tt == "dummy":
        return "dummy"
    if tt in ("smart table", "sub folder", "sub_folder"):
        # Job that orders another folder (same rule as the LLM parse prompt)
        return "sub_flow"
    if at == "aws":
        # EMR cluster ID present → emr_add_steps (EC2-based, not Serverless)
        if variables and variables.get("AWS-EMR_CLUSTER_ID"):
            return "emr_add_steps"
        return "aws_lambda"
    if tt == "command":
        return "command"
    if tt == "job":
        return "script" if script_type else "command"
    return "command"


def _build_script_path(memlib: str, memname: str) -> Optional[str]:
    if not memname:
        return None
    return f"{memlib.rstrip('/')}/{memname}" if memlib else memname


def _extract_script_from_cmdline(cmdline: str) -> Optional[str]:
    patterns = [
        r'-f\s+(\S+\.(?:hql|sql|py|sh|ksh))',
        r'spark-submit\s+.*?(\S+\.(?:py|sql|hql))',
        r'python[23]?\s+(\S+\.py)',
        r'bash\s+(\S+\.sh)',
        r'ksh\s+(\S+\.ksh)',
        r'(\S+\.(?:hql|sql|py|sh|ksh))',
    ]
    for pat in patterns:
        m = re.search(pat, cmdline, re.IGNORECASE)
        if m:
            return m.group(1)
    return None


def _detect_script_type(script_path: str) -> Optional[str]:
    if not script_path:
        return None
    ext = Path(script_path).suffix.lstrip(".").lower()
    return ext if ext in ("hql", "sql", "py", "sh", "ksh", "bash") else None


def _extract_dependency_from_condition(cond_name: str, current_job: str) -> Optional[str]:
    if "-TO-" not in cond_name:
        return None
    parts = cond_name.split("-TO-", 1)
    source_job = parts[0].strip()
    target_job = parts[1].strip()
    if target_job == current_job and source_job != current_job:
        return source_job
    return None


def _safe_filename(name: str) -> str:
    return re.sub(r'[^a-zA-Z0-9_\-]', '_', name).strip('_') or "folder"


# =============================================================================
# Airflow DAG Parser — on-prem Airflow → MWAA
# =============================================================================

def build_airflow_parse_prompt(dag_file: Path, output_dir: Path) -> str:
    """Read an on-prem Airflow DAG .py file and return an LLM prompt for parsing.

    The LLM reads the DAG Python file, extracts all tasks, operators,
    dependencies, and schedule, then produces JobIR JSON in the same
    schema used by the Control-M XML parser.

    The MCP server calls this, sends the prompt to the LLM, then calls
    validate_and_save_airflow_job_ir() to schema-validate the output.
    """
    dag_content = dag_file.read_text(encoding="utf-8", errors="replace")
    job_ir_dir = output_dir / "job_ir"
    job_ir_dir.mkdir(parents=True, exist_ok=True)

    # Pre-scan to give LLM a task count to verify against
    task_summary = _prescan_airflow_dag(dag_content)

    from converter.prompts.prompt_loader import load_prompt
    template = load_prompt("parse_airflow_dag")

    prompt = template.replace("{dag_file_content}", dag_content)
    prompt += f"""

PRE-SCAN SUMMARY (use this to verify your output is complete):
{task_summary}

OUTPUT INSTRUCTIONS:
- Save one JobIR JSON file named after the DAG id to: {job_ir_dir}
- The file must be valid JSON matching the schema exactly
- After saving, confirm with: "JobIR saved: <dag_id> → <file_path>"

Output directory for JobIR files: {job_ir_dir}
"""
    return prompt


def validate_and_save_airflow_job_ir(
    llm_output: str,
    output_dir: Path,
    dag_file: Path,
) -> List[Path]:
    """Validate LLM-produced JobIR JSON from an Airflow DAG and save to disk.

    Falls back to deterministic AST-based parsing if LLM output fails validation.

    Returns list of written JobIR file paths.
    """
    job_ir_dir = output_dir / "job_ir"
    job_ir_dir.mkdir(parents=True, exist_ok=True)

    written: List[Path] = []

    # Try to extract JSON from LLM output
    json_blocks = _extract_json_blocks(llm_output)

    for block in json_blocks:
        try:
            data = json.loads(block)
        except json.JSONDecodeError:
            continue

        if isinstance(data, dict) and "jobs" in data:
            folder = data.get("folder", dag_file.stem)
            jobs = data["jobs"]
            if _validate_jobs(jobs, folder):
                out_file = job_ir_dir / f"{_safe_filename(folder)}.json"
                out_file.write_text(json.dumps(data, indent=2), encoding="utf-8")
                written.append(out_file)
                logger.info("✓ LLM Airflow JobIR validated: dag='%s' (%d tasks) → %s",
                            folder, len(jobs), out_file.name)

    if written:
        return written

    # LLM output didn't produce valid JobIR — fall back to deterministic AST parser
    logger.warning("LLM Airflow output failed schema validation — falling back to AST parser")
    return _deterministic_airflow_fallback(dag_file, job_ir_dir)


def _deterministic_airflow_fallback(dag_file: Path, job_ir_dir: Path) -> List[Path]:
    """Parse an Airflow DAG .py file deterministically using Python AST.

    Extracts:
    - DAG id and schedule
    - All task definitions (operator type, task_id, bash_command, etc.)
    - Dependencies from >> operator chains and set_upstream/set_downstream calls
    """
    import ast as _ast

    try:
        source = dag_file.read_text(encoding="utf-8", errors="replace")
        tree = _ast.parse(source)
    except (SyntaxError, OSError) as e:
        logger.error("AST parse error for %s: %s", dag_file.name, e)
        return []

    dag_id = dag_file.stem
    schedule = None
    jobs: List[Dict] = []
    task_map: Dict[str, Dict] = {}   # var_name → job dict
    dep_lines: List[str] = []        # raw source lines containing >>

    # ── Walk AST ──────────────────────────────────────────────────────────
    for node in _ast.walk(tree):

        # DAG(...) call — extract dag_id and schedule
        if isinstance(node, _ast.Call):
            func_name = ""
            if isinstance(node.func, _ast.Name):
                func_name = node.func.id
            elif isinstance(node.func, _ast.Attribute):
                func_name = node.func.attr

            if func_name == "DAG":
                for kw in node.keywords:
                    if kw.arg == "dag_id" and isinstance(kw.value, _ast.Constant):
                        dag_id = kw.value.value
                    elif kw.arg in ("schedule", "schedule_interval") and isinstance(kw.value, _ast.Constant):
                        schedule = kw.value.value
                # Positional first arg is dag_id
                if node.args and isinstance(node.args[0], _ast.Constant):
                    dag_id = node.args[0].value

        # task_var = SomeOperator(task_id="...", ...)
        if isinstance(node, _ast.Assign):
            if not node.targets or not isinstance(node.value, _ast.Call):
                continue
            call = node.value
            operator_name = ""
            if isinstance(call.func, _ast.Name):
                operator_name = call.func.id
            elif isinstance(call.func, _ast.Attribute):
                operator_name = call.func.attr

            if not operator_name or "Operator" not in operator_name and "Sensor" not in operator_name and operator_name not in ("DummyOperator", "EmptyOperator"):
                continue

            task_id = ""
            bash_command = ""
            python_callable = ""

            for kw in call.keywords:
                if kw.arg == "task_id" and isinstance(kw.value, _ast.Constant):
                    task_id = kw.value.value
                elif kw.arg == "bash_command" and isinstance(kw.value, _ast.Constant):
                    bash_command = kw.value.value
                elif kw.arg == "python_callable" and isinstance(kw.value, _ast.Name):
                    python_callable = kw.value.id

            if not task_id:
                continue

            var_name = node.targets[0].id if isinstance(node.targets[0], _ast.Name) else task_id

            # Classify operator → job_type + script extraction
            job_type, script_path, script_type, command = _classify_airflow_operator(
                operator_name, bash_command, python_callable
            )

            job = {
                "job_id": task_id,
                "job_name": task_id,
                "job_type": job_type,
                "description": f"Converted from {operator_name}",
                "command": command,
                "script_path": script_path,
                "script_type": script_type,
                "folder": dag_id,
                "application": "",
                "sub_application": "",
                "run_as": "",
                "dependencies": [],
                "variables": {},
                "metadata": {
                    "task_type": operator_name,
                    "appl_type": "airflow",
                    "node_id": "",
                    "original_operator": operator_name,
                },
            }
            task_map[var_name] = job
            jobs.append(job)

    # ── Extract dependencies from >> chains ───────────────────────────────
    # Parse source lines for patterns like: a >> b >> c  or  [a, b] >> c
    for line in source.splitlines():
        stripped = line.strip()
        if ">>" in stripped and not stripped.startswith("#"):
            dep_lines.append(stripped)

    _resolve_airflow_dependencies(dep_lines, task_map)

    if not jobs:
        logger.warning("AST fallback found no tasks in %s", dag_file.name)
        return []

    ir = {
        "folder": dag_id,
        "dag_id": dag_id,
        "schedule": schedule,
        "source_type": "airflow",
        "jobs": jobs,
    }
    out_file = job_ir_dir / f"{_safe_filename(dag_id)}.json"
    out_file.write_text(json.dumps(ir, indent=2), encoding="utf-8")
    logger.info("✓ AST fallback parsed Airflow DAG '%s': %d tasks → %s",
                dag_id, len(jobs), out_file.name)
    return [out_file]


def _classify_airflow_operator(
    operator_name: str,
    bash_command: str,
    python_callable: str,
) -> tuple:
    """Map an Airflow operator to (job_type, script_path, script_type, command)."""
    op = operator_name.lower()

    if "dummy" in op or "empty" in op:
        return "dummy", None, None, None

    if "bash" in op:
        script_path = _extract_script_from_cmdline(bash_command) if bash_command else None
        script_type = _detect_script_type(script_path) if script_path else None
        return "command", script_path, script_type, bash_command or None

    if "python" in op:
        return "script", None, "py", python_callable or None

    if "emrserverless" in op or "emr_serverless" in op:
        return "aws_lambda", None, None, None

    if "emraddsteps" in op or "emr_add_steps" in op:
        return "aws_lambda", None, None, None

    if "externaltask" in op or "external_task" in op:
        return "dummy", None, None, None

    if "triggerdagrun" in op or "trigger_dag_run" in op:
        return "sub_flow", None, None, None

    # Generic fallback
    return "command", None, None, None


def _resolve_airflow_dependencies(dep_lines: List[str], task_map: Dict[str, Dict]):
    """Parse >> chains and set dependencies on job dicts in task_map."""
    for line in dep_lines:
        # Remove list brackets and split on >>
        line = re.sub(r'[\[\]]', '', line)
        parts = [p.strip() for p in line.split(">>")]
        parts = [p for p in parts if p]

        for i in range(1, len(parts)):
            upstream_var = parts[i - 1].strip()
            downstream_var = parts[i].strip()

            # Handle comma-separated groups: a, b >> c
            upstream_vars = [v.strip() for v in upstream_var.split(",") if v.strip()]
            downstream_vars = [v.strip() for v in downstream_var.split(",") if v.strip()]

            for dv in downstream_vars:
                if dv in task_map:
                    for uv in upstream_vars:
                        if uv in task_map:
                            upstream_task_id = task_map[uv]["job_id"]
                            if upstream_task_id not in task_map[dv]["dependencies"]:
                                task_map[dv]["dependencies"].append(upstream_task_id)


def _prescan_airflow_dag(source: str) -> str:
    """Quick scan to count tasks in an Airflow DAG — used in the prompt."""
    import ast as _ast
    try:
        tree = _ast.parse(source)
        task_count = 0
        dag_id = "unknown"
        for node in _ast.walk(tree):
            if isinstance(node, _ast.Call):
                func = ""
                if isinstance(node.func, _ast.Name):
                    func = node.func.id
                elif isinstance(node.func, _ast.Attribute):
                    func = node.func.attr
                if func == "DAG" and node.args and isinstance(node.args[0], _ast.Constant):
                    dag_id = node.args[0].value
                if "Operator" in func or "Sensor" in func or func in ("DummyOperator", "EmptyOperator"):
                    task_count += 1
        return f"  DAG id: '{dag_id}', approximately {task_count} task(s) detected"
    except SyntaxError:
        return "  (could not pre-scan DAG file)"

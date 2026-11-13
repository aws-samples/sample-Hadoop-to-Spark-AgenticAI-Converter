"""
MWAA Validator MCP Server
Deterministic validation for MWAA DAGs — no LLM needed.
Uses ast, regex, and DagBag subprocess checks from validator.py.
"""
import ast
import json
import logging
import sys
import re
from pathlib import Path
from fastmcp import FastMCP

from converter.validator import (
    _count_job_tasks_in_dag,
    _check_dagbag_loads,
    _fix_validation_errors_deterministic,
    _fix_variable_get_default_var,
)
from converter.config_loader import load_config, ConverterConfig

# Setup logging
# Shared input/output locations (see project_paths.py at the repo root)
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
import project_paths

log_dir = project_paths.LOG_DIR
log_dir.mkdir(exist_ok=True, parents=True)
log_file = log_dir / "mwaa_validator.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

mcp = FastMCP("mwaa-validator")

_cfg = ConverterConfig(load_config())


@mcp.tool()
def validate_syntax(dag_file: str) -> str:
    """Validate DAG Python syntax.

    Deterministic: uses ast.parse + compile. No LLM.
    """
    logger.info("validate_syntax called with: %s", dag_file)
    dag_path = Path(dag_file).resolve()
    if not dag_path.exists():
        return json.dumps({"is_valid": False, "error": f"DAG file not found: {dag_file}"})

    code = dag_path.read_text(encoding="utf-8")
    issues = []

    # 1. Python syntax
    try:
        ast.parse(code)
        compile(code, str(dag_path), "exec")
    except SyntaxError as e:
        issues.append({"category": "syntax", "message": f"line {e.lineno}: {e.msg}"})

    # 2. Required imports
    for token, import_str in {
        "DAG": "from airflow",
        "Variable": "from airflow.models",
    }.items():
        if token in code and import_str not in code:
            issues.append({"category": "import", "message": f"Missing import for {token}"})

    # 3. DAG object defined
    if "DAG(" not in code and "with DAG" not in code:
        issues.append({"category": "structure", "message": "No DAG object defined"})

    is_valid = len(issues) == 0
    result = {"is_valid": is_valid, "dag_file": dag_path.name, "issues": issues}

    # Save result
    val_dir = dag_path.parent.parent / "validation"
    val_dir.mkdir(parents=True, exist_ok=True)
    out = val_dir / f"{dag_path.stem}_syntax.json"
    out.write_text(json.dumps(result, indent=2), encoding="utf-8")

    status = "PASS" if is_valid else "FAIL"
    return json.dumps(result) + f"\n\nVALIDATION RESULT: {status}"


@mcp.tool()
def validate_structure(dag_file: str, conversion_plan_file: str) -> str:
    """Validate DAG structure against conversion plan.

    Deterministic: compares task counts, checks dependencies. No LLM.
    """
    logger.info("validate_structure called with: %s", dag_file)
    dag_path = Path(dag_file).resolve()
    plan_path = Path(conversion_plan_file).resolve()

    if not dag_path.exists():
        return json.dumps({"is_valid": False, "error": f"DAG file not found: {dag_file}"})
    if not plan_path.exists():
        return json.dumps({"is_valid": False, "error": f"Plan file not found: {conversion_plan_file}"})

    code = dag_path.read_text(encoding="utf-8")
    plan = json.loads(plan_path.read_text(encoding="utf-8"))
    issues = []

    # 1. Task count (1:1 mapping)
    expected = int(plan.get("total_tasks") or len(plan.get("tasks", [])))
    actual = _count_job_tasks_in_dag(code)
    task_count_ok = expected == 0 or actual == expected
    if not task_count_ok:
        issues.append({"category": "task_count", "message": f"Expected {expected} tasks, found {actual}"})

    # 2. Task IDs from plan present in DAG
    plan_task_ids = set()
    for task in plan.get("tasks", []):
        tid = task.get("task_id") or task.get("source_job_id") or ""
        safe_tid = re.sub(r'[^a-z0-9_]', '_', tid.lower()).strip('_')
        if safe_tid:
            plan_task_ids.add(safe_tid)
            if f'task_id="{safe_tid}"' not in code:
                issues.append({"category": "missing_task", "message": f"Task '{safe_tid}' not found in DAG"})

    # 3. Unique task IDs
    task_ids = re.findall(r'task_id="([^"]+)"', code)
    dupes = [t for t in set(task_ids) if task_ids.count(t) > 1]
    if dupes:
        issues.append({"category": "duplicate_task", "message": f"Duplicate task IDs: {dupes}"})

    # 4. DAG ID matches plan
    plan_dag_id = plan.get("dag_config", {}).get("dag_id") or plan.get("dag_id") or ""
    if plan_dag_id and f'dag_id="{plan_dag_id}"' not in code:
        safe_dag_id = re.sub(r'[^a-z0-9_]', '_', plan_dag_id.lower()).strip('_')
        if f'dag_id="{safe_dag_id}"' not in code:
            issues.append({"category": "dag_id", "message": f"DAG ID mismatch: expected '{plan_dag_id}'"})

    # 5. Dependency wiring — every edge in the plan's dependency_graph must appear in the DAG
    dep_graph = plan.get("dependency_graph", {})
    for task_id, upstream_list in dep_graph.items():
        for upstream_id in upstream_list:
            # Both must be in this DAG (not external) and the >> edge must exist
            if upstream_id in plan_task_ids and task_id in plan_task_ids:
                edge = f"{upstream_id} >> {task_id}"
                if edge not in code:
                    issues.append({
                        "category": "missing_dependency",
                        "message": f"Dependency edge missing in DAG: {edge}",
                    })

    is_valid = len(issues) == 0
    result = {"is_valid": is_valid, "dag_file": dag_path.name, "issues": issues,
              "task_count": {"expected": expected, "actual": actual}}

    val_dir = dag_path.parent.parent / "validation"
    val_dir.mkdir(parents=True, exist_ok=True)
    out = val_dir / f"{dag_path.stem}_structure.json"
    out.write_text(json.dumps(result, indent=2), encoding="utf-8")

    status = "PASS" if is_valid else "FAIL"
    return json.dumps(result) + f"\n\nVALIDATION RESULT: {status}"


@mcp.tool()
def validate_security(dag_file: str) -> str:
    """Validate DAG security best practices.

    Deterministic: regex checks for hardcoded creds, S3 paths, missing default_var. No LLM.
    """
    logger.info("validate_security called with: %s", dag_file)
    dag_path = Path(dag_file).resolve()
    if not dag_path.exists():
        return json.dumps({"is_valid": False, "error": f"DAG file not found: {dag_file}"})

    code = dag_path.read_text(encoding="utf-8")
    issues = []

    # 1. Hardcoded credentials
    cred_patterns = [
        (r'(?:password|passwd|secret)\s*=\s*["\'][^"\']+["\']', "Hardcoded password/secret"),
        (r'(?:aws_access_key_id|aws_secret_access_key)\s*=\s*["\']', "Hardcoded AWS credentials"),
    ]
    for pat, msg in cred_patterns:
        if re.search(pat, code, re.IGNORECASE):
            issues.append({"category": "credentials", "severity": "CRITICAL", "message": msg})

    # 2. Hardcoded S3 paths (not inside Variable.get or comments)
    for i, line in enumerate(code.splitlines(), 1):
        stripped = line.strip()
        if stripped.startswith("#"):
            continue
        if "s3://" in line and "Variable.get" not in line:
            issues.append({"category": "hardcoded_s3", "severity": "WARNING",
                           "message": f"Hardcoded S3 path at line {i}"})

    # 3. Variable.get without default_var
    for m in re.finditer(r'Variable\.get\(([^)]+)\)', code):
        if "default_var" not in m.group(1):
            issues.append({"category": "variable_no_default", "severity": "WARNING",
                           "message": f"Variable.get() without default_var: {m.group(0)}"})

    is_valid = not any(i.get("severity") == "CRITICAL" for i in issues)
    result = {"is_valid": is_valid, "dag_file": dag_path.name, "issues": issues}

    val_dir = dag_path.parent.parent / "validation"
    val_dir.mkdir(parents=True, exist_ok=True)
    out = val_dir / f"{dag_path.stem}_security.json"
    out.write_text(json.dumps(result, indent=2), encoding="utf-8")

    status = "PASS" if is_valid else "FAIL"
    return json.dumps(result) + f"\n\nVALIDATION RESULT: {status}"


@mcp.tool()
def validate_dagbag(dag_file: str) -> str:
    """Validate DAG can be loaded by Airflow DagBag.

    Deterministic: runs DagBag in a subprocess using _check_dagbag_loads. No LLM.
    """
    logger.info("validate_dagbag called with: %s", dag_file)
    dag_path = Path(dag_file).resolve()
    if not dag_path.exists():
        return json.dumps({"is_valid": False, "error": f"DAG file not found: {dag_file}"})

    ok, msg = _check_dagbag_loads(dag_path, _cfg)

    if ok is None:
        result = {"is_valid": True, "dag_file": dag_path.name, "dagbag": "skipped", "message": msg}
    elif ok:
        result = {"is_valid": True, "dag_file": dag_path.name, "dagbag": "passed", "message": msg}
    else:
        result = {"is_valid": False, "dag_file": dag_path.name, "dagbag": "failed", "message": msg}

    val_dir = dag_path.parent.parent / "validation"
    val_dir.mkdir(parents=True, exist_ok=True)
    out = val_dir / f"{dag_path.stem}_dagbag.json"
    out.write_text(json.dumps(result, indent=2), encoding="utf-8")

    status = "PASS" if result["is_valid"] else "FAIL"
    return json.dumps(result) + f"\n\nVALIDATION RESULT: {status}"


@mcp.tool()
def fix_validation_errors(dag_file: str, validation_errors: str) -> str:
    """Fix validation errors in DAG.

    Deterministic first (regex auto-fixes), LLM fallback if issues remain.
    """
    logger.info("fix_validation_errors called with: %s", dag_file)
    dag_path = Path(dag_file).resolve()
    if not dag_path.exists():
        return f"Error: DAG file not found: {dag_file}"

    # ── Stage A: Deterministic auto-fixes ─────────────────────────────
    _fix_validation_errors_deterministic(dag_path, _cfg)
    _fix_variable_get_default_var(dag_path)
    logger.info("✅ fix_validation_errors: deterministic fixes applied to %s", dag_path.name)

    # Re-validate to see if deterministic fixes resolved everything
    code = dag_path.read_text(encoding="utf-8")
    remaining_issues = []
    try:
        ast.parse(code)
        compile(code, str(dag_path), "exec")
    except SyntaxError as e:
        remaining_issues.append(f"Syntax error at line {e.lineno}: {e.msg}")

    if "DAG(" not in code and "with DAG" not in code:
        remaining_issues.append("No DAG object defined")

    if not remaining_issues:
        return f"Deterministic auto-correction applied to {dag_file} — all issues resolved"

    # ── Stage B: LLM fallback ─────────────────────────────────────────
    issues_str = "; ".join(remaining_issues)
    logger.warning("⚠️ fix_validation_errors: deterministic fixes insufficient for %s: %s — falling back to LLM",
                    dag_path.name, issues_str)

    return f"""LLM_FALLBACK:You are an expert Airflow DAG developer. Deterministic auto-fixes were applied \
but these issues remain: {issues_str}

Original validation errors:
{validation_errors}

Current DAG code:
{code}

Fix ALL remaining issues while:
- Preserving the conversion logic and all task mappings
- Keeping all dependencies intact
- Using Variable.get() with default_var='__PLACEHOLDER__' for all env values
- Following Airflow 2.8+ syntax
- No hardcoded credentials or S3 paths

You MUST save the corrected DAG code to: {dag_file} (overwrite the file)
After fixing, confirm: "ERROR CORRECTION COMPLETE: {dag_file}" """


if __name__ == "__main__":
    mcp.run()

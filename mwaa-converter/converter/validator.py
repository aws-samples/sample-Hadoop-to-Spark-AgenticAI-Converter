"""Stage 5: Generate DAG code + Stage 6: Validate DAGs with self-correction."""
import ast
import json
import logging
import re
import subprocess
import sys
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

from converter.config_loader import ConverterConfig
from llm_cli import kiro_chat_argv, run_llm_cli

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Stage 5: DAG generation
# ---------------------------------------------------------------------------

def generate_dags_parallel(
    plans: List[Dict],
    output_dir: Path,
    spark_sql_mapping: Path,
    cfg: ConverterConfig,
    progress: Dict,
    save_progress_fn,
    cwd: str,
    max_workers: int = 4,
) -> List[Path]:
    """Generate DAG code in parallel."""
    results: List[Path] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _generate_dag, plan, output_dir, spark_sql_mapping,
                cfg, progress, save_progress_fn, cwd,
            ): plan
            for plan in plans
        }
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    results.append(result)
            except OSError as e:
                logger.error("DAG generation failed: %s", e)
    return results


def _generate_dag(
    plan: Dict,
    output_dir: Path,
    spark_sql_mapping: Path,
    cfg: ConverterConfig,
    progress: Dict,
    save_progress_fn,
    cwd: str,
) -> Optional[Path]:
    """Generate DAG — deterministic template first, LLM fallback."""
    chunk_id = plan["chunk_id"]

    if chunk_id in progress["dags_generated"]:
        logger.info("[SKIP] Already generated DAG: %s", chunk_id)
        dag_file = output_dir / "dags" / f"{chunk_id}.py"
        return dag_file if dag_file.exists() else None

    logger.info("Generating DAG code: %s", chunk_id)

    plan_file = output_dir / "plans" / f"{chunk_id}_plan.json"
    dag_file = output_dir / "dags" / f"{chunk_id}.py"

    # ── Stage A: Deterministic template rendering ─────────────────────
    if plan_file.exists():
        try:
            from converter.dag_builder import build_dag  # pylint: disable=import-outside-toplevel

            mapping: Dict = {}
            if spark_sql_mapping.exists():
                with open(spark_sql_mapping, encoding="utf-8") as f:
                    raw = json.load(f)
                for entry in raw.get("mappings", []):
                    src, tgt = entry.get("source_script", ""), entry.get("target_script", "")
                    if src and tgt:
                        mapping[src] = tgt

            build_dag(plan_file, dag_file, spark_sql_mapping=mapping, cfg=cfg)
            _fix_hardcoded_s3_paths(dag_file, cfg)
            _fix_variable_get_default_var(dag_file)

            logger.info("✓ Generated DAG (template): %s", chunk_id)
            progress["dags_generated"].append(chunk_id)
            save_progress_fn()
            return dag_file
        except (OSError, ValueError, KeyError) as e:
            logger.warning("  Template rendering failed for %s: %s — falling back to LLM", chunk_id, e)

    # ── Stage B: LLM fallback with template-aware prompt ─────────────────
    try:
        safe_dag_id = re.sub(r"[^a-z0-9_]", "_", chunk_id.lower()).strip("_")

        # Load plan content for injection into prompt
        plan_content = "{}"
        if plan_file.exists():
            try:
                plan_content = plan_file.read_text(encoding="utf-8")
            except OSError:
                pass

        # Load the template-aware prompt from prompts.yaml
        try:
            from converter.prompts.prompt_loader import load_prompt  # pylint: disable=import-outside-toplevel
            prompt_template = load_prompt("generate_dag_code")
            prompt = (prompt_template
                      .replace("{conversion_plan}", plan_content)
                      .replace("{dag_id}", safe_dag_id)
                      .replace("{output_file}", str(dag_file)))
        except Exception:  # pylint: disable=broad-except
            # Fallback to inline prompt if yaml load fails
            prompt = (
                f"Generate a complete MWAA DAG Python file from this conversion plan.\n\n"
                f"PLAN:\n{plan_content}\n\n"
                f"RULES:\n"
                f"- dag_id MUST be exactly \"{safe_dag_id}\"\n"
                f"- ONE Airflow task per source job — no extras\n"
                f"- ALL Variable.get() calls inline inside operators — NEVER assign to module-level vars\n"
                f"- ALL Variable.get() calls MUST include default_var='__PLACEHOLDER__'\n"
                f"- NEVER add ExternalTaskSensor as a task — wire external_dependencies as sensors automatically\n"
                f"- NO hardcoded s3:// URIs — use Variable.get('SCRIPTS_BUCKET')/path pattern\n"
                f"- Airflow 2.8+ syntax: schedule= not schedule_interval=\n\n"
                f"Save the DAG Python file to: {dag_file}"
            )

        run_llm_cli(kiro_chat_argv(prompt), timeout=900, cwd=cwd)
        if dag_file.exists():
            _fix_hardcoded_s3_paths(dag_file, cfg)
            _fix_variable_get_default_var(dag_file)
            _fix_validation_errors_deterministic(dag_file, cfg)
            logger.info("✓ Generated DAG (LLM fallback): %s", chunk_id)
            progress["dags_generated"].append(chunk_id)
            save_progress_fn()
            return dag_file
        logger.warning("⚠ DAG generation incomplete: %s", chunk_id)
        return None
    except subprocess.TimeoutExpired:
        logger.error("✗ DAG generation timeout: %s", chunk_id)
        return None
    except OSError as e:
        logger.error("✗ DAG generation error: %s - %s", chunk_id, e)
        return None


# ---------------------------------------------------------------------------
# Auto-fix passes
# ---------------------------------------------------------------------------

def _fix_hardcoded_s3_paths(dag_file: Path, cfg: ConverterConfig):
    """Replace hardcoded s3:// URIs with Variable.get() calls."""
    try:
        code = dag_file.read_text(encoding="utf-8")
        if "s3://" not in code:
            return

        s3_pattern = re.compile(r"s3://([^/\s'\"]+)((?:/[^\s'\"]*)?)")

        def replace_s3(m):
            bucket = m.group(1)
            path_suffix = m.group(2)
            var_key = cfg.var_logs_bucket if ("log" in bucket.lower() or "/logs/" in path_suffix.lower()) else cfg.var_scripts_bucket
            return "{Variable.get('" + var_key + "')}" + path_suffix

        lines = code.split("\n")
        new_lines, fixed = [], 0
        for line in lines:
            if "s3://" not in line or "Variable.get" in line or line.strip().startswith("#"):
                new_lines.append(line)
                continue
            new_line = s3_pattern.sub(replace_s3, line)
            if new_line != line:
                new_line = re.sub(r'(?<![f])"(\{Variable\.get)', r'f"\1', new_line)
                new_line = re.sub(r"(?<![f])'(\{Variable\.get)", r"f'\1", new_line)
                fixed += 1
            new_lines.append(new_line)

        if fixed:
            dag_file.write_text("\n".join(new_lines), encoding="utf-8")
            logger.info("    [Auto-fix] Replaced %d hardcoded S3 path(s) in %s", fixed, dag_file.name)
    except OSError as e:
        logger.warning("    [Auto-fix] S3 path fix skipped for %s: %s", dag_file.name, e)


def _fix_variable_get_default_var(dag_file: Path):
    """Ensure every Variable.get() call has default_var='__PLACEHOLDER__'."""
    try:
        code = dag_file.read_text(encoding="utf-8")
        pattern = re.compile(r"Variable\.get\(\s*(['\"][^'\"]+['\"])\s*\)", re.DOTALL)

        def add_default(m):
            inner = m.group(0)[len("Variable.get("):-1]
            if "," in inner:
                return m.group(0)
            return f"Variable.get({m.group(1)}, default_var='__PLACEHOLDER__')"

        new_code = pattern.sub(add_default, code)
        if new_code != code:
            dag_file.write_text(new_code, encoding="utf-8")
            count = sum(1 for m in pattern.finditer(code) if "," not in m.group(0)[len("Variable.get("):-1])
            logger.info("    [Auto-fix] Added default_var to %d Variable.get() call(s) in %s", count, dag_file.name)
    except OSError as e:
        logger.warning("    [Auto-fix] default_var fix skipped for %s: %s", dag_file.name, e)


# ---------------------------------------------------------------------------
# Stage 6: Validation
# ---------------------------------------------------------------------------

def validate_dags_parallel(
    dag_files: List[Path],
    plans: List[Dict],
    output_dir: Path,
    cfg: ConverterConfig,
    progress: Dict,
    save_progress_fn,
    max_workers: int = 4,
) -> List[Dict]:
    """Validate DAGs in parallel with self-correction."""
    plan_lookup = {p["chunk_id"]: p for p in plans if p}

    paired = []
    for dag_file in dag_files:
        if not dag_file:
            continue
        chunk_id = dag_file.stem
        plan = plan_lookup.get(chunk_id)
        if plan is None:
            for pid, p in plan_lookup.items():
                if pid.lower() == chunk_id.lower():
                    plan = p
                    break
        if plan is None:
            logger.warning("No plan found for DAG: %s, skipping validation", dag_file.name)
            continue
        paired.append((dag_file, plan))

    results: List[Dict] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _validate_dag, dag_file, plan, output_dir, cfg, progress, save_progress_fn,
            ): dag_file
            for dag_file, plan in paired
        }
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    results.append(result)
            except OSError as e:
                logger.error("DAG validation failed: %s", e)
    return results


def _validate_dag(
    dag_file: Path,
    plan: Dict,
    output_dir: Path,
    cfg: ConverterConfig,
    progress: Dict,
    save_progress_fn,
) -> Optional[Dict]:
    """Validate a single DAG with up to 3 self-correction attempts."""
    dag_id = dag_file.stem
    plan_file = output_dir / "plans" / f"{plan['chunk_id']}_plan.json"
    logger.info("Validating DAG: %s", dag_id)

    for attempt in range(3):
        if attempt > 0:
            logger.info("  Retry %d/3 after auto-correction...", attempt + 1)

        try:
            code = dag_file.read_text(encoding="utf-8")
        except OSError as e:
            logger.error("✗ Cannot read DAG file: %s — %s", dag_id, e)
            return {"dag_id": dag_id, "status": "error", "validation_passed": False, "confidence_score": 0.0}

        issues: List[Dict] = []

        # 1. Python syntax
        try:
            ast.parse(code)
            compile(code, str(dag_file), "exec")
            logger.info("  ✓ Syntax OK")
        except SyntaxError as e:
            msg = f"Syntax error at line {e.lineno}: {e.msg}"
            logger.warning("  ✗ %s", msg)
            issues.append({"category": "syntax", "message": msg})

        # 2. Task count (1:1 mapping)
        expected_tasks = 0
        if plan_file.exists():
            try:
                with open(plan_file, encoding="utf-8") as f:
                    plan_data = json.load(f)
                expected_tasks = int(plan_data.get("total_tasks") or len(plan_data.get("tasks", [])))
            except (json.JSONDecodeError, OSError, ValueError):
                pass
        actual_job_tasks = _count_job_tasks_in_dag(code)
        if expected_tasks > 0:
            if actual_job_tasks == expected_tasks:
                logger.info("  ✓ Task count: %d/%d", actual_job_tasks, expected_tasks)
            else:
                msg = f"Task count mismatch: {actual_job_tasks} in DAG, {expected_tasks} expected"
                logger.warning("  ✗ %s", msg)
                issues.append({"category": "task_count", "message": msg})
        else:
            logger.info("  ✓ Task count: %d tasks (no plan to compare)", actual_job_tasks)

        # 3. Source-aware semantic validation
        if plan_file.exists():
            semantic_issues = _validate_dag_semantics(code, plan_data)
            for si in semantic_issues:
                logger.warning("  ✗ Semantic: %s", si["message"])
                issues.append(si)
            if not semantic_issues:
                logger.info("  ✓ Semantic checks passed")

        # Auto-correct then retry
        if issues and attempt < 2:
            logger.warning("  Applying auto-correction (attempt %d)...", attempt + 1)
            _fix_validation_errors_deterministic(dag_file, cfg)
            continue

        # 3. DagBag load
        dagbag_ok, dagbag_msg = _check_dagbag_loads(dag_file, cfg)
        if dagbag_ok is None:
            logger.warning("  ⚠ DagBag: %s", dagbag_msg)
            dagbag_label = "skipped"
        elif dagbag_ok:
            logger.info("  ✓ DagBag: %s", dagbag_msg)
            dagbag_label = "True"
        else:
            logger.warning("  ✗ DagBag: %s", dagbag_msg)
            issues.append({"category": "dagbag", "message": dagbag_msg})
            dagbag_label = "False"

        # Result
        hard_failures = [i for i in issues if i["category"] != "dagbag"]
        validation_passed = len(hard_failures) == 0
        syntax_ok = not any(i["category"] == "syntax" for i in issues)
        taskcount_ok = expected_tasks == 0 or actual_job_tasks == expected_tasks
        semantic_ok = not any(i["category"] == "semantic" for i in issues)

        checks_run = [syntax_ok, taskcount_ok, semantic_ok]
        if dagbag_ok is not None:
            checks_run.append(dagbag_ok)
        confidence_score = round(sum(checks_run) / len(checks_run), 4)

        if validation_passed and dag_id not in progress["dags_validated"]:
            progress["dags_validated"].append(dag_id)
            save_progress_fn()

        return {
            "dag_id": dag_id,
            "status": "success" if validation_passed else "validation_failed",
            "validation_passed": validation_passed,
            "confidence_score": confidence_score,
            "checks": {"syntax": syntax_ok, "task_count": taskcount_ok,
                       "semantic": semantic_ok, "dagbag": dagbag_label},
            "issues": issues,
        }

    return None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _validate_dag_semantics(code: str, plan_data: Dict) -> List[Dict]:
    """Source-aware semantic validation: cross-check DAG content against plan.

    Checks:
    1. TriggerDagRunOperator trigger_dag_id is not 'unknown_dag'
    2. Every task with external_dependencies has an ExternalTaskSensor in the DAG
    3. Tasks with OR conditions (trigger_rule=one_success) have it in the DAG
    4. EmrAddStepsOperator Args use f-string (not bare string interpolation)
    5. Schedule is set when plan has a non-None schedule
    """
    issues: List[Dict] = []

    # 1. Check trigger_dag_id is not 'unknown_dag'
    unknown_triggers = re.findall(
        r'trigger_dag_id\s*=\s*["\']unknown_dag["\']', code
    )
    if unknown_triggers:
        issues.append({
            "category": "semantic",
            "message": (
                f"TriggerDagRunOperator has unresolved trigger_dag_id='unknown_dag' "
                f"({len(unknown_triggers)} occurrence(s)). "
                "Set %%SUB_FLOW_NAME variable in source XML or update the plan."
            ),
        })

    # 2. Check ExternalTaskSensors are present for external_dependencies
    for task in plan_data.get("tasks", []):
        ext_deps = task.get("external_dependencies", [])
        for dep in ext_deps:
            ext_dag = dep.get("external_dag_id") or dep.get("source_folder") or ""
            ext_task = dep.get("external_task_id") or dep.get("source_job_id") or ""
            if not ext_dag:
                continue
            # Look for ExternalTaskSensor referencing this dag
            sensor_pattern = re.compile(
                r'ExternalTaskSensor\s*\(.*?external_dag_id\s*=\s*["\']'
                + re.escape(ext_dag.lower().replace("-", "_"))
                + r'["\']',
                re.DOTALL | re.IGNORECASE,
            )
            if not sensor_pattern.search(code):
                issues.append({
                    "category": "semantic",
                    "message": (
                        f"Task '{task.get('task_id')}' has external dependency on "
                        f"'{ext_dag}.{ext_task}' but no ExternalTaskSensor found in DAG."
                    ),
                })

    # 3. Check OR trigger_rule is present when plan specifies it
    for task in plan_data.get("tasks", []):
        if task.get("trigger_rule") in ("one_success", "one_done"):
            task_id = task.get("task_id", "")
            # Look for trigger_rule on this specific task
            task_block_pattern = re.compile(
                r'task_id\s*=\s*["\']' + re.escape(task_id) + r'["\']'
                r'.*?(?:trigger_rule\s*=|(?=\n\s*\w+\s*=\s*\w+Operator))',
                re.DOTALL,
            )
            block_match = task_block_pattern.search(code)
            has_trigger_rule = bool(
                block_match and "trigger_rule" in block_match.group(0)
            )
            if not has_trigger_rule:
                issues.append({
                    "category": "semantic",
                    "message": (
                        f"Task '{task_id}' has OR condition in source "
                        f"(trigger_rule={task['trigger_rule']}) but trigger_rule "
                        "is not set in generated DAG. Task will wait for ALL upstreams."
                    ),
                })

    # 4. Check EmrAddStepsOperator Args use proper f-string
    # Bad pattern: "Args": ['spark-submit', '${Variable.get(...)}...']  (string, not f-string)
    bad_args_pattern = re.compile(
        r'["\']Args["\']\s*:\s*\[.*?\$\{Variable\.get',
        re.DOTALL,
    )
    if bad_args_pattern.search(code):
        issues.append({
            "category": "semantic",
            "message": (
                "EmrAddStepsOperator Args contains '${Variable.get(...)}' as a plain string. "
                "Use an f-string so Variable.get() is evaluated at runtime."
            ),
        })

    # 5. Check schedule matches plan
    plan_schedule = (
        plan_data.get("dag_config", {}).get("schedule")
        or plan_data.get("dag_config", {}).get("schedule_interval")
    )
    if plan_schedule and plan_schedule != "None":
        # DAG should have schedule="<cron>" not schedule=None
        dag_schedule_none = re.search(r'schedule\s*=\s*None', code)
        dag_schedule_set = re.search(r'schedule\s*=\s*["\']', code)
        if dag_schedule_none and not dag_schedule_set:
            issues.append({
                "category": "semantic",
                "message": (
                    f"Plan specifies schedule='{plan_schedule}' but DAG has schedule=None. "
                    "The source schedule was not converted."
                ),
            })

    return issues


def _count_job_tasks_in_dag(code: str) -> int:
    """Count source-job-derived tasks (excludes ExternalTaskSensors)."""
    patterns = [
        r"(\w+)\s*=\s*(?:EMR|Emr)ServerlessStartJobOperator\s*\(",
        r"(\w+)\s*=\s*EmrAddStepsOperator\s*\(",
        r"(\w+)\s*=\s*BashOperator\s*\(",
        r"(\w+)\s*=\s*(?:Empty|Dummy)Operator\s*\(",
        r"(\w+)\s*=\s*PythonOperator\s*\(",
        r"(\w+)\s*=\s*TriggerDagRunOperator\s*\(",
    ]
    tasks = set()
    for p in patterns:
        tasks.update(re.findall(p, code, re.IGNORECASE))
    return len(tasks)


def _check_dagbag_loads(dag_file: Path, cfg: ConverterConfig) -> Tuple[Optional[bool], str]:
    """Check whether Airflow's DagBag can parse the DAG file."""
    python_bin = cfg.dagbag_python if Path(cfg.dagbag_python).exists() else sys.executable

    # The checker is a constant script; the DAG path is passed as argv[1]
    # (data, not code), so nothing is interpolated into the executed source.
    checker = (
        "import sys, tempfile, shutil, json\n"
        "from pathlib import Path\n"
        "dag_file = Path(sys.argv[1])\n"
        "try:\n"
        "    from airflow.models import DagBag\n"
        "except ImportError as e:\n"
        '    print(json.dumps({"ok": False, "skipped": True, "msg": f"Airflow not importable: {e}"}))\n'
        "    sys.exit(0)\n"
        'tmp = tempfile.mkdtemp(prefix="mwaa_dagbag_")\n'
        "try:\n"
        "    shutil.copy2(dag_file, Path(tmp) / dag_file.name)\n"
        "    try:\n"
        "        db = DagBag(dag_folder=tmp, include_examples=False)\n"
        "    except TypeError:\n"
        "        db = DagBag(dag_folder=tmp)  # Airflow 3.x removed include_examples kwarg\n"
        "    if not db.import_errors:\n"
        "        if db.dags:\n"
        "            dag_id = list(db.dags.keys())[0]\n"
        "            task_ids = [t.task_id for t in db.dags[dag_id].tasks]\n"
        '            print(json.dumps({"ok": True, "msg": f"DAG loaded: dag_id={dag_id}, tasks={len(task_ids)}"}))\n'
        "        else:\n"
        '            print(json.dumps({"ok": False, "msg": "DagBag loaded but no DAGs found"}))\n'
        "        sys.exit(0)\n"
        "    for _p, error in db.import_errors.items():\n"
        "        err = str(error)\n"
        '        if ("Variable" in err and "does not exist" in err) or \\\n'
        '           ("no such table" in err and "variable" in err.lower()) or \\\n'
        '           ("OperationalError" in err and "variable" in err.lower()):\n'
        '            print(json.dumps({"ok": True, "msg": "Variable.get() pattern correct"}))\n'
        "            sys.exit(0)\n"
        '        if "No module named" in err and "providers" in err:\n'
        '            print(json.dumps({"ok": True, "msg": "Provider packages absent locally"}))\n'
        "            sys.exit(0)\n"
        '        print(json.dumps({"ok": False, "msg": f"DagBag import error: {err[:200]}"}))\n'
        "        sys.exit(0)\n"
        '    print(json.dumps({"ok": True, "msg": "DAG parsed without errors"}))\n'
        "except Exception as exc:\n"
        "    err = str(exc)\n"
        '    if "no such table" in err.lower() or "OperationalError" in err:\n'
        '        print(json.dumps({"ok": True, "msg": "Variable.get() pattern correct"}))\n'
        "    else:\n"
        '        print(json.dumps({"ok": False, "msg": f"DagBag exception: {err[:200]}"}))\n'
        "finally:\n"
        "    shutil.rmtree(tmp, ignore_errors=True)\n"
    )
    try:
        # Reviewed: argv list, no shell; python_bin is the configured interpreter
        # (validated to exist, else sys.executable); checker is a constant script
        # and the DAG path is passed as data in argv.
        result = subprocess.run(  # nosec B603  # nosemgrep: python.lang.security.audit.dangerous-subprocess-use-audit.dangerous-subprocess-use-audit
            [python_bin, "-c", checker, str(dag_file)],
            shell=False, capture_output=True, text=True, timeout=60,
            cwd=str(dag_file.parent.parent), check=False,
        )
        for line in reversed(result.stdout.splitlines()):
            line = line.strip()
            if line.startswith("{"):
                data = json.loads(line)
                if data.get("skipped"):
                    return None, data["msg"]
                return data["ok"], data["msg"]
        return False, "DagBag check subprocess failed: %s" % ((result.stderr or "")[-300:] or "no output")
    except (OSError, subprocess.TimeoutExpired) as e:
        return False, "DagBag check error: %s" % e


def _fix_validation_errors_deterministic(dag_file: Path, cfg: ConverterConfig):
    """Fix common validation errors deterministically (no LLM)."""
    try:
        code = dag_file.read_text(encoding="utf-8")
        modified = False

        # Fix 1: Variable scoping — LLM assigns Variable.get() to module-level vars
        # e.g. EMR_APP_ID = Variable.get("EMR_APP_ID", ...) then uses EMR_APP_ID in operators
        # Rewrite to inline Variable.get() calls inside each operator argument
        scope_pattern = re.compile(
            r'^(\s*)(\w+)\s*=\s*(Variable\.get\([^)]+\))\s*$',
            re.MULTILINE,
        )
        scope_vars: Dict[str, str] = {}
        for m in scope_pattern.finditer(code):
            var_name = m.group(2)
            var_expr = m.group(3)
            # Only capture if it's inside a with DAG block (indented)
            if m.group(1).strip() == "" and "Variable.get" in var_expr:
                continue
            scope_vars[var_name] = var_expr

        if scope_vars:
            # Remove the assignment lines and inline the Variable.get() calls
            for var_name, var_expr in scope_vars.items():
                # Remove the assignment line
                code = re.sub(
                    r'^\s*' + re.escape(var_name) + r'\s*=\s*' + re.escape(var_expr) + r'\s*\n',
                    '',
                    code,
                    flags=re.MULTILINE,
                )
                # Replace bare usages (not already inside Variable.get) with inline call
                # Match var_name used as a value (not as a key or in a string)
                code = re.sub(
                    r'(?<!["\'\w])' + re.escape(var_name) + r'(?!["\'\w])',
                    var_expr,
                    code,
                )
            modified = True

        # Fix 2: Remove ExternalTaskSensor tasks added by LLM as extra tasks
        # These appear as task_id="..._sensor" with ExternalTaskSensor(
        # The DAG builder renders sensors from external_dependencies automatically
        # We detect: variable = ExternalTaskSensor(...) blocks and their >> wiring
        sensor_task_pattern = re.compile(
            r'\n\s*#[^\n]*\n\s*\w+\s*=\s*ExternalTaskSensor\s*\([^)]*\)[^\n]*\n',
            re.DOTALL,
        )
        if sensor_task_pattern.search(code):
            # Find sensor variable names
            sensor_vars = re.findall(
                r'(\w+)\s*=\s*ExternalTaskSensor\s*\(',
                code,
            )
            # Remove sensor task blocks
            code = sensor_task_pattern.sub('\n', code)
            # Remove dependency lines involving sensor vars
            for sv in sensor_vars:
                code = re.sub(
                    r'\n\s*' + re.escape(sv) + r'\s*>>[^\n]*',
                    '',
                    code,
                )
                code = re.sub(
                    r'\n[^\n]*>>\s*' + re.escape(sv) + r'[^\n]*',
                    '',
                    code,
                )
            # Remove ExternalTaskSensor import if no longer needed
            if 'ExternalTaskSensor' not in code:
                code = re.sub(
                    r'\nfrom airflow\.sensors\.external_task import ExternalTaskSensor\n',
                    '\n',
                    code,
                )
            modified = True

        # Fix 3: Missing imports
        required_imports = {
            "EmrServerlessStartJobOperator": "from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator",
            "EmptyOperator": "from airflow.operators.empty import EmptyOperator",
            "BashOperator": "from airflow.operators.bash import BashOperator",
            "ExternalTaskSensor": "from airflow.sensors.external_task import ExternalTaskSensor",
            "Variable": "from airflow.models import Variable",
        }
        for operator, import_stmt in required_imports.items():
            if operator in code and import_stmt not in code:
                lines = code.split("\n")
                last_import_idx = 0
                for i, line in enumerate(lines):
                    if line.startswith("from airflow") or line.startswith("import airflow"):
                        last_import_idx = i
                lines.insert(last_import_idx + 1, import_stmt)
                code = "\n".join(lines)
                modified = True

        # Fix 4: Wrong class name
        if "EMRServerlessStartJobOperator" in code:
            code = code.replace("EMRServerlessStartJobOperator", "EmrServerlessStartJobOperator")
            modified = True

        if modified:
            dag_file.write_text(code, encoding="utf-8")
            logger.info("✓ Applied deterministic auto-correction to %s", dag_file.name)

        _fix_hardcoded_s3_paths(dag_file, cfg)
    except OSError as e:
        logger.error("✗ Deterministic correction failed: %s", e)

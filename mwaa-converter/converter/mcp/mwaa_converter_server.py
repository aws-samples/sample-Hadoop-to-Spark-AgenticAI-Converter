"""
MWAA Converter MCP Server

Stage 1 (parse_control_m_job):
  - Reads XML, builds LLM prompt, returns it for LLM execution
  - The LLM reads the XML, produces JobIR JSON, and SAVES it to disk
  - After LLM responds, the orchestrator calls validate_parsed_job_ir
    to schema-validate what the LLM wrote

Stage 3 (generate_conversion_plan):
  - Reads JobIR, builds LLM prompt, returns it for LLM execution
  - The LLM reads the JobIR, produces ConversionPlan JSON, and SAVES it to disk
  - After LLM responds, the orchestrator calls validate_conversion_plan
    to schema-validate what the LLM wrote

Stage 4 (generate_dag_code):
  - Deterministic Jinja2 templates (no LLM)
  - LLM_FALLBACK only if template rendering fails

All validation stages: deterministic (no LLM)
"""
import json
import logging
import sys
from pathlib import Path

from fastmcp import FastMCP

from converter.prompts.prompt_loader import load_prompt
from converter.config_loader import load_config, ConverterConfig

# Setup logging
# Shared input/output locations (see project_paths.py at the repo root)
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
import project_paths

log_dir = project_paths.LOG_DIR
log_dir.mkdir(exist_ok=True, parents=True)
log_file = log_dir / "mwaa_converter.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

mcp = FastMCP("mwaa-converter")
_cfg = ConverterConfig(load_config())


# ─────────────────────────────────────────────────────────────────────────────
# Stage 1: Parse Control-M XML → JobIR  (LLM-powered)
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def parse_control_m_job(job_file: str) -> str:
    """Parse a Control-M XML file into structured JobIR JSON using LLM.

    Returns a prompt instructing the LLM to:
    1. Read the XML file
    2. Extract all SMART_FOLDERs and their jobs
    3. Produce JobIR JSON matching the strict schema
    4. Save one JSON file per SMART_FOLDER to output/mwaa/job_ir/

    The LLM writes the files directly. validate_parsed_job_ir is called
    afterwards to schema-validate what was written.

    Args:
        job_file: Path to Control-M XML file (.xml)

    Returns:
        LLM prompt string for the orchestrator to dispatch.
    """
    logger.info("parse_control_m_job called with: %s", job_file)

    job_path = Path(job_file)
    if not job_path.exists():
        return f"Error: File not found: {job_file}"

    if job_path.suffix.lower() != ".xml":
        return f"Error: Only XML files are supported. Got: {job_path.suffix}"

    output_dir = project_paths.MWAA_OUTPUT
    output_dir.mkdir(parents=True, exist_ok=True)

    from converter.parser import build_parse_prompt
    prompt = build_parse_prompt(job_path, output_dir)

    logger.info("Built parse prompt for %s (%d chars)", job_path.name, len(prompt))
    return prompt


@mcp.tool()
def validate_parsed_job_ir(job_file: str) -> str:
    """Validate JobIR files written by the LLM after parse_control_m_job.

    Reads the JobIR JSON files the LLM wrote to output/mwaa/job_ir/ and validates
    them against the required schema. If any file fails validation, it is
    regenerated using the deterministic XML parser as a fallback.

    Args:
        job_file: Path to the original Control-M XML file

    Returns:
        Validation summary with folder names and job counts.
    """
    logger.info("validate_parsed_job_ir called for: %s", job_file)

    job_path = Path(job_file)
    output_dir = project_paths.MWAA_OUTPUT
    job_ir_dir = output_dir / "job_ir"

    if not job_ir_dir.exists():
        # LLM didn't write anything — run deterministic fallback
        logger.warning("No job_ir directory found — running deterministic fallback")
        from converter.parser import _deterministic_fallback, _safe_filename
        job_ir_dir.mkdir(parents=True, exist_ok=True)
        written = _deterministic_fallback(job_path, job_ir_dir)
        if not written:
            return f"Error: Could not parse {job_file}"
        return _summarise_job_ir_files(written)

    # Find JobIR files that correspond to this XML
    # The LLM may have written files with folder names as stems
    existing_files = sorted(job_ir_dir.glob("*.json"))
    if not existing_files:
        logger.warning("LLM wrote no JobIR files — running deterministic fallback")
        from converter.parser import _deterministic_fallback
        written = _deterministic_fallback(job_path, job_ir_dir)
        if not written:
            return f"Error: Could not parse {job_file}"
        return _summarise_job_ir_files(written)

    # Validate each file
    from converter.parser import _validate_jobs, _deterministic_fallback
    valid_files = []
    invalid_files = []

    for ir_file in existing_files:
        try:
            with open(ir_file, encoding="utf-8") as f:
                ir = json.load(f)
            folder = ir.get("folder", ir_file.stem)
            jobs = ir.get("jobs", [])
            if _validate_jobs(jobs, folder):
                valid_files.append(ir_file)
                logger.info("✓ Valid JobIR: %s (%d jobs)", ir_file.name, len(jobs))
            else:
                invalid_files.append(ir_file)
                logger.warning("✗ Invalid JobIR schema: %s — will regenerate", ir_file.name)
        except (json.JSONDecodeError, OSError) as e:
            invalid_files.append(ir_file)
            logger.warning("✗ Cannot read %s: %s — will regenerate", ir_file.name, e)

    # Regenerate invalid files using deterministic fallback
    if invalid_files:
        logger.info("Regenerating %d invalid file(s) using deterministic XML parser", len(invalid_files))
        # Remove invalid files first
        for f in invalid_files:
            f.unlink(missing_ok=True)
        fallback_written = _deterministic_fallback(job_path, job_ir_dir)
        valid_files.extend(fallback_written)

    if not valid_files:
        return f"Error: No valid JobIR files produced for {job_file}"

    return _summarise_job_ir_files(valid_files)


def _summarise_job_ir_files(files: list) -> str:
    lines = [f"JobIR validated: {len(files)} folder(s)"]
    for f in files:
        try:
            with open(f, encoding="utf-8") as fh:
                ir = json.load(fh)
            folder = ir.get("folder", Path(f).stem)
            job_count = len(ir.get("jobs", []))
            lines.append(f"  Folder '{folder}': {job_count} jobs → {Path(f).name}")
        except (json.JSONDecodeError, OSError):
            lines.append(f"  {Path(f).name}")
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# Stage 1b: Parse on-prem Airflow DAG → JobIR  (LLM-powered)
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def parse_airflow_dag(dag_file: str) -> str:
    """Parse an on-prem Airflow DAG Python file into structured JobIR JSON using LLM.

    Returns a prompt instructing the LLM to:
    1. Read the Airflow DAG .py file
    2. Extract all tasks, operators, dependencies, and schedule
    3. Produce JobIR JSON matching the strict schema
    4. Save one JSON file (named after the dag_id) to output/mwaa/job_ir/

    The LLM writes the file directly. validate_parsed_airflow_job_ir is called
    afterwards to schema-validate what was written.

    Args:
        dag_file: Path to on-prem Airflow DAG Python file (.py)

    Returns:
        LLM prompt string for the orchestrator to dispatch.
    """
    logger.info("parse_airflow_dag called with: %s", dag_file)

    dag_path = Path(dag_file)
    if not dag_path.exists():
        return f"Error: File not found: {dag_file}"

    if dag_path.suffix.lower() != ".py":
        return f"Error: Expected a Python DAG file (.py). Got: {dag_path.suffix}"

    output_dir = project_paths.MWAA_OUTPUT
    output_dir.mkdir(parents=True, exist_ok=True)

    from converter.parser import build_airflow_parse_prompt
    prompt = build_airflow_parse_prompt(dag_path, output_dir)

    logger.info("Built Airflow parse prompt for %s (%d chars)", dag_path.name, len(prompt))
    return prompt


@mcp.tool()
def validate_parsed_airflow_job_ir(dag_file: str) -> str:
    """Validate JobIR files written by the LLM after parse_airflow_dag.

    Reads the JobIR JSON file the LLM wrote to output/mwaa/job_ir/ and validates
    it against the required schema. If validation fails, falls back to the
    deterministic AST-based Airflow parser.

    Args:
        dag_file: Path to the original Airflow DAG Python file

    Returns:
        Validation summary with dag_id and task count.
    """
    logger.info("validate_parsed_airflow_job_ir called for: %s", dag_file)

    dag_path = Path(dag_file)
    output_dir = project_paths.MWAA_OUTPUT
    job_ir_dir = output_dir / "job_ir"

    from converter.parser import _validate_jobs, _deterministic_airflow_fallback, _safe_filename

    if not job_ir_dir.exists() or not list(job_ir_dir.glob("*.json")):
        logger.warning("No JobIR files found — running AST fallback for %s", dag_path.name)
        job_ir_dir.mkdir(parents=True, exist_ok=True)
        written = _deterministic_airflow_fallback(dag_path, job_ir_dir)
        if not written:
            return f"Error: Could not parse {dag_file}"
        return _summarise_job_ir_files(written)

    existing_files = sorted(job_ir_dir.glob("*.json"))
    valid_files = []
    invalid_files = []

    for ir_file in existing_files:
        try:
            with open(ir_file, encoding="utf-8") as f:
                ir = json.load(f)
            folder = ir.get("folder", ir_file.stem)
            jobs = ir.get("jobs", [])
            if _validate_jobs(jobs, folder):
                valid_files.append(ir_file)
                logger.info("✓ Valid Airflow JobIR: %s (%d tasks)", ir_file.name, len(jobs))
            else:
                invalid_files.append(ir_file)
                logger.warning("✗ Invalid Airflow JobIR schema: %s — will regenerate", ir_file.name)
        except (json.JSONDecodeError, OSError) as e:
            invalid_files.append(ir_file)
            logger.warning("✗ Cannot read %s: %s — will regenerate", ir_file.name, e)

    if invalid_files:
        logger.info("Regenerating %d invalid file(s) using AST parser", len(invalid_files))
        for f in invalid_files:
            f.unlink(missing_ok=True)
        fallback_written = _deterministic_airflow_fallback(dag_path, job_ir_dir)
        valid_files.extend(fallback_written)

    if not valid_files:
        return f"Error: No valid JobIR files produced for {dag_file}"

    return _summarise_job_ir_files(valid_files)


# ─────────────────────────────────────────────────────────────────────────────
# Stage 3: Generate ConversionPlan  (LLM-powered)
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def generate_conversion_plan(chunk_id: str, jobs_file: str, spark_sql_mapping_file: str) -> str:
    """Generate a ConversionPlan for one SMART_FOLDER using LLM.

    Returns a prompt instructing the LLM to:
    1. Read the JobIR JSON
    2. Apply operator mapping, resolve script paths, convert schedules
    3. Produce a ConversionPlan JSON matching the strict schema
    4. Save it to output/mwaa/plans/{chunk_id}_plan.json

    The LLM writes the file directly. validate_conversion_plan is called
    afterwards to schema-validate what was written.

    Args:
        chunk_id:               Folder identifier (stem of the JobIR file)
        jobs_file:              Path to the JobIR JSON file for this chunk
        spark_sql_mapping_file: Path to spark_sql_mapping.json

    Returns:
        LLM prompt string for the orchestrator to dispatch.
    """
    logger.info("generate_conversion_plan called: chunk_id=%s", chunk_id)

    jobs_path = Path(jobs_file)
    if not jobs_path.exists():
        return f"Error: JobIR file not found: {jobs_file}"

    try:
        with open(jobs_path, encoding="utf-8") as f:
            job_ir = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        return f"Error reading JobIR file: {e}"

    chunk = {
        "chunk_id":    chunk_id,
        "folder":      job_ir.get("folder", chunk_id),
        "job_ir_file": jobs_path,
        "job_count":   len(job_ir.get("jobs", [])),
        "jobs":        job_ir.get("jobs", []),
    }

    output_dir = jobs_path.parent.parent

    from converter.planner import build_plan_prompt
    prompt = build_plan_prompt(
        chunk=chunk,
        spark_sql_mapping_file=Path(spark_sql_mapping_file),
        cfg=_cfg,
        output_dir=output_dir,
    )

    logger.info("Built plan prompt for chunk '%s' (%d chars)", chunk_id, len(prompt))
    return prompt


@mcp.tool()
def validate_conversion_plan(chunk_id: str, jobs_file: str, spark_sql_mapping_file: str) -> str:
    """Validate the ConversionPlan JSON written by the LLM.

    Reads the plan file the LLM wrote to output/mwaa/plans/{chunk_id}_plan.json
    and validates it against the required schema. If validation fails,
    regenerates the plan using the deterministic OPERATOR_MAP planner.

    Args:
        chunk_id:               Folder identifier
        jobs_file:              Path to the JobIR JSON file
        spark_sql_mapping_file: Path to spark_sql_mapping.json

    Returns:
        Validation summary with DAG ID, task count, and operator breakdown.
    """
    logger.info("validate_conversion_plan called: chunk_id=%s", chunk_id)

    jobs_path = Path(jobs_file)
    if not jobs_path.exists():
        return f"Error: JobIR file not found: {jobs_file}"

    try:
        with open(jobs_path, encoding="utf-8") as f:
            job_ir = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        return f"Error reading JobIR file: {e}"

    chunk = {
        "chunk_id":    chunk_id,
        "folder":      job_ir.get("folder", chunk_id),
        "job_ir_file": jobs_path,
        "job_count":   len(job_ir.get("jobs", [])),
        "jobs":        job_ir.get("jobs", []),
    }

    output_dir = jobs_path.parent.parent
    plan_file = output_dir / "plans" / f"{chunk_id}_plan.json"

    from converter.planner import _validate_plan, _normalise_plan_operators, _deterministic_fallback_plan

    # Try to validate what the LLM wrote
    if plan_file.exists():
        try:
            with open(plan_file, encoding="utf-8") as f:
                plan = json.load(f)
            _normalise_plan_operators(plan)
            errors = _validate_plan(plan, chunk)
            if not errors:
                logger.info("✓ LLM plan valid: chunk='%s' (%d tasks)", chunk_id, plan.get("total_tasks", 0))
                # Apply deterministic post-processing before saving
                from converter.planner import _derive_schedule, _apply_or_trigger_rules, _apply_schedule
                _apply_schedule(plan, output_dir, chunk_id)
                _apply_or_trigger_rules(plan, output_dir, chunk_id)
                # Re-save with normalised operators + post-processing
                plan_file.write_text(json.dumps(plan, indent=2), encoding="utf-8")
                return _summarise_plan(plan, plan_file)
            else:
                logger.warning("LLM plan validation errors for '%s': %s — regenerating", chunk_id, errors)
        except (json.JSONDecodeError, OSError) as e:
            logger.warning("Cannot read LLM plan for '%s': %s — regenerating", chunk_id, e)
    else:
        logger.warning("LLM did not write plan file for '%s' — generating deterministically", chunk_id)

    # Fallback: generate plan deterministically
    plan_file.parent.mkdir(parents=True, exist_ok=True)
    plan = _deterministic_fallback_plan(chunk, Path(spark_sql_mapping_file), _cfg)
    plan_file.write_text(json.dumps(plan, indent=2), encoding="utf-8")
    logger.info("✓ Fallback plan saved: chunk='%s' (%d tasks)", chunk_id, plan["total_tasks"])
    return _summarise_plan(plan, plan_file)


def _summarise_plan(plan: dict, plan_file: Path) -> str:
    return (
        f"Conversion plan validated: {plan_file}\n"
        f"  DAG ID:      {plan['dag_config']['dag_id']}\n"
        f"  Source jobs: {plan['source_job_count']}\n"
        f"  Tasks:       {plan['total_tasks']}\n"
        f"  Operators:   {plan['conversion_summary']['operator_breakdown']}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Stage 4: Generate DAG code  (Deterministic Jinja2, LLM fallback)
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def generate_dag_code(conversion_plan_file: str) -> str:
    """Generate MWAA DAG Python code from a conversion plan.

    Deterministic first (Jinja2 templates). Returns LLM_FALLBACK: prompt
    only if template rendering fails.

    Args:
        conversion_plan_file: Path to *_plan.json

    Returns:
        DAG code string on success, or LLM_FALLBACK: prompt if template fails.
    """
    logger.info("generate_dag_code called with: %s", conversion_plan_file)

    plan_path = Path(conversion_plan_file)
    if not plan_path.exists():
        return f"Error: Plan file not found: {conversion_plan_file}"

    dag_id = plan_path.stem.replace("_plan", "")
    output_dir = plan_path.parent.parent / "dags"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"{dag_id}.py"

    # ── Deterministic Jinja2 template rendering ────────────────────────
    try:
        from converter.dag_builder import build_dag

        mapping_file = project_paths.MWAA_SPARK_SQL_MAPPING
        spark_sql_mapping = {}
        if mapping_file.exists():
            with open(mapping_file, encoding="utf-8") as f:
                raw = json.load(f)
            for entry in raw.get("mappings", []):
                src = entry.get("source_script", "")
                tgt = entry.get("target_script", "")
                if src and tgt:
                    spark_sql_mapping[src] = tgt

        result_path = build_dag(plan_path, output_file, spark_sql_mapping=spark_sql_mapping, cfg=_cfg)
        dag_code = result_path.read_text(encoding="utf-8")
        logger.info("✅ generate_dag_code: Jinja2 template succeeded for %s", dag_id)
        return f"DAG code saved to {result_path}\n\n{dag_code}"

    except Exception as e:
        error_msg = str(e)
        logger.warning("⚠️ generate_dag_code: template failed for %s: %s — LLM fallback", dag_id, error_msg)

    # ── LLM fallback ──────────────────────────────────────────────────
    conversion_plan = plan_path.read_text(encoding="utf-8")
    llm_prompt = load_prompt("generate_dag_code")
    formatted = llm_prompt.replace("{conversion_plan}", conversion_plan)

    return (
        f"LLM_FALLBACK:{formatted}\n\n"
        f"Template error was: {error_msg}\n"
        f"You MUST save the DAG code to: {output_file}\n"
        f'After saving, confirm: "DAG code saved to {output_file}"'
    )


# ─────────────────────────────────────────────────────────────────────────────
# Stage 7: Mermaid diagram  (LLM)
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def create_orchestration_mermaid(conversion_plan_file: str) -> str:
    """Create a Mermaid orchestration diagram from a conversion plan."""
    logger.info("create_orchestration_mermaid called with: %s", conversion_plan_file)

    plan_path = Path(conversion_plan_file)
    if not plan_path.exists():
        return f"Error: Plan file not found: {conversion_plan_file}"

    try:
        prompt = load_prompt("create_mermaid_diagram")
        conversion_plan = plan_path.read_text(encoding="utf-8")
        formatted_prompt = prompt.replace("{conversion_plan}", conversion_plan)

        dag_id = plan_path.stem.replace("_plan", "")
        output_file = plan_path.parent.parent / "mermaid" / f"{dag_id}_orchestration.md"
        output_file.parent.mkdir(parents=True, exist_ok=True)

        return (
            f"{formatted_prompt}\n\n"
            f"You MUST save the mermaid diagram to: {output_file}\n"
            f'After saving, confirm: "Mermaid diagram saved to {output_file}"'
        )

    except (OSError, ValueError, KeyError) as e:
        logger.error("Error in create_orchestration_mermaid: %s", e)
        return f"Error: {e}"


# ─────────────────────────────────────────────────────────────────────────────
# Stage 8: Report and variable merging  (LLM)
# ─────────────────────────────────────────────────────────────────────────────

@mcp.tool()
def merge_conversion_reports(report_directory: str) -> str:
    """Merge conversion reports from all chunks into a final report."""
    logger.info("merge_conversion_reports called with: %s", report_directory)

    report_dir = Path(report_directory)
    if not report_dir.exists():
        return f"Error: Report directory not found: {report_directory}"

    try:
        prompt = load_prompt("merge_reports")
        reports = [f.read_text(encoding="utf-8") for f in sorted(report_dir.glob("*_report.json"))]
        if not reports:
            return f"Error: No report files found in {report_directory}"
        chunk_reports = "\n\n".join(reports)
        formatted_prompt = prompt.replace("{chunk_reports}", chunk_reports)
        output_file = report_dir.parent / "final_conversion_report.json"

        return (
            f"{formatted_prompt}\n\n"
            f"CRITICAL: Verify job count = task count (1:1 mapping guarantee)\n"
            f"You MUST save the merged report to: {output_file}\n"
            f'After saving, confirm: "Final report saved to {output_file}"'
        )

    except (OSError, ValueError, KeyError) as e:
        logger.error("Error in merge_conversion_reports: %s", e)
        return f"Error: {e}"


@mcp.tool()
def merge_airflow_variables(variables_directory: str) -> str:
    """Merge Airflow variables from all chunks."""
    logger.info("merge_airflow_variables called with: %s", variables_directory)

    var_dir = Path(variables_directory)
    if not var_dir.exists():
        return f"Error: Variables directory not found: {variables_directory}"

    try:
        prompt = load_prompt("merge_variables")
        all_vars = [f.read_text(encoding="utf-8") for f in sorted(var_dir.glob("*_variables.json"))]
        if not all_vars:
            return f"Error: No variable files found in {variables_directory}"
        all_variables = "\n\n".join(all_vars)
        formatted_prompt = prompt.replace("{all_variables}", all_variables)
        output_file = var_dir.parent / "airflow_variables.json"

        return (
            f"{formatted_prompt}\n\n"
            f"You MUST save merged variables to: {output_file}\n"
            f'After saving, confirm: "Variables saved to {output_file}"'
        )

    except (OSError, ValueError, KeyError) as e:
        logger.error("Error in merge_airflow_variables: %s", e)
        return f"Error: {e}"


if __name__ == "__main__":
    mcp.run()

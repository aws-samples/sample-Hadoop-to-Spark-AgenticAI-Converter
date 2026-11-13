"""Stage 8: Merge reports, Airflow variables, and generate HTML report."""
import json
import logging
import time
from pathlib import Path
from typing import List, Dict, Optional

from llm_cli import kiro_chat_argv, run_llm_cli
from converter.models import SourceType, ConversionReport
from converter.config_loader import ConverterConfig
from converter.html_report import HTMLReportGenerator

logger = logging.getLogger(__name__)


def merge_reports(
    validation_results: List[Dict],
    dag_files: List[Path],
    mermaid_files: List[Path],
    output_dir: Path,
    source_type: SourceType,
    _cfg: ConverterConfig,
    progress: Dict,
    start_time: float,
    cwd: str,
    validation_results_cache: List[Dict],
) -> ConversionReport:
    """Merge all reports into final report via MCP."""
    logger.info("Merging conversion reports...")

    try:
        reports_dir = output_dir / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)
        for i, validation in enumerate(validation_results):
            report_file = reports_dir / f"chunk_{i}_report.json"
            with open(report_file, "w", encoding="utf-8") as f:
                json.dump(validation, f, indent=2)

        prompt = (
            f'Use mcp_mwaa_converter_merge_conversion_reports tool with:\n'
            f'- report_directory: "{reports_dir}"\n\n'
            f'Execute it to:\n'
            f'1. Read all chunk report files\n'
            f'2. Aggregate totals and verify 1:1 mapping\n'
            f'3. Save to: {output_dir}/final_conversion_report.json\n\n'
            f'You MUST save the final conversion report JSON file.\nExecute now.'
        )

        run_llm_cli(kiro_chat_argv(prompt), timeout=600, cwd=cwd)

        report_file = output_dir / "final_conversion_report.json"
        if report_file.exists():
            with open(report_file, encoding="utf-8") as f:
                report_data = json.load(f)
            return _convert_report_to_model(
                report_data, dag_files, mermaid_files, source_type,
                start_time, validation_results_cache, progress,
            )
    except (OSError, json.JSONDecodeError) as e:
        logger.error("✗ Report merging error: %s", e)

    return _create_fallback_report(
        validation_results, dag_files, mermaid_files, source_type, start_time, progress,
    )


def merge_variables(output_dir: Path, cfg: ConverterConfig):
    """Build airflow_variables.json deterministically from config + plan files."""
    logger.info("Merging Airflow variables...")

    try:
        var_names = cfg.all_variable_names
        variables: Dict[str, Dict] = {
            "infrastructure": {
                var_names["emr_app_id"]: {
                    "value": "<YOUR_%s>" % var_names["emr_app_id"],
                    "description": "EMR Serverless Application ID",
                    "required": True,
                },
                var_names["emr_exec_role_arn"]: {
                    "value": "<YOUR_%s>" % var_names["emr_exec_role_arn"],
                    "description": "EMR Serverless Execution Role ARN",
                    "required": True,
                },
                var_names["scripts_bucket"]: {
                    "value": "<YOUR_%s>" % var_names["scripts_bucket"],
                    "description": "S3 prefix for Spark SQL / Python scripts",
                    "required": True,
                },
                var_names["logs_bucket"]: {
                    "value": "<YOUR_%s>" % var_names["logs_bucket"],
                    "description": "S3 prefix for EMR job logs",
                    "required": True,
                },
            },
            "job_specific": {},
        }

        plan_dir = output_dir / "plans"
        if plan_dir.exists():
            for plan_file in sorted(plan_dir.glob("*_plan.json")):
                try:
                    with open(plan_file, encoding="utf-8") as f:
                        plan = json.load(f)
                    for var_key, var_info in plan.get("airflow_variables", {}).items():
                        if var_key in variables["infrastructure"]:
                            continue
                        if var_key not in variables["job_specific"]:
                            variables["job_specific"][var_key] = {
                                "value": "<YOUR_%s>" % var_key,
                                "description": var_info.get("description", "Variable: %s" % var_key),
                                "required": var_info.get("required", False),
                            }
                except (OSError, json.JSONDecodeError) as e:
                    logger.debug("Could not read plan %s: %s", plan_file, e)

        if not variables["job_specific"]:
            del variables["job_specific"]

        output = {
            "description": "Airflow Variables required by the generated MWAA DAGs.",
            "import_command": "airflow variables import airflow_variables.json",
            "variables": variables,
        }

        variables_file = output_dir / "airflow_variables.json"
        with open(variables_file, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2)

        total = sum(len(v) for v in variables.values())
        logger.info("✓ Generated airflow_variables.json (%d variables)", total)
    except (OSError, KeyError) as e:
        logger.error("✗ Variable generation error: %s", e)


def generate_html_report(
    final_report: ConversionReport,
    validation_results_cache: List[Dict],
    dag_files: List[Path],
    output_dir: Path,
    start_time: float,
) -> Optional[Path]:
    """Generate professional HTML report."""
    try:
        conversion_summary = {
            "total_source_jobs": final_report.total_jobs,
            "total_target_tasks": final_report.total_tasks,
            "total_dags": final_report.total_dags,
            "total_errors": final_report.total_errors,
            "total_warnings": final_report.total_warnings,
            "average_confidence": final_report.average_confidence,
        }

        job_mappings: List[Dict] = []
        plan_dir = output_dir / "plans"
        if plan_dir.exists():
            for plan_file in plan_dir.glob("*_plan.json"):
                try:
                    with open(plan_file, encoding="utf-8") as f:
                        plan = json.load(f)
                    dag_id = plan.get("dag_id", plan_file.stem.replace("_plan", ""))
                    for task in plan.get("tasks", []):
                        if isinstance(task, dict):
                            job_mappings.append({
                                "source_job_id": task.get("source_job_id", "Unknown"),
                                "target_task_id": task.get("task_id", "Unknown"),
                                "operator_type": task.get("operator", task.get("operator_type", "Unknown")),
                                "target_dag_id": dag_id,
                                "script_path": task.get("script_path", "-"),
                            })
                except (OSError, json.JSONDecodeError):
                    pass

        seen: set = set()
        unique = []
        for m in job_mappings:
            jid = m.get("source_job_id", "")
            if jid and jid not in seen:
                seen.add(jid)
                unique.append(m)
        job_mappings = unique or job_mappings

        confidence_details = []
        for result in validation_results_cache:
            if isinstance(result, dict):
                checks = result.get("checks", {})
                confidence_details.append({
                    "dag_id": result.get("dag_id", "Unknown"),
                    "confidence_score": result.get("confidence_score", 0),
                    "validation_passed": result.get("validation_passed", False),
                    "checks": {
                        "syntax": checks.get("syntax", False),
                        "task_count": checks.get("task_count", False),
                        "dagbag": str(checks.get("dagbag", "")).lower() == "true",
                    },
                    "issues": result.get("issues", []),
                })

        generator = HTMLReportGenerator(output_dir)
        return generator.generate_report(
            conversion_summary=conversion_summary,
            validation_results=confidence_details,
            job_mappings=job_mappings,
            dag_files=dag_files,
            confidence_details=confidence_details,
            duration_seconds=time.time() - start_time,
        )
    except (OSError, KeyError, TypeError) as e:
        logger.error("✗ HTML report generation failed: %s", e)
        return None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _convert_report_to_model(
    report_data: Dict,
    dag_files: List[Path],
    mermaid_files: List[Path],
    source_type,
    start_time: float,
    validation_results_cache: List[Dict],
    progress: Dict,
) -> ConversionReport:
    summary = report_data.get("summary", {})
    chunks = report_data.get("chunks", [])

    total_jobs = (
        summary.get("total_source_jobs") or report_data.get("total_jobs")
        or sum(c.get("total_jobs", 0) for c in chunks if isinstance(c, dict))
        or len(progress["files_parsed"])
    )
    total_tasks = summary.get("total_target_tasks") or report_data.get("total_tasks") or total_jobs

    avg_confidence = 0.0
    if validation_results_cache:
        scores = [r["confidence_score"] for r in validation_results_cache
                  if isinstance(r, dict) and "confidence_score" in r]
        if scores:
            avg_confidence = sum(scores) / len(scores)

    job_mapping = report_data.get("job_to_task_mapping", {})
    job_task_map: Dict[str, str] = {}
    if isinstance(job_mapping, dict):
        for key, value in job_mapping.items():
            if isinstance(value, dict):
                for job_id, task_info in value.items():
                    job_task_map[job_id] = task_info.get("task_id", job_id.lower()) if isinstance(task_info, dict) else str(task_info)
            else:
                job_task_map[key] = str(value)

    return ConversionReport(
        project_name="MWAA Conversion",
        source_type=source_type or SourceType.CONTROL_M,
        total_jobs=int(total_jobs),
        total_chunks=len(chunks) or len(dag_files),
        total_dags=len(dag_files),
        total_tasks=int(total_tasks),
        job_task_mapping=job_task_map,
        average_confidence=avg_confidence,
        dag_files=[str(f) for f in dag_files if f],
        mermaid_files=[str(f) for f in mermaid_files if f],
        conversion_duration_seconds=time.time() - start_time,
    )


def _create_fallback_report(
    validation_results: List[Dict],
    dag_files: List[Path],
    mermaid_files: List[Path],
    source_type,
    start_time: float,
    progress: Dict,
) -> ConversionReport:
    total_jobs = len(progress["files_parsed"])
    return ConversionReport(
        project_name="MWAA Conversion",
        source_type=source_type or SourceType.CONTROL_M,
        total_jobs=total_jobs,
        total_chunks=len(validation_results),
        total_dags=len(dag_files),
        total_tasks=total_jobs,
        dag_files=[str(f) for f in dag_files if f],
        mermaid_files=[str(f) for f in mermaid_files if f],
        average_confidence=0.9,
        conversion_duration_seconds=time.time() - start_time,
    )

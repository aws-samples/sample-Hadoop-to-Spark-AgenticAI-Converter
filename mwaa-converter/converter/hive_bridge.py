"""Stage 4.5: Bridge to HiveQL-SparkSQL converter.

Scans conversion plans for .hql scripts, stages them from
input/mwaa/hive_scripts/ into output/mwaa/hive_staging/, invokes the
converter via kiro-cli, writes the Spark SQL to output/mwaa/spark_sql/, and
auto-generates spark_sql_mapping.json from the output.

Directory locations come from the hive_conversion section of
config/converter_config.yaml (relative to the project root).
"""
import json
import logging
import shutil
import subprocess

from llm_cli import kiro_chat_argv, run_llm_cli
from pathlib import Path
from typing import List, Dict, Tuple

from converter.config_loader import ConverterConfig

logger = logging.getLogger(__name__)

# Project root: mwaa-converter/converter/hive_bridge.py -> <root>
_PROJECT_ROOT = Path(__file__).resolve().parents[2]


def convert_hive_scripts(
    plans_dir: Path,
    input_dir: Path,
    cfg: ConverterConfig,
    cwd: str,
    spark_sql_mapping_path: Path,
) -> Path:
    """Orchestrate HiveQL → Spark SQL conversion for scripts found in plans.

    Args:
        plans_dir:              Path to _debug/plans/ containing *_plan.json
        input_dir:              Control-M input dir (kept for API compatibility; the
                                .hql source folder comes from the config)
        cfg:                    ConverterConfig instance
        cwd:                    Working directory for subprocess calls
        spark_sql_mapping_path: Where to write the generated mapping JSON

    Returns:
        Path to the generated spark_sql_mapping.json
    """
    hql_source = _PROJECT_ROOT / cfg.hive_input_dir      # input/mwaa/hive_scripts
    hql_input = _PROJECT_ROOT / cfg.hive_staging_dir     # output/mwaa/hive_staging
    sql_output = _PROJECT_ROOT / cfg.spark_sql_output_dir  # output/mwaa/spark_sql

    hql_input.mkdir(parents=True, exist_ok=True)
    sql_output.mkdir(parents=True, exist_ok=True)

    # 1. Scan plans for .hql script paths
    hql_scripts = _collect_hql_paths(plans_dir)
    if not hql_scripts:
        logger.info("No .hql scripts found in plans — skipping HiveQL conversion")
        return spark_sql_mapping_path

    logger.info("Found %d .hql script(s) in plans", len(hql_scripts))

    # 2. Copy available .hql files to converter input dir
    found, missing = _stage_hql_files(hql_scripts, hql_source, hql_input)

    for path in missing:
        logger.warning(
            "⚠ HQL file not found for %s — DAG will use placeholder path", path
        )

    if not found:
        logger.warning("No .hql files available to convert — generating mapping with placeholders only")
        _write_mapping([], cfg.scripts_s3_prefix, spark_sql_mapping_path)
        return spark_sql_mapping_path

    # 3. Invoke HiveQL converter for each file (serial)
    converted = _run_conversions(found, sql_output, cwd)

    # 4. Generate spark_sql_mapping.json
    _write_mapping(converted, cfg.scripts_s3_prefix, spark_sql_mapping_path)

    logger.info(
        "✓ HiveQL conversion complete: %d converted, %d missing",
        len(converted), len(missing),
    )
    return spark_sql_mapping_path


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _collect_hql_paths(plans_dir: Path) -> List[str]:
    """Extract unique .hql script paths from all plan files."""
    hql_paths: set = set()
    for plan_file in plans_dir.glob("*_plan.json"):
        try:
            with open(plan_file, encoding="utf-8") as f:
                plan = json.load(f)
            for task in plan.get("tasks", []):
                script_path = task.get("script_path") or ""
                if script_path.lower().endswith(".hql"):
                    hql_paths.add(script_path)
        except (OSError, json.JSONDecodeError) as e:
            logger.warning("Could not read plan %s: %s", plan_file.name, e)
    return sorted(hql_paths)


def _stage_hql_files(
    hql_scripts: List[str],
    source_dir: Path,
    target_dir: Path,
) -> Tuple[List[Path], List[str]]:
    """Copy .hql files from source_dir to target_dir.

    Returns (found_files, missing_source_paths).
    """
    found: List[Path] = []
    missing: List[str] = []

    for script_path in hql_scripts:
        filename = Path(script_path).name
        source_file = source_dir / filename

        if source_file.exists():
            dest = target_dir / filename
            shutil.copy2(source_file, dest)
            found.append(dest)
            logger.info("  Staged: %s", filename)
        else:
            missing.append(script_path)

    return found, missing


def _run_conversions(
    hql_files: List[Path],
    sql_output: Path,
    cwd: str,
) -> List[Dict]:
    """Invoke the HiveQL converter for each .hql file via kiro-cli.

    Returns list of {source_script, target_file, stem} for successfully converted files.
    """
    converted: List[Dict] = []

    for hql_file in hql_files:
        stem = hql_file.stem
        logger.info("  Converting: %s", hql_file.name)

        prompt = (
            f'Use the generate_spark_sql MCP tool with:\n'
            f'- hive_file: "{hql_file}"\n\n'
            f'This returns a conversion prompt. Execute it to:\n'
            f'1. Read the HiveQL file\n'
            f'2. Convert to Spark SQL following all conversion rules\n'
            f'3. Save the converted file to: {sql_output}/{stem}.sql\n\n'
            f'CRITICAL: Save the Spark SQL file. Execute now.'
        )

        try:
            run_llm_cli(kiro_chat_argv(prompt), timeout=600, cwd=cwd)

            output_file = sql_output / f"{stem}.sql"
            if output_file.exists():
                logger.info("  ✓ Converted: %s → %s.sql", hql_file.name, stem)
                converted.append({"stem": stem, "target_file": output_file})
            else:
                logger.warning("  ⚠ Conversion produced no output: %s", hql_file.name)

        except subprocess.TimeoutExpired:
            logger.error("  ✗ Conversion timeout: %s", hql_file.name)
        except OSError as e:
            logger.error("  ✗ Conversion error: %s - %s", hql_file.name, e)

    return converted


def _write_mapping(
    converted: List[Dict],
    s3_prefix: str,
    mapping_path: Path,
):
    """Generate spark_sql_mapping.json from converted files."""
    s3_prefix = s3_prefix.rstrip("/")
    mappings = []

    for entry in converted:
        stem = entry["stem"]
        mappings.append({
            "source_script": "/scripts/%s.hql" % stem,
            "target_script": "%s/%s.sql" % (s3_prefix, stem),
            "script_type": "sql",
        })

    output = {"mappings": mappings}
    mapping_path.parent.mkdir(parents=True, exist_ok=True)
    with open(mapping_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    logger.info("  Generated %s (%d entries)", mapping_path.name, len(mappings))

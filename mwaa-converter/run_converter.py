#!/usr/bin/env python
"""MWAA Converter — standalone CLI entry point.

Converts Control-M XML exports to Amazon MWAA DAGs.
This CLI runs the pipeline directly without the orchestrator.
For the full multi-agent pipeline, use:
  python -m orchestrator.orchestrator_agent --intent mwaa_only

Pipeline stages:
  Stage 1: Parse XML  → LLM (via kiro-cli) + deterministic fallback
  Stage 2: Chunk      → deterministic (one SMART_FOLDER = one chunk)
  Stage 3: Plan       → LLM (via kiro-cli) + deterministic fallback
  Stage 4: Build DAGs → deterministic (Jinja2 templates, LLM fallback)
  Stage 5: Validate   → deterministic (AST + regex + DagBag)
  Stage 6: Mermaid    → LLM (via kiro-cli)
  Stage 7: Reports    → deterministic (merger.py)
"""
import json
import logging
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Dict

import click

# Shared input/output locations (see project_paths.py at the repo root)
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import project_paths

from converter.config_loader import load_config, ConverterConfig
from converter import parser, chunker, planner, validator, mermaid_gen, merger, hive_bridge

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
log_dir = project_paths.LOG_DIR
log_dir.mkdir(parents=True, exist_ok=True)
log_file = log_dir / ("converter_%s.log" % datetime.now().strftime("%Y%m%d_%H%M%S"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(log_file), logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

progress_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------
class MWAAConverter:
    """Orchestrates the conversion pipeline for Control-M XML → MWAA DAGs."""

    def __init__(
        self,
        input_dir: str,
        output_dir: str,
        spark_sql_mapping: str = None,
        max_workers: int = 4,
        convert_hive: bool = False,
    ):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.spark_sql_mapping = (
            Path(spark_sql_mapping)
            if spark_sql_mapping
            else project_paths.MWAA_SPARK_SQL_MAPPING
        )
        self.cfg = ConverterConfig(load_config())
        self.max_workers = max_workers if max_workers != 4 else self.cfg.max_workers
        self.convert_hive = convert_hive or self.cfg.hive_conversion_enabled
        self.cwd = str(Path(__file__).parent)
        self.start_time = time.time()
        self.validation_results_cache: list = []

        # Create output directories
        for d in ("dags", "mermaid", "job_ir", "plans", "validation", "_debug/reports"):
            (self.output_dir / d).mkdir(parents=True, exist_ok=True)

        self.progress = self._load_progress()

    # -- progress helpers --------------------------------------------------

    def _load_progress(self) -> Dict:
        pf = self.output_dir / "progress.json"
        if pf.exists():
            try:
                with open(pf, encoding="utf-8") as f:
                    return json.load(f)
            except (json.JSONDecodeError, OSError):
                pass
        return {
            "files_parsed": [],
            "plans_generated": [],
            "dags_generated": [],
            "dags_validated": [],
            "mermaid_created": [],
        }

    def _save_progress(self):
        with progress_lock:
            with open(self.output_dir / "progress.json", "w", encoding="utf-8") as f:
                json.dump(self.progress, f, indent=2)

    # -- main pipeline -----------------------------------------------------

    def run(self):
        """Run complete conversion pipeline."""
        logger.info("=" * 70)
        logger.info("MWAA Converter — Control-M XML → Amazon MWAA")
        logger.info("=" * 70)
        logger.info("Input:  %s", self.input_dir)
        logger.info("Output: %s", self.output_dir)

        # ── Stage 1: Parse XML ────────────────────────────────────────────
        logger.info("\n[Stage 1] Parsing Control-M XML files (LLM + fallback)...")
        # Use deterministic fallback directly for CLI mode (no kiro-cli subprocess)
        from converter.parser import _deterministic_fallback, _safe_filename

        job_ir_dir = self.output_dir / "job_ir"
        job_ir_dir.mkdir(parents=True, exist_ok=True)

        xml_files = sorted(self.input_dir.glob("*.xml"))
        if not xml_files:
            logger.error("No XML files found in %s", self.input_dir)
            return

        parsed_files = []
        for xml_file in xml_files:
            file_key = str(xml_file)
            if file_key in self.progress.get("files_parsed", []):
                logger.info("[SKIP] Already parsed: %s", xml_file.name)
                parsed_files.append(xml_file)
                continue
            written = _deterministic_fallback(xml_file, job_ir_dir)
            if written:
                parsed_files.append(xml_file)
                self.progress.setdefault("files_parsed", []).append(file_key)
                self._save_progress()
                logger.info("✓ Parsed: %s (%d folder(s))", xml_file.name, len(written))
            else:
                logger.error("✗ Failed to parse: %s", xml_file.name)

        if not parsed_files:
            logger.error("No XML files parsed. Check input directory: %s", self.input_dir)
            return
        logger.info("✓ Parsed %d XML file(s)", len(parsed_files))

        # ── Stage 2: Chunk by SMART_FOLDER ───────────────────────────────
        logger.info("\n[Stage 2] Grouping jobs by SMART_FOLDER...")
        chunks = chunker.create_chunks(job_ir_dir=self.output_dir / "job_ir")
        if not chunks:
            logger.error("No chunks created. Check JobIR output.")
            return
        logger.info("✓ Created %d chunk(s)", len(chunks))

        # ── Stage 3: Generate conversion plans ───────────────────────────
        logger.info("\n[Stage 3] Generating conversion plans (deterministic)...")
        from converter.planner import _deterministic_fallback_plan, _load_spark_sql_mapping
        import json as _json

        plans_dir = self.output_dir / "plans"
        plans_dir.mkdir(parents=True, exist_ok=True)
        spark_sql_mapping_data = _load_spark_sql_mapping(self.spark_sql_mapping)

        plans = []
        for chunk in chunks:
            chunk_id = chunk["chunk_id"]
            if chunk_id in self.progress.get("plans_generated", []):
                plan_file = plans_dir / f"{chunk_id}_plan.json"
                if plan_file.exists():
                    with open(plan_file, encoding="utf-8") as f:
                        plans.append(_json.load(f))
                    logger.info("[SKIP] Already generated plan: %s", chunk_id)
                    continue

            plan = _deterministic_fallback_plan(chunk, self.spark_sql_mapping, self.cfg)
            plan_file = plans_dir / f"{chunk_id}_plan.json"
            with open(plan_file, "w", encoding="utf-8") as f:
                _json.dump(plan, f, indent=2)
            plans.append(plan)
            self.progress.setdefault("plans_generated", []).append(chunk_id)
            self._save_progress()
            logger.info("✓ Plan: %s (%d tasks)", chunk_id, plan["total_tasks"])

        if not plans:
            logger.error("No plans generated.")
            return
        logger.info("✓ Generated %d plan(s)", len(plans))

        # ── Stage 3.5 (optional): HiveQL → Spark SQL ─────────────────────
        if self.convert_hive:
            logger.info("\n[Stage 3.5] Converting HiveQL scripts to Spark SQL...")
            self.spark_sql_mapping = hive_bridge.convert_hive_scripts(
                plans_dir=self.output_dir / "plans",
                input_dir=self.input_dir,
                cfg=self.cfg,
                cwd=self.cwd,
                spark_sql_mapping_path=self.spark_sql_mapping,
            )

        # ── Stage 4: Generate DAG code ────────────────────────────────────
        logger.info("\n[Stage 4] Generating MWAA DAG code...")
        dag_files = validator.generate_dags_parallel(
            plans=plans,
            output_dir=self.output_dir,
            spark_sql_mapping=self.spark_sql_mapping,
            cfg=self.cfg,
            progress=self.progress,
            save_progress_fn=self._save_progress,
            cwd=self.cwd,
            max_workers=self.max_workers,
        )
        logger.info("✓ Generated %d DAG(s)", len(dag_files))

        # ── Stage 5: Validate DAGs ────────────────────────────────────────
        logger.info("\n[Stage 5] Validating DAGs...")
        validation_results = validator.validate_dags_parallel(
            dag_files=dag_files,
            plans=plans,
            output_dir=self.output_dir,
            cfg=self.cfg,
            progress=self.progress,
            save_progress_fn=self._save_progress,
            max_workers=self.max_workers,
        )
        self.validation_results_cache = validation_results
        passed = sum(1 for r in validation_results if r and r.get("validation_passed"))
        logger.info("✓ Validated %d DAG(s) — %d passed", len(validation_results), passed)

        # ── Stage 6: Mermaid diagrams ─────────────────────────────────────
        if self.cfg.generate_mermaid:
            logger.info("\n[Stage 6] Creating orchestration diagrams...")
            mermaid_files = mermaid_gen.create_mermaid_parallel(
                plans=plans,
                output_dir=self.output_dir,
                progress=self.progress,
                save_progress_fn=self._save_progress,
                cwd=self.cwd,
                max_workers=self.max_workers,
            )
            logger.info("✓ Created %d diagram(s)", len(mermaid_files))
        else:
            mermaid_files = []

        # ── Stage 7: Reports and variables ───────────────────────────────
        logger.info("\n[Stage 7] Generating reports and Airflow variables...")
        final_report = merger.merge_reports(
            validation_results=validation_results,
            dag_files=dag_files,
            mermaid_files=mermaid_files,
            output_dir=self.output_dir,
            source_type=None,
            _cfg=self.cfg,
            progress=self.progress,
            start_time=self.start_time,
            cwd=self.cwd,
            validation_results_cache=self.validation_results_cache,
        )
        merger.merge_variables(self.output_dir, self.cfg)

        html_path = None
        if self.cfg.generate_html_report:
            html_path = merger.generate_html_report(
                final_report=final_report,
                validation_results_cache=self.validation_results_cache,
                dag_files=dag_files,
                output_dir=self.output_dir,
                start_time=self.start_time,
            )

        # ── Summary ───────────────────────────────────────────────────────
        duration = time.time() - self.start_time
        one_to_one = "✓" if final_report.total_jobs == final_report.total_tasks else "✗"

        logger.info("\n%s", "=" * 70)
        logger.info("CONVERSION COMPLETE")
        logger.info("=" * 70)
        logger.info("Source Jobs:  %d", final_report.total_jobs)
        logger.info("Tasks:        %d  (1:1: %s)", final_report.total_tasks, one_to_one)
        logger.info("DAGs:         %d", final_report.total_dags)
        logger.info("Confidence:   %.0f%%", final_report.average_confidence * 100)
        logger.info("Duration:     %.1fs", duration)
        logger.info("\nOutputs:")
        logger.info("  DAGs:       %s/dags/", self.output_dir)
        logger.info("  Variables:  %s/airflow_variables.json", self.output_dir)
        logger.info("  Diagrams:   %s/mermaid/", self.output_dir)
        if html_path:
            logger.info("  HTML:       %s", html_path)
        logger.info("  Log:        %s", log_file)
        logger.info("=" * 70)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
@click.command()
@click.option("--input-dir", default=str(project_paths.MWAA_CONTROL_M_INPUT), show_default=True,
              help="Input directory containing Control-M XML files")
@click.option("--output-dir", default=str(project_paths.MWAA_OUTPUT), show_default=True,
              help="Output directory for generated DAGs and reports")
@click.option("--spark-sql-mapping", default=None,
              help="Path to spark_sql_mapping.json (default: input/mwaa/spark_sql_mapping.json)")
@click.option("--max-workers", default=4, help="Maximum parallel workers")
@click.option("--convert-hive", "--convert-hiveql", "convert_hive", is_flag=True, default=False,
              help="Convert .hql scripts referenced in jobs to Spark SQL before DAG generation "
                   "(--convert-hiveql is a deprecated alias)")
def main(input_dir, output_dir, spark_sql_mapping, max_workers, convert_hive):
    """MWAA Converter — Convert Control-M XML to Amazon MWAA DAGs."""
    try:
        conv = MWAAConverter(
            input_dir=input_dir,
            output_dir=output_dir,
            spark_sql_mapping=spark_sql_mapping,
            max_workers=max_workers,
            convert_hive=convert_hive,
        )
        conv.run()
    except KeyboardInterrupt:
        logger.info("\nInterrupted by user")
        sys.exit(1)
    except (OSError, ValueError) as e:
        logger.error("\nConversion failed: %s", e)
        sys.exit(1)


if __name__ == "__main__":
    main()  # pylint: disable=no-value-for-parameter

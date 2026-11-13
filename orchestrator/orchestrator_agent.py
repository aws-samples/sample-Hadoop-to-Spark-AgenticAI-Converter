"""Unified Orchestrator Agent — coordinates all agents via native MCP calls.

All inputs are read from input/ and all results are written to output/
(see project_paths.py for the full layout).

Usage:
    python -m orchestrator.orchestrator_agent --intent full_pipeline            # hive + impala + mwaa
    python -m orchestrator.orchestrator_agent --intent hive_only                 # input/hive/
    python -m orchestrator.orchestrator_agent --intent hive_only --input some/dir/
    python -m orchestrator.orchestrator_agent --intent mwaa_only                   # input/mwaa/control_m/
    python -m orchestrator.orchestrator_agent --intent mwaa_only --source-type airflow   # input/mwaa/airflow/
"""
import asyncio
import json
import logging
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import project_paths as P
from orchestrator.logging_config import setup_logging, current_run_id, current_agent, TaskTimer
from orchestrator.llm_dispatch import (
    needs_llm, needs_llm_fallback, extract_fallback_prompt,
    execute_via_llm, DEFAULT_BACKEND,
)
from orchestrator.mcp_client import MCPClientPool
from orchestrator.models import (
    AgentName, AgentRequest, AgentResponse, RunStatus, TaskStatus,
)
from orchestrator.parallel import run_parallel, run_pipeline_per_file
from orchestrator.self_correction import SelfCorrectionLoop
from orchestrator.state_store import StateStore

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent.parent.resolve()

# Deprecated intent names, still accepted and mapped to their current name
LEGACY_INTENTS = {"hiveql_only": "hive_only"}

# Map agent names to MCP server names (from mcp.json)
AGENT_SERVER_MAP = {
    AgentName.CODE_CONVERTER: "hive-converter",
    AgentName.HIVE_EVAL: "hive-eval",
    AgentName.WORKFLOW_CONVERTER: "mwaa-converter",
    AgentName.WORKFLOW_VALIDATOR: "mwaa-validator",
    AgentName.ACCURACY_CALCULATOR: "accuracy-calculator",
    AgentName.TEST_ORCHESTRATOR: "test-orchestrator",
    AgentName.TEST_EXECUTION: "test-execution",
    AgentName.IMPALA_CONVERTER: "impala-converter",
}


class OrchestratorAgent:
    """Central coordinator that dispatches tasks to agents via MCP."""

    def __init__(
        self,
        mcp_json: str | Path = ".kiro/settings/mcp.json",
        state_dir: str | Path = P.STATE_DIR,
        backend: str = DEFAULT_BACKEND,
    ):
        self._mcp_json = PROJECT_ROOT / mcp_json
        self._pool: Optional[MCPClientPool] = None
        P.ensure_dirs()
        self._store = StateStore(state_dir)
        self._corrector = SelfCorrectionLoop()
        self._backend = backend
        logger.info("LLM dispatch backend: %s", backend)

    async def __aenter__(self):
        self._pool = MCPClientPool.from_mcp_json(self._mcp_json)
        await self._pool.__aenter__()
        return self

    async def __aexit__(self, *exc):
        if self._pool:
            await self._pool.__aexit__(*exc)

    # -- public API --------------------------------------------------------

    async def run(self, intent: str, **kwargs) -> str:
        """Execute a high-level intent. Returns run_id."""
        resume_run_id = kwargs.pop("resume_run_id", None)
        if intent in LEGACY_INTENTS:
            logger.warning("Intent %r is deprecated -- use %r", intent, LEGACY_INTENTS[intent])
            intent = LEGACY_INTENTS[intent]

        if resume_run_id:
            # Resume mode: load previous run state, keep completed tasks
            loaded = self._store.load_run(resume_run_id)
            if not loaded:
                logger.error("Cannot resume — run %s not found in %s", resume_run_id, P.rel(P.STATE_DIR))
                return resume_run_id
            run_id = resume_run_id
            setup_logging(log_dir=P.LOG_DIR, run_id=run_id)
            self._store._state.status = RunStatus.RUNNING
            self._store._state.completed_at = None
            completed = [tid for tid, t in self._store.state.tasks.items()
                         if t.status == TaskStatus.SUCCESS]
            skipped = [tid for tid, t in self._store.state.tasks.items()
                       if t.status == TaskStatus.SKIPPED]
            logger.info("=== RESUMING Run: %s — intent: %s ===", run_id, intent)
            logger.info("   Previously completed: %d tasks, skipped: %d tasks",
                        len(completed), len(skipped))
        else:
            # Fresh run
            run_id = f"run_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
            self._store.new_run(run_id, intent)
            setup_logging(log_dir=P.LOG_DIR, run_id=run_id)
            logger.info("=== Orchestrator Run: %s — intent: %s ===", run_id, intent)

        try:
            if intent == "hive_only":
                await self._run_hive_pipeline(kwargs.get("input_dir") or str(P.HIVE_INPUT))
            elif intent == "impala_only":
                await self._run_impala_pipeline(kwargs.get("input_dir") or str(P.IMPALA_INPUT))
            elif intent == "mwaa_only":
                await self._run_mwaa_pipeline(
                    kwargs.get("input_dir") or str(self._default_mwaa_input(kwargs)), kwargs)
            elif intent == "full_pipeline":
                await self._run_full_pipeline(kwargs)
            elif intent == "accuracy_only":
                await self._run_accuracy(kwargs)
            elif intent == "test_only":
                await self._run_test_pipeline(kwargs)
            else:
                logger.error("Unknown intent: %s", intent)
                self._store.complete_run(RunStatus.FAILED)
                return run_id

            # Determine final status
            failed = any(
                a.status == TaskStatus.FAILED
                for a in self._store.state.agents.values()
            )
            self._store.complete_run(RunStatus.PARTIAL if failed else RunStatus.COMPLETED)

        except Exception as e:
            logger.error("Run failed: %s", e, exc_info=True)
            self._store.complete_run(RunStatus.FAILED)

        self._print_summary()
        return run_id

    # -- pipelines ---------------------------------------------------------

    # Large file threshold — files above this line count use chunk→convert→merge
    CHUNK_LINE_THRESHOLD = 1000

    @staticmethod
    def _default_mwaa_input(kwargs: Dict) -> Path:
        """Default MWAA input folder for the selected source type."""
        if kwargs.get("source_type", "control_m") == "airflow":
            return P.MWAA_AIRFLOW_INPUT
        return P.MWAA_CONTROL_M_INPUT

    @staticmethod
    def _resolve_dir(path: str | Path) -> Path:
        """Resolve a user-supplied directory (absolute, or relative to the project root)."""
        return PROJECT_ROOT / path

    async def _generate_accuracy_dashboard(
        self, source_type: str, stems: List[str], task_id: str,
        task_id_fmt: str = "{stem}_accuracy",
    ) -> None:
        """Write per-file metrics JSON and build the dashboard for one SQL dialect.

        Reads each accuracy task result (``task_id_fmt``) from the run state and
        writes everything under output/<source_type>/accuracy/.
        """
        report_dir = P.accuracy_dir(source_type)
        report_dir.mkdir(parents=True, exist_ok=True)
        metrics_files = []

        for stem in stems:
            task = self._store.get_task(task_id_fmt.format(source_type=source_type, stem=stem))
            if task and task.status == TaskStatus.SUCCESS and task.result:
                try:
                    metrics = json.loads(task.result) if isinstance(task.result, str) else task.result
                    if isinstance(metrics, dict) and "accuracy_percentage" in metrics:
                        metrics["file"] = stem
                        metrics_file = report_dir / f"{stem}_metrics.json"
                        metrics_file.write_text(json.dumps(metrics, indent=2, default=str))
                        metrics_files.append(str(metrics_file))
                except (json.JSONDecodeError, TypeError):
                    pass

        if metrics_files:
            logger.info("📊 Generating %s accuracy dashboard from %d file(s)",
                        source_type, len(metrics_files))
            await self._dispatch(AgentRequest(
                task_id=task_id,
                agent=AgentName.ACCURACY_CALCULATOR,
                mcp_server="accuracy-calculator",
                tool="generate_dashboard",
                arguments={
                    "metrics_json_files": metrics_files,
                    "output_dir": str(report_dir),
                },
            ))

    async def _run_hive_pipeline(self, input_dir: str):
        """HiveQL → Spark SQL conversion pipeline (parallel across files).

        Reads from input/hive/, writes to output/hive/ (spark_sql, chunks,
        mermaid, accuracy, validation).
        """
        self._store.init_agent(AgentName.CODE_CONVERTER)
        self._store.init_agent(AgentName.HIVE_EVAL)
        self._store.init_agent(AgentName.ACCURACY_CALCULATOR)

        input_path = self._resolve_dir(input_dir)
        hql_files = list(input_path.glob("*.hql")) + list(input_path.glob("*.txt"))
        if not hql_files:
            logger.warning("No HiveQL files found in %s", input_path)
            return

        # Phase 1: Analyze all files to determine which need chunking
        analyze_requests = [
            AgentRequest(
                task_id=f"{f.stem}_analyze",
                agent=AgentName.CODE_CONVERTER,
                mcp_server="hive-converter",
                tool="identify_hive_file",
                arguments={"file_path": str(f.resolve())},
            )
            for f in hql_files
        ]
        await run_parallel(self._dispatch, analyze_requests, max_concurrent=len(hql_files))

        # Phase 2: Build per-file pipelines based on analysis results
        small_pipelines = []
        large_files = []

        for hql_file in hql_files:
            stem = hql_file.stem
            abs_path = str(hql_file.resolve())
            line_count = self._get_line_count(stem)

            if line_count > self.CHUNK_LINE_THRESHOLD:
                logger.info("📦 %s has %d lines (>%d) — using chunk→convert→merge pipeline",
                            stem, line_count, self.CHUNK_LINE_THRESHOLD)
                large_files.append(hql_file)
            else:
                output_sql = str(P.spark_sql_dir("hive") / f"{stem}.sql")
                small_pipelines.append(self._build_small_file_pipeline(stem, abs_path, output_sql))

        # Phase 3a: Run small file pipelines in parallel
        if small_pipelines:
            logger.info("Processing %d small HQL files in parallel", len(small_pipelines))
            await run_pipeline_per_file(self._dispatch, small_pipelines, max_parallel_files=3)

        # Phase 3b: Run large file pipelines (chunk→convert→merge) sequentially
        for hql_file in large_files:
            await self._run_chunked_pipeline(hql_file)

        # Phase 4: Generate accuracy dashboard from all per-file accuracy results
        await self._generate_accuracy_dashboard(
            "hive", [f.stem for f in hql_files], "hive_generate_dashboard")

        self._store.update_agent(AgentName.CODE_CONVERTER, status=TaskStatus.SUCCESS)

    def _get_line_count(self, stem: str) -> int:
        """Extract line count from a completed analyze task result."""
        task = self._store.get_task(f"{stem}_analyze")
        if task and task.status == TaskStatus.SUCCESS:
            for line in task.result.splitlines():
                if "Total Lines:" in line:
                    try:
                        return int(line.split(":")[-1].strip())
                    except ValueError:
                        pass
        return 0

    def _build_small_file_pipeline(self, stem: str, abs_path: str, output_sql: str) -> list:
        """Build the standard pipeline for small files."""
        return [
            AgentRequest(
                task_id=f"{stem}_convert",
                agent=AgentName.CODE_CONVERTER,
                mcp_server="hive-converter",
                tool="generate_spark_sql",
                arguments={"hive_file": abs_path},
                depends_on=[f"{stem}_analyze"],
            ),
            AgentRequest(
                task_id=f"{stem}_validate",
                agent=AgentName.CODE_CONVERTER,
                mcp_server="hive-converter",
                tool="validate_spark_sql",
                arguments={"sql_file": output_sql},
                depends_on=[f"{stem}_convert"],
            ),
            AgentRequest(
                task_id=f"{stem}_execute",
                agent=AgentName.HIVE_EVAL,
                mcp_server="hive-eval",
                tool="execute_spark_sql",
                arguments={"sql_file": output_sql},
                depends_on=[f"{stem}_validate"],
            ),
            AgentRequest(
                task_id=f"{stem}_accuracy",
                agent=AgentName.ACCURACY_CALCULATOR,
                mcp_server="accuracy-calculator",
                tool="analyze_accuracy",
                arguments={"spark_sql_file": output_sql, "source_file": abs_path},
                depends_on=[f"{stem}_convert"],
            ),
            AgentRequest(
                task_id=f"{stem}_mermaid",
                agent=AgentName.CODE_CONVERTER,
                mcp_server="hive-converter",
                tool="create_detailed_mermaid",
                arguments={"hive_file": abs_path},
                depends_on=[f"{stem}_analyze"],
            ),
        ]

    async def _run_chunked_pipeline(self, hql_file: Path):
        """Chunk→Convert→Merge pipeline for large HiveQL files."""
        stem = hql_file.stem
        abs_path = str(hql_file.resolve())
        chunk_dir = str(P.chunks_dir("hive") / stem)
        output_sql = str(P.spark_sql_dir("hive") / f"{stem}.sql")

        # Step 1: Chunk the file
        logger.info("📦 Chunking %s ...", stem)
        chunk_resp = await self._dispatch(AgentRequest(
            task_id=f"{stem}_chunk",
            agent=AgentName.CODE_CONVERTER,
            mcp_server="hive-converter",
            tool="chunk_hive_file",
            arguments={"file_path": abs_path},
            timeout=900,
        ))
        if chunk_resp.status == TaskStatus.FAILED:
            logger.error("Chunking failed for %s", stem)
            return

        # Step 2: Discover chunk files
        chunk_path = P.chunks_dir("hive") / stem
        chunk_files = sorted(chunk_path.glob("*.hql")) if chunk_path.exists() else []
        if not chunk_files:
            logger.warning("No chunks found in %s — falling back to direct conversion", chunk_dir)
            pipeline = self._build_small_file_pipeline(stem, abs_path, output_sql)
            for req in pipeline:
                req.timeout = 900
            await run_pipeline_per_file(self._dispatch, [pipeline], max_parallel_files=1)
            return

        logger.info("📦 Found %d chunks for %s", len(chunk_files), stem)

        # Step 3: Convert each chunk in parallel
        convert_requests = [
            AgentRequest(
                task_id=f"{stem}_convert_chunk_{i+1:03d}",
                agent=AgentName.CODE_CONVERTER,
                mcp_server="hive-converter",
                tool="generate_spark_sql",
                arguments={"hive_file": str(cf.resolve())},
                timeout=900,
            )
            for i, cf in enumerate(chunk_files)
        ]
        await run_parallel(self._dispatch, convert_requests, max_concurrent=2)

        # Check if all chunks converted
        all_ok = all(
            self._store.get_task(r.task_id) and self._store.get_task(r.task_id).status == TaskStatus.SUCCESS
            for r in convert_requests
        )
        if not all_ok:
            logger.error("Some chunks failed to convert for %s", stem)
            return

        # Step 4: Merge chunks
        spark_chunk_dir = str(P.spark_sql_dir("hive") / stem)
        merge_resp = await self._dispatch(AgentRequest(
            task_id=f"{stem}_merge",
            agent=AgentName.CODE_CONVERTER,
            mcp_server="hive-converter",
            tool="merge_spark_sql_chunks",
            arguments={"chunk_directory": spark_chunk_dir},
            timeout=900,
        ))
        if merge_resp.status == TaskStatus.FAILED:
            logger.error("Merge failed for %s", stem)
            return

        # Step 5: Validate + Execute + Accuracy on the merged file
        final_sql = str(P.spark_sql_dir("hive") / f"{stem}_FINAL.sql")
        # Use _FINAL.sql if it exists, otherwise try {stem}.sql
        if not Path(final_sql).exists():
            final_sql = output_sql

        post_pipeline = [
            AgentRequest(
                task_id=f"{stem}_validate",
                agent=AgentName.CODE_CONVERTER,
                mcp_server="hive-converter",
                tool="validate_spark_sql",
                arguments={"sql_file": final_sql},
                timeout=900,
            ),
            AgentRequest(
                task_id=f"{stem}_execute",
                agent=AgentName.HIVE_EVAL,
                mcp_server="hive-eval",
                tool="execute_spark_sql",
                arguments={"sql_file": final_sql},
                depends_on=[f"{stem}_validate"],
                timeout=900,
            ),
            AgentRequest(
                task_id=f"{stem}_accuracy",
                agent=AgentName.ACCURACY_CALCULATOR,
                mcp_server="accuracy-calculator",
                tool="analyze_accuracy",
                arguments={"spark_sql_file": final_sql, "source_file": abs_path},
                depends_on=[f"{stem}_validate"],
            ),
        ]
        for req in post_pipeline:
            await self._dispatch(req)

        # Mermaid diagram (runs independently, depends only on analyze)
        await self._dispatch(AgentRequest(
            task_id=f"{stem}_mermaid",
            agent=AgentName.CODE_CONVERTER,
            mcp_server="hive-converter",
            tool="create_detailed_mermaid",
            arguments={"hive_file": abs_path},
            timeout=900,
        ))

    async def _run_mwaa_pipeline(self, input_dir: str, kwargs: Dict):
        """Control-M XML or on-prem Airflow DAG → MWAA conversion pipeline.

        source_type="control_m" (default): scans for *.xml files
        source_type="airflow":             scans for *.py files

        Both paths share the same plan → DAG build → validate stages.
        """
        source_type = kwargs.get("source_type", "control_m")

        if source_type == "airflow":
            await self._run_airflow_to_mwaa_pipeline(input_dir)
        else:
            await self._run_control_m_to_mwaa_pipeline(input_dir)

    async def _run_control_m_to_mwaa_pipeline(self, input_dir: str):
        """Control-M XML → MWAA conversion pipeline.

        Parallel processing:
        - All N XML files parsed in parallel (max 5 concurrent LLM calls)
        - All SMART_FOLDERs planned/built/validated in parallel (max 5 concurrent)
        - Output directories cleared at start so only this run's files are processed
        - HTML report generated at end covering all DAGs from all XML files
        """
        import shutil
        _pipeline_start = time.time()
        self._store.init_agent(AgentName.WORKFLOW_CONVERTER)
        self._store.init_agent(AgentName.WORKFLOW_VALIDATOR)

        input_path = self._resolve_dir(input_dir)
        xml_files = sorted(input_path.glob("*.xml"))

        if not xml_files:
            logger.warning("No XML files found in %s", input_path)
            return

        output_base = P.MWAA_OUTPUT
        spark_sql_mapping = str(P.MWAA_SPARK_SQL_MAPPING)

        # ── Clean output directories ──────────────────────────────────────
        # Ensures only THIS run's files are processed — no stale files from
        # previous runs mixed in. Safe because this runs before any parallel work.
        for clean_dir in ["job_ir", "plans", "dags", "validation"]:
            shutil.rmtree(output_base / clean_dir, ignore_errors=True)
            (output_base / clean_dir).mkdir(parents=True, exist_ok=True)
        logger.info("Cleaned output directories — processing %d XML file(s)", len(xml_files))

        # ── Stage 1: Parse ALL XML files in parallel ──────────────────────
        # Each XML file parsed independently by LLM (max 5 concurrent).
        # await blocks until ALL N files are fully parsed before continuing.
        parse_pipelines = []
        for sf in xml_files:
            parse_pipelines.append([
                AgentRequest(
                    task_id=f"parse_{sf.stem}",
                    agent=AgentName.WORKFLOW_CONVERTER,
                    mcp_server="mwaa-converter",
                    tool="parse_control_m_job",
                    arguments={"job_file": str(sf.resolve())},
                ),
                AgentRequest(
                    task_id=f"validate_parse_{sf.stem}",
                    agent=AgentName.WORKFLOW_CONVERTER,
                    mcp_server="mwaa-converter",
                    tool="validate_parsed_job_ir",
                    arguments={"job_file": str(sf.resolve())},
                    depends_on=[f"parse_{sf.stem}"],
                ),
            ])

        logger.info("Parsing %d XML file(s) in parallel (max 5 concurrent LLM calls)", len(xml_files))
        await run_pipeline_per_file(self._dispatch, parse_pipelines, max_parallel_files=5)

        # ── Discover SMART_FOLDERs from this run ──────────────────────────
        # Since we cleaned job_ir/ before parsing, every file here is from this run.
        # One JSON per SMART_FOLDER — one XML may produce multiple SMART_FOLDERs.
        job_ir_dir = output_base / "job_ir"
        job_ir_files = sorted(job_ir_dir.glob("*.json")) if job_ir_dir.exists() else []
        if not job_ir_files:
            logger.error("No JobIR files found after parsing — check XML input")
            return

        logger.info("Found %d SMART_FOLDER(s) across %d XML file(s) — converting to DAGs",
                    len(job_ir_files), len(xml_files))

        # ── Stages 3-6: Convert ALL SMART_FOLDERs in parallel ────────────
        # Each SMART_FOLDER gets its own pipeline: plan → validate → build → validate×4
        # All pipelines run in parallel (max 5 concurrent).
        # No depends_on needed here — await above already guarantees all parsing is done.
        file_pipelines = []
        for job_ir_file in job_ir_files:
            chunk_id = job_ir_file.stem
            jobs_file = str(job_ir_file.resolve())
            plan_file = str((output_base / "plans" / f"{chunk_id}_plan.json").resolve())
            dag_file = str((output_base / "dags" / f"{chunk_id}.py").resolve())

            pipeline = [
                AgentRequest(
                    task_id=f"plan_{chunk_id}",
                    agent=AgentName.WORKFLOW_CONVERTER,
                    mcp_server="mwaa-converter",
                    tool="generate_conversion_plan",
                    arguments={
                        "chunk_id": chunk_id,
                        "jobs_file": jobs_file,
                        "spark_sql_mapping_file": spark_sql_mapping,
                    },
                ),
                AgentRequest(
                    task_id=f"validate_plan_{chunk_id}",
                    agent=AgentName.WORKFLOW_CONVERTER,
                    mcp_server="mwaa-converter",
                    tool="validate_conversion_plan",
                    arguments={
                        "chunk_id": chunk_id,
                        "jobs_file": jobs_file,
                        "spark_sql_mapping_file": spark_sql_mapping,
                    },
                    depends_on=[f"plan_{chunk_id}"],
                ),
                AgentRequest(
                    task_id=f"dag_{chunk_id}",
                    agent=AgentName.WORKFLOW_CONVERTER,
                    mcp_server="mwaa-converter",
                    tool="generate_dag_code",
                    arguments={"conversion_plan_file": plan_file},
                    depends_on=[f"validate_plan_{chunk_id}"],
                ),
                AgentRequest(
                    task_id=f"validate_syntax_{chunk_id}",
                    agent=AgentName.WORKFLOW_VALIDATOR,
                    mcp_server="mwaa-validator",
                    tool="validate_syntax",
                    arguments={"dag_file": dag_file},
                    depends_on=[f"dag_{chunk_id}"],
                ),
                AgentRequest(
                    task_id=f"validate_structure_{chunk_id}",
                    agent=AgentName.WORKFLOW_VALIDATOR,
                    mcp_server="mwaa-validator",
                    tool="validate_structure",
                    arguments={
                        "dag_file": dag_file,
                        "conversion_plan_file": plan_file,
                    },
                    depends_on=[f"validate_syntax_{chunk_id}"],
                ),
                AgentRequest(
                    task_id=f"validate_security_{chunk_id}",
                    agent=AgentName.WORKFLOW_VALIDATOR,
                    mcp_server="mwaa-validator",
                    tool="validate_security",
                    arguments={"dag_file": dag_file},
                    depends_on=[f"validate_syntax_{chunk_id}"],
                ),
                AgentRequest(
                    task_id=f"validate_dagbag_{chunk_id}",
                    agent=AgentName.WORKFLOW_VALIDATOR,
                    mcp_server="mwaa-validator",
                    tool="validate_dagbag",
                    arguments={"dag_file": dag_file},
                    depends_on=[f"validate_structure_{chunk_id}"],
                ),
            ]
            file_pipelines.append(pipeline)

        logger.info("Converting %d SMART_FOLDER(s) in parallel (max 5 concurrent)", len(file_pipelines))
        await run_pipeline_per_file(self._dispatch, file_pipelines, max_parallel_files=5)

        self._store.update_agent(AgentName.WORKFLOW_CONVERTER, status=TaskStatus.SUCCESS)
        self._store.update_agent(AgentName.WORKFLOW_VALIDATOR, status=TaskStatus.SUCCESS)

        # ── Consolidated HTML report ──────────────────────────────────────
        # Reads all plan + validation files — one report covering ALL DAGs
        # from ALL input XML files, each with its own sidebar entry and detail panel.
        self._generate_mwaa_html_report(output_base, start_time=_pipeline_start)

    def _generate_mwaa_html_report(self, output_base: Path, start_time: float = None):
        """Generate the HTML conversion report after pipeline completes."""
        import time as _time
        duration = (_time.time() - start_time) if start_time else 0
        try:
            import sys
            sys.path.insert(0, str(PROJECT_ROOT / "mwaa-converter"))
            from converter.html_report import HTMLReportGenerator
            generator = HTMLReportGenerator(output_base)
            report_path = generator.generate_report(
                conversion_summary={},
                validation_results=[],
                job_mappings=[],
                dag_files=sorted(output_base.glob("dags/*.py")),
                confidence_details=[],
                duration_seconds=duration,
            )
            logger.info("📊 Conversion report: %s", report_path)
        except Exception as e:
            logger.warning("Could not generate HTML report: %s", e)

    async def _run_airflow_to_mwaa_pipeline(self, input_dir: str):
        """On-prem Airflow DAG (.py) → MWAA conversion pipeline.

        Parallel processing:
        - All N Airflow DAG files parsed in parallel (max 5 concurrent LLM calls)
        - All DAGs planned/built/validated in parallel (max 5 concurrent)
        - Output directories cleared at start so only this run's files are processed
        - HTML report generated at end covering all DAGs from all input files
        """
        import shutil
        _pipeline_start = time.time()
        self._store.init_agent(AgentName.WORKFLOW_CONVERTER)
        self._store.init_agent(AgentName.WORKFLOW_VALIDATOR)

        input_path = self._resolve_dir(input_dir)
        py_files = sorted(input_path.glob("*.py"))

        if not py_files:
            logger.warning("No Airflow DAG .py files found in %s", input_path)
            return

        output_base = P.MWAA_OUTPUT
        spark_sql_mapping = str(P.MWAA_SPARK_SQL_MAPPING)

        # ── Clean output directories ──────────────────────────────────────
        for clean_dir in ["job_ir", "plans", "dags", "validation"]:
            shutil.rmtree(output_base / clean_dir, ignore_errors=True)
            (output_base / clean_dir).mkdir(parents=True, exist_ok=True)
        logger.info("Cleaned output directories — processing %d Airflow DAG file(s)", len(py_files))

        # ── Stage 1: Parse ALL Airflow DAG files in parallel ──────────────
        # await blocks until ALL N files are fully parsed before continuing.
        parse_pipelines = []
        for pf in py_files:
            parse_pipelines.append([
                AgentRequest(
                    task_id=f"parse_{pf.stem}",
                    agent=AgentName.WORKFLOW_CONVERTER,
                    mcp_server="mwaa-converter",
                    tool="parse_airflow_dag",
                    arguments={"dag_file": str(pf.resolve())},
                ),
                AgentRequest(
                    task_id=f"validate_parse_{pf.stem}",
                    agent=AgentName.WORKFLOW_CONVERTER,
                    mcp_server="mwaa-converter",
                    tool="validate_parsed_airflow_job_ir",
                    arguments={"dag_file": str(pf.resolve())},
                    depends_on=[f"parse_{pf.stem}"],
                ),
            ])

        logger.info("Parsing %d Airflow DAG file(s) in parallel (max 5 concurrent LLM calls)", len(py_files))
        await run_pipeline_per_file(self._dispatch, parse_pipelines, max_parallel_files=5)

        # ── Discover DAGs from this run ───────────────────────────────────
        job_ir_dir = output_base / "job_ir"
        job_ir_files = sorted(job_ir_dir.glob("*.json")) if job_ir_dir.exists() else []
        if not job_ir_files:
            logger.error("No JobIR files found after parsing — check Airflow DAG input")
            return

        logger.info("Found %d Airflow DAG(s) across %d input file(s) — converting",
                    len(job_ir_files), len(py_files))

        # ── Stages 3-6: Convert ALL DAGs in parallel ─────────────────────
        # No depends_on needed — await above guarantees all parsing is done.
        file_pipelines = []
        for job_ir_file in job_ir_files:
            chunk_id = job_ir_file.stem
            jobs_file = str(job_ir_file.resolve())
            plan_file = str((output_base / "plans" / f"{chunk_id}_plan.json").resolve())
            dag_file = str((output_base / "dags" / f"{chunk_id}.py").resolve())

            pipeline = [
                AgentRequest(
                    task_id=f"plan_{chunk_id}",
                    agent=AgentName.WORKFLOW_CONVERTER,
                    mcp_server="mwaa-converter",
                    tool="generate_conversion_plan",
                    arguments={
                        "chunk_id": chunk_id,
                        "jobs_file": jobs_file,
                        "spark_sql_mapping_file": spark_sql_mapping,
                    },
                ),
                AgentRequest(
                    task_id=f"validate_plan_{chunk_id}",
                    agent=AgentName.WORKFLOW_CONVERTER,
                    mcp_server="mwaa-converter",
                    tool="validate_conversion_plan",
                    arguments={
                        "chunk_id": chunk_id,
                        "jobs_file": jobs_file,
                        "spark_sql_mapping_file": spark_sql_mapping,
                    },
                    depends_on=[f"plan_{chunk_id}"],
                ),
                AgentRequest(
                    task_id=f"dag_{chunk_id}",
                    agent=AgentName.WORKFLOW_CONVERTER,
                    mcp_server="mwaa-converter",
                    tool="generate_dag_code",
                    arguments={"conversion_plan_file": plan_file},
                    depends_on=[f"validate_plan_{chunk_id}"],
                ),
                AgentRequest(
                    task_id=f"validate_syntax_{chunk_id}",
                    agent=AgentName.WORKFLOW_VALIDATOR,
                    mcp_server="mwaa-validator",
                    tool="validate_syntax",
                    arguments={"dag_file": dag_file},
                    depends_on=[f"dag_{chunk_id}"],
                ),
                AgentRequest(
                    task_id=f"validate_structure_{chunk_id}",
                    agent=AgentName.WORKFLOW_VALIDATOR,
                    mcp_server="mwaa-validator",
                    tool="validate_structure",
                    arguments={
                        "dag_file": dag_file,
                        "conversion_plan_file": plan_file,
                    },
                    depends_on=[f"validate_syntax_{chunk_id}"],
                ),
                AgentRequest(
                    task_id=f"validate_security_{chunk_id}",
                    agent=AgentName.WORKFLOW_VALIDATOR,
                    mcp_server="mwaa-validator",
                    tool="validate_security",
                    arguments={"dag_file": dag_file},
                    depends_on=[f"validate_syntax_{chunk_id}"],
                ),
                AgentRequest(
                    task_id=f"validate_dagbag_{chunk_id}",
                    agent=AgentName.WORKFLOW_VALIDATOR,
                    mcp_server="mwaa-validator",
                    tool="validate_dagbag",
                    arguments={"dag_file": dag_file},
                    depends_on=[f"validate_structure_{chunk_id}"],
                ),
            ]
            file_pipelines.append(pipeline)

        logger.info("Converting %d Airflow DAG(s) in parallel (max 5 concurrent)", len(file_pipelines))
        await run_pipeline_per_file(self._dispatch, file_pipelines, max_parallel_files=5)

        self._store.update_agent(AgentName.WORKFLOW_CONVERTER, status=TaskStatus.SUCCESS)
        self._store.update_agent(AgentName.WORKFLOW_VALIDATOR, status=TaskStatus.SUCCESS)

        # ── Consolidated HTML report ──────────────────────────────────────
        self._generate_mwaa_html_report(output_base, start_time=_pipeline_start)

    async def _run_full_pipeline(self, kwargs: Dict):
        """Full pipeline: HiveQL → Impala → MWAA conversion (each with accuracy/validation).

        A stage whose input folder is empty logs a warning and is skipped.
        """
        hql_input = kwargs.get("hive_input") or str(P.HIVE_INPUT)
        impala_input = kwargs.get("impala_input") or str(P.IMPALA_INPUT)
        mwaa_input = kwargs.get("mwaa_input") or str(self._default_mwaa_input(kwargs))

        await self._run_hive_pipeline(hql_input)
        await self._run_impala_pipeline(impala_input)
        await self._run_mwaa_pipeline(mwaa_input, kwargs)

    async def _run_accuracy(self, kwargs: Dict):
        """Standalone accuracy analysis over existing conversions.

        Re-scores every converted file in output/hive/spark_sql/ and
        output/impala/spark_sql/ against its source in input/hive/ or
        input/impala/, writing results to output/<dialect>/accuracy/.
        """
        self._store.init_agent(AgentName.ACCURACY_CALCULATOR)

        # (source dialect, extensions to try for the original source file)
        dialects = (("hive", (".hql", ".txt", ".sql")), ("impala", (".sql", ".txt")))

        for source_type, source_exts in dialects:
            spark_dir = P.spark_sql_dir(source_type)
            src_dir = P.sql_input(source_type)
            if not spark_dir.exists():
                continue

            stems = []
            for sql_file in sorted(spark_dir.glob("*.sql")):
                stem = sql_file.stem.replace("_FINAL", "")
                src_file = next(
                    (src_dir / f"{stem}{ext}" for ext in source_exts
                     if (src_dir / f"{stem}{ext}").exists()),
                    None,
                )
                if not src_file:
                    continue

                task_id = f"{source_type}_{stem}_accuracy"
                await self._dispatch(AgentRequest(
                    task_id=task_id,
                    agent=AgentName.ACCURACY_CALCULATOR,
                    mcp_server="accuracy-calculator",
                    tool="analyze_accuracy",
                    arguments={
                        "spark_sql_file": str(sql_file.resolve()),
                        "source_file": str(src_file.resolve()),
                    },
                ))
                stems.append(stem)

            await self._generate_accuracy_dashboard(
                source_type, stems, f"{source_type}_generate_dashboard",
                task_id_fmt="{source_type}_{stem}_accuracy")

    async def _run_impala_pipeline(self, input_dir: str):
        """Impala SQL → Spark SQL conversion pipeline (parallel across files).
        
        Reads from input/impala/, writes to output/impala/ (spark_sql, chunks,
        mermaid, accuracy, validation) — kept separate from the HiveQL results.
        """
        self._store.init_agent(AgentName.IMPALA_CONVERTER)
        self._store.init_agent(AgentName.HIVE_EVAL)
        self._store.init_agent(AgentName.ACCURACY_CALCULATOR)

        input_path = self._resolve_dir(input_dir)
        sql_files = list(input_path.glob("*.sql"))
        if not sql_files:
            logger.warning("No Impala SQL files found in %s", input_path)
            return

        # Task IDs are prefixed with "impala_" so they never collide with the
        # HiveQL tasks of a file with the same name in full_pipeline.

        # Phase 1: Analyze all files
        analyze_requests = [
            AgentRequest(
                task_id=f"impala_{f.stem}_analyze",
                agent=AgentName.IMPALA_CONVERTER,
                mcp_server="impala-converter",
                tool="identify_impala_file",
                arguments={"file_path": str(f.resolve())},
            )
            for f in sql_files
        ]
        await run_parallel(self._dispatch, analyze_requests, max_concurrent=len(sql_files))

        # Phase 2: Build per-file pipelines
        pipelines = []
        for sql_file in sql_files:
            stem = sql_file.stem
            abs_path = str(sql_file.resolve())
            output_sql = str(P.spark_sql_dir("impala") / f"{stem}.sql")

            pipeline = [
                AgentRequest(
                    task_id=f"impala_{stem}_convert",
                    agent=AgentName.IMPALA_CONVERTER,
                    mcp_server="impala-converter",
                    tool="generate_spark_sql_from_impala",
                    arguments={"impala_file": abs_path},
                    depends_on=[f"impala_{stem}_analyze"],
                ),
                AgentRequest(
                    task_id=f"impala_{stem}_validate",
                    agent=AgentName.IMPALA_CONVERTER,
                    mcp_server="impala-converter",
                    tool="validate_impala_spark_sql",
                    arguments={"sql_file": output_sql, "source_impala_file": abs_path},
                    depends_on=[f"impala_{stem}_convert"],
                ),
                AgentRequest(
                    task_id=f"impala_{stem}_execute",
                    agent=AgentName.HIVE_EVAL,
                    mcp_server="hive-eval",
                    tool="execute_spark_sql",
                    arguments={"sql_file": output_sql},
                    depends_on=[f"impala_{stem}_validate"],
                ),
                AgentRequest(
                    task_id=f"impala_{stem}_accuracy",
                    agent=AgentName.ACCURACY_CALCULATOR,
                    mcp_server="accuracy-calculator",
                    tool="analyze_accuracy",
                    arguments={"spark_sql_file": output_sql, "source_file": abs_path},
                    depends_on=[f"impala_{stem}_convert"],
                ),
                AgentRequest(
                    task_id=f"impala_{stem}_mermaid",
                    agent=AgentName.IMPALA_CONVERTER,
                    mcp_server="impala-converter",
                    tool="create_impala_mermaid",
                    arguments={"impala_file": abs_path},
                    depends_on=[f"impala_{stem}_analyze"],
                ),
            ]
            pipelines.append(pipeline)

        # Phase 3: Run all pipelines in parallel
        if pipelines:
            logger.info("Processing %d Impala SQL files in parallel", len(pipelines))
            await run_pipeline_per_file(self._dispatch, pipelines, max_parallel_files=3)

        # Phase 4: Generate accuracy dashboard in output/impala/accuracy/
        await self._generate_accuracy_dashboard(
            "impala", [f.stem for f in sql_files], "impala_generate_dashboard",
            task_id_fmt="impala_{stem}_accuracy")

        self._store.update_agent(AgentName.IMPALA_CONVERTER, status=TaskStatus.SUCCESS)

    async def _run_test_pipeline(self, kwargs: Dict):
        """Hive vs Spark SQL equivalence testing pipeline.

        Discovers matching pairs by file stem: the original HiveQL in input/hive/
        and its converted Spark SQL in output/hive/spark_sql/. Results are written
        to output/testing/. Then runs the 5-session pipeline for each pair:
          Session 1: DDL extraction
          Session 2: Test data generation
          Session 3: Dependency analysis
          Session 4: Container startup + data loading + execution + MINUS validation
          Session 5: Report generation
        """
        self._store.init_agent(AgentName.TEST_ORCHESTRATOR)
        self._store.init_agent(AgentName.TEST_EXECUTION)

        hive_dir = self._resolve_dir(kwargs.get("hive_dir") or P.HIVE_INPUT)
        spark_dir = self._resolve_dir(kwargs.get("spark_dir") or P.spark_sql_dir("hive"))
        gen_data_dir = P.TESTING_GENERATED_DATA
        exec_output_dir = P.TESTING_EXECUTION_OUTPUT

        # Discover matching pairs
        hive_files = {}
        if hive_dir.exists():
            for f in hive_dir.iterdir():
                if f.suffix in (".hql", ".sql", ".txt") and not f.name.startswith("."):
                    hive_files[f.stem] = f

        pairs = []
        if spark_dir.exists():
            for f in spark_dir.iterdir():
                if f.suffix in (".sql", ".txt") and not f.name.startswith("."):
                    stem = f.stem.replace("_FINAL", "")
                    if stem in hive_files:
                        pairs.append((stem, hive_files[stem], f))

        if not pairs:
            logger.warning("No matching HiveQL/Spark SQL pairs found")
            logger.warning("  HiveQL dir: %s", hive_dir)
            logger.warning("  Spark dir:  %s", spark_dir)
            return

        pairs.sort(key=lambda x: x[0])
        logger.info("Found %d test pair(s) for equivalence testing", len(pairs))

        # Session 4 pre-step: Start containers once for all queries
        await self._dispatch(AgentRequest(
            task_id="test_start_containers",
            agent=AgentName.TEST_EXECUTION,
            mcp_server="test-execution",
            tool="start_test_containers",
            arguments={},
            timeout=300,
        ))

        # Process each query pair sequentially (sessions share container state)
        for query_name, hive_path, spark_path in pairs:
            logger.info("🧪 Testing pair: %s", query_name)

            # Per-query folders (the MCP prompts write into these)
            query_gen_dir = gen_data_dir / query_name
            query_exec_dir = exec_output_dir / query_name
            for d in (query_gen_dir / "tables", query_exec_dir / "hive", query_exec_dir / "spark"):
                d.mkdir(parents=True, exist_ok=True)

            # Session 1: DDL Extraction
            await self._dispatch(AgentRequest(
                task_id=f"test_{query_name}_ddl",
                agent=AgentName.TEST_ORCHESTRATOR,
                mcp_server="test-orchestrator",
                tool="extract_or_load_ddl",
                arguments={"hive_file": str(hive_path.resolve())},
                timeout=900,
            ))
            self._expect_files(f"test_{query_name}_ddl", query_gen_dir / "schemas.json")

            # Check if Session 1 marked the file as skipped (too complex for testing)
            ddl_task = self._store.get_task(f"test_{query_name}_ddl")
            if ddl_task and ddl_task.status == TaskStatus.SUCCESS:
                try:
                    schema_file = query_gen_dir / "schemas.json"
                    if schema_file.exists():
                        import json as _json
                        schema_data = _json.loads(schema_file.read_text())
                        if schema_data.get("skipped", False):
                            logger.info("⏭️  %s SKIPPED — %s", query_name,
                                        schema_data.get("skip_reason", "not suitable for testing")[:80])
                            continue
                except Exception:
                    pass

            # Session 2: Test Data Generation
            schema_file = str(query_gen_dir / "schemas.json")
            await self._dispatch(AgentRequest(
                task_id=f"test_{query_name}_datagen",
                agent=AgentName.TEST_ORCHESTRATOR,
                mcp_server="test-orchestrator",
                tool="generate_test_data",
                arguments={
                    "schema_file": schema_file,
                    "hive_file": str(hive_path.resolve()),
                },
                depends_on=[f"test_{query_name}_ddl"],
                timeout=900,
            ))
            self._expect_files(f"test_{query_name}_datagen",
                               query_gen_dir / "setup_hive.sql",
                               query_gen_dir / "setup_spark.sql",
                               query_gen_dir / "sample_data.json")

            # Session 3: Dependency Analysis
            await self._dispatch(AgentRequest(
                task_id=f"test_{query_name}_deps",
                agent=AgentName.TEST_ORCHESTRATOR,
                mcp_server="test-orchestrator",
                tool="analyze_dependencies",
                arguments={
                    "hive_file": str(hive_path.resolve()),
                    "schema_file": schema_file,
                    "data_file": str(query_gen_dir / "sample_data.json"),
                },
                depends_on=[f"test_{query_name}_datagen"],
                timeout=900,
            ))
            self._expect_files(f"test_{query_name}_deps", query_gen_dir / "dependency_graph.json")

            # Session 4a: Load data into Hive
            await self._dispatch(AgentRequest(
                task_id=f"test_{query_name}_load_hive",
                agent=AgentName.TEST_EXECUTION,
                mcp_server="test-execution",
                tool="load_test_data_hive",
                arguments={"setup_file": str(query_gen_dir / "setup_hive.sql")},
                depends_on=[f"test_{query_name}_deps", "test_start_containers"],
                timeout=600,
            ))

            # Session 4b: Load data into Spark
            await self._dispatch(AgentRequest(
                task_id=f"test_{query_name}_load_spark",
                agent=AgentName.TEST_EXECUTION,
                mcp_server="test-execution",
                tool="load_test_data_spark",
                arguments={"setup_file": str(query_gen_dir / "setup_spark.sql")},
                depends_on=[f"test_{query_name}_deps", "test_start_containers"],
                timeout=600,
            ))

            # Session 4c: Execute HiveQL query
            await self._dispatch(AgentRequest(
                task_id=f"test_{query_name}_exec_hive",
                agent=AgentName.TEST_EXECUTION,
                mcp_server="test-execution",
                tool="execute_hive_query",
                arguments={"query_file": str(hive_path.resolve())},
                depends_on=[f"test_{query_name}_load_hive"],
                timeout=900,
            ))

            # Session 4d: Execute Spark SQL query
            await self._dispatch(AgentRequest(
                task_id=f"test_{query_name}_exec_spark",
                agent=AgentName.TEST_EXECUTION,
                mcp_server="test-execution",
                tool="execute_spark_query",
                arguments={"query_file": str(spark_path.resolve())},
                depends_on=[f"test_{query_name}_load_spark"],
                timeout=900,
            ))

            # Session 4e: Compare results
            hive_stdout = str(query_exec_dir / "hive" / f"{query_name}_stdout.txt")
            spark_stdout = str(query_exec_dir / "spark" / f"{query_name}_stdout.txt")
            self._expect_files(f"test_{query_name}_exec_hive", hive_stdout)
            self._expect_files(f"test_{query_name}_exec_spark", spark_stdout)
            await self._dispatch(AgentRequest(
                task_id=f"test_{query_name}_compare",
                agent=AgentName.TEST_ORCHESTRATOR,
                mcp_server="test-orchestrator",
                tool="compare_results",
                arguments={
                    "hive_output_file": hive_stdout,
                    "spark_output_file": spark_stdout,
                },
                depends_on=[f"test_{query_name}_exec_hive", f"test_{query_name}_exec_spark"],
                timeout=600,
            ))

            # Session 4f: MINUS validation
            await self._dispatch(AgentRequest(
                task_id=f"test_{query_name}_minus",
                agent=AgentName.TEST_ORCHESTRATOR,
                mcp_server="test-orchestrator",
                tool="run_minus_validation",
                arguments={
                    "hive_results_file": hive_stdout,
                    "spark_results_file": spark_stdout,
                },
                depends_on=[f"test_{query_name}_compare"],
                timeout=600,
            ))
            self._expect_files(f"test_{query_name}_minus", query_exec_dir / "minus_results.json")

            # Session 5: Report Generation
            await self._dispatch(AgentRequest(
                task_id=f"test_{query_name}_report",
                agent=AgentName.TEST_ORCHESTRATOR,
                mcp_server="test-orchestrator",
                tool="generate_test_report",
                arguments={"test_name": query_name},
                depends_on=[f"test_{query_name}_minus"],
                timeout=600,
            ))

        # Teardown containers
        keep_containers = kwargs.get("keep_containers", False)
        if not keep_containers:
            await self._dispatch(AgentRequest(
                task_id="test_stop_containers",
                agent=AgentName.TEST_EXECUTION,
                mcp_server="test-execution",
                tool="stop_test_containers",
                arguments={},
                timeout=120,
            ))

        self._store.update_agent(AgentName.TEST_ORCHESTRATOR, status=TaskStatus.SUCCESS)
        self._store.update_agent(AgentName.TEST_EXECUTION, status=TaskStatus.SUCCESS)
        self._generate_test_html_reports([name for name, _, _ in pairs])
        logger.info("🧪 Testing pipeline complete — %d pair(s) processed", len(pairs))

    def _expect_files(self, task_id: str, *paths) -> bool:
        """Mark a finished LLM task FAILED if it did not write its expected files.

        LLM tools "succeed" whenever the CLI exits, even if the work was not done.
        Checking the artifacts keeps the run status honest, and dependent tasks
        are then skipped instead of running on missing input.
        """
        task = self._store.get_task(task_id)
        if not task or task.status != TaskStatus.SUCCESS:
            return False
        missing = [P.rel(p) for p in paths if not Path(p).exists()]
        if not missing:
            return True
        logger.error("❌ %s finished but did not create: %s", task_id, ", ".join(missing))
        task.status = TaskStatus.FAILED
        task.errors = [f"expected output missing: {', '.join(missing)}"]
        self._store.record_task(task)
        return False

    def _generate_test_html_reports(self, query_names: List[str]) -> None:
        """Write TEST_REPORT.html per query and SUMMARY_REPORT.html; log the MINUS verdicts."""
        try:
            import sys
            sys.path.insert(0, str(PROJECT_ROOT / "testing_framework" / "src"))
            from report_generator.html_report import HTMLReportGenerator
        except Exception as e:
            logger.warning("Could not load the test report generator: %s", e)
            return

        results = []
        for name in query_names:
            result = {"query_name": name}
            schemas_file = P.TESTING_GENERATED_DATA / name / "schemas.json"
            try:
                schemas = json.loads(schemas_file.read_text()) if schemas_file.exists() else {}
            except (ValueError, OSError):
                schemas = {}
            if schemas.get("skipped"):
                result.update(skipped=True, skip_reason=schemas.get("skip_reason", ""))
            try:
                HTMLReportGenerator.generate_query_report(name, P.TESTING_OUTPUT, P.HIVE_INPUT)
            except Exception as e:
                logger.warning("Could not write TEST_REPORT.html for %s: %s", name, e)
            results.append(result)

            minus_file = P.TESTING_EXECUTION_OUTPUT / name / "minus_results.json"
            try:
                minus = json.loads(minus_file.read_text()) if minus_file.exists() else None
            except (ValueError, OSError):
                minus = None
            if result.get("skipped"):
                logger.info("⏭️  %s: skipped (%s)", name, result["skip_reason"][:80])
            elif minus is None:
                logger.info("❌ %s: no MINUS result (see failed tasks above)", name)
            else:
                logger.info("%s %s: MINUS %s (hive-spark: %s rows, spark-hive: %s rows)",
                            "✅" if minus.get("minus_passed") else "❌", name,
                            minus.get("verdict", "?"),
                            minus.get("hive_minus_spark", {}).get("row_count", "?"),
                            minus.get("spark_minus_hive", {}).get("row_count", "?"))
        try:
            summary = P.TESTING_REPORTS / "SUMMARY_REPORT.html"
            HTMLReportGenerator.generate_summary_report(results, summary)
            logger.info("📊 Test report: %s", P.rel(summary))
        except Exception as e:
            logger.warning("Could not write SUMMARY_REPORT.html: %s", e)

    # -- core dispatch with retry ------------------------------------------

    async def _dispatch(self, request: AgentRequest) -> AgentResponse:
        """Dispatch a task to an agent with retry logic."""
        # Skip if already completed (resume mode) — but NEVER skip accuracy tasks
        existing = self._store.get_task(request.task_id)
        is_accuracy_task = request.tool in ("analyze_accuracy", "generate_accuracy_report", "generate_dashboard")

        if existing and existing.status == TaskStatus.SUCCESS and not is_accuracy_task:
            logger.info("⏭️  %s already completed (resume) — skipping", request.task_id)
            return existing

        # Skip if output artifact already exists on disk (manual fix scenario)
        # Never skip accuracy tasks — they must always re-run
        if existing and existing.status == TaskStatus.FAILED and not is_accuracy_task:
            if self._output_exists_on_disk(request):
                logger.info("⏭️  %s output exists on disk (manual fix detected) — marking SUCCESS",
                            request.task_id)
                resp = AgentResponse(
                    task_id=request.task_id,
                    agent=request.agent,
                    tool=request.tool,
                    status=TaskStatus.SUCCESS,
                    result="Output file exists (manually provided)",
                    metrics=dict(request.arguments),
                )
                self._store.record_task(resp)
                return resp

        # Check dependencies
        if request.depends_on and not self._store.all_deps_met(request.depends_on):
            logger.warning("Deps not met for %s, skipping", request.task_id)
            resp = AgentResponse(
                task_id=request.task_id,
                agent=request.agent,
                tool=request.tool,
                status=TaskStatus.SKIPPED,
                errors=["Dependencies not met"],
            )
            self._store.record_task(resp)
            return resp

        for attempt in range(1, request.max_retries + 1):
            start = time.time()
            current_agent.set(request.agent.value)
            try:
                # Phase 1: Call MCP tool (may return a prompt or a direct result)
                mcp_result = await self._pool.call(
                    request.mcp_server,
                    request.tool,
                    request.arguments,
                    timeout=request.timeout,
                )

                # Phase 2: If the tool returns a prompt, send it to the LLM
                if needs_llm(request.tool):
                    logger.info("🤖 %s requires LLM — dispatching prompt (%d chars)",
                                request.task_id, len(mcp_result))
                    result = await execute_via_llm(
                        mcp_result, backend=self._backend, timeout=int(request.timeout))
                elif needs_llm_fallback(mcp_result):
                    prompt = extract_fallback_prompt(mcp_result)
                    logger.warning("🔄 %s deterministic failed — falling back to LLM (%d chars)",
                                   request.task_id, len(prompt))
                    result = await execute_via_llm(
                        prompt, backend=self._backend, timeout=int(request.timeout))
                else:
                    result = mcp_result

                resp = AgentResponse(
                    task_id=request.task_id,
                    agent=request.agent,
                    tool=request.tool,
                    status=TaskStatus.SUCCESS,
                    result=result,
                    metrics=dict(request.arguments),
                    duration_seconds=time.time() - start,
                )
                self._store.record_task(resp)
                logger.info("✅ %s completed (%.1fs)", request.task_id, resp.duration_seconds)

                # Self-correction: check if result triggers a corrective action
                correction = self._corrector.evaluate(resp)
                if correction:
                    logger.info("🔄 Auto-correcting: %s", correction.task_id)
                    await self._dispatch(correction)

                return resp

            except Exception as e:
                logger.error(
                    "❌ %s attempt %d/%d failed: %s",
                    request.task_id, attempt, request.max_retries, e,
                )
                if attempt == request.max_retries:
                    resp = AgentResponse(
                        task_id=request.task_id,
                        agent=request.agent,
                        tool=request.tool,
                        status=TaskStatus.FAILED,
                        errors=[str(e)],
                        duration_seconds=time.time() - start,
                    )
                    self._store.record_task(resp)
                    return resp

                await asyncio.sleep(2 ** attempt)  # exponential backoff

    # -- reporting ---------------------------------------------------------

    def _output_exists_on_disk(self, request: AgentRequest) -> bool:
        """Check if the expected output file for a task already exists.

        Used during resume to detect manually-fixed files.
        Maps tool + arguments to expected output paths.
        """
        tool = request.tool
        args = request.arguments

        if tool == "generate_spark_sql":
            # Expected output: output/hive/spark_sql/{stem}.sql or chunk subdir
            hive_file = args.get("hive_file", "")
            stem = Path(hive_file).stem
            # Check if it's a chunk file
            if "_chunk_" in stem:
                base = stem.split("_chunk_")[0]
                output = P.spark_sql_dir("hive") / base / f"{stem}.sql"
            else:
                output = P.spark_sql_dir("hive") / f"{stem}.sql"
            return output.exists()

        elif tool == "merge_spark_sql_chunks":
            chunk_dir = args.get("chunk_directory", "")
            # Expected: parent_FINAL.sql
            parent = Path(chunk_dir).name
            final = P.spark_sql_dir("hive") / f"{parent}_FINAL.sql"
            regular = P.spark_sql_dir("hive") / f"{parent}.sql"
            return final.exists() or regular.exists()

        elif tool == "generate_dag_code":
            plan_file = args.get("conversion_plan_file", "")
            stem = Path(plan_file).stem.replace("_plan", "")
            dag_file = P.MWAA_OUTPUT / "dags" / f"{stem}.py"
            return dag_file.exists()

        elif tool == "chunk_hive_file":
            file_path = args.get("file_path", "")
            stem = Path(file_path).stem
            chunk_dir = P.chunks_dir("hive") / stem
            return chunk_dir.exists() and any(chunk_dir.glob("*.hql"))

        # For other tools, can't determine output — don't skip
        return False

    def _print_summary(self):
        state = self._store.state
        if not state:
            return

        duration = (state.completed_at - state.started_at).total_seconds() if state.completed_at else 0
        total = len(state.tasks)
        success = sum(1 for t in state.tasks.values() if t.status == TaskStatus.SUCCESS)
        failed = sum(1 for t in state.tasks.values() if t.status == TaskStatus.FAILED)
        skipped = sum(1 for t in state.tasks.values() if t.status == TaskStatus.SKIPPED)

        logger.info("=" * 60)
        logger.info("ORCHESTRATION COMPLETE — %s", state.status.value)
        logger.info("=" * 60)
        logger.info("Run ID:    %s", state.run_id)
        logger.info("Intent:    %s", state.intent)
        logger.info("Tasks:     %d total, %d success, %d failed, %d skipped", total, success, failed, skipped)
        logger.info("Artifacts: %d", len(state.artifacts))
        logger.info("Duration:  %.1fs", duration)

        if failed:
            logger.info("\nFailed tasks:")
            for tid, t in state.tasks.items():
                if t.status == TaskStatus.FAILED:
                    logger.info("  ❌ %s: %s", tid, t.errors[:1])

        logger.info("=" * 60)


# -- CLI entry point -------------------------------------------------------

async def main():
    import argparse
    parser = argparse.ArgumentParser(description="Unified Orchestrator Agent")
    parser.add_argument("--intent", required=True,
                        choices=["hive_only", "impala_only", "mwaa_only", "full_pipeline", "hiveql_only",
                                 "accuracy_only", "test_only"])
    parser.add_argument("--input", dest="input_dir",
                        help="Override the input directory (hive_only, impala_only, mwaa_only). "
                             "Defaults: input/hive, input/impala, input/mwaa/control_m "
                             "(input/mwaa/airflow with --source-type airflow)")
    parser.add_argument("--hql-input", default=None,
                        help="HiveQL input directory for full_pipeline (default: input/hive)")
    parser.add_argument("--impala-input", default=None,
                        help="Impala SQL input directory for full_pipeline (default: input/impala)")
    parser.add_argument("--mwaa-input", default=None,
                        help="Control-M/Airflow input directory for full_pipeline "
                             "(default: input/mwaa/control_m or input/mwaa/airflow)")
    parser.add_argument("--source-type", default="control_m", choices=["control_m", "airflow"],
                        help="MWAA source: Control-M XML or on-prem Airflow DAGs")
    parser.add_argument("--test-hive-dir", default=None,
                        help="Original HiveQL directory for test_only (default: input/hive)")
    parser.add_argument("--test-spark-dir", default=None,
                        help="Converted Spark SQL directory for test_only "
                             "(default: output/hive/spark_sql)")
    parser.add_argument("--keep-containers", action="store_true",
                        help="Keep test containers running after test_only pipeline")
    parser.add_argument("--resume", type=str, default=None, metavar="RUN_ID",
                        help="Resume a previous run by ID (e.g., run_20260521_092506_6071ff). "
                             "Skips tasks already marked SUCCESS in that run's state.")
    parser.add_argument("--backend", default=DEFAULT_BACKEND, choices=["kiro", "claude"],
                        help="LLM dispatch backend: 'kiro' uses kiro-cli (default), "
                             "'claude' uses the Claude Code CLI. Both read their own "
                             "MCP config (.kiro/settings/mcp.json vs .claude/settings.json).")
    args = parser.parse_args()
    if args.intent in LEGACY_INTENTS:
        print(f"Note: --intent {args.intent} is deprecated; use --intent {LEGACY_INTENTS[args.intent]}")
        args.intent = LEGACY_INTENTS[args.intent]

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    kwargs = {"source_type": args.source_type}
    if args.intent == "hive_only":
        kwargs["input_dir"] = args.input_dir or args.hql_input
    elif args.intent == "impala_only":
        kwargs["input_dir"] = args.input_dir
    elif args.intent == "mwaa_only":
        kwargs["input_dir"] = args.input_dir or args.mwaa_input
    elif args.intent == "full_pipeline":
        kwargs["hive_input"] = args.hql_input
        kwargs["impala_input"] = args.impala_input
        kwargs["mwaa_input"] = args.mwaa_input
    elif args.intent == "test_only":
        kwargs["hive_dir"] = args.test_hive_dir
        kwargs["spark_dir"] = args.test_spark_dir
        kwargs["keep_containers"] = args.keep_containers

    if args.resume:
        kwargs["resume_run_id"] = args.resume

    async with OrchestratorAgent(backend=args.backend) as orch:
        run_id = await orch.run(args.intent, **kwargs)
        print(f"\nRun ID: {run_id}")
        print(f"State:  {P.rel(P.STATE_DIR)}/{run_id}.json")


if __name__ == "__main__":
    asyncio.run(main())

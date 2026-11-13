#!/usr/bin/env python3
"""
Hive vs Spark SQL Equivalence Test Framework — Orchestrator

Discovers matching HiveQL/Spark SQL pairs and drives Kiro CLI sessions
to execute the 5-session pipeline via MCP tools.

Each MCP tool returns a prompt that Kiro (the LLM) follows using its own
tools (file I/O, bash, finch exec, etc.). This script orchestrates the
sequence and tracks state for crash recovery.

Usage:
  python run_tests.py                    # Run all pairs
  python run_tests.py --resume           # Resume from last completed step
  python run_tests.py --clean            # Wipe state and start fresh
  python run_tests.py --keep-containers  # Leave containers running after tests
"""
import argparse
import json
import logging
import shutil
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

# Shared input/output locations (see project_paths.py at the repo root)
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import project_paths as P
from llm_cli import kiro_chat_argv, run_llm_cli

# HTML report generator
sys.path.insert(0, str(Path(__file__).parent / "src"))
from report_generator.html_report import HTMLReportGenerator

# ── Paths ──────────────────────────────────────────────────────────────────
FRAMEWORK_DIR = Path(__file__).parent.resolve()
STATE_FILE = P.TESTING_STATE / "session_state.json"

HIVE_INPUT = P.HIVE_INPUT                      # original HiveQL        (input/hive)
SPARK_SQL_INPUT = P.spark_sql_dir("hive")        # converted Spark SQL    (output/hive/spark_sql)
GENERATED_DATA = P.TESTING_GENERATED_DATA          # output/testing/generated_data
EXECUTION_OUTPUT = P.TESTING_EXECUTION_OUTPUT      # output/testing/execution_output
REPORTS = P.TESTING_REPORTS                        # output/testing/reports
LOG_DIR = P.LOG_DIR                                # output/logs

COMPOSE_FILE = FRAMEWORK_DIR / "finch-compose-test.yml"

# ── Timeouts ───────────────────────────────────────────────────────────────
TIMEOUT_KIRO = 900       # 15 min per Kiro LLM session
TIMEOUT_CONTAINER = 300  # 5 min for container startup

# ── Logging ────────────────────────────────────────────────────────────────
LOG_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "orchestrator.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# Kiro CLI Helper
# ═══════════════════════════════════════════════════════════════════════════

def call_kiro(prompt: str, timeout: int = TIMEOUT_KIRO) -> str:
    """Run a single Kiro CLI non-interactive session with the given prompt."""
    logger.info(f"Kiro session (timeout={timeout}s): {prompt[:150]}...")
    try:
        # prompts use project-root-relative paths (input/, output/)
        r = run_llm_cli(kiro_chat_argv(prompt), timeout=timeout, cwd=P.ROOT)
        if r.returncode != 0 and r.stderr:
            logger.warning(f"Kiro stderr: {r.stderr[:500]}")
        return r.stdout
    except subprocess.TimeoutExpired:
        logger.error(f"Kiro timed out after {timeout}s")
        return ""
    except FileNotFoundError:
        logger.error("kiro-cli not found on PATH")
        return ""
    except Exception as e:
        logger.error(f"Kiro failed: {e}")
        return ""


# ═══════════════════════════════════════════════════════════════════════════
# State Management
# ═══════════════════════════════════════════════════════════════════════════

def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {}

def save_state(state: dict):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))

def step_done(state: dict, query: str, step: str) -> bool:
    return state.get("queries", {}).get(query, {}).get(step, {}).get("status") == "complete"

def mark_step(state: dict, query: str, step: str, status: str = "complete", **extra):
    state.setdefault("queries", {}).setdefault(query, {})[step] = {
        "status": status, "timestamp": datetime.now().isoformat(), **extra,
    }
    save_state(state)


# ═══════════════════════════════════════════════════════════════════════════
# File Discovery
# ═══════════════════════════════════════════════════════════════════════════

def discover_pairs(hive_dir: Path, spark_dir: Path) -> list:
    """Find matching HiveQL/Spark SQL file pairs by stem name."""
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
    return sorted(pairs, key=lambda x: x[0])


# ═══════════════════════════════════════════════════════════════════════════
# Per-Query Pipeline (5 Sessions via Kiro + MCP)
# ═══════════════════════════════════════════════════════════════════════════

def process_query(query_name: str, hive_path: Path, spark_path: Path,
                  state: dict, kiro_timeout: int) -> dict:
    """
    Run the full 5-session pipeline for one query pair.
    Each session calls a Kiro CLI session that invokes the appropriate MCP tool.
    The MCP tool returns a prompt, and Kiro follows it using its built-in tools.
    """
    logger.info(f"\n{'═' * 60}")
    logger.info(f"  QUERY: {query_name}")
    logger.info(f"  HiveQL: {hive_path.name}  |  Spark: {spark_path.name}")
    logger.info(f"{'═' * 60}")

    # Ensure per-query directories exist
    gen_dir = GENERATED_DATA / query_name
    exec_dir = EXECUTION_OUTPUT / query_name
    report_dir = REPORTS / query_name
    for d in [gen_dir, gen_dir / "tables", exec_dir / "hive", exec_dir / "spark", report_dir]:
        d.mkdir(parents=True, exist_ok=True)

    result = {"query_name": query_name, "steps": {}}

    # ── Session 1: DDL Extraction ──────────────────────────────────────
    if not step_done(state, query_name, "session1"):
        logger.info(f"[{query_name}] Session 1: DDL Extraction...")
        call_kiro(
            f"Use the extract_or_load_ddl tool with hive_file: \"{hive_path}\". "
            f"Follow the returned prompt instructions completely. "
            f"Save schemas.json to {gen_dir}/schemas.json",
            kiro_timeout,
        )
        if (gen_dir / "schemas.json").exists():
            # Check if this is a skip marker (non-executable query)
            try:
                schema_data = json.loads((gen_dir / "schemas.json").read_text())
                if schema_data.get("skipped", False):
                    logger.info(f"[{query_name}] Session 1: SKIPPED — {schema_data.get('skip_reason', 'Not a valid query')}")
                    mark_step(state, query_name, "session1", "skipped",
                              skip_reason=schema_data.get("skip_reason", ""),
                              file_type=schema_data.get("file_type_detected", ""))
                    result["steps"]["session1"] = "skipped"
                    result["skipped"] = True
                    result["skip_reason"] = schema_data.get("skip_reason", "Not a valid executable query")
                    result["file_type_detected"] = schema_data.get("file_type_detected", "unknown")
                    return result
            except (json.JSONDecodeError, Exception):
                pass  # Not a skip marker, proceed normally
            mark_step(state, query_name, "session1")
            result["steps"]["session1"] = "complete"
        else:
            # Check if it landed in the default location
            default = GENERATED_DATA / "schemas.json"
            if default.exists():
                default.rename(gen_dir / "schemas.json")
                mark_step(state, query_name, "session1")
                result["steps"]["session1"] = "complete"
            else:
                mark_step(state, query_name, "session1", "failed")
                result["steps"]["session1"] = "failed"
                logger.error(f"[{query_name}] Session 1 FAILED: schemas.json not created")
                return result
    else:
        logger.info(f"[{query_name}] Session 1: Already done, skipping.")
        result["steps"]["session1"] = "complete"

    # ── Session 2: Test Data Generation ────────────────────────────────
    if not step_done(state, query_name, "session2"):
        logger.info(f"[{query_name}] Session 2: Data Generation...")
        call_kiro(
            f"Use the generate_test_data tool with schema_file: \"{gen_dir}/schemas.json\" "
            f"and hive_file: \"{hive_path}\". "
            f"Follow the returned prompt instructions completely. "
            f"CRITICAL: Read the HiveQL file and extract all WHERE clause literal values. "
            f"Ensure generated test data contains those exact literal values so rows survive filters. "
            f"Save setup_hive.sql, setup_spark.sql, sample_data.json to {gen_dir}/ "
            f"and per-table CSVs to {gen_dir}/tables/",
            kiro_timeout,
        )
        expected = ["setup_hive.sql", "setup_spark.sql", "sample_data.json"]
        # Move files from default location if needed
        for fname in expected:
            src, dst = GENERATED_DATA / fname, gen_dir / fname
            if src.exists() and not dst.exists():
                src.rename(dst)
        default_tables = GENERATED_DATA / "tables"
        if default_tables.exists():
            for csv in default_tables.glob("*.csv"):
                dst = gen_dir / "tables" / csv.name
                if not dst.exists():
                    csv.rename(dst)

        if all((gen_dir / f).exists() for f in expected):
            mark_step(state, query_name, "session2")
            result["steps"]["session2"] = "complete"
        else:
            mark_step(state, query_name, "session2", "failed")
            result["steps"]["session2"] = "failed"
            logger.error(f"[{query_name}] Session 2 FAILED: missing output files")
            return result
    else:
        logger.info(f"[{query_name}] Session 2: Already done, skipping.")
        result["steps"]["session2"] = "complete"

    # ── Session 3: Dependency Analysis ─────────────────────────────────
    if not step_done(state, query_name, "session3"):
        logger.info(f"[{query_name}] Session 3: Dependency Analysis...")
        call_kiro(
            f"Use the analyze_dependencies tool with "
            f"hive_file: \"{hive_path}\", "
            f"schema_file: \"{gen_dir}/schemas.json\", "
            f"data_file: \"{gen_dir}/sample_data.json\". "
            f"Follow the returned prompt instructions completely. "
            f"Save dependency_graph.json to {gen_dir}/",
            kiro_timeout,
        )
        dep_file = gen_dir / "dependency_graph.json"
        default_dep = GENERATED_DATA / "dependency_graph.json"
        if default_dep.exists() and not dep_file.exists():
            default_dep.rename(dep_file)
        if dep_file.exists():
            mark_step(state, query_name, "session3")
            result["steps"]["session3"] = "complete"
        else:
            mark_step(state, query_name, "session3", "failed")
            result["steps"]["session3"] = "failed"
            logger.error(f"[{query_name}] Session 3 FAILED: dependency_graph.json not created")
            return result
    else:
        logger.info(f"[{query_name}] Session 3: Already done, skipping.")
        result["steps"]["session3"] = "complete"

    # ── Session 4: Container Setup + Load Data + Execute + MINUS ───────
    if not step_done(state, query_name, "session4"):
        logger.info(f"[{query_name}] Session 4: Execution + MINUS Validation...")

        # 4a: Start containers (idempotent — skips if already running)
        call_kiro(
            "Use the start_test_containers tool. "
            "Follow the returned prompt instructions completely. "
            "Start the Hive and Spark containers via finch-compose and wait until both are ready.",
            kiro_timeout,
        )

        # 4b: Load test data into Hive
        call_kiro(
            f"Use the load_test_data_hive tool with "
            f"setup_file: \"{gen_dir}/setup_hive.sql\". "
            f"Follow the returned prompt instructions completely. "
            f"The file inside the container is at "
            f"/test_workspace/generated_data/{query_name}/setup_hive.sql",
            kiro_timeout,
        )

        # 4c: Load test data into Spark
        call_kiro(
            f"Use the load_test_data_spark tool with "
            f"setup_file: \"{gen_dir}/setup_spark.sql\". "
            f"Follow the returned prompt instructions completely. "
            f"The file inside the container is at "
            f"/test_workspace/generated_data/{query_name}/setup_spark.sql",
            kiro_timeout,
        )

        # 4d: Execute HiveQL query
        call_kiro(
            f"Use the execute_hive_query tool with "
            f"query_file: \"{hive_path}\". "
            f"Follow the returned prompt instructions completely. "
            f"Save prepared SQL and outputs to {exec_dir}/hive/",
            kiro_timeout,
        )

        # 4e: Execute Spark SQL query
        call_kiro(
            f"Use the execute_spark_query tool with "
            f"query_file: \"{spark_path}\". "
            f"Follow the returned prompt instructions completely. "
            f"Save prepared SQL and outputs to {exec_dir}/spark/",
            kiro_timeout,
        )

        # 4f: Compare results
        hive_stdout = exec_dir / "hive" / f"{query_name}_stdout.txt"
        spark_stdout = exec_dir / "spark" / f"{query_name}_stdout.txt"
        # Fall back to alternate naming
        if not hive_stdout.exists():
            candidates = list((exec_dir / "hive").glob("*stdout*"))
            if candidates:
                hive_stdout = candidates[0]
        if not spark_stdout.exists():
            candidates = list((exec_dir / "spark").glob("*stdout*"))
            if candidates:
                spark_stdout = candidates[0]

        if hive_stdout.exists() and spark_stdout.exists():
            call_kiro(
                f"Use the compare_results tool with "
                f"hive_output_file: \"{hive_stdout}\", "
                f"spark_output_file: \"{spark_stdout}\". "
                f"Follow the returned prompt instructions completely.",
                kiro_timeout,
            )

            # 4g: MINUS validation
            call_kiro(
                f"Use the run_minus_validation tool with "
                f"hive_results_file: \"{hive_stdout}\", "
                f"spark_results_file: \"{spark_stdout}\". "
                f"Follow the returned prompt instructions completely. "
                f"Save minus_results.json to {exec_dir}/",
                kiro_timeout,
            )

        mark_step(state, query_name, "session4")
        result["steps"]["session4"] = "complete"
    else:
        logger.info(f"[{query_name}] Session 4: Already done, skipping.")
        result["steps"]["session4"] = "complete"

    # ── Session 5: Report Generation ───────────────────────────────────
    if not step_done(state, query_name, "session5"):
        logger.info(f"[{query_name}] Session 5: Report Generation...")
        call_kiro(
            f"Use the generate_test_report tool with test_name: \"{query_name}\". "
            f"Follow the returned prompt instructions completely. "
            f"Read all outputs from {gen_dir}/ and {exec_dir}/ "
            f"and save TEST_REPORT.md to {report_dir}/",
            kiro_timeout,
        )
        mark_step(state, query_name, "session5")
        result["steps"]["session5"] = "complete"
    else:
        logger.info(f"[{query_name}] Session 5: Already done, skipping.")
        result["steps"]["session5"] = "complete"

    return result


# ═══════════════════════════════════════════════════════════════════════════
# Summary Report
# ═══════════════════════════════════════════════════════════════════════════

def generate_summary(all_results: list):
    """Generate SUMMARY_REPORT.html and per-query TEST_REPORT.html files."""
    REPORTS.mkdir(parents=True, exist_ok=True)

    # Generate per-query HTML reports
    for r in all_results:
        name = r["query_name"]
        try:
            HTMLReportGenerator.generate_query_report(name, P.TESTING_OUTPUT, HIVE_INPUT)
            logger.info(f"  ✓ {name}/TEST_REPORT.html created")
        except Exception as e:
            logger.error(f"  ✗ Failed to generate HTML report for {name}: {e}")

    # Generate summary HTML report
    HTMLReportGenerator.generate_summary_report(all_results, REPORTS / "SUMMARY_REPORT.html")
    logger.info(f"✓ SUMMARY_REPORT.html created ({len(all_results)} queries)")


# ═══════════════════════════════════════════════════════════════════════════
# Main
# ═══════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(description="Hive vs Spark SQL Equivalence Test Framework")
    parser.add_argument("--hive-dir", type=Path, default=HIVE_INPUT)
    parser.add_argument("--spark-dir", type=Path, default=SPARK_SQL_INPUT)
    parser.add_argument("--keep-containers", action="store_true",
                        help="Keep containers running after tests")
    parser.add_argument("--kiro-timeout", type=int, default=TIMEOUT_KIRO,
                        help=f"Timeout per Kiro session (default: {TIMEOUT_KIRO}s)")
    parser.add_argument("--resume", action="store_true",
                        help="Resume from last completed step")
    parser.add_argument("--clean", action="store_true",
                        help="Delete all state and start fresh")
    args = parser.parse_args()

    P.ensure_dirs()   # container bind mounts require the host folders to exist

    logger.info("╔══════════════════════════════════════════════════════════╗")
    logger.info("║  Hive vs Spark SQL Equivalence Test Framework           ║")
    logger.info("╚══════════════════════════════════════════════════════════╝")
    logger.info(f"Start: {datetime.now().isoformat()}")
    logger.info(f"HiveQL dir: {args.hive_dir}")
    logger.info(f"Spark dir:  {args.spark_dir}")
    logger.info(f"Kiro timeout: {args.kiro_timeout}s")
    logger.info(f"Resume: {args.resume} | Clean: {args.clean} | Keep containers: {args.keep_containers}")

    # Clean mode
    if args.clean:
        logger.info("🧹 Cleaning all state and outputs...")
        for d in [GENERATED_DATA, EXECUTION_OUTPUT, REPORTS]:
            if d.exists():
                shutil.rmtree(d)
                d.mkdir(parents=True, exist_ok=True)
        if STATE_FILE.exists():
            STATE_FILE.unlink()
        logger.info("✓ Clean complete\n")

    # Load or init state
    state = load_state() if args.resume else {}

    # Discover pairs
    pairs = discover_pairs(args.hive_dir, args.spark_dir)
    if not pairs:
        logger.error("No matching HiveQL/Spark SQL pairs found.")
        logger.error(f"  HiveQL dir: {args.hive_dir}")
        logger.error(f"  Spark dir:  {args.spark_dir}")
        logger.error("  Files must share the same stem (e.g. query.hql ↔ query.sql)")
        sys.exit(1)

    logger.info(f"Discovered {len(pairs)} query pair(s):")
    for name, hf, sf in pairs:
        logger.info(f"  • {name}  ({hf.name} ↔ {sf.name})")
    logger.info("")

    # Process each query pair
    all_results = []
    for idx, (query_name, hive_path, spark_path) in enumerate(pairs, 1):
        logger.info(f"\n[{idx}/{len(pairs)}] Processing: {query_name}")
        result = process_query(query_name, hive_path, spark_path, state, args.kiro_timeout)
        all_results.append(result)

    # Summary report
    generate_summary(all_results)

    # Teardown containers
    if not args.keep_containers:
        logger.info("\n🧹 Tearing down containers...")
        call_kiro(
            "Use the stop_test_containers tool. "
            "Follow the returned prompt instructions completely.",
            120,
        )
    else:
        logger.info("\n💡 --keep-containers: leaving Hive/Spark running")

    # Final summary
    logger.info("\n╔══════════════════════════════════════════════════════════╗")
    logger.info("║  DONE                                                    ║")
    logger.info("╚══════════════════════════════════════════════════════════╝")
    logger.info(f"Queries processed: {len(all_results)}")
    logger.info(f"Summary: {REPORTS / 'SUMMARY_REPORT.html'}")
    for r in all_results:
        steps = sum(1 for v in r.get("steps", {}).values() if v == "complete")
        if r.get("skipped"):
            logger.info(f"  ⊘ {r['query_name']} (SKIPPED — {r.get('skip_reason', 'not a data load statement')[:60]})")
        else:
            logger.info(f"  {'✅' if steps == 5 else '⚠️'} {r['query_name']} ({steps}/5 steps)")


if __name__ == "__main__":
    main()

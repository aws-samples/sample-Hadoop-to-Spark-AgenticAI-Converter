"""
Hive Spark Test Orchestrator — MCP Server
Session tools for DDL extraction, data generation, analysis, comparison, and reporting.
Fully independent from the converter.
"""
import os
import sys
import logging
from pathlib import Path

from fastmcp import FastMCP

# Add src/ to path for prompt_library import
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from prompt_library.prompt_loader import PromptLoader

# Logging
# Shared input/output locations (see project_paths.py at the repo root)
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
import project_paths
import test_paths as TP

log_dir = project_paths.LOG_DIR
log_dir.mkdir(exist_ok=True, parents=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_dir / "test_orchestrator.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Prompt loader
prompt_file = Path(__file__).parent.parent / "prompt_library" / "prompts.yaml"
loader = PromptLoader(str(prompt_file))

mcp = FastMCP('Hive Spark Test Orchestrator')


# ── Session 1: DDL Extraction ──────────────────────────────────────────────

@mcp.tool(name="extract_or_load_ddl")
def extract_or_load_ddl(hive_file: str) -> str:
    """
    Session 1: Automatically extract table schemas from DDL files and HiveQL.
    Scans input/ddl/ for any DDL files, cross-references with HiveQL,
    fills gaps by inference. No user interaction.

    Args:
        hive_file: Path to the HiveQL file to analyze
    Returns:
        Prompt for LLM to execute DDL extraction
    """
    logger.info(f"Session 1: DDL extraction for {hive_file}")
    try:
        name = TP.query_name(hive_file)
        out = TP.gen_dir(name)
        out.mkdir(parents=True, exist_ok=True)
        return loader.get_prompt('ddl_extraction', hive_file=hive_file,
                                 schemas_file=out / "schemas.json")
    except Exception as e:
        logger.error(f"Error: {e}")
        return f"Error loading ddl_extraction prompt: {e}"


# ── Session 2: Test Data Generation ────────────────────────────────────────

@mcp.tool(name="generate_test_data")
def generate_test_data(schema_file: str, hive_file: str = "") -> str:
    """
    Session 2: Generate test data from schemas.
    Produces dialect-aware SQL scripts, sample_data.json, and per-table CSVs.
    Extracts WHERE clause literals from HiveQL to ensure test data passes filters.

    Args:
        schema_file: Path to schemas.json from Session 1
        hive_file: Path to the original HiveQL file (for WHERE clause literal extraction)
    Returns:
        Prompt for LLM to execute data generation
    """
    logger.info(f"Session 2: Test data generation from {schema_file} (hive: {hive_file})")
    try:
        out = Path(schema_file).parent   # generated_data/<query>/
        (out / "tables").mkdir(parents=True, exist_ok=True)
        return loader.get_prompt('test_data_generation', schema_file=schema_file,
                                 hive_file=hive_file, gen_dir=out)
    except Exception as e:
        logger.error(f"Error: {e}")
        return f"Error loading test_data_generation prompt: {e}"


# ── Session 3: Dependency Analysis ─────────────────────────────────────────

@mcp.tool(name="analyze_dependencies")
def analyze_dependencies(hive_file: str, schema_file: str, data_file: str) -> str:
    """
    Session 3: Analyze table dependencies, validate schemas and data.

    Args:
        hive_file: Path to HiveQL file
        schema_file: Path to schemas.json
        data_file: Path to sample_data.json
    Returns:
        Prompt for LLM to execute dependency analysis
    """
    logger.info(f"Session 3: Dependency analysis for {hive_file}")
    try:
        return loader.get_prompt(
            'dependency_analysis',
            hive_file=hive_file,
            schema_file=schema_file,
            data_file=data_file,
            gen_dir=Path(schema_file).parent,
        )
    except Exception as e:
        logger.error(f"Error: {e}")
        return f"Error loading dependency_analysis prompt: {e}"



# ── Session 4 (sub): Result Comparison ─────────────────────────────────────

@mcp.tool(name="compare_results")
def compare_results(hive_output_file: str, spark_output_file: str) -> str:
    """
    Session 4 sub-step: Compare Hive and Spark outputs row-by-row.

    Args:
        hive_output_file: Path to Hive stdout file
        spark_output_file: Path to Spark stdout file
    Returns:
        Prompt for LLM to execute comparison
    """
    logger.info(f"Session 4: Comparing {hive_output_file} vs {spark_output_file}")
    try:
        return loader.get_prompt(
            'result_comparison',
            hive_output_file=hive_output_file,
            spark_output_file=spark_output_file,
            exec_dir=Path(hive_output_file).parent.parent,   # execution_output/<query>/
        )
    except Exception as e:
        logger.error(f"Error: {e}")
        return f"Error loading result_comparison prompt: {e}"


# ── Session 4 (sub): MINUS Validation ──────────────────────────────────────

def _minus_paths(hive_results_file: str) -> dict:
    """CSV / JSON locations for the MINUS step of one query (host and container)."""
    exec_dir = Path(hive_results_file).parent.parent   # execution_output/<query>/
    name = exec_dir.name
    hive_csv = exec_dir / "hive" / f"{name}_results.csv"
    spark_csv = exec_dir / "spark" / f"{name}_results.csv"
    return {
        "query_name": name,
        "exec_dir": exec_dir,
        "hive_csv": hive_csv,
        "spark_csv": spark_csv,
        "hive_csv_container": TP.in_container(hive_csv),
        "spark_csv_container": TP.in_container(spark_csv),
    }


@mcp.tool(name="run_minus_validation")
def run_minus_validation(hive_results_file: str, spark_results_file: str) -> str:
    """
    Session 4 sub-step: Run MINUS/EXCEPT queries in both directions.
    This is the equivalence check (same result set on the generated test data).

    Args:
        hive_results_file: Path to Hive results file
        spark_results_file: Path to Spark results file
    Returns:
        Prompt for LLM to execute MINUS validation
    """
    logger.info(f"Session 4: MINUS validation — {hive_results_file} vs {spark_results_file}")
    try:
        return loader.get_prompt(
            'minus_validation',
            hive_results_file=hive_results_file,
            spark_results_file=spark_results_file,
            **_minus_paths(hive_results_file),
        )
    except Exception as e:
        logger.error(f"Error: {e}")
        return f"Error loading minus_validation prompt: {e}"


# ── Session 5: Report Generation ───────────────────────────────────────────

@mcp.tool(name="generate_test_report")
def generate_test_report(test_name: str) -> str:
    """
    Session 5: Generate comprehensive TEST_REPORT.md from all session outputs.

    Args:
        test_name: Name of the test (used in report title)
    Returns:
        Prompt for LLM to generate the report
    """
    logger.info(f"Session 5: Generating report for {test_name}")
    try:
        return loader.get_prompt('report_generation', test_name=test_name,
                                 gen_dir=TP.gen_dir(test_name),
                                 exec_dir=TP.exec_dir(test_name))
    except Exception as e:
        logger.error(f"Error: {e}")
        return f"Error loading report_generation prompt: {e}"


def main():
    """Run the MCP server."""
    mcp.run()


if __name__ == '__main__':
    main()

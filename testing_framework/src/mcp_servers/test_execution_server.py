"""
Hive Spark Test Execution — MCP Server
Container lifecycle and SQL execution tools.
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
        logging.FileHandler(log_dir / "test_execution.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Prompt loader
prompt_file = Path(__file__).parent.parent / "prompt_library" / "prompts.yaml"
loader = PromptLoader(str(prompt_file))

mcp = FastMCP('Hive Spark Test Execution')


def _execution_paths(query_file: str, engine: str) -> dict:
    """Per-query locations for executing one query on one engine (host and container)."""
    name = TP.query_name(query_file)
    out = TP.exec_dir(name) / engine
    out.mkdir(parents=True, exist_ok=True)
    prepared = out / f"{name}_prepared.sql"
    return {
        "query_name": name,
        "out_dir": out,
        "prepared_file": prepared,
        "prepared_file_container": TP.in_container(prepared),
        "variables_file": TP.gen_dir(name) / "variables.json",
        "hive_modifications_file": TP.exec_dir(name) / "hive" / f"{name}_modifications.json",
    }


@mcp.tool(name="start_test_containers")
def start_test_containers() -> str:
    """
    Start Hive and Spark test containers via finch-compose.
    Returns prompt with instructions for container startup and health checks.
    """
    logger.info("Starting test containers")
    try:
        return loader.get_prompt('container_startup', compose_file=TP.COMPOSE_FILE)
    except Exception as e:
        logger.error(f"Error: {e}")
        return f"Error loading container_startup prompt: {e}"


@mcp.tool(name="load_test_data_hive")
def load_test_data_hive(setup_file: str) -> str:
    """
    Load generated test data into Hive via beeline.

    Args:
        setup_file: Path to setup_hive.sql
    """
    logger.info(f"Loading Hive test data from {setup_file}")
    try:
        return loader.get_prompt('hive_data_loading', setup_file=setup_file,
                                 setup_file_container=TP.in_container(setup_file))
    except Exception as e:
        logger.error(f"Error: {e}")
        return f"Error loading hive_data_loading prompt: {e}"


@mcp.tool(name="load_test_data_spark")
def load_test_data_spark(setup_file: str) -> str:
    """
    Load generated test data into Spark via spark-sql.

    Args:
        setup_file: Path to setup_spark.sql
    """
    logger.info(f"Loading Spark test data from {setup_file}")
    try:
        return loader.get_prompt('spark_data_loading', setup_file=setup_file,
                                 setup_file_container=TP.in_container(setup_file))
    except Exception as e:
        logger.error(f"Error: {e}")
        return f"Error loading spark_data_loading prompt: {e}"


@mcp.tool(name="execute_hive_query")
def execute_hive_query(query_file: str) -> str:
    """
    Execute full HiveQL file (7000+ lines) in Hive container.
    Strips non-executable lines, captures output.

    Args:
        query_file: Path to HiveQL file
    """
    logger.info(f"Executing Hive query: {query_file}")
    try:
        return loader.get_prompt('hive_execution', query_file=query_file,
                                 **_execution_paths(query_file, "hive"))
    except Exception as e:
        logger.error(f"Error: {e}")
        return f"Error loading hive_execution prompt: {e}"


@mcp.tool(name="execute_spark_query")
def execute_spark_query(query_file: str) -> str:
    """
    Execute full Spark SQL file (7000+ lines) in Spark container.
    Strips non-executable lines, captures output.

    Args:
        query_file: Path to Spark SQL file
    """
    logger.info(f"Executing Spark query: {query_file}")
    try:
        return loader.get_prompt('spark_execution', query_file=query_file,
                                 **_execution_paths(query_file, "spark"))
    except Exception as e:
        logger.error(f"Error: {e}")
        return f"Error loading spark_execution prompt: {e}"


@mcp.tool(name="stop_test_containers")
def stop_test_containers() -> str:
    """
    Stop and remove Hive and Spark test containers.
    """
    logger.info("Stopping test containers")
    try:
        return loader.get_prompt('container_teardown', compose_file=TP.COMPOSE_FILE)
    except Exception as e:
        logger.error(f"Error: {e}")
        return f"Error loading container_teardown prompt: {e}"


def main():
    """Run the MCP server."""
    mcp.run()


if __name__ == '__main__':
    main()

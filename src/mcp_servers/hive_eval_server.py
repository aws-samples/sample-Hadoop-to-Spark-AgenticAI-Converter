"""
HiveQL Evaluation Server - Simple Spark SQL Validation
Returns prompts for LLM to validate converted Spark SQL in Finch/Spark container
"""
import logging
import sys
import os
from pathlib import Path
from fastmcp import FastMCP

# Add the project root to the Python path to import prompt_library
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from prompt_library.prompt_loader import load_prompt

# Setup logging
# Shared input/output locations (see project_paths.py at the repo root)
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import project_paths

log_dir = project_paths.LOG_DIR
log_dir.mkdir(exist_ok=True, parents=True)
log_file = log_dir / "hive_eval.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

mcp = FastMCP('HiveQL Evaluation Server')


def _validation_dir_for(sql_file: str) -> str:
    """Return the project-relative validation folder for a converted Spark SQL file.

    output/hive/spark_sql/x.sql          -> output/hive/validation
    output/impala/spark_sql/x/x_chunk.sql  -> output/impala/validation
    Files outside a ".../spark_sql/" folder fall back to output/hive/validation.
    """
    path = Path(sql_file).resolve()
    for parent in path.parents:
        if parent.name == "spark_sql":
            return project_paths.rel(parent.parent / "validation")
    return project_paths.rel(project_paths.validation_dir("hive"))


@mcp.tool(name="execute_spark_sql")
def execute_spark_sql(sql_file: str) -> str:
    """
    Returns prompt for LLM to execute Spark SQL file in Finch container.
    
    Args:
        sql_file: Path to Spark SQL file to execute
        
    Returns:
        Prompt from prompts.yaml for executing SQL
    """
    logger.info(f"Loading execution prompt for: {sql_file}")
    
    try:
        # Read the prompt template directly
        prompt_file = Path(__file__).parent.parent / "prompt_library" / "prompts.yaml"
        
        with open(prompt_file, 'r') as f:
            import yaml
            prompts = yaml.safe_load(f)
        
        template = prompts['finch_testing']['template']
        
        # Replace placeholders. The validation report goes to the "validation" folder
        # that sits next to the "spark_sql" folder holding the converted file, so
        # HiveQL and Impala results stay separate (output/<dialect>/validation/).
        prompt = template.replace('{sql_file}', sql_file)
        prompt = prompt.replace('{validation_dir}', _validation_dir_for(sql_file))
        
        return prompt
        
    except Exception as e:
        error_msg = f"Error generating testing prompt: {str(e)}"
        logger.error(error_msg)
        return error_msg





def main():
    """Run the MCP server"""
    mcp.run()


if __name__ == '__main__':
    main()

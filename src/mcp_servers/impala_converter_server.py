"""
Impala to Spark SQL Converter MCP Server
Handles Impala-specific SQL constructs: COMPUTE STATS, NDV, APPX_MEDIAN,
Kudu DML (UPSERT/UPDATE/DELETE), performance hints, TABLESAMPLE, etc.
"""
import os
import re
import logging
import sys
from pathlib import Path
from fastmcp import FastMCP

# Add the project root to the Python path to import prompt_library
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Setup logging
# Shared input/output locations (see project_paths.py at the repo root)
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import project_paths

log_dir = project_paths.LOG_DIR
log_dir.mkdir(exist_ok=True, parents=True)
log_file = log_dir / "impala_converter.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

mcp = FastMCP('Impala to Spark SQL Converter')


def _load_impala_prompt(prompt_name: str) -> str:
    """Load a prompt template from impala_prompts.yaml"""
    import yaml
    prompt_file = Path(__file__).parent.parent / "prompt_library" / "impala_prompts.yaml"
    with open(prompt_file, 'r') as f:
        prompts = yaml.safe_load(f)
    if prompt_name not in prompts:
        raise ValueError(f"Prompt '{prompt_name}' not found in impala_prompts.yaml")
    return prompts[prompt_name]['template']


@mcp.tool(name="identify_impala_file")
def identify_impala_file(file_path: str) -> str:
    """
    Identifies Impala SQL file and analyzes its structure, detecting
    Impala-specific constructs like COMPUTE STATS, NDV, Kudu DML, etc.

    Args:
        file_path: Path to Impala SQL file

    Returns:
        Analysis of file structure, Impala-specific constructs, and complexity
    """
    logger.info(f"Analyzing Impala SQL file: {file_path}")

    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()

        lines = content.split('\n')
        total_lines = len(lines)

        # Count key SQL patterns
        cte_count = len(re.findall(r'\bWITH\s+\w+\s+AS\s*\(', content, re.IGNORECASE))
        union_count = len(re.findall(r'\bUNION\s+ALL\b', content, re.IGNORECASE))
        insert_count = len(re.findall(r'\bINSERT\s+(OVERWRITE|INTO)\b', content, re.IGNORECASE))
        select_count = len(re.findall(r'\bSELECT\b', content, re.IGNORECASE))

        # Impala-specific constructs
        invalidate_metadata = len(re.findall(r'\bINVALIDATE\s+METADATA\b', content, re.IGNORECASE))
        refresh_count = len(re.findall(r'\bREFRESH\s+\w+', content, re.IGNORECASE))
        compute_stats = len(re.findall(r'\bCOMPUTE\s+(INCREMENTAL\s+)?STATS\b', content, re.IGNORECASE))
        ndv_count = len(re.findall(r'\bNDV\s*\(', content, re.IGNORECASE))
        appx_median_count = len(re.findall(r'\bAPPX_MEDIAN\s*\(', content, re.IGNORECASE))
        broadcast_hints = len(re.findall(r'/\*\s*\+BROADCAST', content, re.IGNORECASE))
        straight_join = len(re.findall(r'\bSTRAIGHT_JOIN\b', content, re.IGNORECASE))
        tablesample = len(re.findall(r'\bTABLESAMPLE\s+SYSTEM\s*\(', content, re.IGNORECASE))
        upsert_count = len(re.findall(r'\bUPSERT\s+INTO\b', content, re.IGNORECASE))
        update_count = len(re.findall(r'\bUPDATE\s+\w+.*\bSET\b', content, re.IGNORECASE))
        delete_count = len(re.findall(r'\bDELETE\s+FROM\b', content, re.IGNORECASE))
        kudu_storage = len(re.findall(r'\bSTORED\s+AS\s+KUDU\b', content, re.IGNORECASE))
        nvl_count = len(re.findall(r'\bNVL\s*\(', content, re.IGNORECASE))
        trunc_count = len(re.findall(r'\bTRUNC\s*\([^)]+,\s*\'', content, re.IGNORECASE))
        ds_hll_count = len(re.findall(r'\bDS_HLL\b', content, re.IGNORECASE))
        sort_by = len(re.findall(r'\bSORT\s+BY\s*\(', content, re.IGNORECASE))
        create_function = len(re.findall(r'\bCREATE\s+(AGGREGATE\s+)?FUNCTION\b', content, re.IGNORECASE))
        fnv_hash_count = len(re.findall(r'\bfnv_hash\s*\(', content, re.IGNORECASE))

        # Detect UDFs
        udf_pattern = r'CREATE\s+(?:AGGREGATE\s+)?FUNCTION\s+(?:IF\s+NOT\s+EXISTS\s+)?(\w+\.?\w+)'
        udfs = re.findall(udf_pattern, content, re.IGNORECASE)

        # Complexity assessment
        impala_construct_count = (
            invalidate_metadata + refresh_count + compute_stats +
            ndv_count + appx_median_count + broadcast_hints +
            straight_join + tablesample + upsert_count +
            update_count + delete_count + kudu_storage +
            ds_hll_count + sort_by + create_function
        )

        if kudu_storage > 0 or upsert_count > 0 or ds_hll_count > 0 or create_function > 0:
            complexity = 'HIGH'
        elif impala_construct_count > 5 or total_lines > 500:
            complexity = 'MEDIUM'
        else:
            complexity = 'LOW'

        analysis = f"""
Impala SQL File Analysis: {Path(file_path).name}
{'='*60}
Total Lines: {total_lines}
CTEs (WITH clauses): {cte_count}
UNION ALL operations: {union_count}
INSERT statements: {insert_count}
SELECT statements: {select_count}
Custom UDFs/UDAFs: {len(udfs)} ({', '.join(udfs) if udfs else 'None'})

IMPALA-SPECIFIC CONSTRUCTS DETECTED:
{'─'*40}
  INVALIDATE METADATA:     {invalidate_metadata}
  REFRESH:                 {refresh_count}
  COMPUTE STATS:           {compute_stats}
  NDV():                   {ndv_count}
  APPX_MEDIAN():           {appx_median_count}
  BROADCAST hints:         {broadcast_hints}
  STRAIGHT_JOIN:           {straight_join}
  TABLESAMPLE SYSTEM:      {tablesample}
  UPSERT INTO:             {upsert_count}
  UPDATE ... SET:          {update_count}
  DELETE FROM:             {delete_count}
  STORED AS KUDU:          {kudu_storage}
  NVL():                   {nvl_count}
  TRUNC(ts, 'X'):          {trunc_count}
  DS_HLL functions:        {ds_hll_count}
  SORT BY:                 {sort_by}
  fnv_hash():              {fnv_hash_count}
  CREATE FUNCTION/UDAF:    {create_function}

Complexity: {complexity}
Total Impala-specific constructs: {impala_construct_count}

CONVERSION STRATEGY:
- {'Chunking recommended (large file)' if total_lines > 1000 else 'Process entire file in one pass'}
- {'Kudu DML → Delta Lake MERGE/UPDATE/DELETE' if kudu_storage > 0 else 'Standard SQL conversion'}
- {'UDF registration conversion needed' if create_function > 0 else 'No UDF conversion needed'}
- {'DS_HLL → approx_count_distinct() mapping' if ds_hll_count > 0 else ''}

Next Steps:
1. Use generate_spark_sql_from_impala for conversion
2. Use create_impala_mermaid for visualization
3. Use validate_impala_spark_sql for post-conversion validation
"""
        logger.info(f"Impala analysis complete: {total_lines} lines, complexity={complexity}")
        return analysis

    except Exception as e:
        error_msg = f"Error analyzing Impala file: {str(e)}"
        logger.error(error_msg)
        return error_msg


@mcp.tool(name="generate_spark_sql_from_impala")
def generate_spark_sql_from_impala(impala_file: str) -> str:
    """
    Generates Spark SQL from an Impala SQL file.
    Handles all Impala-specific constructs: NDV, APPX_MEDIAN, COMPUTE STATS,
    Kudu DML, performance hints, TABLESAMPLE, etc.

    Args:
        impala_file: Path to Impala SQL file

    Returns:
        Prompt for LLM to execute the Impala-to-Spark conversion
    """
    logger.info(f"Generating Spark SQL conversion prompt for Impala file: {impala_file}")

    try:
        template = _load_impala_prompt('impala_spark_sql_generation')
        return template.replace('{impala_file}', impala_file)
    except Exception as e:
        error_msg = f"Error generating Impala conversion prompt: {str(e)}"
        logger.error(error_msg)
        return error_msg


@mcp.tool(name="validate_impala_spark_sql")
def validate_impala_spark_sql(sql_file: str, source_impala_file: str = "") -> str:
    """
    Validates converted Spark SQL against the original Impala source.
    Checks for unconverted Impala constructs and completeness.

    Args:
        sql_file: Path to converted Spark SQL file
        source_impala_file: Path to original Impala SQL file for comparison

    Returns:
        Prompt for LLM to execute validation
    """
    logger.info(f"Generating validation prompt for: {sql_file}")

    try:
        template = _load_impala_prompt('impala_validation')
        prompt = template.replace('{sql_file}', sql_file)
        prompt = prompt.replace('{source_impala_file}', source_impala_file)
        return prompt
    except Exception as e:
        error_msg = f"Error generating validation prompt: {str(e)}"
        logger.error(error_msg)
        return error_msg


@mcp.tool(name="fix_impala_conversion_errors")
def fix_impala_conversion_errors(sql_file: str, error_details: str) -> str:
    """
    Fixes conversion errors in Impala-to-Spark converted files.
    Handles Impala-specific error patterns like unconverted NDV, APPX_MEDIAN, etc.

    Args:
        sql_file: Path to Spark SQL file with errors
        error_details: Detailed error information from validation

    Returns:
        Prompt for LLM to execute error correction
    """
    logger.info(f"Generating Impala error correction prompt for: {sql_file}")

    try:
        template = _load_impala_prompt('impala_error_correction')
        prompt = template.replace('{sql_file}', sql_file)
        prompt = prompt.replace('{error_details}', error_details)
        return prompt
    except Exception as e:
        error_msg = f"Error generating error correction prompt: {str(e)}"
        logger.error(error_msg)
        return error_msg


@mcp.tool(name="create_impala_mermaid")
def create_impala_mermaid(impala_file: str) -> str:
    """
    Creates a comprehensive mermaid diagram for an Impala SQL file.
    Highlights Impala-specific operations like COMPUTE STATS, Kudu DML, etc.

    Args:
        impala_file: Path to Impala SQL file

    Returns:
        Prompt for LLM to create the mermaid diagram
    """
    logger.info(f"Generating Impala mermaid prompt for: {impala_file}")

    try:
        template = _load_impala_prompt('impala_mermaid_generation')
        return template.replace('{impala_file}', impala_file)
    except Exception as e:
        error_msg = f"Error generating mermaid prompt: {str(e)}"
        logger.error(error_msg)
        return error_msg


@mcp.tool(name="chunk_impala_file")
def chunk_impala_file(file_path: str) -> str:
    """
    Chunks a large Impala SQL file into logical segments for processing.
    Respects Impala-specific boundaries (COMPUTE STATS, Kudu DML blocks).

    Args:
        file_path: Path to Impala SQL file to chunk

    Returns:
        Prompt for LLM to execute chunking
    """
    logger.info(f"Generating Impala chunking prompt for: {file_path}")

    try:
        template = _load_impala_prompt('impala_chunking')
        return template.replace('{file_path}', file_path)
    except Exception as e:
        error_msg = f"Error generating chunking prompt: {str(e)}"
        logger.error(error_msg)
        return error_msg


@mcp.tool(name="merge_impala_spark_chunks")
def merge_impala_spark_chunks(chunk_directory: str) -> str:
    """
    Merges converted Spark SQL chunks from Impala source into a single file.
    Handles ANALYZE TABLE ordering and UDF deduplication.

    Args:
        chunk_directory: Path to directory containing converted Spark SQL chunks

    Returns:
        Prompt for LLM to execute merging
    """
    logger.info(f"Generating Impala merging prompt for: {chunk_directory}")

    try:
        template = _load_impala_prompt('impala_merging')
        return template.replace('{chunk_directory}', chunk_directory)
    except Exception as e:
        error_msg = f"Error generating merging prompt: {str(e)}"
        logger.error(error_msg)
        return error_msg


def main():
    """Run the MCP server"""
    mcp.run()


if __name__ == '__main__':
    main()

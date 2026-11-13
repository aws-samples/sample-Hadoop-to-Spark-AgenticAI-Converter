"""
HiveQL to Spark SQL Converter MCP Server
Main conversion agent for HiveQL transformation
"""
import os
import re
import logging
import sys
from pathlib import Path
from typing import List, Dict, Optional
from datetime import datetime
from fastmcp import FastMCP
from pydantic import BaseModel, Field

# Add the project root to the Python path to import prompt_library
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from prompt_library.prompt_loader import load_prompt

# Setup logging
# Shared input/output locations (see project_paths.py at the repo root)
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import project_paths

log_dir = project_paths.LOG_DIR
log_dir.mkdir(exist_ok=True, parents=True)
log_file = log_dir / "hive_converter.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

mcp = FastMCP('HiveQL to Spark SQL Converter')




@mcp.tool(name="identify_hive_file")
def identify_hive_file(file_path: str) -> str:
    """
    Identifies HiveQL file and analyzes its structure.
    
    Args:
        file_path: Path to HiveQL file
        
    Returns:
        Analysis of file structure and complexity
    """
    logger.info(f"Analyzing HiveQL file: {file_path}")
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        # Analyze file
        lines = content.split('\n')
        total_lines = len(lines)
        
        # Count key patterns
        cte_count = len(re.findall(r'\bWITH\s+\w+\s+AS\s*\(', content, re.IGNORECASE))
        union_count = len(re.findall(r'\bUNION\s+ALL\b', content, re.IGNORECASE))
        insert_count = len(re.findall(r'\bINSERT\s+(OVERWRITE|INTO)\b', content, re.IGNORECASE))
        select_count = len(re.findall(r'\bSELECT\b', content, re.IGNORECASE))
        
        # Detect UDFs
        udf_pattern = r'create\s+temporary\s+function\s+(\w+)'
        udfs = re.findall(udf_pattern, content, re.IGNORECASE)
        
        analysis = f"""
HiveQL File Analysis: {Path(file_path).name}
{'='*60}
Total Lines: {total_lines}
CTEs (WITH clauses): {cte_count}
UNION ALL operations: {union_count}
INSERT statements: {insert_count}
SELECT statements: {select_count}
Custom UDFs: {len(udfs)} ({', '.join(udfs) if udfs else 'None'})

Complexity: {'HIGH' if total_lines > 2000 else 'MEDIUM' if total_lines > 500 else 'LOW'}

CONVERSION STRATEGY: Process entire file in one pass (no chunking)
- LLM can handle large context windows
- Preserves all transformations and dependencies
- Maintains query relationships and CTEs
- Produces single, coherent Spark SQL output

Next Steps:
1. Proceed directly to conversion (no chunking needed)
2. Use create_detailed_mermaid for visualization
3. Use generate_spark_sql for complete conversion
"""
        
        logger.info(f"Analysis complete: {total_lines} lines, {cte_count} CTEs")
        return analysis
        
    except Exception as e:
        error_msg = f"Error analyzing file: {str(e)}"
        logger.error(error_msg)
        return error_msg







@mcp.tool(name="generate_spark_sql")
def generate_spark_sql(hive_file: str) -> str:
    """
    Generates Spark SQL from HiveQL file.
    
    Args:
        hive_file: Path to HiveQL file
        
    Returns:
        Instructions for Spark SQL generation
    """
    logger.info(f"Generating Spark SQL conversion prompt for: {hive_file}")
    
    try:
        prompt_file = Path(__file__).parent.parent / "prompt_library" / "prompts.yaml"
        
        with open(prompt_file, 'r') as f:
            import yaml
            prompts = yaml.safe_load(f)
        
        template = prompts['spark_sql_generation']['template']
        prompt = template.replace('{hive_file}', hive_file)
        
        return prompt
        
    except Exception as e:
        error_msg = f"Error generating Spark SQL prompt: {str(e)}"
        logger.error(error_msg)
        return error_msg


@mcp.tool(name="chunk_hive_file")
def chunk_hive_file(file_path: str) -> str:
    """
    Returns the generalized_file_chunking prompt for manual chunking.
    
    This is a simple tool that returns the comprehensive chunking prompt
    which instructs the LLM to:
    - Analyze file structure (UNION ALL boundaries, INSERT statements)
    - Identify natural safe boundaries
    - Create 5-6 logical chunks preserving complete transformations
    - Use sed commands to extract exact line ranges
    - Verify line counts match
    - Save chunks to output/hive/chunks/{file_name}/{file_name}_chunk_001.hql, etc.
    
    Args:
        file_path: Path to HiveQL file to chunk
        
    Returns:
        Comprehensive chunking prompt from prompts.yaml
    """
    logger.info(f"Generating generalized chunking prompt for: {file_path}")
    
    try:
        # Read the prompt template directly and only replace file_path
        # Other placeholders like {workspace_root}, {base_filename} are examples for the LLM
        prompt_file = Path(__file__).parent.parent / "prompt_library" / "prompts.yaml"
        
        with open(prompt_file, 'r') as f:
            import yaml
            prompts = yaml.safe_load(f)
        
        template = prompts['generalized_file_chunking']['template']
        
        # Only replace {file_path}, leave other placeholders as-is for the LLM to interpret
        prompt = template.replace('{file_path}', file_path)
        
        return prompt
        
    except FileNotFoundError:
        error_msg = f"Error: File not found: {file_path}"
        logger.error(error_msg)
        return error_msg
    except Exception as e:
        error_msg = f"Error generating chunking prompt: {str(e)}"
        logger.error(error_msg)
        return error_msg


@mcp.tool(name="create_detailed_mermaid")
def create_detailed_mermaid(hive_file: str) -> str:
    """
    Returns prompt for LLM to create comprehensive mermaid diagram.
    
    Args:
        hive_file: Path to HiveQL file
        
    Returns:
        Prompt string for LLM to execute mermaid diagram creation
    """
    logger.info(f"Generating mermaid prompt for: {hive_file}")
    
    try:
        prompt_file = Path(__file__).parent.parent / "prompt_library" / "prompts.yaml"
        
        with open(prompt_file, 'r') as f:
            import yaml
            prompts = yaml.safe_load(f)
        
        template = prompts['mermaid_generation']['template']
        prompt = template.replace('{hive_file}', hive_file)
        
        return prompt
        
    except Exception as e:
        error_msg = f"Error generating mermaid prompt: {str(e)}"
        logger.error(error_msg)
        return error_msg


@mcp.tool(name="validate_spark_sql")
def validate_spark_sql(sql_file: str) -> str:
    """
    Returns prompt for LLM to validate converted Spark SQL.
    
    Args:
        sql_file: Path to Spark SQL file to validate
        
    Returns:
        Prompt string for LLM to execute validation
    """
    logger.info(f"Generating validation prompt for: {sql_file}")
    
    try:
        prompt_file = Path(__file__).parent.parent / "prompt_library" / "prompts.yaml"
        
        with open(prompt_file, 'r') as f:
            import yaml
            prompts = yaml.safe_load(f)
        
        template = prompts['validation']['template']
        prompt = template.replace('{sql_file}', sql_file)
        
        return prompt
        
    except Exception as e:
        error_msg = f"Error generating validation prompt: {str(e)}"
        logger.error(error_msg)
        return error_msg


@mcp.tool(name="fix_conversion_errors")
def fix_conversion_errors(sql_file: str, error_details: str) -> str:
    """
    Returns prompt for LLM to fix conversion errors.
    
    Args:
        sql_file: Path to Spark SQL file with errors
        error_details: Detailed error information from validation
        
    Returns:
        Prompt string for LLM to execute error correction
    """
    logger.info(f"Generating error correction prompt for: {sql_file}")
    
    try:
        prompt_file = Path(__file__).parent.parent / "prompt_library" / "prompts.yaml"
        
        with open(prompt_file, 'r') as f:
            import yaml
            prompts = yaml.safe_load(f)
        
        template = prompts['error_correction']['template']
        prompt = template.replace('{sql_file}', sql_file).replace('{error_details}', error_details)
        
        return prompt
        
    except Exception as e:
        error_msg = f"Error generating error correction prompt: {str(e)}"
        logger.error(error_msg)
        return error_msg


@mcp.tool(name="merge_spark_sql_chunks")
def merge_spark_sql_chunks(chunk_directory: str) -> str:
    """
    Returns prompt for LLM to intelligently merge Spark SQL chunks.
    
    Args:
        chunk_directory: Path to directory containing Spark SQL chunk files
        
    Returns:
        Prompt string for LLM to execute merging
    """
    logger.info(f"Generating merging prompt for: {chunk_directory}")
    
    try:
        prompt_file = Path(__file__).parent.parent / "prompt_library" / "prompts.yaml"
        
        with open(prompt_file, 'r') as f:
            import yaml
            prompts = yaml.safe_load(f)
        
        template = prompts['smart_merging']['template']
        prompt = template.replace('{chunk_directory}', chunk_directory)
        
        return prompt
        
    except Exception as e:
        error_msg = f"Error generating merging prompt: {str(e)}"
        logger.error(error_msg)
        return error_msg





def main():
    """Run the MCP server"""
    mcp.run()


if __name__ == '__main__':
    main()

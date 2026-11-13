"""LLM dispatch — handles tasks that require LLM reasoning.

Some MCP tools return prompts (instructions for an LLM to execute).
This module provides the bridge: it takes the prompt from an MCP tool
and sends it to an LLM to get the actual work done.

Backends (CLI-based, select at runtime via --backend):
  1. kiro    — spawns kiro-cli (default; reads .kiro/settings/mcp.json)
  2. claude  — spawns the Claude Code CLI (reads .claude/settings.json)

Both CLIs read their own MCP + permissions config from the project root, so
the same prompt works unchanged across backends — only the CLI invocation
differs. A direct Bedrock/Claude API path is also available for callers that
do not want a subprocess.
"""
import asyncio
import logging
from pathlib import Path
from typing import Optional

from llm_cli import claude_print_argv, kiro_chat_argv, run_llm_cli

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).parent.parent.resolve()

# Supported CLI backends. "kiro" preserves the original behavior.
AVAILABLE_BACKENDS = ("kiro", "claude")
DEFAULT_BACKEND = "kiro"

# Tools that return prompts (need LLM to execute)
LLM_REQUIRED_TOOLS = {
    # hive-converter
    "generate_spark_sql",
    "chunk_hive_file",
    "create_detailed_mermaid",
    "validate_spark_sql",
    "fix_conversion_errors",
    "merge_spark_sql_chunks",
    # hive-eval
    "execute_spark_sql",
    # mwaa-converter — parse and plan return prompts for LLM execution
    "parse_control_m_job",
    "parse_airflow_dag",
    "generate_conversion_plan",
    "create_orchestration_mermaid",
    "merge_conversion_reports",
    "merge_airflow_variables",
    # test-orchestrator — all session tools return prompts for LLM execution
    "extract_or_load_ddl",
    "generate_test_data",
    "analyze_dependencies",
    "compare_results",
    "run_minus_validation",
    "generate_test_report",
    # test-execution — container and execution tools return prompts
    "start_test_containers",
    "load_test_data_hive",
    "load_test_data_spark",
    "execute_hive_query",
    "execute_spark_query",
    "stop_test_containers",
    # impala-converter — all return prompts for LLM execution
    "generate_spark_sql_from_impala",
    "validate_impala_spark_sql",
    "fix_impala_conversion_errors",
    "create_impala_mermaid",
    "chunk_impala_file",
    "merge_impala_spark_chunks",
}

# Tools that are deterministic (MCP tool does the actual work, no LLM needed)
DETERMINISTIC_TOOLS = {
    # hive-converter
    "identify_hive_file",
    # impala-converter
    "identify_impala_file",
    # mwaa-converter — validate tools read files LLM wrote, schema-check, fallback if needed
    "validate_parsed_job_ir",
    "validate_parsed_airflow_job_ir",
    "validate_conversion_plan",
    "generate_dag_code",          # Jinja2 templates; LLM_FALLBACK prefix if template fails
    # mwaa-validator — all deterministic
    "validate_syntax",
    "validate_structure",
    "validate_security",
    "validate_dagbag",
    "fix_validation_errors",      # deterministic regex fixes; LLM_FALLBACK if needed
    # accuracy-calculator
    "analyze_accuracy",
    "generate_accuracy_report",
    "generate_dashboard",
}


def needs_llm(tool_name: str) -> bool:
    """Check if a tool returns a prompt that needs LLM execution."""
    return tool_name in LLM_REQUIRED_TOOLS


# Prefix returned by hybrid tools when deterministic path fails
LLM_FALLBACK_PREFIX = "LLM_FALLBACK:"


def needs_llm_fallback(result: str) -> bool:
    """Check if a deterministic tool's result is requesting LLM fallback."""
    return isinstance(result, str) and result.startswith(LLM_FALLBACK_PREFIX)


def extract_fallback_prompt(result: str) -> str:
    """Extract the LLM prompt from a fallback result."""
    return result[len(LLM_FALLBACK_PREFIX):].strip()


async def execute_via_kiro(prompt: str, timeout: int = 600) -> str:
    """Send a prompt to kiro-cli for LLM execution.

    This is the current approach — spawns a kiro-cli session that has
    access to all MCP tools and can read/write files.

    Args:
        prompt: The prompt text (returned by an MCP tool)
        timeout: Max seconds to wait

    Returns:
        kiro-cli stdout (the LLM's response/actions)
    """
    logger.info("Dispatching to kiro-cli (%d char prompt)", len(prompt))

    def _run():
        return run_llm_cli(kiro_chat_argv(prompt), timeout=timeout, cwd=PROJECT_ROOT)

    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, _run)

    if result.returncode != 0:
        logger.warning("kiro-cli returned code %d", result.returncode)
        if result.stderr:
            logger.warning("kiro-cli stderr: %s", result.stderr[:500])

    return result.stdout


async def execute_via_claude(
    prompt: str,
    timeout: int = 600,
    model: Optional[str] = None,
    permission_mode: str = "bypassPermissions",
) -> str:
    """Send a prompt to the Claude Code CLI for LLM execution.

    Mirrors the kiro-cli path: spawns a non-interactive `claude` session that
    has access to all MCP tools (from .claude/settings.json) and can read/write
    files. The same prompt returned by an MCP tool is used unchanged.

    Args:
        prompt: The prompt text (returned by an MCP tool)
        timeout: Max seconds to wait
        model: Optional Claude model alias (e.g. "sonnet", "opus")
        permission_mode: Claude Code permission mode. "bypassPermissions"
            matches kiro-cli's --trust-all-tools for unattended runs.

    Returns:
        claude stdout (the LLM's response/actions)
    """
    logger.info("Dispatching to claude (%d char prompt)", len(prompt))

    cmd = claude_print_argv(prompt, permission_mode=permission_mode, model=model)

    def _run():
        return run_llm_cli(cmd, timeout=timeout, cwd=PROJECT_ROOT)

    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, _run)

    if result.returncode != 0:
        logger.warning("claude returned code %d", result.returncode)
        if result.stderr:
            logger.warning("claude stderr: %s", result.stderr[:500])

    return result.stdout


# Registry of CLI dispatch functions, keyed by backend name.
_BACKEND_DISPATCH = {
    "kiro": execute_via_kiro,
    "claude": execute_via_claude,
}


async def execute_via_llm(
    prompt: str,
    backend: str = DEFAULT_BACKEND,
    timeout: int = 600,
) -> str:
    """Dispatch a prompt to the selected CLI backend.

    This is the backend-agnostic entry point. Callers pass the backend name
    ("kiro" or "claude"); the matching CLI is invoked. Both backends read
    their own MCP/permissions config from the project root, so the prompt is
    identical across backends.

    Args:
        prompt: The prompt text (returned by an MCP tool)
        backend: Backend identifier — one of AVAILABLE_BACKENDS
        timeout: Max seconds to wait

    Returns:
        The CLI's stdout (the LLM's response/actions)
    """
    dispatch = _BACKEND_DISPATCH.get(backend)
    if dispatch is None:
        raise ValueError(
            f"Unknown backend: {backend!r}. Available: {list(AVAILABLE_BACKENDS)}"
        )
    return await dispatch(prompt, timeout=timeout)


async def execute_via_bedrock(
    prompt: str,
    model_id: str = "anthropic.claude-sonnet-4-20250514",
    region: str = "us-east-1",
    max_tokens: int = 16000,
) -> str:
    """Send a prompt directly to Bedrock Claude API.

    This is the faster approach — no subprocess, direct API call.
    Requires boto3 and appropriate AWS credentials.

    Args:
        prompt: The prompt text
        model_id: Bedrock model ID
        region: AWS region
        max_tokens: Max response tokens

    Returns:
        LLM response text
    """
    try:
        import boto3
        import json

        def _call():
            client = boto3.client("bedrock-runtime", region_name=region)
            body = json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": max_tokens,
                "messages": [{"role": "user", "content": prompt}],
            })
            response = client.invoke_model(modelId=model_id, body=body)
            result = json.loads(response["body"].read())
            return result["content"][0]["text"]

        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, _call)

    except ImportError:
        logger.error("boto3 not installed — falling back to kiro-cli")
        return await execute_via_kiro(prompt)
    except Exception as e:
        logger.error("Bedrock call failed: %s — falling back to kiro-cli", e)
        return await execute_via_kiro(prompt)

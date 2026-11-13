"""Stage 7: Create Mermaid orchestration diagrams via kiro-cli MCP calls."""
import logging
import subprocess

from llm_cli import kiro_chat_argv, run_llm_cli
from pathlib import Path
from typing import List, Dict, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)


def create_mermaid_parallel(
    plans: List[Dict],
    output_dir: Path,
    progress: Dict,
    save_progress_fn,
    cwd: str,
    max_workers: int = 4,
) -> List[Path]:
    """Create mermaid diagrams in parallel."""
    results: List[Path] = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                _create_mermaid, plan, output_dir, progress, save_progress_fn, cwd,
            ): plan
            for plan in plans
        }
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    results.append(result)
            except OSError as e:
                logger.error("Mermaid creation failed: %s", e)
    return results


def _create_mermaid(
    plan: Dict,
    output_dir: Path,
    progress: Dict,
    save_progress_fn,
    cwd: str,
) -> Optional[Path]:
    """Create mermaid diagram for a single plan via MCP."""
    chunk_id = plan["chunk_id"]

    if chunk_id in progress["mermaid_created"]:
        logger.info("[SKIP] Already created mermaid: %s", chunk_id)
        mermaid_file = output_dir / "mermaid" / f"{chunk_id}_orchestration.md"
        return mermaid_file if mermaid_file.exists() else None

    logger.info("Creating mermaid diagram: %s", chunk_id)

    try:
        plan_file = output_dir / "plans" / f"{chunk_id}_plan.json"
        mermaid_file = output_dir / "mermaid" / f"{chunk_id}_orchestration.md"

        prompt = (
            f'Use mcp_mwaa_converter_create_orchestration_mermaid tool with:\n'
            f'- conversion_plan_file: "{plan_file}"\n\n'
            f'This returns a mermaid generation prompt. Execute it to:\n'
            f'1. Read conversion plan JSON\n'
            f'2. Create mermaid flowchart showing source jobs → target tasks\n'
            f'3. Color code by operator type\n'
            f'4. Save to: {mermaid_file}\n\n'
            f'You MUST save the mermaid diagram markdown file.\nExecute now.'
        )

        run_llm_cli(kiro_chat_argv(prompt), timeout=600, cwd=cwd)

        if mermaid_file.exists():
            logger.info("✓ Created mermaid: %s", chunk_id)
            progress["mermaid_created"].append(chunk_id)
            save_progress_fn()
            return mermaid_file

        logger.warning("⚠ Mermaid creation incomplete: %s", chunk_id)
        return None

    except subprocess.TimeoutExpired:
        logger.error("✗ Mermaid creation timeout: %s", chunk_id)
        return None
    except OSError as e:
        logger.error("✗ Mermaid creation error: %s - %s", chunk_id, e)
        return None

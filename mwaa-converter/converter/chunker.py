"""Stage 2: Group jobs into chunks by SMART_FOLDER boundaries.

Each JobIR file produced by parser.py represents one SMART_FOLDER.
One SMART_FOLDER = one chunk = one DAG. No arbitrary splitting.
"""
import json
import logging
from pathlib import Path
from typing import Dict, List

logger = logging.getLogger(__name__)


def create_chunks(
    job_ir_dir: Path,
) -> List[Dict]:
    """Read all JobIR JSON files and return one chunk per SMART_FOLDER.

    Args:
        job_ir_dir: Directory containing *_job_ir.json files written by parser.

    Returns:
        List of chunk dicts:
            {
                "chunk_id":  str,   # safe folder name (used as DAG id base)
                "folder":    str,   # original SMART_FOLDER name
                "job_ir_file": Path,
                "job_count": int,
                "jobs":      list,  # raw job dicts from JobIR
            }
    """
    job_ir_files = sorted(job_ir_dir.glob("*.json"))
    if not job_ir_files:
        logger.warning("No JobIR files found in %s", job_ir_dir)
        return []

    chunks: List[Dict] = []

    for job_ir_file in job_ir_files:
        try:
            with open(job_ir_file, encoding="utf-8") as f:
                job_ir = json.load(f)

            folder = job_ir.get("folder", job_ir_file.stem)
            jobs = job_ir.get("jobs", [])

            if not jobs:
                logger.warning("No jobs in %s — skipping", job_ir_file.name)
                continue

            chunk_id = job_ir_file.stem  # already safe filename from parser

            chunks.append({
                "chunk_id":    chunk_id,
                "folder":      folder,
                "job_ir_file": job_ir_file,
                "job_count":   len(jobs),
                "jobs":        jobs,
            })
            logger.info("  Chunk '%s': %d jobs (folder: %s)", chunk_id, len(jobs), folder)

        except (json.JSONDecodeError, OSError, KeyError) as e:
            logger.error("Error reading %s: %s", job_ir_file.name, e)

    logger.info("Created %d chunk(s) from %d JobIR file(s)", len(chunks), len(job_ir_files))
    return chunks

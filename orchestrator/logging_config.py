"""Unified logging and tracing — propagates run_id across all agents."""
import json
import logging
import sys
import time
from contextvars import ContextVar
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import project_paths

# Context variable for run_id propagation
current_run_id: ContextVar[str] = ContextVar("current_run_id", default="no_run")
current_agent: ContextVar[str] = ContextVar("current_agent", default="orchestrator")


class StructuredFormatter(logging.Formatter):
    """JSON-structured log formatter with run_id and agent context."""

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": datetime.fromtimestamp(record.created).isoformat(),
            "level": record.levelname,
            "run_id": current_run_id.get("no_run"),
            "agent": current_agent.get("orchestrator"),
            "module": record.module,
            "msg": record.getMessage(),
        }
        if record.exc_info and record.exc_info[0]:
            entry["exception"] = self.formatException(record.exc_info)
        return json.dumps(entry)


class HumanFormatter(logging.Formatter):
    """Human-readable formatter with run_id prefix."""

    def format(self, record: logging.LogRecord) -> str:
        run = current_run_id.get("no_run")
        agent = current_agent.get("orchestrator")
        ts = datetime.fromtimestamp(record.created).strftime("%H:%M:%S")
        return f"{ts} [{record.levelname}] [{run}] [{agent}] {record.getMessage()}"


class TaskTimer:
    """Context manager for timing tasks."""

    def __init__(self, task_id: str, agent: str):
        self.task_id = task_id
        self.agent = agent
        self._start = 0.0
        self.duration = 0.0

    def __enter__(self):
        self._start = time.time()
        return self

    def __exit__(self, *exc):
        self.duration = time.time() - self._start


def setup_logging(
    log_dir: str | Path = project_paths.LOG_DIR,
    run_id: str = "default",
    level: int = logging.INFO,
    structured: bool = False,
) -> logging.Logger:
    """Configure unified logging for an orchestration run.

    Args:
        log_dir: Directory for log files
        run_id: Run identifier for tracing
        level: Logging level
        structured: If True, use JSON format; otherwise human-readable

    Returns:
        Root logger configured with file + console handlers.
    """
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    current_run_id.set(run_id)

    root = logging.getLogger()
    root.setLevel(level)

    # Clear existing handlers to avoid duplicates
    root.handlers.clear()

    # Structured JSON log file (always)
    json_file = log_path / f"orchestrator_{run_id}.jsonl"
    json_handler = logging.FileHandler(json_file, encoding="utf-8")
    json_handler.setFormatter(StructuredFormatter())
    root.addHandler(json_handler)

    # Human-readable console
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(HumanFormatter() if not structured else StructuredFormatter())
    root.addHandler(console)

    # Human-readable log file
    text_file = log_path / f"orchestrator_{run_id}.log"
    text_handler = logging.FileHandler(text_file, encoding="utf-8")
    text_handler.setFormatter(HumanFormatter())
    root.addHandler(text_handler)

    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    return root

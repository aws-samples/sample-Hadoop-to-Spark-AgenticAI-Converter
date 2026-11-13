"""MWAA converter package.

Makes the project root importable for the shared root-level modules
(project_paths, llm_cli). This runs before any submodule is imported, so it
works no matter which entry point (MCP server, run_converter.py, orchestrator)
loads the package first.
"""
import sys
from pathlib import Path

_PROJECT_ROOT = str(Path(__file__).resolve().parents[2])
if _PROJECT_ROOT not in sys.path:
    sys.path.append(_PROJECT_ROOT)

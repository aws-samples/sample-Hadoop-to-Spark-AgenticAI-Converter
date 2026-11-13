"""Loads orchestrator_config.yaml and provides typed access."""
import logging
from pathlib import Path
from typing import Any, Dict

import yaml

import project_paths

logger = logging.getLogger(__name__)

_CONFIG_PATH = Path(__file__).parent / "orchestrator_config.yaml"


def load_orchestrator_config(path: str | Path = _CONFIG_PATH) -> Dict[str, Any]:
    """Load and return the full orchestrator config dict."""
    path = Path(path)
    if not path.exists():
        logger.warning("Config not found: %s — using empty config", path)
        return {}
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


class OrchestratorConfig:
    """Typed wrapper around orchestrator_config.yaml."""

    def __init__(self, path: str | Path = _CONFIG_PATH):
        self._cfg = load_orchestrator_config(path)

    # -- global ------------------------------------------------------------
    @property
    def max_workers(self) -> int:
        return self._cfg.get("global", {}).get("max_workers", 4)

    @property
    def max_retries(self) -> int:
        return self._cfg.get("global", {}).get("max_retries", 3)

    @property
    def log_dir(self) -> str:
        return self._cfg.get("global", {}).get("log_dir", project_paths.rel(project_paths.LOG_DIR))

    @property
    def state_dir(self) -> str:
        return self._cfg.get("global", {}).get("state_dir", project_paths.rel(project_paths.STATE_DIR))

    @property
    def mcp_json(self) -> str:
        return self._cfg.get("global", {}).get("mcp_json", ".kiro/settings/mcp.json")

    # -- per-agent config --------------------------------------------------
    def agent_config(self, agent_name: str) -> Dict[str, Any]:
        """Return config section for a specific agent."""
        return self._cfg.get("agents", {}).get(agent_name, {})

    # -- self-correction ---------------------------------------------------
    @property
    def max_corrections(self) -> int:
        return self._cfg.get("self_correction", {}).get("max_corrections_per_task", 3)

    @property
    def correction_rules(self) -> list:
        return self._cfg.get("self_correction", {}).get("rules", [])

    # -- event triggers ----------------------------------------------------
    @property
    def event_triggers(self) -> list:
        return self._cfg.get("event_triggers", [])

"""Agent Registry — loads agent_registry.yaml for capability discovery."""
import logging
from pathlib import Path
from typing import Dict, List, Optional

import yaml

logger = logging.getLogger(__name__)

_REGISTRY_PATH = Path(__file__).parent / "agent_registry.yaml"


class AgentInfo:
    """Parsed agent entry from the registry."""

    def __init__(self, name: str, data: Dict):
        self.name = name
        self.mcp_server: str = data["mcp_server"]
        self.description: str = data.get("description", "")
        self.capabilities: List[str] = data.get("capabilities", [])
        self.input_types: List[str] = data.get("input_types", [])
        self.output_types: List[str] = data.get("output_types", [])
        self.depends_on: List[str] = data.get("depends_on", [])
        self.tools: List[str] = data.get("tools", [])
        self.disabled: bool = data.get("disabled", False)


class AgentRegistry:
    """Reads agent_registry.yaml and provides lookup methods."""

    def __init__(self, path: str | Path = _REGISTRY_PATH):
        with open(path, encoding="utf-8") as f:
            raw = yaml.safe_load(f)
        self._agents: Dict[str, AgentInfo] = {
            name: AgentInfo(name, data)
            for name, data in raw.get("agents", {}).items()
        }
        logger.info("Registry loaded: %d agents", len(self._agents))

    def get(self, name: str) -> Optional[AgentInfo]:
        return self._agents.get(name)

    def all(self) -> List[AgentInfo]:
        return [a for a in self._agents.values() if not a.disabled]

    def find_by_capability(self, capability: str) -> List[AgentInfo]:
        return [a for a in self.all() if capability in a.capabilities]

    def find_by_input_type(self, ext: str) -> List[AgentInfo]:
        return [a for a in self.all() if ext in a.input_types]

    def find_by_tool(self, tool_name: str) -> Optional[AgentInfo]:
        for a in self.all():
            if tool_name in a.tools:
                return a
        return None

    def get_mcp_server(self, agent_name: str) -> Optional[str]:
        agent = self.get(agent_name)
        return agent.mcp_server if agent else None

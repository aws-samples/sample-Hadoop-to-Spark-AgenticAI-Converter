"""Native MCP client pool — replaces subprocess kiro-cli calls.

Usage:
    pool = MCPClientPool.from_mcp_json(".kiro/settings/mcp.json")
    async with pool:
        result = await pool.call("hive-converter", "generate_spark_sql", {"hive_file": "x.hql"})
"""
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, Dict, List

from fastmcp import Client
from fastmcp.client.transports import PythonStdioTransport

logger = logging.getLogger(__name__)

# Project root (where mcp.json lives)
_PROJECT_ROOT = Path(__file__).parent.parent.resolve()


class MCPClientPool:
    """Manages persistent connections to multiple MCP servers."""

    def __init__(self, server_configs: Dict[str, Dict[str, Any]], project_root: Path | None = None):
        self._configs = server_configs
        self._clients: Dict[str, Client] = {}
        self._root = Path(project_root) if project_root else _PROJECT_ROOT

    async def __aenter__(self):
        await self.connect_all()
        return self

    async def __aexit__(self, *exc):
        await self.close_all()

    async def connect(self, name: str) -> Client:
        if name in self._clients:
            return self._clients[name]

        cfg = self._configs.get(name)
        if not cfg:
            raise KeyError(f"Unknown MCP server: {name}")

        command = cfg["command"]
        args = cfg.get("args", [])
        cwd = cfg.get("cwd", "").replace("${workspaceFolder}", str(self._root))

        # Resolve ${workspaceFolder} in command path
        command = command.replace("${workspaceFolder}", str(self._root))

        script = args[0] if args else None
        if script and script.endswith(".py"):
            # Resolve script path relative to cwd or project root
            script_abs = str((Path(cwd or self._root) / script).resolve())

            # Build PYTHONPATH so the server's imports work
            script_dir = str(Path(script_abs).parent)
            server_root = cwd or str(self._root)
            pythonpath = os.pathsep.join([server_root, script_dir])

            # Determine the parent package dir for mwaa-converter servers
            # e.g. mwaa-converter/converter/mcp/server.py needs mwaa-converter/ on sys.path
            if "mwaa-converter" in script_abs:
                mwaa_root = str(self._root / "mwaa-converter")
                pythonpath = os.pathsep.join([mwaa_root, pythonpath])

            env = {**os.environ, "PYTHONPATH": pythonpath}

            transport = PythonStdioTransport(
                script_path=script_abs,
                python_cmd=command,
                env=env,
            )
        else:
            transport = {"mcpServers": {name: {"command": command, "args": args}}}

        client = Client(transport, name=name)
        await client.__aenter__()
        self._clients[name] = client

        tool_names = [t.name for t in await client.list_tools()]
        logger.info("Connected to %s — tools: %s", name, tool_names)
        return client

    async def connect_all(self):
        for name, cfg in self._configs.items():
            if cfg.get("disabled"):
                continue
            try:
                await self.connect(name)
            except Exception as e:
                logger.error("Failed to connect to %s: %s", name, e)

    async def close_all(self):
        for name, client in list(self._clients.items()):
            try:
                await client.__aexit__(None, None, None)
            except Exception:
                pass
        self._clients.clear()

    async def call(
        self,
        server: str,
        tool: str,
        arguments: Dict[str, Any] | None = None,
        timeout: float = 600,
    ) -> str:
        """Call a tool on a named MCP server. Returns text content."""
        if server not in self._clients:
            await self.connect(server)

        client = self._clients[server]
        logger.info("→ %s.%s(%s)", server, tool, arguments or {})

        result = await client.call_tool(tool, arguments or {}, timeout=timeout)

        if hasattr(result, "content"):
            text = "".join(b.text for b in result.content if hasattr(b, "text"))
        elif isinstance(result, str):
            text = result
        else:
            text = str(result)

        logger.info("← %s.%s → %d chars", server, tool, len(text))
        return text

    async def list_tools(self, server: str) -> List[Dict[str, str]]:
        if server not in self._clients:
            await self.connect(server)
        tools = await self._clients[server].list_tools()
        return [{"name": t.name, "description": t.description or ""} for t in tools]

    @staticmethod
    def from_mcp_json(mcp_json_path: str | Path) -> "MCPClientPool":
        with open(Path(mcp_json_path), encoding="utf-8") as f:
            data = json.load(f)
        return MCPClientPool(data.get("mcpServers", {}))

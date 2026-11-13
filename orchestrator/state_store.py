"""Unified state store — single source of truth for all orchestration runs."""
import json
import logging
import threading
from datetime import datetime
from pathlib import Path
from typing import Optional

import project_paths
from orchestrator.models import (
    AgentName, AgentResponse, AgentState, ArtifactInfo,
    RunState, RunStatus, TaskStatus,
)

logger = logging.getLogger(__name__)


class StateStore:
    """Thread-safe persistent state store for orchestration runs."""

    def __init__(self, state_dir: str | Path = project_paths.STATE_DIR):
        self._dir = Path(state_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._state: Optional[RunState] = None

    @property
    def state(self) -> Optional[RunState]:
        return self._state

    # -- run lifecycle -----------------------------------------------------

    def new_run(self, run_id: str, intent: str = "") -> RunState:
        self._state = RunState(run_id=run_id, intent=intent, status=RunStatus.RUNNING)
        self._save()
        logger.info("New run: %s — %s", run_id, intent)
        return self._state

    def load_run(self, run_id: str) -> Optional[RunState]:
        path = self._dir / f"{run_id}.json"
        if not path.exists():
            return None
        with open(path, encoding="utf-8") as f:
            self._state = RunState.model_validate_json(f.read())
        return self._state

    def complete_run(self, status: RunStatus = RunStatus.COMPLETED):
        if self._state:
            self._state.status = status
            self._state.completed_at = datetime.now()
            self._save()

    # -- agent state -------------------------------------------------------

    def init_agent(self, agent: AgentName):
        if not self._state:
            return
        self._state.agents[agent.value] = AgentState(agent=agent, status=TaskStatus.RUNNING)
        self._save()

    def update_agent(self, agent: AgentName, **kwargs):
        if not self._state:
            return
        state = self._state.agents.get(agent.value)
        if state:
            for k, v in kwargs.items():
                if hasattr(state, k):
                    setattr(state, k, v)
            self._save()

    # -- task tracking -----------------------------------------------------

    def record_task(self, response: AgentResponse):
        if not self._state:
            return
        self._state.tasks[response.task_id] = response

        agent_state = self._state.agents.get(response.agent.value)
        if agent_state:
            if response.status == TaskStatus.SUCCESS:
                agent_state.tasks_completed.append(response.task_id)
                agent_state.artifacts.extend(response.artifacts)
                agent_state.metrics.update(response.metrics)
            elif response.status == TaskStatus.FAILED:
                agent_state.tasks_failed.append(response.task_id)
                agent_state.last_error = response.errors[0] if response.errors else None

        # Track artifacts
        for artifact_path in response.artifacts:
            self._state.artifacts[artifact_path] = ArtifactInfo(
                path=artifact_path,
                produced_by=response.task_id,
                agent=response.agent,
            )

        self._save()

    def get_task(self, task_id: str) -> Optional[AgentResponse]:
        if not self._state:
            return None
        return self._state.tasks.get(task_id)

    def is_task_done(self, task_id: str) -> bool:
        task = self.get_task(task_id)
        return task is not None and task.status in (TaskStatus.SUCCESS, TaskStatus.SKIPPED)

    def all_deps_met(self, depends_on: list[str]) -> bool:
        return all(self.is_task_done(d) for d in depends_on)

    # -- persistence -------------------------------------------------------

    def _save(self):
        if not self._state:
            return
        with self._lock:
            path = self._dir / f"{self._state.run_id}.json"
            path.write_text(self._state.model_dump_json(indent=2), encoding="utf-8")

    def list_runs(self) -> list[str]:
        return [p.stem for p in self._dir.glob("*.json")]

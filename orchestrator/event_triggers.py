"""Artifact-based event triggers — dispatches tasks when dependencies are met.

Instead of strictly sequential execution, the Orchestrator can register
"watchers" that fire when a specific artifact is produced.
"""
import logging
from typing import Callable, Dict, List, Optional

from orchestrator.models import AgentRequest, AgentResponse, TaskStatus

logger = logging.getLogger(__name__)


class ArtifactTrigger:
    """Fires when a matching artifact is produced."""

    def __init__(
        self,
        name: str,
        pattern: str,                          # glob-like suffix match (e.g. ".sql", "_FINAL.sql")
        source_agent: str,                     # agent that produces the artifact
        request_factory: Callable[[AgentResponse, str], AgentRequest],  # (response, artifact_path) -> request
    ):
        self.name = name
        self.pattern = pattern
        self.source_agent = source_agent
        self.request_factory = request_factory


class ArtifactEventBus:
    """Evaluates completed tasks and fires matching triggers."""

    def __init__(self):
        self._triggers: List[ArtifactTrigger] = []
        self._fired: set[str] = set()  # track (trigger_name, artifact) to avoid duplicates

    def register(self, trigger: ArtifactTrigger):
        self._triggers.append(trigger)
        logger.info("Registered trigger: %s (pattern=%s)", trigger.name, trigger.pattern)

    def evaluate(self, response: AgentResponse) -> List[AgentRequest]:
        """Check if a completed task's artifacts match any triggers.

        Returns list of AgentRequests to dispatch.
        """
        if response.status != TaskStatus.SUCCESS:
            return []

        requests = []
        for artifact in response.artifacts:
            for trigger in self._triggers:
                key = f"{trigger.name}:{artifact}"
                if key in self._fired:
                    continue

                if (artifact.endswith(trigger.pattern)
                        and response.agent.value == trigger.source_agent):
                    logger.info("🔔 Trigger %s fired for artifact: %s", trigger.name, artifact)
                    req = trigger.request_factory(response, artifact)
                    requests.append(req)
                    self._fired.add(key)

        return requests


# -- built-in triggers ----------------------------------------------------

def accuracy_on_sql_produced(response: AgentResponse, artifact: str) -> AgentRequest:
    """Auto-run accuracy analysis when a .sql file is produced."""
    stem = artifact.rsplit("/", 1)[-1].replace(".sql", "").replace("_FINAL", "")
    # Try to find the corresponding HQL source
    hql_path = (artifact.replace("output/hive/spark_sql", "input/hive")
                .replace(".sql", ".hql").replace("_FINAL", ""))

    return AgentRequest(
        task_id=f"auto_accuracy_{stem}",
        agent="accuracy_calculator",
        mcp_server="accuracy-calculator",
        tool="analyze_accuracy",
        arguments={"spark_sql_file": artifact, "source_file": hql_path},
        depends_on=[response.task_id],
    )


def validation_on_dag_produced(response: AgentResponse, artifact: str) -> AgentRequest:
    """Auto-validate when a DAG .py file is produced."""
    stem = artifact.rsplit("/", 1)[-1].replace(".py", "")
    return AgentRequest(
        task_id=f"auto_validate_{stem}",
        agent="workflow_validator",
        mcp_server="mwaa-validator",
        tool="validate_syntax",
        arguments={"dag_file": artifact},
        depends_on=[response.task_id],
    )


DEFAULT_TRIGGERS = [
    ArtifactTrigger(
        name="accuracy_on_sql",
        pattern=".sql",
        source_agent="code_converter",
        request_factory=accuracy_on_sql_produced,
    ),
    ArtifactTrigger(
        name="validate_on_dag",
        pattern=".py",
        source_agent="workflow_converter",
        request_factory=validation_on_dag_produced,
    ),
]

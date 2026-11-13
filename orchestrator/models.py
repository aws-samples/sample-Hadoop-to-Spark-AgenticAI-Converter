"""Structured inter-agent message contracts for the Orchestrator."""
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field


class AgentName(str, Enum):
    CODE_CONVERTER = "code_converter"
    HIVE_EVAL = "hive_eval"
    WORKFLOW_CONVERTER = "workflow_converter"
    WORKFLOW_VALIDATOR = "workflow_validator"
    ACCURACY_CALCULATOR = "accuracy_calculator"
    TEST_VALIDATOR = "test_validator"
    TEST_ORCHESTRATOR = "test_orchestrator"
    TEST_EXECUTION = "test_execution"
    IMPALA_CONVERTER = "impala_converter"


class TaskStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    NEEDS_RETRY = "needs_retry"
    SKIPPED = "skipped"


class AgentRequest(BaseModel):
    """Request dispatched from Orchestrator to an agent."""
    task_id: str
    agent: AgentName
    mcp_server: str
    tool: str
    arguments: Dict[str, Any] = Field(default_factory=dict)
    depends_on: List[str] = Field(default_factory=list)
    max_retries: int = 3
    timeout: float = 600


class AgentResponse(BaseModel):
    """Response returned from an agent tool call."""
    task_id: str
    agent: AgentName
    tool: str
    status: TaskStatus
    result: str = ""
    artifacts: List[str] = Field(default_factory=list)
    metrics: Dict[str, Any] = Field(default_factory=dict)
    errors: List[str] = Field(default_factory=list)
    retry_hint: Optional[str] = None
    duration_seconds: float = 0.0
    timestamp: datetime = Field(default_factory=datetime.now)


class RunStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    PARTIAL = "partial"


class AgentState(BaseModel):
    """State of a single agent within a run."""
    agent: AgentName
    status: TaskStatus = TaskStatus.PENDING
    tasks_completed: List[str] = Field(default_factory=list)
    tasks_failed: List[str] = Field(default_factory=list)
    artifacts: List[str] = Field(default_factory=list)
    metrics: Dict[str, Any] = Field(default_factory=dict)
    last_error: Optional[str] = None


class ArtifactInfo(BaseModel):
    """Tracks who produced and who consumes an artifact."""
    path: str
    produced_by: str  # task_id
    agent: AgentName
    consumed_by: List[str] = Field(default_factory=list)


class RunState(BaseModel):
    """Complete state of an orchestration run."""
    run_id: str
    intent: str = ""
    status: RunStatus = RunStatus.PENDING
    started_at: datetime = Field(default_factory=datetime.now)
    completed_at: Optional[datetime] = None
    agents: Dict[str, AgentState] = Field(default_factory=dict)
    tasks: Dict[str, AgentResponse] = Field(default_factory=dict)
    artifacts: Dict[str, ArtifactInfo] = Field(default_factory=dict)
    config: Dict[str, Any] = Field(default_factory=dict)

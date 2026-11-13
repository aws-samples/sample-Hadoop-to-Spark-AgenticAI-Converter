"""Data models for MWAA converter."""
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field
from datetime import datetime
from enum import Enum


class SourceType(str, Enum):
    """Source orchestration type."""
    CONTROL_M = "control_m"
    AIRFLOW = "airflow"


class JobType(str, Enum):
    """Job type in source system."""
    COMMAND = "command"
    SCRIPT = "script"
    DUMMY = "dummy"
    SUB_FLOW = "sub_flow"
    AWS_LAMBDA = "aws_lambda"
    EMR_ADD_STEPS = "emr_add_steps"
    UNKNOWN = "unknown"


class OperatorType(str, Enum):
    """Target MWAA operator types."""
    EMR_SERVERLESS = "emr_serverless"
    EMR_ADD_STEPS = "emr_add_steps"
    BASH = "bash"
    PYTHON = "python"
    DUMMY = "dummy"
    EXTERNAL_TASK_SENSOR = "external_task_sensor"
    TRIGGER_DAG_RUN = "trigger_dag_run"


class ValidationLevel(str, Enum):
    """Validation severity levels."""
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


class JobIR(BaseModel):
    """Intermediate Representation of a job (Control-M or Airflow)."""
    job_id: str
    job_name: str
    source_type: SourceType
    job_type: JobType
    
    # Execution details
    command: Optional[str] = None
    script_path: Optional[str] = None
    script_type: Optional[str] = None  # hql, sql, py, sh
    
    # Dependencies
    dependencies: List[str] = Field(default_factory=list)
    conditions: List[Dict[str, Any]] = Field(default_factory=list)
    
    # Scheduling
    schedule: Optional[str] = None
    calendar: Optional[str] = None
    
    # Environment
    variables: Dict[str, str] = Field(default_factory=dict)
    run_as: Optional[str] = None
    
    # Metadata
    description: Optional[str] = None
    folder: Optional[str] = None
    application: Optional[str] = None
    sub_application: Optional[str] = None
    
    # Original source
    raw_definition: Dict[str, Any] = Field(default_factory=dict)


class MWAATaskSpec(BaseModel):
    """Specification for MWAA task."""
    task_id: str
    operator_type: OperatorType
    
    # Operator-specific config
    config: Dict[str, Any] = Field(default_factory=dict)
    
    # Dependencies
    upstream_tasks: List[str] = Field(default_factory=list)
    external_dependencies: List[Dict[str, str]] = Field(default_factory=list)
    
    # Metadata
    source_job_id: str
    description: Optional[str] = None
    
    # Conversion metadata
    confidence_score: float = 1.0
    conversion_notes: List[str] = Field(default_factory=list)


class ChunkMetadata(BaseModel):
    """Metadata for a chunk."""
    chunk_id: str
    chunk_name: str
    source_folder: str
    job_count: int
    job_ids: List[str]
    
    # Dependencies
    internal_dependencies: List[Dict[str, str]] = Field(default_factory=list)
    external_dependencies: List[Dict[str, str]] = Field(default_factory=list)
    
    # Shared resources
    shared_variables: Dict[str, str] = Field(default_factory=dict)
    shared_calendars: List[str] = Field(default_factory=list)


class ConversionPlan(BaseModel):
    """Complete conversion plan for a chunk."""
    chunk_metadata: ChunkMetadata
    jobs: List[JobIR]
    tasks: List[MWAATaskSpec]
    
    # DAG configuration
    dag_id: str
    dag_schedule: Optional[str] = None
    dag_description: str
    default_args: Dict[str, Any] = Field(default_factory=dict)
    
    # Variables and connections
    airflow_variables: Dict[str, str] = Field(default_factory=dict)
    airflow_connections: List[Dict[str, str]] = Field(default_factory=list)
    
    # Conversion metadata
    conversion_timestamp: datetime = Field(default_factory=datetime.now)
    total_confidence: float = 1.0


class ValidationIssue(BaseModel):
    """Single validation issue."""
    level: ValidationLevel
    category: str
    message: str
    location: Optional[str] = None
    suggestion: Optional[str] = None


class ValidationResult(BaseModel):
    """Validation result for a DAG."""
    dag_id: str
    is_valid: bool
    
    # Issues by category
    syntax_issues: List[ValidationIssue] = Field(default_factory=list)
    structure_issues: List[ValidationIssue] = Field(default_factory=list)
    security_issues: List[ValidationIssue] = Field(default_factory=list)
    dagbag_issues: List[ValidationIssue] = Field(default_factory=list)
    
    # Summary
    error_count: int = 0
    warning_count: int = 0
    info_count: int = 0
    
    # Validation metadata
    validation_timestamp: datetime = Field(default_factory=datetime.now)


class ConversionReport(BaseModel):
    """Complete conversion report."""
    project_name: str
    source_type: SourceType
    
    # Input summary
    total_jobs: int
    total_chunks: int
    chunk_summaries: List[Dict[str, Any]] = Field(default_factory=list)
    
    # Output summary
    total_dags: int
    total_tasks: int
    
    # Job-to-task mapping
    job_task_mapping: Dict[str, str] = Field(default_factory=dict)
    
    # Validation summary
    validation_results: List[ValidationResult] = Field(default_factory=list)
    total_errors: int = 0
    total_warnings: int = 0
    
    # Conversion metrics
    average_confidence: float = 0.0
    conversion_duration_seconds: float = 0.0
    
    # Files generated
    dag_files: List[str] = Field(default_factory=list)
    variable_files: List[str] = Field(default_factory=list)
    mermaid_files: List[str] = Field(default_factory=list)
    
    # Timestamp
    report_timestamp: datetime = Field(default_factory=datetime.now)

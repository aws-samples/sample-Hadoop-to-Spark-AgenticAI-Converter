"""
Deterministic operator mapping for Control-M and Airflow to MWAA.

Design principle: the LLM never decides operator names. This module is the
single source of truth. Add new mappings here as new job types are encountered.
"""
import re
from pathlib import Path
from typing import Dict, Tuple, Optional

from converter.models import JobType, OperatorType, JobIR


# ── Primary mapping ───────────────────────────────────────────────────────────
# Key: (source_type, job_type, script_type)
# Value: (operator_type, confidence_score)
#
# script_type is the file extension (lowercase, no dot): hql, sql, py, sh, ksh
# Use None for job types where script_type is irrelevant (dummy, sub_flow, etc.)

OPERATOR_MAP: Dict[Tuple[str, str, Optional[str]], Tuple[OperatorType, float]] = {

    # ── Control-M: HQL / SQL scripts → EMR Serverless ────────────────────
    ("control_m", "command", "hql"):  (OperatorType.EMR_SERVERLESS, 0.95),
    ("control_m", "command", "sql"):  (OperatorType.EMR_SERVERLESS, 0.95),
    ("control_m", "script",  "hql"):  (OperatorType.EMR_SERVERLESS, 0.95),
    ("control_m", "script",  "sql"):  (OperatorType.EMR_SERVERLESS, 0.95),

    # ── Control-M: Python scripts → EMR Serverless ───────────────────────
    ("control_m", "command", "py"):   (OperatorType.EMR_SERVERLESS, 0.90),
    ("control_m", "script",  "py"):   (OperatorType.EMR_SERVERLESS, 0.90),

    # ── Control-M: Shell scripts → BashOperator ──────────────────────────
    ("control_m", "command", "sh"):   (OperatorType.BASH, 0.85),
    ("control_m", "command", "ksh"):  (OperatorType.BASH, 0.85),
    ("control_m", "command", "bash"): (OperatorType.BASH, 0.85),
    ("control_m", "script",  "sh"):   (OperatorType.BASH, 0.85),
    ("control_m", "script",  "ksh"):  (OperatorType.BASH, 0.85),
    ("control_m", "script",  "bash"): (OperatorType.BASH, 0.85),

    # ── Control-M: AWS Lambda jobs → EMR Serverless (via Lambda trigger) ─
    # APPL_TYPE=AWS jobs in Control-M typically invoke Lambda which runs Spark.
    # Map to EMR Serverless — the Lambda wrapper is replaced by direct EMR call.
    ("control_m", "aws_lambda", "hql"): (OperatorType.EMR_SERVERLESS, 0.90),
    ("control_m", "aws_lambda", "sql"): (OperatorType.EMR_SERVERLESS, 0.90),
    ("control_m", "aws_lambda", "py"):  (OperatorType.EMR_SERVERLESS, 0.85),
    ("control_m", "aws_lambda", None):  (OperatorType.EMR_SERVERLESS, 0.80),

    # ── Control-M: EMR EC2 cluster jobs → EmrAddStepsOperator ────────────
    # AWS-EMR_CLUSTER_ID present in variables → EC2-based EMR, not Serverless
    ("control_m", "emr_add_steps", "hql"): (OperatorType.EMR_ADD_STEPS, 0.95),
    ("control_m", "emr_add_steps", "sql"): (OperatorType.EMR_ADD_STEPS, 0.95),
    ("control_m", "emr_add_steps", "py"):  (OperatorType.EMR_ADD_STEPS, 0.90),
    ("control_m", "emr_add_steps", None):  (OperatorType.EMR_ADD_STEPS, 0.90),

    # ── Control-M: Dummy / sentinel jobs → EmptyOperator ─────────────────
    ("control_m", "dummy",    None):  (OperatorType.DUMMY, 1.0),

    # ── Control-M: Sub-flow / nested folder → TriggerDagRunOperator ──────
    ("control_m", "sub_flow", None):  (OperatorType.TRIGGER_DAG_RUN, 0.80),

    # ── Airflow: same mappings ────────────────────────────────────────────
    ("airflow", "command", "hql"):  (OperatorType.EMR_SERVERLESS, 0.95),
    ("airflow", "command", "sql"):  (OperatorType.EMR_SERVERLESS, 0.95),
    ("airflow", "script",  "hql"):  (OperatorType.EMR_SERVERLESS, 0.95),
    ("airflow", "script",  "sql"):  (OperatorType.EMR_SERVERLESS, 0.95),
    ("airflow", "command", "py"):   (OperatorType.EMR_SERVERLESS, 0.90),
    ("airflow", "script",  "py"):   (OperatorType.EMR_SERVERLESS, 0.90),
    ("airflow", "command", "sh"):   (OperatorType.BASH, 0.85),
    ("airflow", "command", "ksh"):  (OperatorType.BASH, 0.85),
    ("airflow", "script",  "sh"):   (OperatorType.BASH, 0.85),
    ("airflow", "script",  "ksh"):  (OperatorType.BASH, 0.85),
    ("airflow", "dummy",   None):   (OperatorType.DUMMY, 1.0),
}


# ── Command-line patterns for script type detection ───────────────────────────
# When a job has a Command field instead of ScriptPath, extract the script type
# from the command string. Checked in order — first match wins.
_COMMAND_PATTERNS: list = [
    # hive / beeline → HQL
    (re.compile(r'\bhive\b.*-f\s+\S+\.hql', re.I),          "hql"),
    (re.compile(r'\bbeeline\b.*-f\s+\S+\.hql', re.I),        "hql"),
    (re.compile(r'-f\s+(\S+\.hql)', re.I),                   "hql"),
    # spark-submit with SQL file
    (re.compile(r'spark-submit\b.*\S+\.sql', re.I),           "sql"),
    (re.compile(r'-f\s+(\S+\.sql)', re.I),                   "sql"),
    # spark-submit with Python file
    (re.compile(r'spark-submit\b.*\S+\.py', re.I),            "py"),
    (re.compile(r'\bpython[23]?\b\s+\S+\.py', re.I),          "py"),
    (re.compile(r'\bpyspark\b\s+\S+\.py', re.I),              "py"),
    # shell scripts
    (re.compile(r'\bbash\b\s+\S+\.sh', re.I),                 "sh"),
    (re.compile(r'\bksh\b\s+\S+\.ksh', re.I),                 "ksh"),
    (re.compile(r'\bsh\b\s+\S+\.(sh|ksh)', re.I),             "sh"),
    (re.compile(r'\S+\.(sh|ksh)\b', re.I),                    "sh"),
]


def detect_script_type_from_command(command: str) -> Optional[str]:
    """
    Extract script type from a Control-M Command field.

    Examples:
        "hive -f /scripts/extract.hql"  → "hql"
        "spark-submit /scripts/load.py" → "py"
        "/scripts/cleanup.sh"           → "sh"
        "python3 /scripts/process.py"   → "py"

    Returns None if no pattern matches.
    """
    if not command:
        return None
    for pattern, script_type in _COMMAND_PATTERNS:
        if pattern.search(command):
            return script_type
    # Last resort: check file extension of any path-like token
    for token in command.split():
        ext = Path(token).suffix.lstrip('.').lower()
        if ext in ('hql', 'sql', 'py', 'sh', 'ksh', 'bash'):
            return ext
    return None


def get_target_operator(job: JobIR) -> Tuple[OperatorType, float]:
    """
    Get the target MWAA operator for a job.

    Resolution order:
    1. Direct lookup in OPERATOR_MAP using (source_type, job_type, script_type)
    2. If script_type is None, try to detect it from the Command field
    3. Dummy job type → EmptyOperator (always)
    4. Unknown → BashOperator at 50% confidence (flags for manual review)

    Args:
        job: Job intermediate representation

    Returns:
        Tuple of (operator_type, confidence_score)
    """
    source = job.source_type.value
    jtype  = job.job_type.value
    stype  = job.script_type

    # 1. Direct lookup
    key = (source, jtype, stype)
    if key in OPERATOR_MAP:
        return OPERATOR_MAP[key]

    # 2. Try to detect script type from Command field if not already set
    if stype is None and job.command:
        detected = detect_script_type_from_command(job.command)
        if detected:
            key2 = (source, jtype, detected)
            if key2 in OPERATOR_MAP:
                return OPERATOR_MAP[key2]

    # 3. Dummy always → EmptyOperator
    if job.job_type == JobType.DUMMY or jtype == "dummy":
        return (OperatorType.DUMMY, 1.0)

    # 4. aws_lambda without script type → EMR Serverless (best guess)
    if jtype == "aws_lambda":
        return (OperatorType.EMR_SERVERLESS, 0.75)

    # 5. Unknown — BashOperator at low confidence, flagged for manual review
    return (OperatorType.BASH, 0.50)


def get_operator_config_template(operator_type: OperatorType) -> Dict:
    """Config skeleton for each operator type — used by plan generation."""
    templates = {
        OperatorType.EMR_SERVERLESS: {
            "application_id":     "Variable.get('EMR_APP_ID')",
            "execution_role_arn": "Variable.get('EMR_EXEC_ROLE_ARN')",
            "job_driver":         "spark_submit",
            "configuration_overrides": {},
        },
        OperatorType.EMR_ADD_STEPS: {
            "job_flow_id": "Variable.get('EMR_CLUSTER_ID')",
            "steps": [],
        },
        OperatorType.BASH: {
            "bash_command": "",
        },
        OperatorType.PYTHON: {
            "python_callable": "",
        },
        OperatorType.DUMMY: {},
        OperatorType.EXTERNAL_TASK_SENSOR: {
            "external_dag_id":  "",
            "external_task_id": "",
            "mode":             "reschedule",
            "timeout":          3600,
        },
        OperatorType.TRIGGER_DAG_RUN: {
            "trigger_dag_id":       "",
            "wait_for_completion":  True,
        },
    }
    return templates.get(operator_type, {})

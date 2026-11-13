"""Orchestrator-level self-correction — reasons about failures across agents."""
import json
import logging
from typing import Optional

from orchestrator.models import AgentName, AgentRequest, AgentResponse, TaskStatus

logger = logging.getLogger(__name__)


class CorrectionRule:
    """A rule that maps a failure pattern to a corrective action."""

    def __init__(self, name: str, trigger_agent: AgentName, trigger_tool: str,
                 condition, corrective_request_fn):
        self.name = name
        self.trigger_agent = trigger_agent
        self.trigger_tool = trigger_tool
        self.condition = condition  # (response) -> bool
        self.corrective_request_fn = corrective_request_fn  # (response) -> AgentRequest


# -- built-in correction rules -------------------------------------------

def _low_accuracy(resp: AgentResponse) -> bool:
    """Trigger if accuracy score < 70%.

    AccuracyAnalyzer.analyze_conversion() returns `accuracy_percentage` on a
    0-100 scale (see accuracy_framework/core/accuracy_analyzer.py) -- there is
    no `overall_confidence` key, so this used to always fall through to the
    1.0 default and never fire.
    """
    try:
        metrics = json.loads(resp.result)
        return metrics.get("accuracy_percentage", 100.0) < 70.0
    except (json.JSONDecodeError, TypeError):
        return False


def _accuracy_correction(resp: AgentResponse) -> AgentRequest:
    """Re-run conversion with error feedback from accuracy analysis."""
    try:
        metrics = json.loads(resp.result)
    except (json.JSONDecodeError, TypeError):
        metrics = {}

    issues = metrics.get("top_issues", [])
    error_details = "; ".join(str(i) for i in issues[:5]) if issues else "Low accuracy score"
    sql_file = resp.metrics.get("spark_sql_file", "")

    return AgentRequest(
        task_id=f"{resp.task_id}_fix",
        agent=AgentName.CODE_CONVERTER,
        mcp_server="hive-converter",
        tool="fix_conversion_errors",
        arguments={"sql_file": sql_file, "error_details": error_details},
        depends_on=[resp.task_id],
    )


def _validation_failed(resp: AgentResponse) -> bool:
    """Trigger if validation result contains a definitive FAIL verdict."""
    if resp.status != TaskStatus.SUCCESS:
        return False
    result = resp.result
    # Look for structured verdict patterns, not incidental "FAIL" in verbose output
    import re
    # Match patterns like "VALIDATION RESULT: FAIL", '"is_valid": false', "Syntax: FAIL"
    if re.search(r'VALIDATION\s+RESULT:\s*FAIL', result, re.IGNORECASE):
        return True
    if re.search(r'"is_valid"\s*:\s*false', result, re.IGNORECASE):
        return True
    return False


def _validation_correction(resp: AgentResponse) -> AgentRequest:
    """Fix validation errors in DAG."""
    dag_file = resp.metrics.get("dag_file", "")
    return AgentRequest(
        task_id=f"{resp.task_id}_fix",
        agent=AgentName.WORKFLOW_VALIDATOR,
        mcp_server="mwaa-validator",
        tool="fix_validation_errors",
        arguments={"dag_file": dag_file, "validation_errors": resp.result},
        depends_on=[resp.task_id],
    )


def _spark_execution_failed(resp: AgentResponse) -> bool:
    """Trigger if Spark execution failed.

    Uses the PASS/FAIL verdict the execution prompt asks for. A bare word match on
    "error" is not enough: results such as "ran with no errors" would trigger a
    pointless fix step.
    """
    import re
    if resp.status == TaskStatus.FAILED:
        return True
    head = (resp.result or "")[:500]
    if re.search(r"\bFAIL(ED|URE)?\b", head):
        return True
    if re.search(r"\bPASS(ED)?\b", head):
        return False
    # No verdict: fall back to error words, ignoring "no error(s)" / "without error(s)"
    cleaned = re.sub(r"\b(no|without|zero|0)\s+(runtime\s+)?errors?\b", "", head, flags=re.I)
    cleaned = re.sub(r"\berrors?\s*:\s*(none|0)\b", "", cleaned, flags=re.I)
    # Substring match so class names like AnalysisException / ParseError count too
    return bool(re.search(r"(error|exception)", cleaned, re.I))


def _spark_execution_correction(resp: AgentResponse) -> AgentRequest:
    """Re-validate and fix the SQL after Spark execution failure."""
    sql_file = resp.metrics.get("sql_file", "")
    return AgentRequest(
        task_id=f"{resp.task_id}_revalidate",
        agent=AgentName.CODE_CONVERTER,
        mcp_server="hive-converter",
        tool="fix_conversion_errors",
        arguments={"sql_file": sql_file, "error_details": resp.result[:500]},
        depends_on=[resp.task_id],
    )


# Default correction rules
DEFAULT_RULES = [
    CorrectionRule(
        name="low_accuracy_correction",
        trigger_agent=AgentName.ACCURACY_CALCULATOR,
        trigger_tool="analyze_accuracy",
        condition=_low_accuracy,
        corrective_request_fn=_accuracy_correction,
    ),
    CorrectionRule(
        name="dag_syntax_correction",
        trigger_agent=AgentName.WORKFLOW_VALIDATOR,
        trigger_tool="validate_syntax",
        condition=_validation_failed,
        corrective_request_fn=_validation_correction,
    ),
    CorrectionRule(
        name="dag_structure_correction",
        trigger_agent=AgentName.WORKFLOW_VALIDATOR,
        trigger_tool="validate_structure",
        condition=_validation_failed,
        corrective_request_fn=_validation_correction,
    ),
    CorrectionRule(
        name="dag_security_correction",
        trigger_agent=AgentName.WORKFLOW_VALIDATOR,
        trigger_tool="validate_security",
        condition=_validation_failed,
        corrective_request_fn=_validation_correction,
    ),
    CorrectionRule(
        name="dag_dagbag_correction",
        trigger_agent=AgentName.WORKFLOW_VALIDATOR,
        trigger_tool="validate_dagbag",
        condition=_validation_failed,
        corrective_request_fn=_validation_correction,
    ),
    CorrectionRule(
        name="spark_execution_correction",
        trigger_agent=AgentName.HIVE_EVAL,
        trigger_tool="execute_spark_sql",
        condition=_spark_execution_failed,
        corrective_request_fn=_spark_execution_correction,
    ),
]


class SelfCorrectionLoop:
    """Evaluates agent responses and generates corrective actions."""

    def __init__(self, rules: list[CorrectionRule] | None = None, max_corrections: int = 3):
        self.rules = rules or DEFAULT_RULES
        self.max_corrections = max_corrections
        self._correction_counts: dict[str, int] = {}

    def evaluate(self, response: AgentResponse) -> Optional[AgentRequest]:
        """Check if a response triggers any correction rule.

        Returns an AgentRequest for the corrective action, or None.
        """
        base_task = response.task_id.split("_fix")[0].split("_revalidate")[0]
        count = self._correction_counts.get(base_task, 0)
        if count >= self.max_corrections:
            logger.warning("Max corrections (%d) reached for %s", self.max_corrections, base_task)
            return None

        for rule in self.rules:
            if (response.agent == rule.trigger_agent
                    and response.tool == rule.trigger_tool
                    and rule.condition(response)):
                logger.info("🔄 Correction triggered: %s for task %s", rule.name, response.task_id)
                self._correction_counts[base_task] = count + 1
                return rule.corrective_request_fn(response)

        return None

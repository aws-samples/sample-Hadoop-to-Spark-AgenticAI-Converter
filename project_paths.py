"""Single source of truth for every input / output location in this project.

Layout
------
    input/                      <- everything you provide
        hive/                   HiveQL scripts (*.hql, *.txt)
        impala/                 Impala SQL scripts (*.sql)
        mwaa/
            control_m/          Control-M XML exports (*.xml)
            airflow/            On-prem Airflow DAGs (*.py)
            hive_scripts/       (optional) .hql scripts referenced by Control-M jobs
            spark_sql_mapping.json   (optional; copy from spark_sql_mapping.example.json)
        ddl/                    (optional) DDL files that improve schema inference in testing

    output/                     <- everything that is generated (git-ignored)
        hive/                   spark_sql/ chunks/ mermaid/ accuracy/ validation/
        impala/                 spark_sql/ chunks/ mermaid/ accuracy/ validation/
        mwaa/                   dags/ job_ir/ plans/ chunks/ mermaid/ validation/ reports/ ...
        testing/                generated_data/ execution_output/ reports/ state/
        state/                  orchestrator run state (--resume)
        logs/                   all log files

Only the standard library is used so this module can be imported by the
orchestrator, every MCP server and the stand-alone scripts alike.
"""
from pathlib import Path
from typing import Union

ROOT = Path(__file__).resolve().parent

# ── Top level ──────────────────────────────────────────────────────────────
INPUT_DIR = ROOT / "input"
OUTPUT_DIR = ROOT / "output"

# ── Inputs ─────────────────────────────────────────────────────────────────
HIVE_INPUT = INPUT_DIR / "hive"
IMPALA_INPUT = INPUT_DIR / "impala"

MWAA_INPUT = INPUT_DIR / "mwaa"
MWAA_CONTROL_M_INPUT = MWAA_INPUT / "control_m"
MWAA_AIRFLOW_INPUT = MWAA_INPUT / "airflow"
MWAA_HIVE_SCRIPTS_INPUT = MWAA_INPUT / "hive_scripts"
MWAA_SPARK_SQL_MAPPING = MWAA_INPUT / "spark_sql_mapping.json"

DDL_INPUT = INPUT_DIR / "ddl"

# ── Outputs: SQL conversion (one tree per source dialect) ──────────────────
HIVE_OUTPUT = OUTPUT_DIR / "hive"
IMPALA_OUTPUT = OUTPUT_DIR / "impala"

_SQL_SUBDIRS = ("spark_sql", "chunks", "mermaid", "accuracy", "validation")


def sql_input(source_type: str) -> Path:
    """Input directory for a SQL source dialect ('hive' or 'impala')."""
    return {"hive": HIVE_INPUT, "impala": IMPALA_INPUT}[source_type]


def sql_output(source_type: str) -> Path:
    """Output root for a SQL source dialect ('hive' or 'impala')."""
    return {"hive": HIVE_OUTPUT, "impala": IMPALA_OUTPUT}[source_type]


def spark_sql_dir(source_type: str) -> Path:
    """Converted Spark SQL files (and merged *_FINAL.sql files)."""
    return sql_output(source_type) / "spark_sql"


def chunks_dir(source_type: str) -> Path:
    """Intermediate chunks created for large input files."""
    return sql_output(source_type) / "chunks"


def mermaid_dir(source_type: str) -> Path:
    """Mermaid flow diagrams."""
    return sql_output(source_type) / "mermaid"


def accuracy_dir(source_type: str) -> Path:
    """Accuracy metrics, reports and dashboard.html."""
    return sql_output(source_type) / "accuracy"


def validation_dir(source_type: str) -> Path:
    """Spark execution/validation reports (<file>_test_report.md)."""
    return sql_output(source_type) / "validation"


# ── Outputs: MWAA workflow conversion ──────────────────────────────────────
MWAA_OUTPUT = OUTPUT_DIR / "mwaa"
MWAA_HIVE_STAGING = MWAA_OUTPUT / "hive_staging"   # .hql copied here before conversion
MWAA_SPARK_SQL_OUTPUT = MWAA_OUTPUT / "spark_sql"      # Spark SQL for scripts referenced by jobs

# ── Outputs: Hive-vs-Spark equivalence testing ─────────────────────────────
TESTING_OUTPUT = OUTPUT_DIR / "testing"
TESTING_GENERATED_DATA = TESTING_OUTPUT / "generated_data"
TESTING_EXECUTION_OUTPUT = TESTING_OUTPUT / "execution_output"
TESTING_REPORTS = TESTING_OUTPUT / "reports"
TESTING_STATE = TESTING_OUTPUT / "state"

# ── Outputs: run bookkeeping ───────────────────────────────────────────────
STATE_DIR = OUTPUT_DIR / "state"
LOG_DIR = OUTPUT_DIR / "logs"


def rel(path: Union[str, Path]) -> str:
    """Return ``path`` relative to the project root as a POSIX string.

    Used when a path is embedded in an LLM prompt: the LLM CLI runs with the
    project root as its working directory, so relative paths are shortest.
    Paths outside the project root are returned unchanged.
    """
    p = Path(path)
    try:
        return p.resolve().relative_to(ROOT).as_posix()
    except ValueError:
        return str(path)


def ensure_dirs() -> None:
    """Create every input and output directory (idempotent)."""
    dirs = [
        HIVE_INPUT, IMPALA_INPUT,
        MWAA_CONTROL_M_INPUT, MWAA_AIRFLOW_INPUT, MWAA_HIVE_SCRIPTS_INPUT,
        DDL_INPUT,
        MWAA_OUTPUT, TESTING_GENERATED_DATA, TESTING_EXECUTION_OUTPUT,
        TESTING_REPORTS, TESTING_STATE, STATE_DIR, LOG_DIR,
    ]
    for source_type in ("hive", "impala"):
        dirs.extend(sql_output(source_type) / sub for sub in _SQL_SUBDIRS)
    dirs.extend(
        MWAA_OUTPUT / sub
        for sub in ("dags", "job_ir", "plans", "chunks", "mermaid", "validation", "reports")
    )
    dirs.extend([TESTING_EXECUTION_OUTPUT / "hive", TESTING_EXECUTION_OUTPUT / "spark"])
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)

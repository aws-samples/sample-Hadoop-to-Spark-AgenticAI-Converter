"""Per-query locations for the Hive vs Spark equivalence tests.

Every test query gets its own folders, so several queries never overwrite each
other's files:

    output/testing/generated_data/<query>/     schemas.json, sample_data.json,
                                               variables.json, setup_*.sql, tables/
    output/testing/execution_output/<query>/   hive/, spark/, comparison_results.json,
                                               minus_results.json
    output/testing/reports/<query>/            TEST_REPORT.html

output/testing/ is mounted at /test_workspace in both test containers
(see testing_framework/finch-compose-test.yml), so a host path under it maps
to a fixed path inside the containers.
"""
from pathlib import Path
from typing import Union

import project_paths as P

CONTAINER_ROOT = "/test_workspace"
COMPOSE_FILE = P.ROOT / "testing_framework" / "finch-compose-test.yml"

PathLike = Union[str, Path]


def query_name(path: PathLike) -> str:
    """Test name for a HiveQL / Spark SQL file: its stem without a _FINAL suffix."""
    return Path(path).stem.replace("_FINAL", "")


def gen_dir(name: str) -> Path:
    return P.TESTING_GENERATED_DATA / name


def exec_dir(name: str) -> Path:
    return P.TESTING_EXECUTION_OUTPUT / name


def report_dir(name: str) -> Path:
    return P.TESTING_REPORTS / name


def in_container(host_path: PathLike) -> str:
    """Path inside the test containers for a host path under output/testing/.

    Raises ValueError if the path is outside output/testing/ (not mounted).
    """
    rel = Path(host_path).resolve().relative_to(P.TESTING_OUTPUT.resolve())
    return f"{CONTAINER_ROOT}/{rel.as_posix()}"

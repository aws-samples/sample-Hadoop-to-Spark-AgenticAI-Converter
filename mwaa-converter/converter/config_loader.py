"""
Config loader for MWAA Converter.
Reads config/converter_config.yaml and provides typed access to all settings.
Falls back to safe defaults if the file is missing or a key is absent.
"""
import logging
from pathlib import Path
from typing import Dict, Any

import yaml

logger = logging.getLogger(__name__)

# Default config — mirrors converter_config.yaml structure
_DEFAULTS: Dict[str, Any] = {
    "airflow_variables": {
        "emr_app_id":        "EMR_APP_ID",
        "emr_exec_role_arn": "EMR_EXEC_ROLE_ARN",
        "scripts_bucket":    "SCRIPTS_BUCKET",
        "logs_bucket":       "LOGS_BUCKET",
        "emr_cluster_id":    "EMR_CLUSTER_ID",
    },
    "dag_defaults": {
        "owner":               "airflow",
        "depends_on_past":     False,
        "email_on_failure":    True,
        "email_on_retry":      False,
        "retries":             1,
        "retry_delay_minutes": 5,
        "max_active_runs":     1,
    },
    "emr_serverless": {
        "spark_submit_parameters": (
            "--conf spark.executor.cores=4 "
            "--conf spark.executor.memory=8g "
            "--conf spark.driver.cores=2 "
            "--conf spark.driver.memory=4g"
        ),
    },
    "conversion": {
        "max_workers":             4,
        "max_correction_attempts": 3,
    },
    "hive_conversion": {
        "enabled":              False,
        # Paths are relative to the project root (see project_paths.py)
        "hive_input_dir":     "input/mwaa/hive_scripts",
        "hive_staging_dir":   "output/mwaa/hive_staging",
        "spark_sql_output_dir": "output/mwaa/spark_sql",
        "scripts_s3_prefix":    "s3://${MWAA_SCRIPTS_BUCKET}/spark_sql",
    },
    "chunking": {
        "strategy": "organizational",
    },
    "validation": {
        "enabled":       True,
        "dagbag_python": "venv-airflow/bin/python",
    },
    "output": {
        "generate_mermaid":     True,
        "generate_html_report": True,
        "save_intermediate":    True,
    },
}


def _deep_merge(base: Dict, override: Dict) -> Dict:
    """Recursively merge override into base, returning a new dict."""
    result = dict(base)
    for key, value in override.items():
        if key in result and isinstance(result[key], dict) and isinstance(value, dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def _migrate_legacy_keys(user_config: Dict[str, Any]) -> Dict[str, Any]:
    """Accept the deprecated ``hiveql_conversion`` section (now ``hive_conversion``).

    Keys ``hiveql_input_dir`` / ``hiveql_staging_dir`` are renamed the same way.
    Values set under the new section win if both are present.
    """
    legacy = user_config.pop("hiveql_conversion", None)
    if isinstance(legacy, dict):
        logger.warning("converter_config.yaml: 'hiveql_conversion' is deprecated -- "
                       "rename it to 'hive_conversion' (and hiveql_*_dir keys to hive_*_dir)")
        migrated = {(k.replace("hiveql_", "hive_", 1) if k.startswith("hiveql_") else k): v
                    for k, v in legacy.items()}
        user_config["hive_conversion"] = {**migrated, **(user_config.get("hive_conversion") or {})}
    return user_config


def load_config(config_path: Path = None) -> Dict[str, Any]:
    """
    Load converter_config.yaml and merge with defaults.

    Args:
        config_path: Path to config file. Defaults to config/converter_config.yaml
                     relative to the project root.

    Returns:
        Merged configuration dict.
    """
    if config_path is None:
        config_path = Path(__file__).parent.parent / "config" / "converter_config.yaml"

    config = dict(_DEFAULTS)

    if config_path.exists():
        try:
            with open(config_path, encoding="utf-8") as f:
                user_config = yaml.safe_load(f) or {}
            user_config = _migrate_legacy_keys(user_config)
            config = _deep_merge(_DEFAULTS, user_config)
            logger.debug("Loaded config from %s", config_path)
        except (yaml.YAMLError, OSError) as e:
            logger.warning("Could not load %s: %s — using defaults", config_path, e)
    else:
        logger.warning("Config file not found: %s — using defaults", config_path)

    return config


class ConverterConfig:
    """Typed wrapper around the raw config dict."""

    def __init__(self, config: Dict[str, Any]):
        self._cfg = config

    # ── Airflow variable names ────────────────────────────────────────────
    @property
    def var_emr_app_id(self) -> str:
        return self._cfg["airflow_variables"]["emr_app_id"]

    @property
    def var_emr_exec_role_arn(self) -> str:
        return self._cfg["airflow_variables"]["emr_exec_role_arn"]

    @property
    def var_scripts_bucket(self) -> str:
        return self._cfg["airflow_variables"]["scripts_bucket"]

    @property
    def var_logs_bucket(self) -> str:
        return self._cfg["airflow_variables"]["logs_bucket"]

    @property
    def var_emr_cluster_id(self) -> str:
        return self._cfg["airflow_variables"]["emr_cluster_id"]

    @property
    def all_variable_names(self) -> Dict[str, str]:
        """Return all variable name mappings."""
        return dict(self._cfg["airflow_variables"])

    # ── DAG defaults ─────────────────────────────────────────────────────
    @property
    def dag_defaults(self) -> Dict[str, Any]:
        return dict(self._cfg["dag_defaults"])

    # ── EMR Serverless ────────────────────────────────────────────────────
    @property
    def spark_submit_parameters(self) -> str:
        return self._cfg["emr_serverless"]["spark_submit_parameters"].strip()

    # ── Conversion ────────────────────────────────────────────────────────
    @property
    def max_workers(self) -> int:
        return int(self._cfg["conversion"]["max_workers"])

    @property
    def max_correction_attempts(self) -> int:
        return int(self._cfg["conversion"]["max_correction_attempts"])

    # ── Validation ────────────────────────────────────────────────────────
    @property
    def dagbag_python(self) -> str:
        """Return absolute path to the Python executable for DagBag validation.

        If the configured path is relative (e.g. 'venv-airflow/bin/python'), resolve it
        relative to the project root without following symlinks.
        Falls back to sys.executable if the resolved path doesn't exist.
        """
        import sys
        raw = self._cfg["validation"]["dagbag_python"]
        p = Path(raw)
        if p.is_absolute():
            return raw if p.exists() else sys.executable
        # Resolve relative to project root — use absolute() not resolve()
        # to avoid following the venv symlink to system Python
        project_root = Path(__file__).parent.parent.parent
        resolved = (project_root / raw).absolute()
        return str(resolved) if resolved.exists() else sys.executable

    # ── HiveQL Conversion ─────────────────────────────────────────────────
    @property
    def hive_conversion_enabled(self) -> bool:
        return bool(self._cfg["hive_conversion"]["enabled"])

    @property
    def hive_input_dir(self) -> str:
        return self._cfg["hive_conversion"]["hive_input_dir"]

    @property
    def hive_staging_dir(self) -> str:
        return self._cfg["hive_conversion"]["hive_staging_dir"]

    @property
    def spark_sql_output_dir(self) -> str:
        return self._cfg["hive_conversion"]["spark_sql_output_dir"]

    @property
    def scripts_s3_prefix(self) -> str:
        return self._cfg["hive_conversion"]["scripts_s3_prefix"]

    # ── Output ────────────────────────────────────────────────────────────
    @property
    def generate_mermaid(self) -> bool:
        return bool(self._cfg["output"]["generate_mermaid"])

    @property
    def generate_html_report(self) -> bool:
        return bool(self._cfg["output"]["generate_html_report"])

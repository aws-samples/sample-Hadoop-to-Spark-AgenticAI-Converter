# Security & Production Hardening Guide

## Overview

This document outlines security measures implemented in the tool and recommendations for production hardening when deploying generated outputs in customer environments.

## Implemented Security Controls

| Control | Status |
|---------|--------|
| No hardcoded credentials in generated DAGs | ✅ Enforced — `Variable.get()` used |
| Pydantic input validation on all MCP tools | ✅ 45+ tool inputs type-checked |
| Container ports bound to localhost (127.0.0.1) | ✅ Compose files and the containers started by `setup.sh` |
| Read-only container root filesystem | ✅ tmpfs only for the paths Spark/Hive write to (discarded on stop) |
| Container capabilities dropped (CAP_DROP ALL) | ✅ Minimal privilege |
| Container memory/CPU limited | ✅ 4GB / 2 CPU per container |
| no-new-privileges security option | ✅ Prevents privilege escalation (verified `NoNewPrivs: 1` under Finch) |
| Generated DAG code (Jinja2) | ✅ Templates render Python, so HTML autoescape is intentionally off (it corrupted generated code); DAGs are syntax- and DagBag-checked and must be reviewed |
| HTML reports escape all dynamic values | ✅ `html.escape` on every value from input files, plans and LLM output (MWAA conversion report, equivalence test reports); no inline JavaScript handlers |
| Safe XML parsing (`defusedxml`) | ✅ Control-M XML parsed with `defusedxml` — entity expansion and XXE are rejected |
| Single reviewed LLM CLI launcher (`llm_cli.py`) | ✅ argv list only (no shell), allow-listed executables, fixed flags, prompt cannot be parsed as a CLI option |
| yaml.safe_load() throughout | ✅ No unsafe deserialization |
| DAG security scanner (validate_security tool) | ✅ Scans for hardcoded secrets |
| Outbound network from containers | ⚠️ Not blocked; containers only process locally generated synthetic test data |
| No PII/customer data in repository | ✅ Only synthetic test data |

## Production Hardening Recommendations

### 1. Dependencies Are Pinned

`requirements.txt` pins every runtime package to an exact, tested version, and
`setup.sh` installs from it. Apache Airflow (optional, used only for local DagBag
validation) is installed into a separate `venv-airflow/` under the official Airflow
constraints file, so its pins never conflict with the converter's. Review and update
the pins deliberately; do not replace them with open ranges.

`venv-airflow/` uses Apache Airflow 2.11.2, the latest 2.x release. Airflow 2.x still
has published advisories that are only fixed in Airflow 3 (for example CVE-2026-33264,
serialized-DAG deserialization in the scheduler / API server). They are not reachable
here: the interpreter only loads generated DAGs into a `DagBag` in a short-lived
subprocess, and no scheduler, webserver or API server is ever started. Do not reuse
`venv-airflow/` to run Airflow services.

### 2. Only Process Trusted Input Files (LLM Tool Auto-Approval)

The LLM-driven steps run `kiro-cli` / `claude` non-interactively with tool
auto-approval (`--trust-all-tools` / `bypassPermissions`), and the prompts embed
the content of the input files (HiveQL, Impala SQL, Control-M XML, Airflow DAGs).
A crafted input file could therefore attempt prompt injection to make the agent
run unintended tool actions on the machine running the converter.

- Only process files from the customer's own, trusted source repositories.
- Run conversions on a dedicated development machine or isolated environment,
  not on hosts with access to production credentials.
- The subprocess launch itself is hardened in `llm_cli.py` (no shell,
  allow-listed executables, fixed flags); the residual risk is the agent's tool
  access, which the testing workflow needs (it runs `finch exec`).

### 3. Container Hardening

Already applied in `finch-compose.yml`, `testing_framework/finch-compose-test.yml`
and the `finch run` commands in `setup.sh`: localhost-only ports, read-only root
filesystem with tmpfs for the engines' write paths, `cap_drop: ALL`,
`no-new-privileges`, memory/CPU limits. Notes if you adapt them:

- List `no-new-privileges` in **both** spellings: Finch/nerdctl only enforces the plain
  form and silently ignores `no-new-privileges:true`, while Docker and security scanners
  (Semgrep) expect the `:true` form. With both listed, Finch reports `NoNewPrivs: 1`.
- `/tmp` must be mounted `exec` (the JVM extracts native libraries there), and Hive's
  scratch directory must be mode `733` or HiveServer2 refuses to start.
- For shared or CI/CD hosts, also consider blocking outbound networking for the
  containers (for example an internal-only network).

### 4. S3 Bucket Configuration

Generated DAGs reference S3 paths via Airflow Variables:

```python
s3_bucket = Variable.get("mwaa_scripts_bucket")
```

Ensure the MWAA environment has this variable configured with the actual bucket name. Never hardcode bucket names in DAG code.

### 5. Review Generated SQL Before Production Deployment

Generated Spark SQL should be reviewed for:
- Correct table/schema references for the target environment
- Appropriate partition strategies
- Resource configuration (executor memory, cores)

### 6. Logging Considerations

The tool logs file names, line counts, and structural metadata. When processing customer data:
- Ensure log files don't capture SQL content containing sensitive column values
- Rotate logs and restrict access to the `output/logs/` directory
- Consider disabling verbose logging in customer environments

## Vulnerability Scanning

This project is scanned with Bandit and Semgrep (via the open-source
[Automated Security Helper](https://github.com/awslabs/automated-security-helper)),
dependency vulnerability scanning, and container configuration scanning.

## Reporting Security Issues

If you discover a potential security issue in this project, please notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public GitHub issue.

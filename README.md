# Hadoop to Spark SQL Converter

**Automation framework for migrating HiveQL & Impala SQL to Spark SQL, using AI-driven multi-agent orchestration with built-in validation, accuracy scoring and result-set equivalence testing. Generated output is intended to be reviewed before use.**

---

## Problem Statement

Large enterprises running Hadoop ecosystems have thousands of HiveQL and Impala SQL scripts powering critical ETL pipelines. Migrating these to Spark SQL manually is:
- **Error-prone** — subtle semantic differences between Hive/Impala and Spark lead to silent data discrepancies
- **Time-consuming** — large, complex scripts (1000-7000+ lines) need substantial manual effort
- **Risky** — no easy way to verify that the converted SQL produces the same results

This tool automates the migration pipeline: conversion, validation, accuracy scoring, and equivalence testing on generated test data. It is intended to reduce manual migration effort; it does not remove the need to review the results (see [Limitations](#limitations)).

---

## Key Features

| Feature | Description |
|---------|-------------|
| **Fidelity-Focused Conversion** | Designed to preserve source logic, transformations and columns, and checked by syntax validation, accuracy scoring and equivalence testing. Output must be reviewed. |
| **Multi-Source Support** | HiveQL (.hql), Impala SQL (.sql), Control-M XML, Airflow DAGs |
| **AI Self-Correction** | Automatic error detection and fix (max 3 attempts per task) |
| **8-Dimension Accuracy** | Scored across syntactic, semantic, functional, structural, and other dimensions |
| **Result-Set Equivalence Testing** | MINUS/EXCEPT queries in both directions on generated test data — 0 rows indicates equivalent results for that data |
| **Parallel Processing** | 3-5 files processed simultaneously with async concurrency |
| **MWAA Workflow Migration** | Control-M and on-prem Airflow → Amazon Managed Workflows for Apache Airflow (Amazon MWAA) DAGs |
| **Interactive HTML Reports** | Accuracy dashboards, test reports, conversion reports |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          INPUT SOURCES                                       │
│  HiveQL (.hql)  │  Impala (.sql)  │  Control-M (.xml)  │  Airflow (.py)    │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │
                                 ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      UNIFIED ORCHESTRATOR                                    │
│  Intent Routing │ Parallel Scheduling │ Self-Correction │ State Tracking    │
│                                                                             │
│  Intents: hive_only │ impala_only │ mwaa_only │ test_only │ full_pipeline │
└────────────────────────────────┬────────────────────────────────────────────┘
                                 │
            ┌────────────────────┼────────────────────┐
            ▼                    ▼                     ▼
┌───────────────────┐ ┌──────────────────┐ ┌────────────────────┐
│  CODE CONVERSION  │ │ WORKFLOW MIGR.   │ │ TESTING & ACCURACY │
│  hive-converter │ │ mwaa-converter   │ │ test-orchestrator  │
│  impala-converter │ │ mwaa-validator   │ │ test-execution     │
│  hive-eval      │ │                  │ │ accuracy-calculator│
│  14 tools         │ │ 15 tools         │ │ 15 tools           │
└────────┬──────────┘ └────────┬─────────┘ └─────────┬──────────┘
         │                     │                      │
         ▼                     ▼                      ▼
┌───────────────────┐ ┌──────────────────┐ ┌────────────────────┐
│  Spark SQL Output │ │  MWAA DAGs       │ │  Test Reports      │
│  Accuracy Report  │ │  Airflow Vars    │ │  PASS/FAIL Verdict │
│  Mermaid Diagrams │ │  HTML Report     │ │  MINUS Results     │
└───────────────────┘ └──────────────────┘ └────────────────────┘
                                 │
                                 ▼
                    ┌──────────────────────┐
                    │  FINCH CONTAINERS    │
                    │  Spark 3.5 │ Hive 4.0│
                    └──────────────────────┘
```

---

## Security Measures

| Measure | Implementation |
|---------|----------------|
| **No hardcoded credentials** | All DAGs use `Variable.get()` for sensitive values. Security validator flags any hardcoded secrets. |
| **Controlled data flow** | Conversion is orchestrated locally. The contents of your input files are sent, as part of prompts, to the LLM backend you select (Kiro CLI or Claude Code CLI) — use only a backend approved for your data. Apart from that backend, the converter does not transmit input files or generated output to other external services. For the Claude backend, access Claude models through **Amazon Bedrock** for production or customer data (see [Backends](#backends)). |
| **Parameterized variables** | `${VAR}` substitutions preserved as-is — no interpolation of actual values during conversion. |
| **Container isolation** | Finch containers run with restricted volumes. Only `output/hive/spark_sql/`, `output/impala/spark_sql/` (read-only) and `output/testing/` are mounted. |
| **Container hardening** | Spark and Hive containers publish ports on `127.0.0.1` only, run with a read-only root filesystem (tmpfs for the paths the engines write to), drop all Linux capabilities, set `no-new-privileges`, and have memory/CPU limits. Outbound container networking is not blocked. Test data is locally generated and synthetic. |
| **Logging** | Logs are designed to record file names, counts and structural information rather than data values or credentials. Review logs before sharing them (see [SECURITY.md](SECURITY.md)). |
| **DAG security validation** | `validate_security` tool scans generated DAGs for: hardcoded AWS keys, S3 paths without Variable.get(), embedded passwords, insecure configurations. |
| **Input validation** | All MCP tool inputs validated via Pydantic models before processing. |
| **State file isolation** | Run state stored in `output/state/` with run-scoped IDs. No cross-run data leakage. |
| **Minimal dependencies** | A small set of well-known packages, pinned to exact versions in `requirements.txt` (fastmcp, pydantic, jinja2, pyyaml, click, defusedxml). |

---

## Prerequisites

- **Python 3.10+** (3.10-3.12 for optional Airflow-based DagBag validation of generated DAGs)
- **Finch** — lightweight container runtime (auto-installed by setup.sh on macOS)
- **An LLM CLI backend** — either **Kiro CLI** (`kiro-cli`, default) or the **Claude Code CLI** (`claude`). If you use the Claude Code CLI with production or customer data, configure it to use **Amazon Bedrock** as its model provider. See [Backends](#backends).
- **4GB+ RAM**, **5GB+ disk space** (for container images)

---

## Quick Start

```bash
# 1. One-time setup: installs dependencies, pulls images, starts the Spark
#    container, writes .kiro/settings/mcp.json and checks that every MCP server
#    starts. Exits non-zero and lists what to fix if anything is not ready.
chmod +x setup.sh
./setup.sh

# 2. Activate virtual environment
source venv/bin/activate

# 3. Add your input files (everything you provide goes in input/)
cp /path/to/*.hql input/hive/

# 4. Run conversion
python -m orchestrator.orchestrator_agent --intent hive_only

# 5. View results (everything generated lands in output/)
open output/hive/accuracy/dashboard.html
```

---

## Input & Output Layout

There is exactly **one input folder** and **one output folder**. The layout is
defined in a single place — [`project_paths.py`](project_paths.py).

```
input/                              ← everything you provide
├── hive/                           HiveQL scripts (*.hql, *.txt)
├── impala/                         Impala SQL scripts (*.sql)
├── mwaa/
│   ├── control_m/                  Control-M XML exports (*.xml)
│   ├── airflow/                    On-prem Airflow DAGs (*.py)
│   ├── hive_scripts/               (optional) .hql scripts referenced by Control-M jobs
│   ├── spark_sql_mapping.example.json   template — copy to spark_sql_mapping.json
│   └── spark_sql_mapping.json      (optional, yours) script path → Spark SQL location
└── ddl/                            (optional) DDL files for better schema inference in testing

output/                             ← everything that is generated (git-ignored)
├── hive/
│   ├── spark_sql/                  converted Spark SQL  (*.sql, *_FINAL.sql)
│   ├── accuracy/                   dashboard.html, summary_report.md, per-file reports
│   ├── mermaid/                    flow diagrams
│   ├── chunks/                     intermediate chunks of large files
│   └── validation/                 Spark execution reports (*_test_report.md)
├── impala/                         same structure as hive/
├── mwaa/
│   ├── dags/                       generated MWAA DAGs (*.py)
│   ├── conversion_report.html      conversion report
│   ├── airflow_variables.json      Airflow Variables to create in MWAA
│   └── job_ir/ plans/ chunks/ mermaid/ validation/ reports/   intermediates
├── testing/
│   ├── reports/                    SUMMARY_REPORT.html + {query}/TEST_REPORT.html
│   └── generated_data/ execution_output/ state/               intermediates
├── state/                          orchestrator run state (for --resume)
└── logs/                           all log files
```

Customer scripts placed in `input/` are git-ignored (only the folder structure
is tracked), so they are never committed by accident. Files in `output/` are
re-generated on every run.

---

## Backends

The orchestrator dispatches LLM work to a pluggable CLI backend. Pick one with
the `--backend` flag:

| Backend | CLI | MCP config | Select with |
|---------|-----|------------|-------------|
| `kiro` (default) | `kiro-cli` | `.kiro/settings/mcp.json` | `--backend kiro` |
| `claude` | `claude` (Claude Code) | `.claude/settings.json` | `--backend claude` |

Both backends run the **same** pipeline, prompts, steering rules, skills, and
MCP conversion servers — only the CLI invocation differs. Each CLI reads its own
MCP + permissions config from the project root, so no code changes are needed to
switch.

```bash
# Default (Kiro)
python -m orchestrator.orchestrator_agent --intent hive_only

# Claude Code, using Claude models through Amazon Bedrock
export CLAUDE_CODE_USE_BEDROCK=1
export AWS_REGION=us-east-1        # a region where your Claude models are enabled in Bedrock
python -m orchestrator.orchestrator_agent --intent hive_only --backend claude
```

> **Use Amazon Bedrock for the Claude backend.** The prompts sent to the backend
> contain the contents of your input files. For production or customer workloads,
> access Claude models through **Amazon Bedrock** rather than a third-party API
> endpoint, so that access control, logging and monitoring, data-handling terms and
> cost controls are managed in your AWS account. Claude Code supports Bedrock
> natively (`CLAUDE_CODE_USE_BEDROCK=1`); your IAM identity needs permission to invoke
> the models, and Amazon Bedrock Guardrails can be applied. Setup details:
> [docs/CLAUDE_CODE.md — Use Amazon Bedrock](docs/CLAUDE_CODE.md#use-amazon-bedrock-for-production-and-customer-workloads)
> and the official guide, https://code.claude.com/docs/en/amazon-bedrock.

Shared assets used by both backends:

- `.kiro/steering/*.md` — conversion rules (also summarized in `CLAUDE.md` for Claude Code)
- `.kiro/skills/**/SKILL.md` — reference conversion/accuracy/testing patterns
- The 8 MCP conversion servers (mirrored in `.kiro/settings/mcp.json` and `.claude/settings.json`)

**Full Claude Code walkthrough** — setup, every intent (HiveQL, Impala, MWAA,
testing, accuracy, full pipeline), CLI reference, and troubleshooting:
[`docs/CLAUDE_CODE.md`](docs/CLAUDE_CODE.md).

Both the HiveQL (`--intent hive_only`) and Impala (`--intent impala_only`)
pipelines are verified end to end on the Claude backend.

---

## Execution Steps (Detailed)

### Step 1: Setup

```bash
./setup.sh
```

This automatically:
- Installs Python virtual environment with all dependencies
- Installs and initializes Finch VM (macOS)
- Pulls container images (Spark 3.5.0, Hive 4.0.0)
- Creates project directory structure
- Configures 8 MCP servers in `.kiro/settings/mcp.json`
- Starts Spark container for validation

### Step 2: Add Input Files

```bash
# HiveQL files
cp /path/to/*.hql input/hive/

# OR Impala SQL files
cp /path/to/*.sql input/impala/

# OR Control-M workflows
cp /path/to/*.xml input/mwaa/control_m/

# OR on-prem Airflow DAGs (run with --source-type airflow)
cp /path/to/*.py input/mwaa/airflow/

# (Optional) DDL files for better schema inference in testing
cp /path/to/*.ddl input/ddl/
```

### Step 3: Run Conversion

```bash
source venv/bin/activate

# HiveQL → Spark SQL
python -m orchestrator.orchestrator_agent --intent hive_only

# Impala → Spark SQL
python -m orchestrator.orchestrator_agent --intent impala_only

# Control-M → MWAA DAGs   (add --source-type airflow to convert input/mwaa/airflow/ instead)
python -m orchestrator.orchestrator_agent --intent mwaa_only

# Full pipeline (HiveQL + Impala + MWAA; stages with an empty input folder are skipped)
python -m orchestrator.orchestrator_agent --intent full_pipeline
```

### Step 4: Run Equivalence Testing

No copying needed — testing compares the original HiveQL in `input/hive/`
with its converted Spark SQL in `output/hive/spark_sql/` (matched by file
name). Run the HiveQL conversion first, then:

```bash
# Run Hive vs Spark equivalence testing
python -m orchestrator.orchestrator_agent --intent test_only
```

### Step 5: View Results

| Output | Location |
|--------|----------|
| Converted Spark SQL (HiveQL) | `output/hive/spark_sql/*.sql` |
| Converted Spark SQL (Impala) | `output/impala/spark_sql/*.sql` |
| Accuracy Dashboard | `output/hive/accuracy/dashboard.html` (`output/impala/accuracy/` for Impala) |
| MWAA DAGs | `output/mwaa/dags/*.py` |
| Conversion Report | `output/mwaa/conversion_report.html` |
| Test Reports | `output/testing/reports/{query}/TEST_REPORT.html` |
| Test Summary | `output/testing/reports/SUMMARY_REPORT.html` |
| Logs | `output/logs/` |

---

## Conversion Pipeline (Per File)

```
1. ANALYZE     → Detect file complexity, UDFs, CTEs, line count
2. CHUNK       → Split files >1000 lines at safe boundaries (if needed)
3. CONVERT     → Apply 13 HiveQL→Spark or 12 Impala→Spark rules
4. VALIDATE    → Syntax + semantic checks
5. AUTO-FIX    → Self-correction loop (max 3 attempts)
6. EXECUTE     → Run in Spark container to verify it compiles
7. ACCURACY    → Score across 8 dimensions
8. DASHBOARD   → Generate HTML accuracy report
```

---

## 8-Dimension Accuracy Scoring

| Dimension | Weight | What It Measures |
|-----------|--------|------------------|
| Syntactic Correctness (SCS) | 15% | Does the Spark SQL parse without errors? |
| Semantic Equivalence (SES) | 20% | Does it mean the same thing as the source? |
| Functional Correctness (FCS) | 15% | Are all functions mapped correctly? |
| Structural Fidelity (SFS) | 10% | Is the query structure preserved? |
| Chunking Integrity (CIS) | 15% | Did chunking/merging lose any logic? |
| Coverage Score (CVS) | 15% | What % of source lines were converted? |
| NULL Handling (NHS) | 5% | Are NULL semantics preserved? |
| Breaking Changes (BCS) | 5% | Any changes that alter output data? |

---

## Equivalence Testing

The testing framework proves conversion correctness by executing both queries against identical data:

```
1. Extract DDL → Infer table schemas from HiveQL + DDL files
2. Generate Data → Create test data with matching join keys and WHERE literals
3. Start Containers → Hive 4.0 + Spark 3.5 via Finch
4. Execute Both → Run original HiveQL in Hive, converted SQL in Spark
5. MINUS Check → Run EXCEPT both directions:
     (Hive results) EXCEPT (Spark results) → must be 0 rows
     (Spark results) EXCEPT (Hive results) → must be 0 rows
6. Report → HTML report with PASS/FAIL verdict
```

**MINUS returns 0 rows both ways = the converted query returned the same result set as the original on the generated test data.**

This is strong evidence, not a proof of correctness. The test uses small synthetic datasets (5-10 rows per table), UDFs that are not available in the containers are stubbed, and non-deterministic functions are replaced, so behaviour on production data, edge cases not present in the generated data, and stubbed UDF logic are not covered. Treat a pass as one input to your review, not a substitute for it.

---

## Technology Stack

| Component | Technology |
|-----------|-----------|
| Language | Python 3.10+ |
| AI/LLM | Kiro CLI or Claude Code CLI (`--backend`); Claude models via Amazon Bedrock |
| Containers | Finch (Lima + containerd) |
| Spark | Apache Spark 3.5.0 |
| Hive | Apache Hive 4.0.0 |
| Airflow | Apache Airflow 2.11.2 (for DAG validation; installed by setup.sh) |
| MCP Framework | FastMCP 2.0+ |
| Templates | Jinja2 |
| Validation | Pydantic, Python AST |
| Configuration | YAML, JSON |

---

## Project Structure

```
HiveQL-SparkQL-Converter/
├── input/                     # ★ ALL inputs go here (hive/, impala/, mwaa/, ddl/)
├── output/                    # ★ ALL generated files land here (git-ignored)
├── project_paths.py           # Single definition of every input/output location
├── orchestrator/              # Unified multi-agent orchestrator
├── src/
│   ├── mcp_servers/           # HiveQL & Impala MCP servers
│   └── prompt_library/        # Conversion prompts
├── mwaa-converter/            # Control-M / Airflow → MWAA (code + config)
├── accuracy_framework/        # 8-dimension accuracy scoring
├── testing_framework/         # Hive vs Spark equivalence testing (code + compose file)
├── .kiro/
│   ├── settings/mcp.json      # MCP server configuration (Kiro backend)
│   ├── steering/              # AI conversion rules (always loaded)
│   └── skills/                # Reference material (loaded on-demand)
├── .claude/
│   └── settings.json          # MCP servers + permissions + hooks (Claude backend)
├── CLAUDE.md                  # Project guidance for the Claude Code CLI
├── setup.sh                   # One-time setup script
├── finch-compose.yml          # Spark container definition
└── requirements.txt           # Python dependencies
```

---

## CLI Reference

```bash
# Available intents
python -m orchestrator.orchestrator_agent --intent hive_only    # HiveQL → Spark SQL
python -m orchestrator.orchestrator_agent --intent impala_only    # Impala → Spark SQL
python -m orchestrator.orchestrator_agent --intent mwaa_only      # Control-M/Airflow → MWAA
python -m orchestrator.orchestrator_agent --intent test_only      # Equivalence testing
python -m orchestrator.orchestrator_agent --intent full_pipeline  # HiveQL + Impala + MWAA combined
python -m orchestrator.orchestrator_agent --intent accuracy_only  # Re-run accuracy scoring

# Options
--backend {kiro,claude}  # LLM dispatch backend (default: kiro)
--input DIR              # Override the input directory (defaults: input/hive, input/impala,
                         #   input/mwaa/control_m — or input/mwaa/airflow with --source-type airflow)
--source-type TYPE       # control_m (default) or airflow
--hql-input / --impala-input / --mwaa-input DIR  # Input directories for full_pipeline
--test-hive-dir / --test-spark-dir DIR  # Override test_only sources
                                        #   (defaults: input/hive, output/hive/spark_sql)
--keep-containers        # Don't tear down test containers after testing
--resume RUN_ID          # Resume a failed run from where it left off
```

---

## Naming: `hive` (formerly `hiveql`)

Everything that refers to the Hive dialect is now called **hive** (the language is
still written "HiveQL" in prose). If you are upgrading, re-run `./setup.sh` — it
regenerates `.kiro/settings/mcp.json` with the new server names and removes the
old containers.

| Old name | New name | Still accepted? |
|----------|----------|-----------------|
| `--intent hiveql_only` | `--intent hive_only` | Yes (deprecated alias, prints a notice) |
| `--convert-hiveql` (`run_converter.py`) | `--convert-hive` | Yes (deprecated alias) |
| `--source-type hiveql` (`analyze_accuracy.py`) | `--source-type hive` | Yes (deprecated alias) |
| `hiveql_conversion:` in `converter_config.yaml` (and `hiveql_*_dir` keys) | `hive_conversion:` (`hive_*_dir`) | Yes (migrated on load, logs a warning) |
| MCP servers `hiveql-converter`, `hiveql-eval` | `hive-converter`, `hive-eval` | No — update any custom MCP/steering config |
| Tools `identify_hiveql_file`, `chunk_hiveql_file` | `identify_hive_file`, `chunk_hive_file` | No |
| Tool argument `hiveql_file` / `hiveql_source` | `hive_file` / `source_file` | No |
| Containers `hiveql-spark`, `hiveql-hive` | `converter-spark` (Hive runs in the test containers) | No (setup.sh removes the old ones, and `converter-hive`) |
| Agent `hiveql_eval` | `hive_eval` | No |

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| Finch VM not running | `finch vm start` (may ask for your password), then `./setup.sh` |
| `spark-sql` fails with `UnknownHostException` | Re-run `./setup.sh`: it recreates `converter-spark` with `SPARK_LOCAL_IP=127.0.0.1` |
| `test_only` fails: port 10000 already allocated | `finch rm -f converter-hive` (created by older setup.sh versions; re-running `./setup.sh` also removes it) |
| MCP tools missing in Kiro IDE | Open this project folder itself as the Kiro workspace; Kiro reads `.kiro/settings/mcp.json` from the workspace root |
| Port conflict (4040) | `lsof -ti:4040 \| xargs kill -9` |
| Orchestrator timeout | Use `--resume RUN_ID` to continue from last completed step |
| Low accuracy score | Framework auto-fixes up to 3 times. Check `output/<hive|impala>/accuracy/` for details. |
| Container startup slow | First run pulls images (~2GB). Subsequent runs are fast. |
| kiro-cli not found | Install Kiro CLI (`~/.local/bin/kiro-cli`), or switch backend with `--backend claude` |
| claude not found | Install the Claude Code CLI, or switch backend with `--backend kiro` |

---

## Resume After Failure

The orchestrator persists state after each task. To resume a failed run:

```bash
# Find your run ID
ls output/state/

# Resume from where it stopped
python -m orchestrator.orchestrator_agent --intent hive_only --resume run_20260604_111340_949ac7
```

---

## Limitations

- **Review is required.** Conversion is performed by an LLM guided by rules and checked by automated validation. Generated Spark SQL and MWAA DAGs can contain errors; review and test them in a non-production environment before deployment.
- **Accuracy scores are heuristics.** The 8-dimension score estimates conversion quality from pattern analysis; it is not a guarantee of correctness.
- **Equivalence testing is limited to generated test data** (see above), with UDFs stubbed and non-deterministic functions replaced.
- **Input files are processed by an LLM backend.** Their contents are included in prompts sent to the backend you choose. Only process files you trust and are permitted to share with that backend; for the Claude backend, use Amazon Bedrock as the model provider.
- **Not a production service.** This is a sample migration accelerator, provided as is, without warranty.

## Disclaimer

This tool is a sample migration accelerator and is not intended for production use without additional security testing. Generated outputs (Spark SQL, MWAA DAGs) must be reviewed before deployment in any production environment. See SECURITY.md for production hardening recommendations.

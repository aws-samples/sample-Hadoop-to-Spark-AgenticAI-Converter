# MWAA Converter

Convert Control-M and Airflow workflows to Amazon MWAA (Managed Workflows for Apache Airflow) with EMR Serverless integration.

```
Control-M JSON/XML  ──►  MWAA Converter  ──►  Amazon MWAA DAGs
Airflow DAG .py     ──►  (This Tool)     ──►  + Airflow Variables
```

This is **Step 2** of a two-tool migration pipeline:
- Step 1 — **MWAA Converter (this tool)**: converts Control-M/Airflow orchestration jobs to MWAA DAGs
- Step 2 — HiveQL Converter (auto-triggered): converts `.hql` scripts to Spark SQL when `--convert-hive` is used

## Quick Start

```bash
# Prerequisites (Python 3.10+). All packages are pinned to exact versions.
pip install -r requirements.txt

# Optional: DagBag validation of generated DAGs needs Apache Airflow in a SEPARATE
# virtual environment (its pinned dependencies conflict with the converter's).
# ../setup.sh creates it as venv-airflow/ on Python 3.10-3.12; manually (Python 3.12):
python3 -m venv ../venv-airflow
../venv-airflow/bin/pip install "apache-airflow==2.11.2" "apache-airflow-providers-amazon" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.11.2/constraints-3.12.txt"

# Edit config for your environment
vi config/converter_config.yaml

# Inputs live in the project-level input/mwaa/ folder, outputs in output/mwaa/
# (run from the mwaa-converter/ directory)

# Mode 1: Standalone (Spark SQL already converted, mapping file provided)
#   All options default to the layout below, so a plain run is enough:
python run_converter.py
# ...which is equivalent to:
python run_converter.py \
  --input-dir ../input/mwaa/control_m \
  --output-dir ../output/mwaa \
  --spark-sql-mapping ../input/mwaa/spark_sql_mapping.json

# Mode 2: With HiveQL conversion (auto-converts .hql → Spark SQL)
#   Place .hql files in input/mwaa/hive_scripts/
python run_converter.py --convert-hive
```

The full multi-agent pipeline reads the same folders:
`python -m orchestrator.orchestrator_agent --intent mwaa_only` (add
`--source-type airflow` to convert `input/mwaa/airflow/` instead).

## Output

| File | Purpose | Action |
|------|---------|--------|
| `output/mwaa/dags/*.py` | MWAA DAG files | Deploy to MWAA S3 |
| `output/mwaa/airflow_variables.json` | Variables template | Import to Airflow |
| `output/mwaa/conversion_report.html` | Visual HTML report | Review |
| `output/mwaa/mermaid/*.md` | Orchestration diagrams | Documentation |
| `output/mwaa/spark_sql/*.sql` | Spark SQL converted from referenced `.hql` (`--convert-hive`) | Upload to the scripts bucket |
| `output/mwaa/job_ir/`, `plans/`, `chunks/`, `validation/`, `reports/` | Intermediate artifacts | Debug only |

## Architecture

### 8-Stage Pipeline

| Stage | What | LLM? | Module |
|-------|------|------|--------|
| 1. Parse | Control-M JSON/XML → JobIR | ✅ | `converter/parser.py` |
| 2. Chunk | Group by Smart Folder | ❌ | `converter/chunker.py` |
| 3. Analyze | Build dependency graph | ✅ | `converter/analyzer.py` |
| 4. Plan | Job → operator mapping | ✅ | `converter/planner.py` |
| 4.5 Convert HQL | HiveQL → Spark SQL (if `--convert-hive`) | ✅ | `converter/hive_bridge.py` |
| 5. Generate | ConversionPlan → DAG Python via Jinja2 | ❌ | `converter/validator.py` |
| 6. Validate | Syntax + task count + DagBag | ❌ | `converter/validator.py` |
| 7. Diagram | Mermaid orchestration chart | ✅ | `converter/mermaid_gen.py` |
| 8. Merge | Reports + variables | ✅ | `converter/merger.py` |

Stage 5 uses **no LLM** — the `DagBuilder` renders Jinja2 templates deterministically.

### Operator Mapping

Deterministic — defined in `converter/operator_map.py`:

| Script Type | MWAA Operator | Confidence |
|-------------|---------------|------------|
| `.hql`, `.sql` | `EmrServerlessStartJobOperator` | 95% |
| `.py` | `EmrServerlessStartJobOperator` | 90% |
| `.sh`, `.ksh` | `BashOperator` | 85% |
| Dummy/sentinel | `EmptyOperator` | 100% |
| Sub-flow | `TriggerDagRunOperator` | 80% |
| Cross-folder dep | `ExternalTaskSensor` | — |
| Unknown | `BashOperator` | 50% (flagged) |

Command-line detection extracts script type from `hive -f`, `spark-submit`, `python3`, `bash` patterns automatically.

### Validation (Stage 6)

Three checks, all must pass:
1. **Syntax** — Python `ast.parse()` + `compile()`
2. **Task count** — matches plan (1:1 mapping validation)
3. **DagBag** — Airflow loads the full DAG

Self-correction: up to 3 retry attempts on syntax/task-count failures.

## Input Format

### Control-M JSON
```json
{
  "Type": "Job:Command",
  "Job": "SALES_ETL_EXTRACT",
  "Folder": "SALES_FOLDER",
  "Command": "hive -f /scripts/sales_extract.hql",
  "Predecessors": []
}
```

### Control-M XML
```xml
<SMART_FOLDER FOLDER_NAME="DP_SALES_ETL">
  <JOB JOBNAME="SALES_Extract" TASKTYPE="Job" APPL_TYPE="AWS">
    <VARIABLE NAME="%%SCRIPT_PATH" VALUE="/scripts/sales_extract.hql"/>
    <INCOND NAME="Start-TO-Extract" ODATE="ODAT"/>
  </JOB>
</SMART_FOLDER>
```

### Spark SQL Mapping (`input/mwaa/spark_sql_mapping.json`)

This file is engagement-specific, so it is **not** in the repo (it is git-ignored).
Create it from the tracked template:

```bash
cp input/mwaa/spark_sql_mapping.example.json input/mwaa/spark_sql_mapping.json
```

```json
{
  "mappings": [
    {
      "source_script": "/scripts/sales_extract.hql",
      "target_script": "s3://${SCRIPTS_BUCKET}/spark_sql/sales_extract.sql"
    }
  ]
}
```

`source_script` must match the path exactly as it appears in the Control-M job.
The file is optional — without it DAGs are still generated and unmapped scripts
are flagged in the conversion report. With `--convert-hive` it is written for you.

## Required Airflow Variables

Set these in MWAA before running DAGs:

| Variable | Description |
|----------|-------------|
| `EMR_APP_ID` | EMR Serverless Application ID |
| `EMR_EXEC_ROLE_ARN` | EMR Execution Role ARN |
| `SCRIPTS_BUCKET` | S3 prefix for scripts, e.g. `s3://<your-scripts-bucket>` |
| `LOGS_BUCKET` | S3 prefix for logs, e.g. `s3://<your-logs-bucket>` |

> **Use buckets you own.** `<your-scripts-bucket>` / `<your-logs-bucket>` are placeholders. Create (or confirm ownership of) uniquely named buckets in your own account — for example include the account ID or a random suffix — **before** setting these variables. EMR Serverless runs the scripts found under `SCRIPTS_BUCKET`, so a bucket you do not control (including a short, guessable name that someone else could register) would let a third party supply the code that runs.

Variable names are configurable in `config/converter_config.yaml`.

## Configuration

Edit `config/converter_config.yaml`:

```yaml
airflow_variables:
  emr_app_id: "EMR_APP_ID"
  scripts_bucket: "SCRIPTS_BUCKET"

dag_defaults:
  owner: "airflow"
  retries: 1

emr_serverless:
  spark_submit_parameters: "--conf spark.executor.cores=4 ..."

validation:
  dagbag_python: "venv-airflow/bin/python"   # or an absolute path to any Python that has Airflow
```

## Conversion Fidelity

Design goals, each checked by the validation stage (results depend on the quality of the source input and on LLM-assisted parsing and planning):

- **1:1 Mapping** — each source job is mapped to one Airflow task; the report flags any mismatch
- **Dependency mapping** — internal dependencies become `>>` chains and cross-folder dependencies become `ExternalTaskSensor`; the report flags missing dependency edges
- **No Hardcoded Values** — environment values are read via `Variable.get()`; the security validator flags hardcoded values
- **Self-Correction** — up to 3 retry attempts on failures

## Limitations

- Operator mappings carry a confidence score (as low as 50% for some job types); low-confidence items are flagged for manual review.
- Generated DAGs are **not** guaranteed to be correct or production-ready. Review them and test them in a non-production MWAA environment before deployment.
- Script paths are interpolated into the generated Python source; only convert Control-M exports and Airflow DAGs that you trust.

## Deploy

```bash
# Upload DAGs
aws s3 cp output/mwaa/dags/ s3://your-mwaa-bucket/dags/ --recursive

# Import variables
airflow variables import output/mwaa/airflow_variables.json
```

## Troubleshooting

| Issue | Solution |
|-------|----------|
| `default` chunk appears | Source JSON missing `Folder` key |
| DagBag skipped | `venv-airflow/` not found — run `../setup.sh` with Python 3.10-3.12, or see Quick Start |
| Wrong variable names | Update `config/converter_config.yaml` → `airflow_variables` |
| Script mapping warning | Add missing entry to `input/mwaa/spark_sql_mapping.json` |
| Resume after failure | Re-run same command — progress saved in `output/mwaa/progress.json` |
| Start fresh | `rm -rf output/mwaa/` |

## Project Structure

```
mwaa-converter/
├── run_converter.py              ← CLI entry point (thin orchestrator)
├── config/converter_config.yaml  ← All configuration
├── converter/
│   ├── parser.py                 ← Stage 1: Parse source files
│   ├── chunker.py                ← Stage 2: Smart chunking
│   ├── analyzer.py               ← Stage 3: Dependency analysis
│   ├── planner.py                ← Stage 4: Conversion planning
│   ├── hive_bridge.py          ← Stage 4.5: HiveQL → Spark SQL bridge
│   ├── validator.py              ← Stage 5+6: DAG gen + validation
│   ├── mermaid_gen.py            ← Stage 7: Diagrams
│   ├── merger.py                 ← Stage 8: Reports + variables
│   ├── dag_builder.py            ← Deterministic Jinja2 DAG renderer
│   ├── operator_map.py           ← Deterministic operator mapping
│   ├── config_loader.py          ← YAML config loader
│   ├── html_report.py            ← HTML report generator
│   ├── models.py                 ← Pydantic data models
│   ├── templates/                ← Jinja2 DAG templates
│   ├── mcp/                      ← MCP server implementations
│   └── prompts/                  ← Prompt templates (prompts.yaml)
└── (no data folders — inputs and outputs live at the project root)

input/mwaa/                       ← Source files
├── control_m/                    ← Control-M XML
├── airflow/                      ← On-prem Airflow DAGs (*.py)
├── hive_scripts/                 ← .hql files (for --convert-hive)
├── spark_sql_mapping.example.json ← Template (tracked)
└── spark_sql_mapping.json        ← Your script path mapping (git-ignored)

output/mwaa/                      ← Generated output
├── dags/                         ← Deploy these to MWAA
├── mermaid/                      ← Orchestration diagrams
├── spark_sql/                    ← Converted .hql scripts (--convert-hive)
└── job_ir/ plans/ chunks/ validation/ reports/   ← Intermediate artifacts
```

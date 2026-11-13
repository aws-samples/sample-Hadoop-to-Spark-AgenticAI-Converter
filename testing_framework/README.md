# Hive vs Spark SQL Equivalence Test Framework

A fully independent framework that validates HiveQL-to-Spark SQL conversions by executing both queries against identical sample data in containerized Hive and Spark environments, then comparing outputs using **MINUS/EXCEPT queries** as the equivalence check. A pass means the two engines returned the same result set for the generated test data; it does not prove equivalence on production data.

## Architecture

The framework runs as **5 sequential Kiro sessions**, each with a clean LLM context window:

```
┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│  Session 1   │───▶│  Session 2   │───▶│  Session 3   │───▶│  Session 4   │───▶│  Session 5   │
│ DDL Extract  │    │ Data Create  │    │ Dep Analysis │    │ Finch Exec   │    │ Report Gen   │
│              │    │              │    │              │    │ + MINUS      │    │              │
│ schemas.json │    │ setup_*.sql  │    │ dep_graph.json│   │ minus_results│    │ TEST_REPORT  │
└──────────────┘    │ sample_data  │    └──────────────┘    │ .json        │    │ .md          │
                    │ tables/*.csv │                        └──────────────┘    └──────────────┘
                    └──────────────┘
```

Each session persists state to `output/testing/state/session_state.json` for crash recovery.

## Prerequisites

- **Python 3.8+**
- **Finch** (container runtime) — [Install via Homebrew](https://github.com/runfinch/finch): `brew install --cask finch`
- ~4GB disk space for container images (Hive 4.0.0 + Spark 3.5.0)

## Quick Start

```bash
# 1. Setup (one-time)
chmod +x setup.sh
./setup.sh

# 2. Provide the files (project root)
#    - Original HiveQL      → input/hive/               (the same files you convert)
#    - Converted Spark SQL  → output/hive/spark_sql/    (produced by the hive_only conversion)
#    - DDL files (optional) → input/ddl/
#    Pairs are matched by file name (query.hql ↔ query.sql / query_FINAL.sql).

# 3. Run tests (results land in output/testing/)
python testing_framework/run_tests.py
# ...or via the orchestrator:
python -m orchestrator.orchestrator_agent --intent test_only
```

## Usage

```bash
# Basic run
python run_tests.py

# Keep containers running after tests (for debugging)
python run_tests.py --keep-containers

# Resume from last completed session (after a crash)
python run_tests.py --resume

# Start fresh (delete all state)
python run_tests.py --clean

# Custom directories and timeout
python run_tests.py --hive-dir /path/to/hive --spark-dir /path/to/spark --timeout 900
```

### CLI Options

| Option | Default | Description |
|--------|---------|-------------|
| `--hive-dir` | `input/hive/` | Directory containing the original HiveQL files |
| `--spark-dir` | `output/hive/spark_sql/` | Directory containing the converted Spark SQL files |
| `--keep-containers` | `false` | Keep Hive/Spark containers running after tests |
| `--timeout` | `600` | Query execution timeout in seconds |
| `--resume` | `false` | Resume from last completed session |
| `--clean` | `false` | Delete state and start fresh |

## DDL Handling (Fully Automatic)

The framework handles DDL **without any user interaction**:

1. **Scan**: Reads ALL files in `input/ddl/` (any extension: `.sql`, `.hql`, `.ddl`, `.txt`)
2. **Parse**: Extracts every `CREATE TABLE` statement — handles messy files with multiple statements, junk lines, bad formatting, comments
3. **Match**: Cross-references with tables referenced in the HiveQL query (`FROM`, `JOIN`, `INSERT` clauses)
4. **Infer gaps**: For tables not found in DDL, infers schema from HiveQL analysis:
   - `*_id`, `*_key` → `INT`
   - `*_dt`, `*_date` → `DATE`
   - `*_amt`, `*_amount`, `*_price` → `DECIMAL(18,2)`
   - `*_ts`, `*_timestamp` → `TIMESTAMP`
   - `*_flag`, `is_*`, `has_*` → `BOOLEAN`
   - Everything else → `STRING`
5. **Output**: `output/testing/generated_data/<query>/schemas.json` with per-table source annotation (`"ddl_file"` or `"inferred"`)

DDL files are a best-effort input, not a requirement. Whatever's there gets used. Whatever's missing gets inferred.


## 5-Session Orchestrator

### Session 1: DDL Extraction
- **MCP Tool**: `extract_or_load_ddl`
- **Input**: `input/hive/*.hql` + `input/ddl/*`
- **Output**: `output/testing/generated_data/<query>/schemas.json`
- Scans DDL files, extracts table schemas, infers missing ones from HiveQL

### Session 2: Sample Data Creation
- **MCP Tool**: `generate_test_data`
- **Input**: `output/testing/generated_data/<query>/schemas.json`
- **Output**: `setup_hive.sql`, `setup_spark.sql`, `sample_data.json`, `tables/*.csv`
- Generates 5-10 rows per table with shared join keys, ~20% NULLs, edge cases
- Produces dialect-aware SQL (Hive: `ROW FORMAT DELIMITED STORED AS TEXTFILE`, Spark: plain syntax)

### Session 3: Dependency Analysis + Validation
- **MCP Tool**: `analyze_dependencies`
- **Input**: HiveQL file + `schemas.json` + `sample_data.json`
- **Output**: `output/testing/generated_data/<query>/dependency_graph.json`
- Maps table dependencies, CTE chains, UNION ALL branches
- Validates all tables have schemas and shared keys have matching data

### Session 4: Finch Execution + MINUS Validation
- **MCP Tools**: `start_test_containers`, `load_test_data_hive`, `load_test_data_spark`, `execute_hive_query`, `execute_spark_query`, `compare_results`, `run_minus_validation`
- **Input**: Setup SQL scripts + query files
- **Output**: `execution_output/hive/*`, `execution_output/spark/*`, `minus_results.json`
- Starts Hive 4.0.0 + Spark 3.5.0 containers via Finch
- Loads identical test data into both engines
- Executes full queries (handles 7000+ lines in one go)
- Runs MINUS/EXCEPT in both directions

### Session 5: Report Generation
- **MCP Tool**: `generate_test_report`
- **Input**: All outputs from Sessions 1-4
- **Output**: `output/testing/reports/TEST_REPORT.md`
- Comprehensive report with 10 sections (see Report Format below)

## Test Data Generation

Generated data includes:
- **Shared key pools**: Join keys get the same value pool across tables so JOINs produce results
- **Type-appropriate values**: INT, STRING, DATE, DECIMAL, BOOLEAN, TIMESTAMP
- **~20% NULLs** on nullable columns (row 0 always has full data)
- **Edge cases**: Empty strings, zero values, boundary dates
- **Reference storage**:
  - `output/testing/generated_data/<query>/sample_data.json` — structured JSON with full schema and all rows
  - `output/testing/generated_data/tables/{table_name}.csv` — one CSV per table for easy inspection

## MINUS/EXCEPT Validation

Both engines run the query on the same generated rows, with every `${VAR}` replaced
by the same value from `output/testing/generated_data/<query>/variables.json`.
`EXCEPT ALL` is used so duplicate-row differences are caught too.
Each query's files live in its own folders: `generated_data/<query>/`,
`execution_output/<query>/` and `reports/<query>/TEST_REPORT.html`.

The pass/fail is a MINUS query run in both directions:

```sql
-- Direction 1: Rows in Hive but not in Spark
(SELECT * FROM hive_results) EXCEPT ALL (SELECT * FROM spark_results)

-- Direction 2: Rows in Spark but not in Hive
(SELECT * FROM spark_results) EXCEPT ALL (SELECT * FROM hive_results)
```

- Both return 0 rows → **MINUS PASSED** ✅
- Either returns rows → **MINUS FAILED** ❌ (differing rows shown in report)

## Folder Structure

Code and configuration live in `testing_framework/`. All inputs are read from
the project-level `input/` folder and all results are written to `output/testing/`
(see `project_paths.py`).

```
testing_framework/                        ← code only
├── src/
│   ├── mcp_servers/
│   │   ├── test_orchestrator_server.py   ← Session tools (DDL, data, analysis, comparison, report)
│   │   └── test_execution_server.py      ← Container tools (start, load, execute, stop)
│   ├── prompt_library/
│   │   ├── prompts.yaml                  ← All 12 prompts (one per session + execution steps)
│   │   └── prompt_loader.py              ← Independent YAML prompt loader
│   └── report_generator/                 ← HTML report generation
├── finch-compose-test.yml                ← Hive 4.0 + Spark 3.5 container definitions
│                                           (mounts output/testing/ as /test_workspace)
├── run_tests.py                          ← Stand-alone runner (5 sequential sessions)
└── README.md                             ← This file

input/                                    ← what you provide
├── hive/                                 ← Original HiveQL files
└── ddl/                                  ← DDL files (optional, auto-scanned)

output/hive/spark_sql/                  ← Converted Spark SQL files (from the conversion step)

output/testing/                           ← everything the tests generate
├── generated_data/{query}/               ← Auto-generated test data + SQL scripts
│   ├── schemas.json                      ← Session 1: extracted table schemas
│   ├── sample_data.json                  ← Session 2: full reference data (JSON)
│   ├── setup_hive.sql                    ← Session 2: Hive-dialect DDL + INSERT
│   ├── setup_spark.sql                   ← Session 2: Spark-dialect DDL + INSERT
│   ├── dependency_graph.json             ← Session 3: table dependencies
│   └── tables/                           ← Session 2: one CSV per table
├── execution_output/{query}/             ← Session 4: raw engine outputs
│   ├── hive/                             ← Hive stdout/stderr per query
│   └── spark/                            ← Spark stdout/stderr per query
├── reports/                              ← Session 5: SUMMARY_REPORT.html + {query}/TEST_REPORT.html
└── state/session_state.json              ← Session state for crash recovery

output/logs/                              ← Per-server logs
```

## MCP Server Tools Reference

### test_orchestrator_server.py (6 tools)

| Tool | Session | Description |
|------|---------|-------------|
| `extract_or_load_ddl(hive_file)` | 1 | Auto-extract DDL from files + infer gaps |
| `generate_test_data(schema_file)` | 2 | Generate test data, SQL scripts, CSVs |
| `analyze_dependencies(hive_file, schema_file, data_file)` | 3 | Map dependencies, validate schemas |
| `compare_results(hive_output_file, spark_output_file)` | 4 | Row-by-row output comparison |
| `run_minus_validation(hive_results_file, spark_results_file)` | 4 | MINUS/EXCEPT equivalence check |
| `generate_test_report(test_name)` | 5 | Generate comprehensive TEST_REPORT.md |

### test_execution_server.py (6 tools)

| Tool | Description |
|------|-------------|
| `start_test_containers()` | Start Hive + Spark via finch-compose |
| `load_test_data_hive(setup_file)` | Load data into Hive via beeline |
| `load_test_data_spark(setup_file)` | Load data into Spark via spark-sql |
| `execute_hive_query(query_file)` | Execute full HiveQL (7000+ lines) |
| `execute_spark_query(query_file)` | Execute full Spark SQL (7000+ lines) |
| `stop_test_containers()` | Tear down containers |

## Sample Test Cases

The framework ships with 4 sample test pairs:

| Test | What It Validates |
|------|-------------------|
| `test_basic_transforms` | JOINs, IF(), COALESCE, filters, ORDER BY |
| `test_cte_window` | WITH clauses, ROW_NUMBER, PARTITION BY, aggregations |
| `test_null_date_string` | NULL handling, CASE, LPAD, DATEDIFF, DATE_FORMAT, UPPER/TRIM, CONCAT |
| `test_union_subquery` | UNION ALL, subqueries, GROUP BY across branches |

## Configuration

### .env

```env
HIVE_CONTAINER_NAME=hive-test-runner
SPARK_CONTAINER_NAME=spark-test-runner
HIVE_THRIFT_PORT=10000
SPARK_UI_PORT=4041
QUERY_TIMEOUT=600
COMPOSE_FILE=finch-compose-test.yml
```

### Container Ports

| Service | Port | Purpose |
|---------|------|---------|
| Hive | 10000 | Thrift (beeline) |
| Hive | 10002 | Web UI |
| Spark | 4041 | Spark UI (offset from converter's 4040) |

## Session State & Crash Recovery

State is persisted to `state/session_state.json` after each session completes. If the process crashes:

```bash
# Resume from where it left off
python run_tests.py --resume

# Or start completely fresh
python run_tests.py --clean
```

The state file tracks: run ID, timestamps, per-session status, output files, and MINUS results.

## Report Format (TEST_REPORT.md)

The final report contains 10 sections:

1. **Executive Summary** — Overall PASS/FAIL, MINUS result, query count
2. **Test Methodology** — 5-session approach, DDL handling, data gen strategy
3. **Infrastructure** — Hive 4.0.0, Spark 3.5.0, Finch runtime
4. **Table Schemas** — Full schema per table, source annotation (DDL vs inferred)
5. **Sample Data** — Data tables, shared keys, CSV references
6. **Dependency Graph** — Creation order, join keys, CTE chains
7. **Execution Results** — Hive/Spark success/fail, row counts, errors
8. **MINUS Query Results** — PASSED/FAILED, differing rows if any
9. **Diff Analysis** — Value mismatches, NULL handling, precision differences
10. **Conclusion** — Final verdict, recommendations

## Troubleshooting

### Containers won't start
```bash
# Check Finch is running
finch vm status

# Start Finch VM if needed
finch vm start

# Check for port conflicts
lsof -i :10000
lsof -i :4041
```

### Hive takes too long to start
Hive 4.0.0 can take 60-180 seconds to initialize. The framework waits up to 180s. If it still fails:
```bash
# Check Hive logs
finch logs hive-test-runner
```

### Query timeout
For very large files (7000+ lines), increase the timeout:
```bash
python run_tests.py --timeout 1200
```

### Stale state
If sessions are incorrectly marked as complete:
```bash
python run_tests.py --clean
```

### Container cleanup
```bash
finch compose -f testing_framework/finch-compose-test.yml down -v
```

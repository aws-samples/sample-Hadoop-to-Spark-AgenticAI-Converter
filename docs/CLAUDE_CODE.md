# Running the Converter with Claude Code

This guide covers running the Hadoop-to-Spark converter end to end using the
**Claude Code CLI (`claude`)** as the LLM backend, across every intent (HiveQL,
Impala, MWAA, testing, accuracy, full pipeline).

The converter is backend-agnostic: the Python orchestrator calls MCP tools, and
whenever a tool returns a prompt it dispatches that prompt to a CLI backend.
Pass `--backend claude` to use Claude Code; omit it (or pass `--backend kiro`)
to use `kiro-cli`. **Both backends run the exact same pipeline, prompts,
steering rules, skills, and MCP conversion servers — only the CLI invocation
differs.**

> Verified: `hive_only` and `impala_only` both run to completion (8/8 tasks)
> with `--backend claude`.

## Use Amazon Bedrock for production and customer workloads

The prompts sent to the backend contain the contents of your input files. For
production or customer workloads, access Claude models through **Amazon
Bedrock** rather than a third-party API endpoint, so that access control,
logging and monitoring, data-handling terms and cost controls are managed in
your AWS account. Claude Code can use Amazon Bedrock as its model provider:

```bash
# Use your AWS credentials (SSO profile, role, or environment) and a region
# where your Claude models are enabled in Amazon Bedrock
export CLAUDE_CODE_USE_BEDROCK=1
export AWS_REGION=us-east-1

python -m orchestrator.orchestrator_agent --intent hive_only --backend claude
```

Your IAM identity needs permission to invoke the Bedrock models you use, and the
models must be enabled for your account in that region. Amazon Bedrock
Guardrails can be applied to Claude Code traffic for content filtering. See the
official documentation for the current setup, IAM policy, model selection and
Guardrails instructions:

- Claude Code on Amazon Bedrock: https://code.claude.com/docs/en/amazon-bedrock
- Amazon Bedrock: https://docs.aws.amazon.com/bedrock/

The `kiro` backend (default) uses `kiro-cli`. Whichever backend you choose, use
only one that is approved for the data you are processing.

---

## How the Claude backend works

```
orchestrator  ──calls──►  MCP tool  ──returns prompt──►  execute_via_llm(backend="claude")
                                                              │
                                                              ▼
                                              claude --print --permission-mode
                                              bypassPermissions -p "<prompt>"
                                                              │
                                                              ▼
                                      Claude reads source files, applies the
                                      conversion rules, writes the output file
```

Key files:

| File | Role |
|------|------|
| `orchestrator/llm_dispatch.py` | `execute_via_claude()` + `execute_via_llm(prompt, backend, timeout)` dispatcher and backend registry |
| `orchestrator/orchestrator_agent.py` | `OrchestratorAgent(backend=...)`, `--backend {kiro,claude}` CLI flag, routes every LLM step through the selected backend |
| `.claude/settings.json` | MCP servers (the conversion engine) + permissions + hooks, read by the `claude` CLI |
| `CLAUDE.md` | Persistent project guidance Claude loads automatically (mirrors `.kiro/steering/`) |

Deterministic tools (`identify_hive_file`, `identify_impala_file`,
`analyze_accuracy`, DAG validators, …) do their work in Python and never call
the backend. Only tools that return a prompt are dispatched to `claude`.

---

## Prerequisites

- **Python 3.10+**
- **Claude Code CLI** on your `PATH` (`claude --version` should work)
- **Finch** (for the Spark/Hive execution + testing steps)
- Project set up once via `./setup.sh` (creates `venv/`, installs deps, pulls
  containers, writes `.kiro/settings/mcp.json` for this checkout)

Check the CLI and the backend flag:

```bash
claude --version
source venv/bin/activate
python -m orchestrator.orchestrator_agent --help   # shows --backend {kiro,claude}
```

---

## One-time setup

```bash
chmod +x setup.sh
./setup.sh
source venv/bin/activate
```

`setup.sh` is backend-neutral: it builds the venv, installs dependencies, starts
the Finch Spark + Hive containers, and (re)generates `.kiro/settings/mcp.json`
with absolute paths for the current checkout. `.claude/settings.json` uses
relative paths and is already correct for any checkout.

> **Note:** `setup.sh` can prompt interactively only if `python3` or `finch` is
> missing. With both present it runs unattended.

---

## Running each intent with Claude

Add `--backend claude` to any orchestrator command. All commands assume the
venv is active (`source venv/bin/activate`).

### 1. HiveQL → Spark SQL

```bash
# Put .hql / .txt files in input/hive/, then:
python -m orchestrator.orchestrator_agent --intent hive_only --backend claude

# Custom input directory:
python -m orchestrator.orchestrator_agent --intent hive_only --backend claude \
  --input path/to/hql_dir
```

Pipeline per file: analyze → (chunk if >1000 lines) → convert → validate →
execute in Spark → accuracy score → mermaid → dashboard.
Output: `output/hive/spark_sql/<name>.sql`, `output/hive/accuracy/dashboard.html`.

### 2. Impala → Spark SQL

```bash
# Put .sql files in input/impala/, then:
python -m orchestrator.orchestrator_agent --intent impala_only --backend claude

# Custom input directory:
python -m orchestrator.orchestrator_agent --intent impala_only --backend claude \
  --input path/to/impala_dir
```

Same pipeline as HiveQL via the `impala-converter` MCP server. Correctly handles
Impala-only constructs — `isnull`/`ifnull` → `COALESCE`, `decode` → `CASE`,
`nullifzero` → `NULLIF`, `STRAIGHT_JOIN` dropped. Output lands in
`output/impala/spark_sql/`, with its dashboard in `output/impala/accuracy/`.

### 3. Control-M / Airflow → MWAA DAGs

```bash
# Control-M (default) — reads input/mwaa/control_m/
python -m orchestrator.orchestrator_agent --intent mwaa_only --backend claude

# On-prem Airflow source — reads input/mwaa/airflow/
python -m orchestrator.orchestrator_agent --intent mwaa_only --backend claude \
  --source-type airflow
```

Output: `output/mwaa/dags/*.py`, plus validation + HTML conversion
report. DAG security/structure/dagbag validation is deterministic (no backend
call); parsing and planning are dispatched to Claude.

### 4. Equivalence testing (Hive vs Spark)

```bash
# No staging needed: originals come from input/hive/, converted SQL from
# output/hive/spark_sql/ (matched by file name). Optional DDL: input/ddl/
python -m orchestrator.orchestrator_agent --intent test_only --backend claude

# Keep containers up for inspection afterwards
python -m orchestrator.orchestrator_agent --intent test_only --backend claude \
  --keep-containers
```

Starts Hive 4.0 + Spark 3.5 containers, generates matching test data, runs both
queries, and checks `MINUS` both directions (0 rows both ways means both
engines returned the same results for the generated test data; it is not a proof
of equivalence on production data). Reports in `output/testing/reports/`.

### 5. Full pipeline (HiveQL + MWAA)

```bash
# Reads input/hive/ and input/mwaa/control_m/ by default
python -m orchestrator.orchestrator_agent --intent full_pipeline --backend claude
```

### 6. Accuracy only (re-score existing output)

```bash
python -m orchestrator.orchestrator_agent --intent accuracy_only --backend claude
```

Accuracy scoring is deterministic, so this intent produces identical results on
either backend.

---

## CLI reference

```bash
# Intents
--intent {hive_only, impala_only, mwaa_only, test_only, full_pipeline, accuracy_only}

# Backend selection
--backend {kiro, claude}     # default: kiro

# Common options
--input DIR                  # custom input dir (hive_only / impala_only / mwaa_only)
--hql-input DIR              # HiveQL dir for full_pipeline
--mwaa-input DIR             # Control-M/Airflow dir for full_pipeline
--source-type {control_m, airflow}
--test-hive-dir DIR          # HiveQL dir for test_only
--test-spark-dir DIR         # Spark SQL dir for test_only
--keep-containers            # don't tear down test containers
--resume RUN_ID              # resume a failed run (skips SUCCESS tasks)
```

Resume works identically on the Claude backend:

```bash
ls output/state/
python -m orchestrator.orchestrator_agent --intent hive_only --backend claude \
  --resume run_20261009_104039_7ad082
```

---

## `.claude/settings.json`

The `claude` CLI reads its MCP servers, permissions, and hooks from
`.claude/settings.json`. It declares the same 8 conversion MCP servers as
`.kiro/settings/mcp.json` (hive-converter, hive-eval, mwaa-converter,
mwaa-validator, accuracy-calculator, test-orchestrator, test-execution,
impala-converter), using relative `venv/bin/python` commands so it is portable
across checkouts.

- `permissions.allow` pre-approves the file, bash, and MCP operations the
  pipeline needs, so unattended runs don't stall on prompts.
- `hooks.afterWrite` prints a notice when a Spark SQL file or MWAA DAG is
  written.

Edit this file to add servers, tighten permissions, or add hooks.

---

## Switching / comparing backends

Because the two backends share everything except the CLI call, you can run the
same input through both and diff the output:

```bash
python -m orchestrator.orchestrator_agent --intent hive_only --backend kiro
mv output/hive/spark_sql output/hive/spark_sql.kiro

python -m orchestrator.orchestrator_agent --intent hive_only --backend claude
diff -ru output/hive/spark_sql.kiro output/hive/spark_sql
```

---

## Troubleshooting

| Issue | Fix |
|-------|-----|
| `claude: command not found` | Install the Claude Code CLI, or run with `--backend kiro` |
| Run stalls at an LLM step | Check `.claude/settings.json` `permissions.allow`; `bypassPermissions` mode avoids interactive approval |
| MCP server fails to connect | Confirm `venv/` exists and `./setup.sh` has been run in this checkout |
| Spark execution step fails | Ensure the Finch Spark container is up: `finch ps`; re-run `./setup.sh` |
| Output wrong directory | Each CLI runs with `cwd` at the project root; keep paths relative in `.claude/settings.json` |

---

## Disclaimer

Generated Spark SQL and MWAA DAGs are automation aids and must be reviewed
before deployment in customer/production environments. See `SECURITY.md`.

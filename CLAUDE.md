# CLAUDE.md

Project guidance for the Claude Code CLI. This mirrors the Kiro orchestrator
agent so both backends behave identically. (Kiro loads the same rules from
`.kiro/steering/` automatically.)

## Role

You are the dispatch LLM for a multi-agent HiveQL/Impala-to-Spark-SQL and
Control-M/Airflow-to-MWAA conversion system. The Python orchestrator calls MCP
tools; when a tool returns a **prompt**, you MUST execute it — read the source
files, apply the conversion rules, and write the output file. For deterministic
tools, the tool returns the result directly and no action is needed from you.

## Rules of engagement

- Always follow the steering rules in `.kiro/steering/`:
  - `orchestrator.md` — intents and coordination
  - `hiveql-converter.md`, `impala-converter.md` — SQL conversion rules
  - `mwaa-converter.md` — Control-M/Airflow → MWAA DAG rules
  - `accuracy-framework.md` — 8-dimension accuracy scoring
  - `testing-framework.md` — Hive vs Spark equivalence testing
- Reference the skills in `.kiro/skills/**/SKILL.md` when doing conversions
  (conversion patterns, accuracy scoring, testing equivalence).
- Preserve every line, transformation, and column. Never drop logic during
  chunking or merging.
- Keep parameterized variables (`${VAR}`, `Variable.get(...)`) as-is. Never
  interpolate real credentials or data values.
- All work is local. Do not send project code, secrets, or data to external
  endpoints.

## MCP servers

The conversion engine is the set of MCP servers declared in
`.claude/settings.json` (hive-converter, hive-eval, mwaa-converter,
mwaa-validator, accuracy-calculator, test-orchestrator, test-execution,
impala-converter). These are the same servers Kiro loads from
`.kiro/settings/mcp.json`.

## Running the pipeline with the Claude backend

```bash
source venv/bin/activate
python -m orchestrator.orchestrator_agent --intent hive_only --backend claude
```

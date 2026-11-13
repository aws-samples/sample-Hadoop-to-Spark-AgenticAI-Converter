# input/

Everything you provide to the converter goes here. All generated files are
written to `output/` (see the project README for the output layout).

| Folder | What to put there | Used by intent |
|--------|-------------------|----------------|
| `hive/` | HiveQL scripts (`*.hql`, `*.txt`) | `hive_only`, `full_pipeline`, `test_only` (original side of the comparison) |
| `impala/` | Impala SQL scripts (`*.sql`) | `impala_only` |
| `mwaa/control_m/` | Control-M XML exports (`*.xml`) | `mwaa_only`, `full_pipeline` |
| `mwaa/airflow/` | On-prem Airflow DAGs (`*.py`) | `mwaa_only --source-type airflow` |
| `mwaa/hive_scripts/` | *(optional)* `.hql` scripts referenced by Control-M jobs | `--convert-hive` in `mwaa-converter/run_converter.py` |
| `mwaa/spark_sql_mapping.json` | *(optional)* Maps script paths in jobs to their Spark SQL location — create it from `mwaa/spark_sql_mapping.example.json` | MWAA DAG generation |
| `ddl/` | *(optional)* `CREATE TABLE` DDL files (any extension) | `test_only` (better schema inference) |

Files in this folder are git-ignored (only the folder structure, this README
and `mwaa/spark_sql_mapping.example.json` are tracked), so customer scripts —
and your engagement-specific `spark_sql_mapping.json` — are never committed by
accident.

### Script mapping for MWAA

```bash
cp input/mwaa/spark_sql_mapping.example.json input/mwaa/spark_sql_mapping.json
# edit: one entry per .hql/.sql script referenced by your Control-M jobs
```

The file is optional: without it, DAGs are still generated and any unmapped
script is flagged in `output/mwaa/conversion_report.html`. With
`--convert-hive`, the converter writes this file for you.

```bash
cp /path/to/*.hql input/hive/
python -m orchestrator.orchestrator_agent --intent hive_only
```

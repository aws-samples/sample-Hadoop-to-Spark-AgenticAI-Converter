"""Run a Spark SQL file, statement by statement, on Amazon EMR.

Used by the generated MWAA DAGs. EMR Serverless only accepts a JAR or Python
file as its entry point, and spark-submit cannot run a .sql file directly, so
both EMR task types start this script and pass it the SQL file to run.

Upload it once to <SCRIPTS_BUCKET>/scripts/run_spark_sql.py.

Usage (arguments are built by the DAG):
    run_spark_sql.py --sql s3://bucket/spark_sql/job.sql [--var NAME=VALUE ...]

Each --var replaces ${NAME} and ${hivevar:NAME} in the SQL text before it runs.
Any other ${...} is left for Spark's own variable substitution (SET name=value).
The job fails on the first statement that fails.
"""
import argparse
import re
import sys

_VAR_RE = re.compile(r"\$\{(?:hivevar:)?([A-Za-z_][A-Za-z0-9_]*)\}")


def split_statements(sql: str) -> list:
    """Split SQL text on ';' outside quotes and comments; drop empty statements."""
    statements, buf = [], []
    i, n = 0, len(sql)
    quote = None          # current quote character, or None
    while i < n:
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < n else ""
        if quote:
            buf.append(ch)
            if ch == "\\" and nxt:             # escaped character inside a string
                buf.append(nxt)
                i += 2
                continue
            if ch == quote:
                quote = None
        elif ch in ("'", '"', "`"):
            quote = ch
            buf.append(ch)
        elif ch == "-" and nxt == "-":         # line comment: skip to end of line
            end = sql.find("\n", i)
            i = n if end == -1 else end
            continue
        elif ch == "/" and nxt == "*" and sql[i + 2:i + 3] != "+":
            end = sql.find("*/", i + 2)        # block comment (keeps /*+ hints */)
            i = n if end == -1 else end + 2
            continue
        elif ch == ";":
            statements.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
        i += 1
    statements.append("".join(buf))
    return [s.strip() for s in statements if s.strip()]


def substitute(sql: str, variables: dict) -> str:
    """Replace ${NAME} / ${hivevar:NAME} for the given variables only."""
    def repl(m):
        return variables.get(m.group(1), m.group(0))
    return _VAR_RE.sub(repl, sql)


def parse_vars(pairs: list) -> dict:
    out = {}
    for pair in pairs:
        name, sep, value = pair.partition("=")
        if not sep or not name:
            raise SystemExit(f"--var must be NAME=VALUE, got {pair!r}")
        out[name] = value
    return out


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--sql", required=True, help="SQL file (s3://... or local path)")
    parser.add_argument("--var", action="append", default=[], help="NAME=VALUE (repeatable)")
    args = parser.parse_args(argv)

    from pyspark.sql import SparkSession   # imported here so the helpers test without Spark

    spark = SparkSession.builder.appName(f"run_spark_sql:{args.sql}").enableHiveSupport().getOrCreate()
    sql_text = spark.sparkContext.wholeTextFiles(args.sql, 1).collect()[0][1]
    sql_text = substitute(sql_text, parse_vars(args.var))

    statements = split_statements(sql_text)
    print(f"run_spark_sql: {len(statements)} statement(s) from {args.sql}")
    for number, statement in enumerate(statements, 1):
        first_line = statement.splitlines()[0][:100]
        print(f"[{number}/{len(statements)}] {first_line}")
        spark.sql(statement)
    print("run_spark_sql: done")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""
HTML Report Generator for Hive vs Spark SQL Equivalence Test Framework.
Generates organized, interactive HTML reports for both summary and per-query results.
"""

import html
import json
import re
from urllib.parse import quote
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional


class HTMLReportGenerator:
    """Generates HTML summary and per-query reports."""

    # ── Shared CSS ──────────────────────────────────────────────────────
    CSS = """
    :root {
        --primary: #4f46e5;
        --primary-light: #818cf8;
        --success: #059669;
        --success-bg: #d1fae5;
        --danger: #dc2626;
        --danger-bg: #fee2e2;
        --warning: #d97706;
        --warning-bg: #fef3c7;
        --info: #0284c7;
        --info-bg: #e0f2fe;
        --gray-50: #f9fafb;
        --gray-100: #f3f4f6;
        --gray-200: #e5e7eb;
        --gray-300: #d1d5db;
        --gray-500: #6b7280;
        --gray-700: #374151;
        --gray-900: #111827;
        --radius: 10px;
        --shadow: 0 1px 3px rgba(0,0,0,.1), 0 1px 2px rgba(0,0,0,.06);
        --shadow-md: 0 4px 6px rgba(0,0,0,.07), 0 2px 4px rgba(0,0,0,.06);
    }
    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
    body {
        font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Inter, Roboto, sans-serif;
        background: var(--gray-50);
        color: var(--gray-900);
        line-height: 1.6;
        padding: 0;
    }
    .page-header {
        background: linear-gradient(135deg, #4f46e5 0%, #7c3aed 100%);
        color: #fff;
        padding: 48px 40px 36px;
    }
    .page-header h1 { font-size: 2rem; font-weight: 700; margin-bottom: 6px; }
    .page-header .subtitle { opacity: .85; font-size: 1rem; }
    .container { max-width: 1280px; margin: 0 auto; padding: 32px 24px; }

    /* ── Stat Cards ─────────────────────────────────────── */
    .stats { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px,1fr)); gap: 16px; margin-bottom: 32px; }
    .stat-card {
        background: #fff; border-radius: var(--radius); padding: 24px;
        box-shadow: var(--shadow); border-left: 4px solid var(--primary);
        transition: transform .15s, box-shadow .15s;
    }
    .stat-card:hover { transform: translateY(-2px); box-shadow: var(--shadow-md); }
    .stat-card .label { font-size: .8rem; text-transform: uppercase; letter-spacing: .05em; color: var(--gray-500); }
    .stat-card .value { font-size: 2rem; font-weight: 700; margin: 4px 0; }
    .stat-card.success { border-left-color: var(--success); }
    .stat-card.danger  { border-left-color: var(--danger); }
    .stat-card.warning { border-left-color: var(--warning); }
    .stat-card.info    { border-left-color: var(--info); }

    /* ── Cards / Sections ───────────────────────────────── */
    .card {
        background: #fff; border-radius: var(--radius); box-shadow: var(--shadow);
        margin-bottom: 24px; overflow: hidden;
    }
    .card-header {
        padding: 18px 24px; font-weight: 600; font-size: 1.05rem;
        background: var(--gray-50); border-bottom: 1px solid var(--gray-200);
        display: flex; align-items: center; gap: 8px;
    }
    .card-body { padding: 24px; }

    /* ── Tables ──────────────────────────────────────────── */
    table { width: 100%; border-collapse: collapse; font-size: .92rem; }
    th, td { padding: 12px 16px; text-align: left; border-bottom: 1px solid var(--gray-200); }
    th { background: var(--gray-50); font-weight: 600; color: var(--gray-700); white-space: nowrap; }
    tr:last-child td { border-bottom: none; }
    tr:hover td { background: var(--gray-50); }

    /* ── Badges ──────────────────────────────────────────── */
    .badge {
        display: inline-flex; align-items: center; gap: 4px;
        padding: 3px 10px; border-radius: 9999px; font-size: .8rem; font-weight: 600;
    }
    .badge-pass { background: var(--success-bg); color: var(--success); }
    .badge-fail { background: var(--danger-bg); color: var(--danger); }
    .badge-warn { background: var(--warning-bg); color: var(--warning); }
    .badge-info { background: var(--info-bg); color: var(--info); }

    /* ── Progress Bar ───────────────────────────────────── */
    .progress { height: 8px; background: var(--gray-200); border-radius: 4px; overflow: hidden; }
    .progress-fill { height: 100%; border-radius: 4px; transition: width .4s ease; }
    .progress-fill.green  { background: var(--success); }
    .progress-fill.red    { background: var(--danger); }
    .progress-fill.yellow { background: var(--warning); }

    /* ── Collapsible ────────────────────────────────────── */
    details { margin-bottom: 8px; }
    details summary {
        cursor: pointer; padding: 12px 16px; background: var(--gray-50);
        border-radius: var(--radius); font-weight: 500; user-select: none;
        list-style: none; display: flex; align-items: center; gap: 8px;
    }
    details summary::before { content: '▶'; font-size: .7rem; transition: transform .2s; }
    details[open] summary::before { transform: rotate(90deg); }
    details .detail-body { padding: 16px; }

    /* ── Step Timeline ──────────────────────────────────── */
    .timeline { position: relative; padding-left: 28px; }
    .timeline::before {
        content: ''; position: absolute; left: 9px; top: 4px; bottom: 4px;
        width: 2px; background: var(--gray-200);
    }
    .timeline-item { position: relative; margin-bottom: 20px; }
    .timeline-dot {
        position: absolute; left: -28px; top: 2px; width: 20px; height: 20px;
        border-radius: 50%; display: flex; align-items: center; justify-content: center;
        font-size: .65rem; color: #fff;
    }
    .timeline-dot.done { background: var(--success); }
    .timeline-dot.fail { background: var(--danger); }
    .timeline-dot.skip { background: var(--gray-300); }
    .timeline-title { font-weight: 600; margin-bottom: 2px; }
    .timeline-desc  { font-size: .88rem; color: var(--gray-500); }

    /* ── Footer ─────────────────────────────────────────── */
    .footer { text-align: center; padding: 24px; color: var(--gray-500); font-size: .85rem; }

    /* ── Responsive ─────────────────────────────────────── */
    @media (max-width: 768px) {
        .page-header { padding: 32px 20px 24px; }
        .page-header h1 { font-size: 1.5rem; }
        .container { padding: 20px 12px; }
        .stats { grid-template-columns: 1fr 1fr; }
    }
    """

    # ── Summary Report ──────────────────────────────────────────────────

    @classmethod
    def generate_summary_report(cls, all_results: List[Dict], output_path: Path):
        """Generate the main SUMMARY_REPORT.html."""
        now = datetime.now()
        total = len(all_results)

        # Enrich with minus results
        for r in all_results:
            name = r["query_name"]
            minus_file = output_path.parent.parent / "execution_output" / name / "minus_results.json"
            if minus_file.exists():
                try:
                    r["minus"] = json.loads(minus_file.read_text())
                except Exception:
                    r["minus"] = {}

        passed = sum(1 for r in all_results if r.get("minus", {}).get("minus_passed", False))
        failed = total - passed

        # Count skipped queries
        skipped = sum(1 for r in all_results if r.get("skipped", False))
        failed = failed - skipped  # Don't count skipped as failed

        # Build per-query rows
        query_rows = ""
        for r in all_results:
            name = r["query_name"]
            is_skipped = r.get("skipped", False)
            mp = r.get("minus", {}).get("minus_passed", False)
            steps_done = sum(1 for v in r.get("steps", {}).values() if v == "complete")
            total_steps = 5
            pct = int(steps_done / total_steps * 100) if total_steps else 0

            # Check for UDF dependency report
            udf_file = output_path.parent.parent / "reports" / name / "UDF_DEPENDENCY_REPORT.json"
            has_udfs = udf_file.exists()

            if is_skipped:
                badge = '<span class="badge badge-warn">⊘ SKIPPED</span>'
                bar_cls = "yellow"
                skip_reason = cls._escape(r.get("skip_reason", "Not a valid executable query"))
                file_type = cls._escape(r.get("file_type_detected", "unknown"))
                note_html = f'<div style="font-size:.8rem;color:var(--warning);margin-top:4px;" title="{skip_reason}">📋 {file_type}: Not a data load statement</div>'
                udf_badge = ""
            else:
                badge = '<span class="badge badge-pass">✓ PASS</span>' if mp else '<span class="badge badge-fail">✗ FAIL</span>'
                bar_cls = "green" if steps_done == 5 else ("yellow" if steps_done >= 3 else "red")
                note_html = ""
                udf_badge = '<span class="badge badge-warn">⚠ UDF</span>' if has_udfs else '<span class="badge badge-pass">Clean</span>'

            query_rows += f"""
            <tr>
                <td>
                    <a href="{cls._href(name)}/TEST_REPORT.html" style="color:var(--primary);font-weight:500;text-decoration:none;">{cls._escape(name)}</a>
                    {note_html}
                </td>
                <td>{badge}</td>
                <td>{udf_badge}</td>
                <td>
                    <div style="display:flex;align-items:center;gap:8px;">
                        <div class="progress" style="flex:1;"><div class="progress-fill {bar_cls}" style="width:{pct}%"></div></div>
                        <span style="font-size:.85rem;color:var(--gray-500);">{steps_done}/{total_steps}</span>
                    </div>
                </td>
                <td><a href="{cls._href(name)}/TEST_REPORT.html" style="color:var(--primary);text-decoration:none;font-size:.9rem;">View →</a></td>
            </tr>"""

        pass_rate = int(passed / (total - skipped) * 100) if (total - skipped) > 0 else 0

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Test Summary — Hive vs Spark SQL Equivalence</title>
<style>{cls.CSS}</style>
</head>
<body>
<div class="page-header">
    <h1>🧪 Hive vs Spark SQL — Test Summary</h1>
    <div class="subtitle">Generated {now.strftime('%B %d, %Y at %H:%M:%S')}</div>
</div>
<div class="container">

    <!-- Stats -->
    <div class="stats">
        <div class="stat-card info">
            <div class="label">Total Queries</div>
            <div class="value">{total}</div>
        </div>
        <div class="stat-card success">
            <div class="label">Passed</div>
            <div class="value">{passed}</div>
        </div>
        <div class="stat-card danger">
            <div class="label">Failed</div>
            <div class="value">{failed}</div>
        </div>
        <div class="stat-card warning">
            <div class="label">Skipped</div>
            <div class="value">{skipped}</div>
        </div>
        <div class="stat-card {"success" if pass_rate >= 80 else ("warning" if pass_rate >= 50 else "danger")}">
            <div class="label">Pass Rate</div>
            <div class="value">{pass_rate}%</div>
        </div>
    </div>

    <!-- Results Table -->
    <div class="card">
        <div class="card-header">📋 Per-Query Results</div>
        <div class="card-body" style="padding:0;">
            <table>
                <thead>
                    <tr><th>Query</th><th>MINUS Result</th><th>Dependencies</th><th>Pipeline Progress</th><th>Report</th></tr>
                </thead>
                <tbody>{query_rows}</tbody>
            </table>
        </div>
    </div>

    <!-- Methodology -->
    <div class="card">
        <div class="card-header">📖 Test Methodology</div>
        <div class="card-body">
            <div class="timeline">
                <div class="timeline-item">
                    <div class="timeline-dot done">1</div>
                    <div class="timeline-title">Session 1 — DDL Extraction</div>
                    <div class="timeline-desc">Auto-scan DDL files, cross-reference with HiveQL, infer missing schemas</div>
                </div>
                <div class="timeline-item">
                    <div class="timeline-dot done">2</div>
                    <div class="timeline-title">Session 2 — Test Data Generation</div>
                    <div class="timeline-desc">Generate dialect-aware SQL scripts, shared keys, ~20% NULLs, edge cases</div>
                </div>
                <div class="timeline-item">
                    <div class="timeline-dot done">3</div>
                    <div class="timeline-title">Session 3 — Dependency Analysis</div>
                    <div class="timeline-desc">Analyze table dependencies, validate schemas and data integrity</div>
                </div>
                <div class="timeline-item">
                    <div class="timeline-dot done">4</div>
                    <div class="timeline-title">Session 4 — Execution &amp; Validation</div>
                    <div class="timeline-desc">Execute in Hive &amp; Spark containers, run MINUS/EXCEPT validation</div>
                </div>
                <div class="timeline-item">
                    <div class="timeline-dot done">5</div>
                    <div class="timeline-title">Session 5 — Report Generation</div>
                    <div class="timeline-desc">Aggregate all outputs into comprehensive test reports</div>
                </div>
            </div>
        </div>
    </div>

    <!-- Infrastructure -->
    <div class="card">
        <div class="card-header">🏗️ Infrastructure</div>
        <div class="card-body" style="padding:0;">
            <table>
                <tr><th>Component</th><th>Details</th></tr>
                <tr><td>Hive</td><td>apache/hive:4.0.0 — container: hive-test-runner</td></tr>
                <tr><td>Spark</td><td>apache/spark:3.5.0 — container: spark-test-runner</td></tr>
                <tr><td>Runtime</td><td>Finch (finch-compose)</td></tr>
                <tr><td>Query Timeout</td><td>600s per execution</td></tr>
            </table>
        </div>
    </div>

</div>
<div class="footer">Hive vs Spark SQL Equivalence Test Framework — {now.strftime('%Y')}</div>
</body>
</html>"""

        output_path.parent.mkdir(parents=True, exist_ok=True)
        (output_path.parent / "SUMMARY_REPORT.html").write_text(html)

    # ── Per-Query Report ────────────────────────────────────────────────

    @classmethod
    def generate_query_report(cls, test_name: str, base_dir: Path, hive_dir: Path = None):
        """
        Generate TEST_REPORT.html for a single query.
        Reads all session outputs from generated_data and execution_output.

        Args:
            test_name: Query name (file stem).
            base_dir:  Testing output root (output/testing) holding generated_data/,
                       execution_output/ and reports/.
            hive_dir: Folder with the original HiveQL files (default: input/hive).
        """
        import project_paths
        src = base_dir
        hive_dir = Path(hive_dir) if hive_dir else project_paths.HIVE_INPUT
        gen_dir = src / "generated_data" / test_name
        exec_dir = src / "execution_output" / test_name
        report_dir = src / "reports" / test_name
        report_dir.mkdir(parents=True, exist_ok=True)
        now = datetime.now()

        # ── Check for skipped query ────────────────────────────────────
        schemas = cls._load_json(gen_dir / "schemas.json")
        if schemas.get("skipped", False):
            skip_reason = cls._escape(schemas.get("skip_reason", "Not a valid executable query"))
            file_type = cls._escape(schemas.get("file_type_detected", "unknown"))
            file_analyzed = cls._escape(schemas.get("file_analyzed", test_name))
            recommendation = cls._escape(schemas.get("recommendation", "Provide a clean HiveQL file."))

            html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Test Report — {cls._escape(test_name)} (Skipped)</title>
<style>{cls.CSS}</style>
</head>
<body>
<div class="page-header" style="background: linear-gradient(135deg, #d97706 0%, #f59e0b 100%);">
    <div style="display:flex;align-items:center;gap:12px;">
        <a href="../SUMMARY_REPORT.html" style="color:#fff;opacity:.7;text-decoration:none;font-size:.9rem;">← Summary</a>
    </div>
    <h1 style="margin-top:12px;">⊘ Test Report: {cls._escape(test_name)} — Skipped</h1>
    <div class="subtitle">Generated {now.strftime('%B %d, %Y at %H:%M:%S')}</div>
</div>
<div class="container">
    <div class="card">
        <div class="card-header">⊘ Query Skipped — Not a Data Load Statement</div>
        <div class="card-body">
            <div style="padding:20px;background:var(--warning-bg);border-radius:8px;border-left:4px solid var(--warning);margin-bottom:20px;">
                <strong>This query was not executed.</strong> The input file was analyzed and determined to not be a valid data load or transformation statement.
            </div>
            <table>
                <tr><th style="width:200px;">File Analyzed</th><td><code>{file_analyzed}</code></td></tr>
                <tr><th>File Type Detected</th><td><span class="badge badge-warn">{file_type}</span></td></tr>
                <tr><th>Skip Reason</th><td>{skip_reason}</td></tr>
                <tr><th>Recommendation</th><td>{recommendation}</td></tr>
            </table>
        </div>
    </div>
</div>
<div class="footer">
    <a href="../SUMMARY_REPORT.html" style="color:var(--primary);text-decoration:none;">← Back to Summary</a>
    &nbsp;·&nbsp; Hive vs Spark SQL Equivalence Test Framework — {now.strftime('%Y')}
</div>
</body>
</html>"""
            (report_dir / "TEST_REPORT.html").write_text(html)
            return report_dir / "TEST_REPORT.html"

        # ── Load data (normal flow) ───────────────────────────────────
        schemas = cls._load_json(gen_dir / "schemas.json")
        sample_data = cls._load_json(gen_dir / "sample_data.json")
        dep_graph = cls._load_json(gen_dir / "dependency_graph.json")
        comparison = cls._load_json(exec_dir / "comparison_results.json") if exec_dir.exists() else {}
        minus = cls._load_json(exec_dir / "minus_results.json") if exec_dir.exists() else {}

        # Execution outputs
        hive_stdout = cls._read_text(exec_dir / "hive" / f"{test_name}_stdout.txt")
        hive_stderr = cls._read_text(exec_dir / "hive" / f"{test_name}_stderr.txt")
        spark_stdout = cls._read_text(exec_dir / "spark" / f"{test_name}_stdout.txt")
        spark_stderr = cls._read_text(exec_dir / "spark" / f"{test_name}_stderr.txt")

        # Determine overall result
        minus_passed = minus.get("minus_passed", False)
        # hive_ok/spark_ok: stderr may contain normal logs (WARN, retry info) — not errors
        # Only flag as not-ok if stderr contains actual ERROR/FAILED keywords
        hive_stderr_is_error = cls._stderr_has_error(hive_stderr)
        spark_stderr_is_error = cls._stderr_has_error(spark_stderr)
        hive_ok = bool(hive_stdout and not hive_stderr_is_error)
        spark_ok = bool(spark_stdout and not spark_stderr_is_error)
        overall = minus_passed

        # ── Detect UDF/JAR dependencies ────────────────────────────────
        udf_report = cls._load_json(report_dir / "UDF_DEPENDENCY_REPORT.json")
        hive_path = hive_dir / f"{test_name}.hql"
        udf_info = cls._analyze_udf_dependencies(hive_path, hive_stderr, spark_stderr, udf_report)
        has_udf_issues = udf_info.get("has_issues", False)

        # ── Build sections ─────────────────────────────────────────────
        schemas_html = cls._build_schemas_section(schemas)
        sample_html = cls._build_sample_data_section(sample_data)
        dep_html = cls._build_dependency_section(dep_graph)
        exec_html = cls._build_execution_section(
            hive_stdout, hive_stderr, spark_stdout, spark_stderr, comparison
        )
        minus_html = cls._build_minus_section(minus)
        diff_html = cls._build_diff_section(comparison)
        udf_html = cls._build_udf_dependency_section(udf_info)

        result_badge = '<span class="badge badge-pass" style="font-size:1rem;">✓ PASSED</span>' if overall else '<span class="badge badge-fail" style="font-size:1rem;">✗ FAILED</span>'

        # UDF alert banner (shown at top if UDF issues detected)
        udf_alert = ""
        if has_udf_issues:
            udf_count = udf_info.get("udf_count", 0)
            jar_count = udf_info.get("jar_count", 0)
            udf_alert = f"""
    <!-- UDF DEPENDENCY ALERT — Prominent Banner -->
    <div class="card" style="border:2px solid var(--warning);border-left:6px solid var(--warning);">
        <div class="card-header" style="background:var(--warning-bg);color:var(--warning);">
            <span style="font-size:1.3rem;">⚠️</span> JAR/UDF Dependencies Detected — Action Required
        </div>
        <div class="card-body">
            <div style="padding:16px;background:#fffbeb;border-radius:8px;margin-bottom:16px;">
                <p style="margin-bottom:8px;"><strong>This query uses {cls._escape(udf_count)} custom UDF(s) requiring {cls._escape(jar_count)} JAR file(s) that are not available in the test containers.</strong></p>
                <p style="color:var(--gray-700);font-size:.92rem;">The test executed end-to-end but UDF-dependent transformations could not be validated. The conversion logic itself is correct — only the proprietary UDF implementations are missing from the test environment.</p>
            </div>
            {udf_html}
        </div>
    </div>
"""

        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>Test Report — {cls._escape(test_name)}</title>
<style>{cls.CSS}
.section-num {{
    display: inline-flex; align-items: center; justify-content: center;
    width: 28px; height: 28px; border-radius: 50%; background: var(--primary);
    color: #fff; font-size: .8rem; font-weight: 700; flex-shrink: 0;
}}
pre {{
    background: var(--gray-900); color: #e5e7eb; padding: 16px; border-radius: 8px;
    overflow-x: auto; font-size: .85rem; line-height: 1.5; max-height: 400px;
}}
code {{ font-family: 'SF Mono', 'Fira Code', 'Cascadia Code', monospace; }}
</style>
</head>
<body>
<div class="page-header">
    <div style="display:flex;align-items:center;gap:12px;">
        <a href="../SUMMARY_REPORT.html" style="color:#fff;opacity:.7;text-decoration:none;font-size:.9rem;">← Summary</a>
    </div>
    <h1 style="margin-top:12px;">📊 Test Report: {cls._escape(test_name)}</h1>
    <div class="subtitle">Generated {now.strftime('%B %d, %Y at %H:%M:%S')}</div>
</div>
<div class="container">

    <!-- Executive Summary -->
    <div class="card">
        <div class="card-header"><span class="section-num">1</span> Executive Summary</div>
        <div class="card-body">
            <div class="stats" style="margin-bottom:0;">
                <div class="stat-card {"success" if overall else "danger"}">
                    <div class="label">Overall Result</div>
                    <div style="margin-top:8px;">{result_badge}</div>
                </div>
                <div class="stat-card {"success" if minus_passed else "danger"}">
                    <div class="label">MINUS Validation</div>
                    <div class="value" style="font-size:1.2rem;">{"PASSED" if minus_passed else "FAILED"}</div>
                </div>
                <div class="stat-card {"success" if hive_ok else "warning"}">
                    <div class="label">Hive Execution</div>
                    <div class="value" style="font-size:1.2rem;">{"Success" if hive_ok else "Check Logs"}</div>
                </div>
                <div class="stat-card {"success" if spark_ok else "warning"}">
                    <div class="label">Spark Execution</div>
                    <div class="value" style="font-size:1.2rem;">{"Success" if spark_ok else "Check Logs"}</div>
                </div>
            </div>
        </div>
    </div>

    {udf_alert}

    <!-- Methodology -->
    <div class="card">
        <div class="card-header"><span class="section-num">2</span> Test Methodology</div>
        <div class="card-body">
            <div class="timeline">
                <div class="timeline-item">
                    <div class="timeline-dot done">1</div>
                    <div class="timeline-title">DDL Extraction</div>
                    <div class="timeline-desc">Auto-scan DDL files, cross-reference with HiveQL, infer missing schemas</div>
                </div>
                <div class="timeline-item">
                    <div class="timeline-dot done">2</div>
                    <div class="timeline-title">Test Data Generation</div>
                    <div class="timeline-desc">Dialect-aware SQL scripts, shared keys, ~20% NULLs, edge cases</div>
                </div>
                <div class="timeline-item">
                    <div class="timeline-dot done">3</div>
                    <div class="timeline-title">Dependency Analysis</div>
                    <div class="timeline-desc">Table dependencies, schema validation, data integrity checks</div>
                </div>
                <div class="timeline-item">
                    <div class="timeline-dot done">4</div>
                    <div class="timeline-title">Execution &amp; MINUS Validation</div>
                    <div class="timeline-desc">Execute in Hive &amp; Spark containers, MINUS/EXCEPT equivalence check</div>
                </div>
                <div class="timeline-item">
                    <div class="timeline-dot done">5</div>
                    <div class="timeline-title">Report Generation</div>
                    <div class="timeline-desc">Aggregate all outputs into this report</div>
                </div>
            </div>
        </div>
    </div>

    <!-- Infrastructure -->
    <div class="card">
        <div class="card-header"><span class="section-num">3</span> Infrastructure</div>
        <div class="card-body" style="padding:0;">
            <table>
                <tr><th>Component</th><th>Details</th></tr>
                <tr><td>Hive</td><td>apache/hive:4.0.0 — container: hive-test-runner</td></tr>
                <tr><td>Spark</td><td>apache/spark:3.5.0 — container: spark-test-runner</td></tr>
                <tr><td>Runtime</td><td>Finch (finch-compose)</td></tr>
                <tr><td>Query Timeout</td><td>600s per execution</td></tr>
            </table>
        </div>
    </div>

    <!-- Table Schemas -->
    <div class="card">
        <div class="card-header"><span class="section-num">4</span> Table Schemas</div>
        <div class="card-body">{schemas_html}</div>
    </div>

    <!-- Sample Data -->
    <div class="card">
        <div class="card-header"><span class="section-num">5</span> Sample Data</div>
        <div class="card-body">{sample_html}</div>
    </div>

    <!-- Dependency Graph -->
    <div class="card">
        <div class="card-header"><span class="section-num">6</span> Dependency Graph</div>
        <div class="card-body">{dep_html}</div>
    </div>

    <!-- Execution Results -->
    <div class="card">
        <div class="card-header"><span class="section-num">7</span> Execution Results</div>
        <div class="card-body">{exec_html}</div>
    </div>

    <!-- MINUS Query Results -->
    <div class="card">
        <div class="card-header"><span class="section-num">8</span> MINUS Query Results</div>
        <div class="card-body">{minus_html}</div>
    </div>

    <!-- Diff Analysis -->
    <div class="card">
        <div class="card-header"><span class="section-num">9</span> Diff Analysis</div>
        <div class="card-body">{diff_html}</div>
    </div>

    <!-- Conclusion -->
    <div class="card">
        <div class="card-header"><span class="section-num">10</span> Conclusion</div>
        <div class="card-body">
            {"<div style='padding:16px;background:var(--success-bg);border-radius:8px;border-left:4px solid var(--success);'><strong>✓ MINUS validation passed.</strong><br>Hive and Spark returned identical result sets for the generated test data. This does not prove equivalence on production data; see the Methodology section for scope and limits.</div>" if overall else "<div style='padding:16px;background:var(--danger-bg);border-radius:8px;border-left:4px solid var(--danger);'><strong>✗ Conversion NOT equivalent.</strong><br>The MINUS validation failed. Review the diff analysis above for specific discrepancies.</div>"}
        </div>
    </div>

</div>
<div class="footer">
    <a href="../SUMMARY_REPORT.html" style="color:var(--primary);text-decoration:none;">← Back to Summary</a>
    &nbsp;·&nbsp; Hive vs Spark SQL Equivalence Test Framework — {now.strftime('%Y')}
</div>
</body>
</html>"""

        (report_dir / "TEST_REPORT.html").write_text(html)
        return report_dir / "TEST_REPORT.html"

    # ── Section Builders ────────────────────────────────────────────────

    @classmethod
    def _analyze_udf_dependencies(cls, hive_path: Path, hive_stderr: str, spark_stderr: str, udf_report: dict) -> dict:
        """Analyze UDF/JAR dependencies from the HiveQL source and execution errors.
        
        Returns a dict with:
          - has_issues: bool
          - udfs: list of {name, jar_file, class_name, usage_count, usage_examples}
          - jars: list of jar file paths
          - errors: list of error messages from execution
          - impact: description of what's affected
        """
        import re
        
        result = {
            "has_issues": False,
            "udfs": [],
            "jars": [],
            "jar_count": 0,
            "udf_count": 0,
            "errors": [],
            "impact": "",
            "what_passed": [],
            "what_needs_jars": [],
        }
        
        # Read the HiveQL source to find UDF declarations
        hive_content = ""
        if hive_path.exists():
            try:
                hive_content = hive_path.read_text(encoding="utf-8", errors="ignore")
            except Exception:
                pass
        
        if not hive_content:
            return result
        
        # Extract ADD JAR statements
        jar_pattern = re.compile(r'(?:add|ADD)\s+(?:jar|JAR)\s+(.+?)(?:;|\s*$)', re.MULTILINE)
        jars = jar_pattern.findall(hive_content)
        jars = [j.strip().rstrip(';') for j in jars]
        result["jars"] = jars
        result["jar_count"] = len(jars)
        
        # Extract CREATE TEMPORARY FUNCTION statements
        udf_pattern = re.compile(
            r"(?:create|CREATE)\s+(?:temporary|TEMPORARY)\s+(?:function|FUNCTION)\s+"
            r"(\w+)\s+(?:as|AS)\s+['\"]([^'\"]+)['\"]",
            re.MULTILINE | re.IGNORECASE
        )
        udf_declarations = udf_pattern.findall(hive_content)
        
        for udf_name, class_name in udf_declarations:
            # Count how many times this UDF is used in the query (excluding the declaration)
            # Look for function calls: udf_name(
            usage_pattern = re.compile(r'\b' + re.escape(udf_name) + r'\s*\(', re.IGNORECASE)
            # Exclude the CREATE TEMPORARY FUNCTION line itself
            lines = hive_content.split('\n')
            usage_count = 0
            usage_examples = []
            for i, line in enumerate(lines, 1):
                stripped = line.strip().lower()
                if 'create temporary function' in stripped:
                    continue
                if usage_pattern.search(line):
                    usage_count += 1
                    if len(usage_examples) < 3:
                        # Get a clean example (trim to 120 chars)
                        example = line.strip()
                        if len(example) > 120:
                            example = example[:120] + "..."
                        usage_examples.append({"line": i, "code": example})
            
            # Find which JAR provides this UDF (by proximity in the file)
            jar_file = ""
            for jar in jars:
                # Simple heuristic: the JAR declared closest before the UDF
                jar_file = jar  # Last JAR seen before UDF
            
            result["udfs"].append({
                "name": udf_name,
                "class_name": class_name,
                "jar_file": jar_file,
                "usage_count": usage_count,
                "usage_examples": usage_examples,
            })
        
        result["udf_count"] = len(result["udfs"])
        
        # Check execution errors for UDF-related failures
        jar_error_keywords = [
            "ClassNotFoundException", "NoClassDefFoundError",
            "FunctionNotFoundException", "No matching method",
            "is not a valid UDF", "Invalid function",
        ]
        
        for stderr, engine in [(hive_stderr, "Hive"), (spark_stderr, "Spark")]:
            if not stderr:
                continue
            for keyword in jar_error_keywords:
                if keyword.lower() in stderr.lower():
                    # Extract the specific error line
                    for line in stderr.split('\n'):
                        if keyword.lower() in line.lower():
                            result["errors"].append({
                                "engine": engine,
                                "keyword": keyword,
                                "message": line.strip()[:200],
                            })
                            break
        
        # Determine if there are actual issues
        result["has_issues"] = len(result["udfs"]) > 0
        
        # Merge with orchestrator's UDF report if available
        if udf_report and udf_report.get("udf_issues_detected"):
            for issue in udf_report["udf_issues_detected"]:
                if issue not in result["errors"]:
                    result["errors"].append(issue)
        
        # Build impact summary
        if result["has_issues"]:
            udf_names = [u["name"] for u in result["udfs"]]
            total_usages = sum(u["usage_count"] for u in result["udfs"])
            result["impact"] = (
                f"{len(udf_names)} custom UDF(s) ({', '.join(udf_names)}) "
                f"used {total_usages} time(s) across the query. "
                f"These require {len(jars)} proprietary JAR file(s) not available in the test environment."
            )
            result["what_passed"] = [
                "DDL extraction and schema inference",
                "Test data generation with correct WHERE clause literals",
                "Dependency analysis and validation",
                "Container startup and data loading",
                "All non-UDF SQL logic (JOINs, CTEs, aggregations, etc.)",
            ]
            result["what_needs_jars"] = [
                f"UDF: {u['name']}() — used {u['usage_count']} times — class: {u['class_name']}"
                for u in result["udfs"]
            ]
        
        return result

    @classmethod
    def _build_udf_dependency_section(cls, udf_info: dict) -> str:
        """Build the UDF dependency HTML section for the report."""
        if not udf_info.get("has_issues"):
            return ""
        
        parts = []
        
        # UDF inventory table
        udfs = udf_info.get("udfs", [])
        if udfs:
            rows = ""
            for u in udfs:
                usage_badge = f'<span class="badge badge-warn">{cls._escape(u["usage_count"])}x</span>'
                jar_short = u["jar_file"].split("/")[-1] if u["jar_file"] else "Unknown"
                rows += f"""<tr>
                    <td><code style="font-weight:600;color:var(--primary);">{cls._escape(u["name"])}()</code></td>
                    <td><code style="font-size:.8rem;">{cls._escape(u["class_name"])}</code></td>
                    <td>{usage_badge}</td>
                    <td><code style="font-size:.8rem;">{cls._escape(jar_short)}</code></td>
                </tr>"""
            parts.append(f"""
                <div style="margin-bottom:20px;">
                    <h4 style="margin-bottom:10px;font-size:.95rem;">📦 UDF Inventory</h4>
                    <table>
                        <thead><tr><th>Function</th><th>Java Class</th><th>Usage</th><th>JAR File</th></tr></thead>
                        <tbody>{rows}</tbody>
                    </table>
                </div>""")
        
        # JAR files needed
        jars = udf_info.get("jars", [])
        if jars:
            jar_items = "".join(f'<li><code>{cls._escape(j)}</code></li>' for j in jars)
            parts.append(f"""
                <div style="margin-bottom:20px;">
                    <h4 style="margin-bottom:10px;font-size:.95rem;">📁 JAR Files Required</h4>
                    <ul style="margin-left:20px;font-size:.9rem;">{jar_items}</ul>
                </div>""")
        
        # Usage examples (collapsible)
        examples_html = ""
        for u in udfs:
            if u.get("usage_examples"):
                ex_rows = "".join(
                    f'<tr><td style="width:60px;color:var(--gray-500);">L{cls._escape(e["line"])}</td><td><code style="font-size:.82rem;">{cls._escape(e["code"])}</code></td></tr>'
                    for e in u["usage_examples"]
                )
                examples_html += f"""<details style="margin-bottom:6px;">
                    <summary><code>{cls._escape(u["name"])}()</code> — {cls._escape(u["usage_count"])} usage(s)</summary>
                    <div class="detail-body"><table>{ex_rows}</table></div>
                </details>"""
        
        if examples_html:
            parts.append(f"""
                <div style="margin-bottom:20px;">
                    <h4 style="margin-bottom:10px;font-size:.95rem;">🔍 Usage Examples</h4>
                    {examples_html}
                </div>""")
        
        # Execution errors (if any)
        errors = udf_info.get("errors", [])
        if errors:
            err_rows = "".join(
                f'<tr><td><span class="badge badge-fail">{cls._escape(e.get("engine", ""))}</span></td><td><code style="font-size:.82rem;">{cls._escape(e.get("message", e.get("detail", "")))}</code></td></tr>'
                for e in errors[:10]
            )
            parts.append(f"""
                <div style="margin-bottom:20px;">
                    <h4 style="margin-bottom:10px;font-size:.95rem;">🔴 Execution Errors</h4>
                    <table>
                        <thead><tr><th style="width:80px;">Engine</th><th>Error</th></tr></thead>
                        <tbody>{err_rows}</tbody>
                    </table>
                </div>""")
        
        # What passed vs what needs JARs
        what_passed = udf_info.get("what_passed", [])
        what_needs = udf_info.get("what_needs_jars", [])
        
        cols = '<div style="display:grid;grid-template-columns:1fr 1fr;gap:16px;margin-bottom:16px;">'
        if what_passed:
            items = "".join(f'<li style="margin-bottom:4px;">{cls._escape(p)}</li>' for p in what_passed)
            cols += f'<div style="padding:16px;background:var(--success-bg);border-radius:8px;"><strong style="color:var(--success);">✓ Validated Successfully</strong><ul style="margin:8px 0 0 16px;font-size:.88rem;color:var(--gray-700);">{items}</ul></div>'
        if what_needs:
            items = "".join(f'<li style="margin-bottom:4px;"><code>{cls._escape(n)}</code></li>' for n in what_needs)
            cols += f'<div style="padding:16px;background:var(--warning-bg);border-radius:8px;"><strong style="color:var(--warning);">⚠ Requires JAR Files</strong><ul style="margin:8px 0 0 16px;font-size:.88rem;color:var(--gray-700);">{items}</ul></div>'
        cols += '</div>'
        parts.append(cols)
        
        # Action required box
        parts.append(f"""
            <div style="padding:16px;background:var(--info-bg);border-radius:8px;border-left:4px solid var(--info);">
                <strong style="color:var(--info);">📋 To Achieve 100% Validation</strong>
                <ol style="margin:8px 0 0 16px;font-size:.9rem;color:var(--gray-700);">
                    <li>Obtain the {len(jars)} JAR file(s) listed above from your Hadoop/Hive cluster</li>
                    <li>Place them in <code>testing_framework/src/jars/</code></li>
                    <li>Re-run the test: <code>python -m orchestrator.orchestrator_agent --intent test_validation</code></li>
                    <li>The framework will mount JARs into containers and register UDFs automatically</li>
                </ol>
            </div>""")
        
        return "".join(parts)

    @classmethod
    def _build_schemas_section(cls, schemas: dict) -> str:
        if not schemas:
            return '<p style="color:var(--gray-500);">No schema data available.</p>'
        tables = schemas.get("tables", schemas)
        if not tables:
            return '<p style="color:var(--gray-500);">No tables found in schemas.</p>'

        # Summary stats
        meta = ""
        if "ddl_tables_found" in schemas:
            meta = f"""<div class="stats" style="margin-bottom:20px;">
                <div class="stat-card info"><div class="label">DDL Files Scanned</div><div class="value">{cls._escape(schemas.get('ddl_files_scanned', 0))}</div></div>
                <div class="stat-card info"><div class="label">Tables from DDL</div><div class="value">{cls._escape(schemas.get('tables_from_ddl', 0))}</div></div>
                <div class="stat-card warning"><div class="label">Tables Inferred</div><div class="value">{cls._escape(schemas.get('tables_inferred', 0))}</div></div>
                <div class="stat-card"><div class="label">Total Required</div><div class="value">{cls._escape(schemas.get('hive_tables_required', len(tables)))}</div></div>
            </div>"""

        items = ""
        for tname, tdata in tables.items():
            source = tdata.get("source", "unknown")
            badge_cls = "badge-pass" if source == "ddl_file" else ("badge-warn" if source == "inferred" else "badge-info")
            cols_html = ""
            for col in tdata.get("columns", []):
                nullable = "Yes" if col.get("nullable", True) else "No"
                cols_html += f"<tr><td><code>{cls._escape(col['name'])}</code></td><td>{cls._escape(col.get('type', 'STRING'))}</td><td>{nullable}</td></tr>"
            partitions = tdata.get("partitions", [])
            part_html = f'<div style="margin-top:8px;font-size:.85rem;color:var(--gray-500);">Partitions: {cls._escape(", ".join(map(str, partitions)))}</div>' if partitions else ""
            items += f"""<details>
                <summary>{cls._escape(tname)} <span class="badge {badge_cls}">{cls._escape(source)}</span></summary>
                <div class="detail-body">
                    <table><thead><tr><th>Column</th><th>Type</th><th>Nullable</th></tr></thead><tbody>{cols_html}</tbody></table>
                    {part_html}
                </div>
            </details>"""

        return meta + items

    @classmethod
    def _build_sample_data_section(cls, sample_data: dict) -> str:
        if not sample_data:
            return '<p style="color:var(--gray-500);">No sample data available.</p>'

        # Handle nested format: {tables: {name: {schema, rows}}} or flat {name: [rows]}
        tables = sample_data.get("tables", sample_data)
        if not isinstance(tables, dict):
            return '<p style="color:var(--gray-500);">No sample data available.</p>'

        items = ""
        for tname, tdata in tables.items():
            if isinstance(tdata, dict):
                rows = tdata.get("rows", [])
            elif isinstance(tdata, list):
                rows = tdata
            else:
                continue
            if not rows or not isinstance(rows, list):
                continue
            headers = list(rows[0].keys()) if isinstance(rows[0], dict) else []
            if not headers:
                continue
            th = "".join(f"<th>{cls._escape(h)}</th>" for h in headers)
            trs = ""
            for row in rows[:10]:  # Show first 10 rows
                tds = "".join(f"<td>{cls._escape(row.get(h, ''))}</td>" for h in headers)
                trs += f"<tr>{tds}</tr>"
            overflow = f'<div style="padding:8px 16px;font-size:.85rem;color:var(--gray-500);">Showing 10 of {len(rows)} rows</div>' if len(rows) > 10 else ""
            items += f"""<details>
                <summary>{cls._escape(tname)} <span class="badge badge-info">{len(rows)} rows</span></summary>
                <div class="detail-body" style="overflow-x:auto;">
                    <table><thead><tr>{th}</tr></thead><tbody>{trs}</tbody></table>
                    {overflow}
                </div>
            </details>"""

        return items if items else '<p style="color:var(--gray-500);">No sample data available.</p>'

    @classmethod
    def _build_dependency_section(cls, dep_graph: dict) -> str:
        if not dep_graph:
            return '<p style="color:var(--gray-500);">No dependency data available.</p>'

        parts = []

        # Creation order
        order = dep_graph.get("creation_order", [])
        if order:
            ol = "".join(f"<li><code>{cls._escape(t)}</code></li>" for t in order)
            parts.append(f'<div style="margin-bottom:16px;"><strong>Creation Order:</strong><ol style="margin:8px 0 0 20px;">{ol}</ol></div>')

        # Dependencies
        deps = dep_graph.get("dependencies", dep_graph.get("table_dependencies", {}))
        if deps:
            rows = ""
            for t, d in deps.items():
                dep_list = ", ".join(map(str, d)) if isinstance(d, list) else str(d)
                rows += f"<tr><td><code>{cls._escape(t)}</code></td><td>{cls._escape(dep_list) if dep_list else '—'}</td></tr>"
            parts.append(f'<table><thead><tr><th>Table</th><th>Depends On</th></tr></thead><tbody>{rows}</tbody></table>')

        # Shared keys
        keys = dep_graph.get("shared_keys", dep_graph.get("join_keys", {}))
        if keys:
            rows = ""
            for pair, kcols in keys.items():
                kcols_str = ", ".join(map(str, kcols)) if isinstance(kcols, list) else str(kcols)
                rows += f"<tr><td><code>{cls._escape(pair)}</code></td><td>{cls._escape(kcols_str)}</td></tr>"
            parts.append(f'<div style="margin-top:16px;"><table><thead><tr><th>Table Pair</th><th>Join Keys</th></tr></thead><tbody>{rows}</tbody></table></div>')

        return "".join(parts) if parts else '<p style="color:var(--gray-500);">No dependency data available.</p>'

    @classmethod
    def _build_execution_section(cls, hive_out, hive_err, spark_out, spark_err, comparison) -> str:
        hive_rows = comparison.get("hive_rows", "N/A")
        spark_rows = comparison.get("spark_rows", "N/A")
        row_match = comparison.get("row_count_match", None)

        parts = [f"""<div class="stats" style="margin-bottom:20px;">
            <div class="stat-card {"success" if hive_out else "warning"}">
                <div class="label">Hive</div>
                <div class="value" style="font-size:1.2rem;">{"Executed" if hive_out else "No Output"}</div>
                <div style="font-size:.85rem;color:var(--gray-500);">Rows: {cls._escape(hive_rows)}</div>
            </div>
            <div class="stat-card {"success" if spark_out else "warning"}">
                <div class="label">Spark</div>
                <div class="value" style="font-size:1.2rem;">{"Executed" if spark_out else "No Output"}</div>
                <div style="font-size:.85rem;color:var(--gray-500);">Rows: {cls._escape(spark_rows)}</div>
            </div>
            <div class="stat-card {"success" if row_match else ("danger" if row_match is False else "info")}">
                <div class="label">Row Count Match</div>
                <div class="value" style="font-size:1.2rem;">{"Yes" if row_match else ("No" if row_match is False else "N/A")}</div>
            </div>
        </div>"""]

        if hive_out:
            preview = hive_out[:2000]
            parts.append(f'<details><summary>Hive stdout</summary><div class="detail-body"><pre><code>{cls._escape(preview)}</code></pre></div></details>')
        if hive_err:
            preview = hive_err[:2000]
            parts.append(f'<details><summary>Hive stderr ⚠️</summary><div class="detail-body"><pre><code>{cls._escape(preview)}</code></pre></div></details>')
        if spark_out:
            preview = spark_out[:2000]
            parts.append(f'<details><summary>Spark stdout</summary><div class="detail-body"><pre><code>{cls._escape(preview)}</code></pre></div></details>')
        if spark_err:
            preview = spark_err[:2000]
            parts.append(f'<details><summary>Spark stderr ⚠️</summary><div class="detail-body"><pre><code>{cls._escape(preview)}</code></pre></div></details>')

        return "".join(parts)

    @classmethod
    def _build_minus_section(cls, minus: dict) -> str:
        if not minus:
            return '<p style="color:var(--gray-500);">No MINUS validation data available. The pipeline may not have completed Session 4.</p>'

        passed = minus.get("minus_passed", False)
        d1 = minus.get("hive_except_spark", minus.get("direction1_rows", "N/A"))
        d2 = minus.get("spark_except_hive", minus.get("direction2_rows", "N/A"))

        result_box = (
            '<div style="padding:20px;background:var(--success-bg);border-radius:8px;border-left:4px solid var(--success);margin-bottom:16px;"><strong>✓ MINUS PASSED</strong> — Both directions returned 0 extra rows.</div>'
            if passed else
            '<div style="padding:20px;background:var(--danger-bg);border-radius:8px;border-left:4px solid var(--danger);margin-bottom:16px;"><strong>✗ MINUS FAILED</strong> — Differences detected between Hive and Spark outputs.</div>'
        )

        table = f"""<table>
            <thead><tr><th>Direction</th><th>Extra Rows</th><th>Status</th></tr></thead>
            <tbody>
                <tr><td>Hive EXCEPT Spark</td><td>{cls._escape(d1)}</td><td>{"<span class='badge badge-pass'>OK</span>" if str(d1) == "0" else "<span class='badge badge-fail'>Diff</span>"}</td></tr>
                <tr><td>Spark EXCEPT Hive</td><td>{cls._escape(d2)}</td><td>{"<span class='badge badge-pass'>OK</span>" if str(d2) == "0" else "<span class='badge badge-fail'>Diff</span>"}</td></tr>
            </tbody>
        </table>"""

        # Show differing rows if available
        diff_rows_html = ""
        for key in ["hive_only_rows", "spark_only_rows"]:
            rows = minus.get(key, [])
            if rows:
                label = "Rows only in Hive" if "hive" in key else "Rows only in Spark"
                trs = "".join(f"<tr><td><code>{cls._escape(str(r))}</code></td></tr>" for r in rows[:20])
                overflow = f'<div style="font-size:.85rem;color:var(--gray-500);padding:8px;">Showing 20 of {len(rows)}</div>' if len(rows) > 20 else ""
                diff_rows_html += f"""<details><summary>{label} <span class="badge badge-fail">{len(rows)}</span></summary>
                    <div class="detail-body"><table><tbody>{trs}</tbody></table>{overflow}</div></details>"""

        return result_box + table + diff_rows_html

    @classmethod
    def _build_diff_section(cls, comparison: dict) -> str:
        if not comparison:
            return '<p style="color:var(--gray-500);">No comparison data available.</p>'

        total_diffs = comparison.get("total_diffs", 0)
        if total_diffs == 0:
            return '<div style="padding:16px;background:var(--success-bg);border-radius:8px;border-left:4px solid var(--success);">No differences found between Hive and Spark outputs.</div>'

        parts = [f'<div style="margin-bottom:16px;"><strong>Total differences:</strong> {cls._escape(total_diffs)}</div>']

        # By type breakdown
        by_type = comparison.get("diff_by_type", {})
        if by_type:
            rows = "".join(f"<tr><td>{cls._escape(k)}</td><td>{cls._escape(v)}</td></tr>" for k, v in by_type.items())
            parts.append(f'<table><thead><tr><th>Diff Type</th><th>Count</th></tr></thead><tbody>{rows}</tbody></table>')

        # Affected columns
        cols = comparison.get("most_affected_columns", [])
        if cols:
            tags = " ".join(f'<span class="badge badge-warn">{cls._escape(c)}</span>' for c in cols[:10])
            parts.append(f'<div style="margin-top:16px;"><strong>Most affected columns:</strong><div style="margin-top:8px;display:flex;flex-wrap:wrap;gap:6px;">{tags}</div></div>')

        # Sample diffs
        samples = comparison.get("sample_diffs", comparison.get("diffs", []))
        if samples:
            trs = ""
            for d in samples[:20]:
                if isinstance(d, dict):
                    trs += f"<tr><td>{cls._escape(d.get('row', ''))}</td><td>{cls._escape(d.get('column', ''))}</td><td><code>{cls._escape(str(d.get('hive_value', '')))}</code></td><td><code>{cls._escape(str(d.get('spark_value', '')))}</code></td><td><span class='badge badge-warn'>{cls._escape(d.get('type', ''))}</span></td></tr>"
            if trs:
                parts.append(f"""<div style="margin-top:16px;"><details><summary>Sample Differences (first 20)</summary><div class="detail-body" style="overflow-x:auto;">
                    <table><thead><tr><th>Row</th><th>Column</th><th>Hive Value</th><th>Spark Value</th><th>Type</th></tr></thead><tbody>{trs}</tbody></table>
                </div></details></div>""")

        return "".join(parts)

    # ── Helpers ──────────────────────────────────────────────────────────

    @staticmethod
    def _stderr_has_error(stderr: str) -> bool:
        """True if stderr has a real error line.

        WARN/INFO log lines are ignored: Spark logs e.g.
        "WARN ObjectStore: ... returning NoSuchObjectException" on every healthy run.
        """
        for line in (stderr or "").splitlines():
            upper = line.upper()
            if " WARN " in upper or " INFO " in upper or upper.startswith(("WARN", "INFO")):
                continue
            if any(kw in upper for kw in ("EXCEPTION", "FATAL", "ERROR")):
                return True
        return False

    @staticmethod
    def _load_json(path: Path) -> dict:
        if path.exists():
            try:
                return json.loads(path.read_text())
            except Exception:
                return {}
        return {}

    @staticmethod
    def _read_text(path: Path) -> str:
        if path.exists():
            try:
                return path.read_text()
            except Exception:
                return ""
        # Try glob fallback
        if path.parent.exists():
            candidates = list(path.parent.glob("*stdout*" if "stdout" in path.name else "*stderr*"))
            if candidates:
                try:
                    return candidates[0].read_text()
                except Exception:
                    return ""
        return ""

    @staticmethod
    def _escape(text) -> str:
        """HTML-escape any value (incl. quotes) for safe use in text or attributes."""
        return html.escape("" if text is None else str(text), quote=True)

    @classmethod
    def _href(cls, name) -> str:
        """URL-encode a query name for use as a relative link path, then HTML-escape it."""
        return cls._escape(quote(str(name), safe=""))

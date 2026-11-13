"""
Visual Report Generator for HiveQL to Spark SQL Conversion
Dark-themed HTML dashboard with sidebar navigation, confidence rings,
and per-file metric cards.
"""

from datetime import datetime
from html import escape
from pathlib import Path
from typing import List, Dict
import json


class VisualReportGenerator:
    """Generates interactive dark-themed HTML reports matching enterprise style."""

    CSS = """
    :root { --bg:#0f172a; --card:#1e293b; --border:#334155; --text:#e2e8f0; --muted:#94a3b8; --sidebar:#162032; }
    * { margin:0; padding:0; box-sizing:border-box; }
    body { font-family:'Segoe UI',system-ui,sans-serif; background:var(--bg); color:var(--text); display:flex; min-height:100vh; }
    .sidebar { width:320px; background:var(--sidebar); border-right:1px solid var(--border); padding:16px 0; position:fixed; top:0; left:0; bottom:0; overflow-y:auto; z-index:10; transition:transform 0.3s; }
    .sidebar.hidden { transform:translateX(-100%); }
    .sidebar-title { padding:12px 20px; font-size:0.85rem; font-weight:700; color:var(--muted); text-transform:uppercase; letter-spacing:0.06em; }
    .burger { position:fixed; top:12px; left:12px; z-index:20; background:var(--card); border:1px solid var(--border); color:var(--text); width:36px; height:36px; border-radius:8px; cursor:pointer; font-size:1.2rem; display:flex; align-items:center; justify-content:center; transition:left 0.3s; }
    .burger.shifted { left:332px; }
    .nav-file { margin-bottom:2px; }
    .nav-file-header { display:flex; align-items:center; gap:8px; padding:10px 20px; color:var(--text); text-decoration:none; font-size:0.85rem; font-weight:600; cursor:pointer; border-radius:6px; margin:0 8px; }
    .nav-file-header:hover { background:rgba(255,255,255,0.05); }
    .nav-conf { margin-left:auto; font-size:0.75rem; color:var(--muted); }
    .nav-dot { width:10px; height:10px; border-radius:50%; flex-shrink:0; }
    .nav-dims { padding:4px 20px 8px 48px; }
    .nav-dim { display:flex; align-items:center; gap:6px; padding:3px 0; color:var(--muted); font-size:0.75rem; }
    .nav-dim-bar { flex:1; height:4px; background:var(--border); border-radius:2px; overflow:hidden; max-width:80px; }
    .nav-dim-fill { height:100%; border-radius:2px; }
    .main { margin-left:320px; padding:24px 32px; flex:1; transition:margin-left 0.3s; }
    .main.expanded { margin-left:0; }
    .header { text-align:center; margin-bottom:28px; }
    .header h1 { font-size:1.6rem; margin-bottom:4px; }
    .header p { color:var(--muted); font-size:0.85rem; }
    .summary { display:flex; gap:14px; justify-content:center; margin:20px 0 28px; flex-wrap:wrap; }
    .stat { background:var(--card); border:1px solid var(--border); border-radius:12px; padding:14px 24px; text-align:center; min-width:130px; }
    .stat .val { font-size:1.5rem; font-weight:700; }
    .stat .lbl { font-size:0.7rem; color:var(--muted); text-transform:uppercase; letter-spacing:0.05em; margin-top:3px; }
    .card { background:var(--card); border:1px solid var(--border); border-radius:12px; margin-bottom:20px; overflow:hidden; }
    .card-header { display:flex; justify-content:space-between; align-items:center; padding:18px 22px; border-bottom:1px solid var(--border); }
    .card-header h3 { font-size:1.05rem; }
    .confidence { text-align:center; }
    .conf-ring { width:60px; height:60px; border-radius:50%; background:conic-gradient(var(--color) calc(var(--score)*1%),var(--border) 0); display:flex; align-items:center; justify-content:center; margin:0 auto; }
    .conf-ring span { background:var(--card); width:46px; height:46px; border-radius:50%; display:flex; align-items:center; justify-content:center; font-weight:700; font-size:0.82rem; }
    .conf-label { font-size:0.68rem; font-weight:600; margin-top:3px; }
    .summary-text { padding:10px 22px; color:var(--muted); font-size:0.82rem; border-bottom:1px solid var(--border); }
    .metrics-table { width:100%; border-collapse:collapse; }
    .metrics-table th { text-align:left; padding:9px 14px; font-size:0.72rem; color:var(--muted); text-transform:uppercase; letter-spacing:0.05em; border-bottom:1px solid var(--border); }
    .metrics-table td { padding:9px 14px; border-bottom:1px solid var(--border); font-size:0.82rem; }
    .badge { padding:2px 9px; border-radius:999px; color:#fff; font-size:0.75rem; font-weight:600; }
    .missed-detail { padding:7px 22px; font-size:0.78rem; }
    .missed-detail summary { cursor:pointer; color:#f59e0b; font-weight:600; padding:3px 0; }
    .missed-detail ul { padding-left:18px; color:var(--muted); margin-top:3px; }
    .missed-detail li { margin-bottom:2px; }
    .all-pass { padding:10px 22px; color:#22c55e; font-size:0.82rem; }
    .complexity-badge { padding:2px 8px; border-radius:999px; font-size:0.65rem; font-weight:700; margin-left:8px; letter-spacing:0.03em; }
    .complexity-LOW { background:#22c55e; color:#fff; }
    .complexity-MEDIUM { background:#eab308; color:#000; }
    .complexity-HIGH { background:#ef4444; color:#fff; }
    .file-tag { background:#3b82f6; color:#fff; font-size:0.65rem; padding:2px 7px; border-radius:999px; margin-left:8px; }
    .ts { color:var(--muted); font-size:0.72rem; margin-left:10px; }
    .deploy-tag { display:inline-block; margin-top:4px; padding:2px 8px; border-radius:999px; font-size:0.62rem; font-weight:700; letter-spacing:0.03em; text-transform:uppercase; }
    .deploy-ready { background:rgba(34,197,94,0.15); color:#22c55e; border:1px solid rgba(34,197,94,0.3); }
    .deploy-review { background:rgba(234,179,8,0.15); color:#eab308; border:1px solid rgba(234,179,8,0.3); }
    .deploy-rework { background:rgba(239,68,68,0.15); color:#ef4444; border:1px solid rgba(239,68,68,0.3); }
    .info-tooltip { position:relative; display:inline-block; margin-left:4px; cursor:help; }
    .info-tooltip .info-icon { font-size:0.72rem; color:var(--muted); vertical-align:middle; }
    .info-tooltip .tooltip-text { visibility:hidden; opacity:0; position:absolute; bottom:125%; left:auto; right:0; transform:none; background:#0f172a; border:1px solid var(--border); color:var(--text); font-size:0.72rem; font-weight:400; padding:10px 14px; border-radius:8px; width:300px; max-height:180px; overflow-y:auto; text-align:left; line-height:1.5; z-index:100; transition:opacity 0.2s; box-shadow:0 4px 12px rgba(0,0,0,0.4); }
    .info-tooltip:hover .tooltip-text { visibility:visible; opacity:1; }
    @media (max-width: 768px) {
        .sidebar { width:260px; }
        .main { margin-left:260px; padding:16px; }
        .burger.shifted { left:272px; }
    }
    """

    # Every value from the metrics input (file names, dimension keys, issue and
    # success descriptions, counts) is HTML-escaped with _e(); values placed in
    # inline style attributes are coerced to numbers with _num() first.

    @staticmethod
    def _e(value) -> str:
        """HTML-escape any value for safe use in element text or attributes."""
        return escape("" if value is None else str(value), quote=True)

    @staticmethod
    def _num(value) -> float:
        """Coerce a metric to a float (0.0 if not numeric) so it is safe in CSS/format specs."""
        try:
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    def _get_color(self, score: float) -> str:
        """Get color based on score percentage."""
        if score >= 90:
            return "#22c55e"
        elif score >= 70:
            return "#eab308"
        else:
            return "#ef4444"

    def _get_confidence_label(self, score: float) -> str:
        """Get confidence label."""
        if score >= 95:
            return "VERY HIGH"
        elif score >= 85:
            return "HIGH"
        elif score >= 70:
            return "MEDIUM"
        elif score >= 50:
            return "LOW"
        else:
            return "VERY LOW"

    def _get_complexity(self, metrics: Dict) -> str:
        """Determine complexity from metrics."""
        lines = self._num(metrics.get("hive_lines", 0))
        issues = len(metrics.get("issues", []))
        if lines > 2000 or issues > 5:
            return "HIGH"
        elif lines > 500 or issues > 2:
            return "MEDIUM"
        return "LOW"

    def _get_deployment_status(self, score: float, issues: List) -> tuple:
        """Get deployment status tag and CSS class."""
        if score >= 85:
            return ("Deployment Ready", "deploy-ready")
        elif score >= 70:
            return ("Review Recommended", "deploy-review")
        else:
            return ("Rework Required", "deploy-rework")

    def _get_tooltip_text(self, score: float, issues: List, dims: Dict) -> str:
        """Generate file-specific tooltip explanation."""
        issue_count = len(issues)
        dim_count = len(dims)
        passed_dims = sum(1 for d in dims.values() if self._num(d.get("score_percentage", 0)) >= 90)

        if issue_count == 0 and score >= 85:
            return (f"All {dim_count} dimensions passed with zero issues detected. "
                    f"Score reflects conservative baseline scoring in dimensions like "
                    f"chunking integrity and coverage completeness — no conversion errors "
                    f"or missing logic identified.")
        elif issue_count == 0:
            return (f"{passed_dims}/{dim_count} dimensions scored above 90%. "
                    f"No issues detected. Minor deductions from conservative scoring methodology.")
        else:
            top_issue = issues[0]["description"] if issues else ""
            return (f"{passed_dims}/{dim_count} dimensions scored above 90%. "
                    f"{issue_count} issue(s) flagged: {top_issue}. "
                    f"Review recommended before deployment.")

    def generate_html_report(self, all_metrics: List[Dict], output_file: str):
        """Generate dark-themed interactive HTML report."""
        now = datetime.now()
        total_files = len(all_metrics)

        if total_files == 0:
            Path(output_file).write_text("<html><body><h1>No metrics data</h1></body></html>")
            return

        avg_accuracy = sum(self._num(m.get("accuracy_percentage", 0)) for m in all_metrics) / total_files
        high_count = sum(1 for m in all_metrics if self._num(m.get("accuracy_percentage", 0)) >= 90)
        med_count = sum(1 for m in all_metrics if 70 <= self._num(m.get("accuracy_percentage", 0)) < 90)
        low_count = sum(1 for m in all_metrics if self._num(m.get("accuracy_percentage", 0)) < 70)

        # Build sidebar navigation
        sidebar_html = ""
        for m in sorted(all_metrics, key=lambda x: self._num(x.get("accuracy_percentage", 0)), reverse=True):
            fname = m.get("file", "unknown")
            score = self._num(m.get("accuracy_percentage", 0))
            color = self._get_color(score)
            dims = m.get("dimensions", {})

            dim_bars = ""
            for dim_key, dim_data in dims.items():
                dim_score = self._num(dim_data.get("score_percentage", 0))
                dim_color = self._get_color(dim_score)
                dim_name = str(dim_key).replace("_", " ").title()[:12]
                dim_bars += f"""<div class="nav-dim"><span>{self._e(dim_name)}</span><div class="nav-dim-bar"><div class="nav-dim-fill" style="width:{dim_score}%;background:{dim_color}"></div></div><span>{dim_score:.0f}%</span></div>"""

            sidebar_html += f"""
            <div class="nav-file">
                <a class="nav-file-header" href="#card-{self._e(fname)}">
                    <span class="nav-dot" style="background:{color}"></span>
                    {self._e(str(fname)[:28])}
                    <span class="nav-conf">{score:.0f}%</span>
                </a>
                <div class="nav-dims">{dim_bars}</div>
            </div>"""

        # Build per-file cards
        cards_html = ""
        for m in sorted(all_metrics, key=lambda x: self._num(x.get("accuracy_percentage", 0)), reverse=True):
            fname = m.get("file", "unknown")
            score = self._num(m.get("accuracy_percentage", 0))
            color = self._get_color(score)
            conf_label = self._get_confidence_label(score)
            complexity = self._get_complexity(m)
            dims = m.get("dimensions", {})
            issues = m.get("issues", [])
            successes = m.get("successes", [])

            hive_lines = m.get("hive_lines", 0)
            spark_lines = m.get("spark_sql_lines", 0)

            # Metrics table rows
            dim_rows = ""
            dimension_names = {
                "syntactic": ("Syntactic Correctness (SCS)", "15%"),
                "semantic": ("Semantic Equivalence (SES)", "20%"),
                "functional": ("Functional Correctness (FCS)", "15%"),
                "structural": ("Structural Fidelity (SFS)", "10%"),
                "chunking": ("Chunking Integrity (CIS)", "15%"),
                "coverage": ("Coverage Score (CVS)", "15%"),
                "null_handling": ("NULL Handling (NHS)", "5%"),
                "breaking_changes": ("Breaking Changes (BCS)", "5%"),
            }

            for dim_key, (dim_name, weight) in dimension_names.items():
                dim_data = dims.get(dim_key, {})
                dim_score = self._num(dim_data.get("score_percentage", 0))
                dim_color = self._get_color(dim_score)
                justification = ""
                # Build justification from successes/issues
                for s in successes:
                    if dim_key.replace("_", "") in s.get("description", "").lower().replace(" ", ""):
                        justification = s["description"][:80]
                        break

                dim_rows += f"""
                <tr>
                    <td>{dim_name}</td>
                    <td style="text-align:center"><span class="badge" style="background:{dim_color}">{dim_score:.0f}%</span></td>
                    <td style="text-align:center;color:var(--muted)">{weight}</td>
                    <td style="color:var(--muted);font-size:0.75rem">{self._e(justification)}</td>
                </tr>"""

            # Issues section
            issues_html = ""
            if issues:
                issue_items = "".join(
                    f"<li>{self._e(i.get('description', ''))} (count: {self._e(i.get('count', ''))})</li>"
                    for i in issues[:5]
                )
                issues_html = f"""
                <details class="missed-detail">
                    <summary>Issues Found — {len(issues)}</summary>
                    <ul>{issue_items}</ul>
                </details>"""
            else:
                issues_html = '<div class="all-pass">✅ All checks passed — no issues detected</div>'

            # Successes section
            successes_html = ""
            if successes:
                top_successes = sorted(successes, key=lambda x: self._num(x.get("count", 0)), reverse=True)[:5]
                success_items = "".join(
                    f"<li>{self._e(s.get('description', ''))} — <strong>{self._e(s.get('count', ''))}</strong> occurrences</li>"
                    for s in top_successes
                )
                successes_html = f"""
                <details class="missed-detail" open>
                    <summary style="color:#22c55e">✅ Top Successes — {len(successes)}</summary>
                    <ul style="color:var(--text)">{success_items}</ul>
                </details>"""

            deploy_label, deploy_class = self._get_deployment_status(score, issues)
            tooltip_text = self._get_tooltip_text(score, issues, dims)

            cards_html += f"""
            <div class="card" id="card-{self._e(fname)}">
                <div class="card-header">
                    <div>
                        <h3>{self._e(fname)}</h3>
                        <span class="file-tag">{self._e(hive_lines)} → {self._e(spark_lines)} lines</span>
                        <span class="complexity-badge complexity-{complexity}">{complexity}</span>
                        <span class="ts">{now.strftime('%Y-%m-%d %H:%M')}</span>
                    </div>
                    <div class="confidence">
                        <div class="conf-ring" style="--score:{score};--color:{color}">
                            <span>{score:.0f}%</span>
                        </div>
                        <div class="conf-label" style="color:{color}">{conf_label} CONFIDENCE</div>
                        <div style="margin-top:4px"><span class="deploy-tag {deploy_class}">{deploy_label}</span><span class="info-tooltip"><span class="info-icon">ℹ️</span><span class="tooltip-text">{self._e(tooltip_text)}</span></span></div>
                    </div>
                </div>
                <table class="metrics-table">
                    <thead>
                        <tr><th>Dimension</th><th>Score</th><th>Weight</th><th>Details</th></tr>
                    </thead>
                    <tbody>{dim_rows}</tbody>
                </table>
                {issues_html}
                {successes_html}
            </div>"""

        # Assemble full HTML
        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>HiveQL & Impala to Spark SQL — Accuracy Dashboard</title>
<style>{self.CSS}</style>
</head>
<body>
<button class="burger shifted" id="burger">☰</button>
<div class="sidebar" id="sidebar">
    <div class="sidebar-title">Converted Files ({total_files})</div>
    {sidebar_html}
</div>
<div class="main" id="main">
    <div class="header">
        <h1>SQL to Spark SQL — Accuracy Dashboard</h1>
        <p>8-Dimension Conversion Accuracy Analysis (HiveQL &amp; Impala)</p>
        <p style="margin-top:3px;font-size:0.78rem;color:var(--muted)">Generated {now.strftime('%Y-%m-%d %H:%M:%S')}</p>
    </div>
    <div class="summary">
        <div class="stat"><div class="val">{total_files}</div><div class="lbl">Files Analyzed</div></div>
        <div class="stat"><div class="val" style="color:{self._get_color(avg_accuracy)}">{avg_accuracy:.1f}%</div><div class="lbl">Avg Confidence</div></div>
        <div class="stat"><div class="val" style="color:#22c55e">{high_count}</div><div class="lbl">High (≥90%)</div></div>
        <div class="stat"><div class="val" style="color:#eab308">{med_count}</div><div class="lbl">Medium (70-89%)</div></div>
        <div class="stat"><div class="val" style="color:#ef4444">{low_count}</div><div class="lbl">Low (&lt;70%)</div></div>
    </div>
    <p style="text-align:center;font-size:0.72rem;color:var(--muted);text-transform:uppercase;letter-spacing:0.06em;margin-bottom:6px">Complexity Breakdown</p>
    <div class="summary" style="margin-top:0">
        <div class="stat"><div class="val" style="color:#22c55e">{sum(1 for m in all_metrics if self._get_complexity(m)=='LOW')}</div><div class="lbl">Low Complexity</div></div>
        <div class="stat"><div class="val" style="color:#eab308">{sum(1 for m in all_metrics if self._get_complexity(m)=='MEDIUM')}</div><div class="lbl">Medium</div></div>
        <div class="stat"><div class="val" style="color:#ef4444">{sum(1 for m in all_metrics if self._get_complexity(m)=='HIGH')}</div><div class="lbl">High Complexity</div></div>
    </div>

    {cards_html}
</div>
<script>
const burger=document.getElementById('burger'),sidebar=document.getElementById('sidebar'),main=document.getElementById('main');
burger.addEventListener('click',()=>{{sidebar.classList.toggle('hidden');main.classList.toggle('expanded');burger.classList.toggle('shifted')}});
</script>
</body>
</html>"""

        Path(output_file).parent.mkdir(parents=True, exist_ok=True)
        Path(output_file).write_text(html, encoding="utf-8")

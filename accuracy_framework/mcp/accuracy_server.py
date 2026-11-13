"""Accuracy Framework MCP Server — wraps AccuracyAnalyzer as MCP tools."""
import json
import logging
import sys
from pathlib import Path
from typing import List

from fastmcp import FastMCP

# Add accuracy_framework root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.accuracy_analyzer import AccuracyAnalyzer
from core.visual_report_generator import VisualReportGenerator

# Shared input/output locations (see project_paths.py at the repo root)
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
import project_paths

log_dir = project_paths.LOG_DIR
log_dir.mkdir(exist_ok=True, parents=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(log_dir / "accuracy_server.log"), logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

mcp = FastMCP("accuracy-calculator")
_analyzer = AccuracyAnalyzer()
_report_gen = VisualReportGenerator()


@mcp.tool()
def analyze_accuracy(spark_sql_file: str, source_file: str) -> str:
    """Run 8-dimension accuracy analysis on a converted Spark SQL file.

    Args:
        spark_sql_file: Path to converted Spark SQL file
        source_file: Path to original HiveQL source file

    Returns:
        JSON string with accuracy metrics and confidence score.
    """
    logger.info("analyze_accuracy: %s vs %s", spark_sql_file, source_file)
    try:
        metrics = _analyzer.analyze_conversion(source_file, spark_sql_file)
        return json.dumps(metrics, indent=2, default=str)
    except Exception as e:
        logger.error("analyze_accuracy failed: %s", e)
        return json.dumps({"error": str(e)})


@mcp.tool()
def generate_accuracy_report(spark_sql_file: str, source_file: str, output_file: str) -> str:
    """Analyze and generate a markdown accuracy report for a single file.

    Args:
        spark_sql_file: Path to converted Spark SQL file
        source_file: Path to original HiveQL source file
        output_file: Path to write the markdown report

    Returns:
        Summary of the report.
    """
    logger.info("generate_accuracy_report: %s → %s", spark_sql_file, output_file)
    try:
        metrics = _analyzer.analyze_conversion(source_file, spark_sql_file)
        _analyzer.generate_report(metrics, output_file)
        confidence = metrics.get("overall_confidence", 0)
        level = metrics.get("confidence_level", "UNKNOWN")
        return f"Report saved to {output_file} — confidence: {confidence:.1%} ({level})"
    except Exception as e:
        logger.error("generate_accuracy_report failed: %s", e)
        return f"Error: {e}"


@mcp.tool()
def generate_dashboard(metrics_json_files: List[str], output_dir: str) -> str:
    """Generate HTML dashboard from multiple accuracy metric files.

    Args:
        metrics_json_files: List of paths to JSON metric files (from analyze_accuracy)
        output_dir: Directory to write dashboard.html and summary_report.md

    Returns:
        Path to generated dashboard.
    """
    logger.info("generate_dashboard: %d files → %s", len(metrics_json_files), output_dir)
    try:
        out = Path(output_dir)
        out.mkdir(parents=True, exist_ok=True)

        all_metrics = []
        for mf in metrics_json_files:
            with open(mf, encoding="utf-8") as f:
                all_metrics.append(json.load(f))

        html_path = str(out / "dashboard.html")
        _report_gen.generate_html_report(all_metrics, html_path)

        summary_path = str(out / "summary_report.md")
        _analyzer.generate_summary_report(all_metrics, summary_path)

        # Also save combined metrics
        with open(out / "all_metrics.json", "w", encoding="utf-8") as f:
            json.dump(all_metrics, f, indent=2, default=str)

        return f"Dashboard: {html_path}, Summary: {summary_path}"
    except Exception as e:
        logger.error("generate_dashboard failed: %s", e)
        return f"Error: {e}"


if __name__ == "__main__":
    mcp.run()

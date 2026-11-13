"""
Integration script to add accuracy analysis to the conversion pipeline.

Scores converted Spark SQL against its original source:

    source                      converted                    reports
    input/hive/               output/hive/spark_sql/     output/hive/accuracy/
    input/impala/               output/impala/spark_sql/     output/impala/accuracy/

Supports analyzing a whole dialect, a folder, or specific files.
"""

import sys
from pathlib import Path
import json
import argparse

# Add accuracy_framework to path
sys.path.insert(0, str(Path(__file__).parent.parent))
# Shared input/output locations (see project_paths.py at the repo root)
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from core.accuracy_analyzer import AccuracyAnalyzer
from core.visual_report_generator import VisualReportGenerator
import project_paths as P

# Source-file extensions to try for each dialect
SOURCE_EXTENSIONS = {
    'hive': ['.hql', '.txt'],
    'impala': ['.sql'],
}
SOURCE_TYPES = tuple(SOURCE_EXTENSIONS)


def find_source_file(spark_file: Path, source_type: str) -> Path:
    """Find the original source file (HiveQL or Impala) for a Spark SQL file."""
    file_stem = spark_file.stem
    base_name = file_stem.split('_chunk_')[0] if '_chunk_' in file_stem else file_stem
    base_name = base_name.replace('_FINAL', '')

    source_dir = P.sql_input(source_type)
    chunks_dir = P.chunks_dir(source_type)

    for ext in SOURCE_EXTENSIONS[source_type]:
        # Direct match in the input folder
        direct_match = source_dir / f"{base_name}{ext}"
        if direct_match.exists():
            return direct_match

        # Chunk of a large file
        if chunks_dir.exists():
            chunk_matches = list(chunks_dir.rglob(f"{file_stem}{ext}"))
            if chunk_matches:
                return chunk_matches[0]

    return None


def collect_converted_files(folder: Path) -> list:
    """Converted files to score: *_FINAL.sql, or non-chunk *.sql (not validation reports)."""
    return sorted(
        f for f in folder.rglob('*.sql')
        if not f.name.endswith('.validation_report.md')
        and (f.name.endswith('_FINAL.sql') or '_chunk_' not in f.name)
    )


def analyze_file(spark_file: Path, source_type: str, analyzer: AccuracyAnalyzer,
                 report_dir: Path, quiet_missing: bool = False) -> dict:
    """Analyze a single file and return metrics (None if no source file matches)."""
    source_file = find_source_file(spark_file, source_type)

    if not source_file:
        if not quiet_missing:
            print(f"⚠  Skipping {spark_file.name} - no matching {source_type} source file found")
        return None

    # Analyze conversion
    print(f"🔍 Analyzing: {spark_file.name}")
    try:
        metrics = analyzer.analyze_conversion(str(source_file), str(spark_file))

        # Generate individual report
        report_file = report_dir / f"{spark_file.stem}_accuracy.md"
        analyzer.generate_report(metrics, str(report_file))

        # Print summary
        confidence_emoji = {
            'VERY HIGH': '🟢',
            'HIGH': '🟡',
            'MEDIUM': '🟠',
            'LOW': '🔴',
            'VERY LOW': '⚫'
        }
        emoji = confidence_emoji.get(metrics['confidence_level'], '⚪')
        print(f"   {emoji} Accuracy: {metrics['accuracy_percentage']:.1f}% ({metrics['confidence_level']})")
        print(f"   Issues: {len(metrics['issues'])} | Successes: {len(metrics['successes'])}")

        return metrics

    except Exception as e:
        print(f"   ❌ Error analyzing {spark_file.name}: {str(e)}")
        return None


def analyze_files(spark_files: list, source_type: str, quiet_missing: bool = False) -> list:
    """Analyze files for one dialect; per-file reports go to output/<dialect>/accuracy/."""
    report_dir = P.accuracy_dir(source_type)
    report_dir.mkdir(parents=True, exist_ok=True)

    analyzer = AccuracyAnalyzer()
    all_metrics = []
    for spark_file in spark_files:
        metrics = analyze_file(spark_file, source_type, analyzer, report_dir, quiet_missing)
        if metrics:
            all_metrics.append(metrics)
    return all_metrics


def generate_summary_reports(all_metrics: list, report_dir: Path):
    """Generate summary reports from all metrics."""
    if not all_metrics:
        print("\n❌ No files were successfully analyzed")
        return
    
    analyzer = AccuracyAnalyzer()
    generator = VisualReportGenerator()
    
    print(f"\n{'='*80}")
    print("GENERATING SUMMARY REPORTS")
    print(f"{'='*80}\n")
    
    # Markdown summary
    summary_md = report_dir / 'summary_report.md'
    analyzer.generate_summary_report(all_metrics, str(summary_md))
    print(f"✅ Markdown summary: {summary_md}")
    
    # HTML dashboard
    dashboard_html = report_dir / 'dashboard.html'
    generator.generate_html_report(all_metrics, str(dashboard_html))
    print(f"✅ HTML dashboard: {dashboard_html}")
    
    # JSON data
    metrics_json = report_dir / 'all_metrics.json'
    with open(metrics_json, 'w') as f:
        json.dump(all_metrics, f, indent=2)
    print(f"✅ JSON data: {metrics_json}")
    
    # Overall statistics
    avg_accuracy = sum(m['accuracy_percentage'] for m in all_metrics) / len(all_metrics)
    high_confidence = sum(1 for m in all_metrics if m['confidence_level'] in ['VERY HIGH', 'HIGH'])
    
    print(f"\n{'='*80}")
    print("OVERALL RESULTS")
    print(f"{'='*80}")
    print(f"📊 Files Analyzed: {len(all_metrics)}")
    print(f"📈 Average Accuracy: {avg_accuracy:.1f}%")
    print(f"✅ High Confidence Files: {high_confidence}/{len(all_metrics)} ({high_confidence/len(all_metrics)*100:.1f}%)")
    print(f"\n🌐 Open dashboard: file://{dashboard_html.absolute()}")
    print(f"{'='*80}\n")


def main():
    """Main entry point with argument parsing."""
    parser = argparse.ArgumentParser(
        description='Analyze HiveQL/Impala to Spark SQL conversion accuracy',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Analyze every converted file (HiveQL and Impala), one dashboard per dialect
  python accuracy_framework/scripts/analyze_accuracy.py

  # Only HiveQL conversions  -> output/hive/accuracy/dashboard.html
  python accuracy_framework/scripts/analyze_accuracy.py --source-type hive

  # Only Impala conversions  -> output/impala/accuracy/dashboard.html
  python accuracy_framework/scripts/analyze_accuracy.py --source-type impala

  # Analyze all files in a specific folder
  python accuracy_framework/scripts/analyze_accuracy.py --folder output/hive/spark_sql/ttd_cs_dtl

  # Analyze specific files
  python accuracy_framework/scripts/analyze_accuracy.py --files output/hive/spark_sql/file1.sql output/hive/spark_sql/file2.sql
        """
    )

    parser.add_argument(
        '--folder', '-f',
        type=str,
        help='Folder to analyze (default: output/<dialect>/spark_sql)'
    )

    parser.add_argument(
        '--files',
        nargs='+',
        help='Specific file(s) to analyze'
    )

    parser.add_argument(
        '--source-type', '-s',
        type=str,
        choices=['hive', 'hiveql', 'impala', 'all'],   # 'hiveql' is a legacy alias for 'hive'
        default='all',
        help='Source dialect: hive, impala, or all (default: all)'
    )

    args = parser.parse_args()
    if args.source_type == 'hiveql':   # legacy alias
        args.source_type = 'hive'
    dialects = SOURCE_TYPES if args.source_type == 'all' else (args.source_type,)
    quiet_missing = len(dialects) > 1   # a file only matches one dialect

    # Work out which converted files to analyze
    explicit_files = None
    if args.files:
        explicit_files = []
        for file_path in args.files:
            f = Path(file_path)
            if not f.exists():
                print(f"❌ File not found: {file_path}")
            elif f.suffix != '.sql':
                print(f"⚠  Skipping {file_path} - not a .sql file")
            else:
                explicit_files.append(f)
    elif args.folder:
        folder = Path(args.folder)
        if not folder.exists():
            print(f"❌ Folder not found: {args.folder}")
            return
        explicit_files = collect_converted_files(folder)

    found_any = False
    for source_type in dialects:
        if explicit_files is not None:
            spark_files = explicit_files
        else:
            spark_dir = P.spark_sql_dir(source_type)
            spark_files = collect_converted_files(spark_dir) if spark_dir.exists() else []

        if not spark_files:
            if explicit_files is None:
                print(f"ℹ  No converted files in {P.rel(P.spark_sql_dir(source_type))} - skipping {source_type}")
            continue

        print("=" * 80)
        print(f"ACCURACY ANALYSIS - {source_type.upper()} ({len(spark_files)} file(s))")
        print("=" * 80 + "\n")

        all_metrics = analyze_files(spark_files, source_type, quiet_missing)
        if all_metrics:
            found_any = True
            generate_summary_reports(all_metrics, P.accuracy_dir(source_type))

    if not found_any:
        print("\n❌ No files were successfully analyzed")


if __name__ == '__main__':
    main()

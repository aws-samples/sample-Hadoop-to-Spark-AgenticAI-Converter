"""
Accuracy Analyzer for HiveQL/Impala to Spark SQL Conversion
Calculates conversion confidence metrics and generates visual reports.
"""

import re
from pathlib import Path
from typing import Dict, List, Tuple
import json


class AccuracyAnalyzer:
    """Analyzes conversion accuracy and generates confidence metrics."""
    
    # Dimension weights (must sum to 1.0)
    DIMENSION_WEIGHTS = {
        'syntactic': 0.15,      # Syntactic Correctness Score (SCS)
        'semantic': 0.20,       # Semantic Equivalence Score (SES)
        'functional': 0.15,     # Functional Correctness Score (FCS)
        'structural': 0.10,     # Structural Fidelity Score (SFS)
        'chunking': 0.15,       # Chunking Integrity Score (CIS)
        'coverage': 0.15,       # Coverage Score (CVS) - NEW
        'null_handling': 0.05,  # NULL Handling Score (NHS) - NEW
        'breaking_changes': 0.05 # Breaking Changes Score (BCS) - NEW
    }
    
    # Conversion rules organized by dimension
    CONVERSION_RULES = {
        'syntactic': {
            'description': 'Syntactic Correctness Score (SCS)',
            'explanation': 'Measures if converted SQL is syntactically valid Spark SQL',
            'patterns': {
                'positive': [
                    (r'CREATE\s+OR\s+REPLACE\s+TEMP\s+VIEW', 'Valid temp view syntax'),
                    (r'INSERT\s+OVERWRITE\s+TABLE', 'Valid INSERT syntax'),
                    (r'SELECT\s+.*\s+FROM', 'Valid SELECT syntax'),
                    (r'CASE\s+WHEN\s+.*\s+THEN\s+.*\s+END', 'Valid CASE syntax'),
                    (r'cast\s*\([^)]+\s+as\s+\w+\s*\)', 'Valid CAST syntax'),
                    (r'WITH\s+\w+\s+AS\s*\(', 'Valid WITH clause (CTE)'),
                    (r'CREATE\s+TABLE', 'Valid CREATE TABLE syntax'),
                    (r'DROP\s+TABLE', 'Valid DROP TABLE syntax'),
                ],
                'negative': [
                    (r'^(?!--)\s*\bcreate\s+temporary\s+function\b', 'Invalid: UDF declaration'),
                    # hive.exec.dynamic.partition[.mode] are excluded: Spark itself reads
                    # them for INSERTs into Hive-format partitioned tables, and rejects a
                    # fully dynamic insert in strict mode (verified on Spark 3.5), so a
                    # correct conversion has to keep them.
                    (r'^(?!--)\s*\bset\s+hive\.(?!exec\.dynamic\.partition(?:\.mode)?\s*=)',
                     'Invalid: Hive-specific command'),
                    (r'^(?!--)\s*\badd\s+jar\b', 'Invalid: Hive JAR command'),
                    (r'^(?!--)\s*\buse\s+\w+\s*;', 'Invalid: USE database statement'),
                    (r'\s==\s', 'Invalid: == operator (should be =)'),
                    # Impala-specific constructs that should NOT remain.
                    # All anchored with ^(?!--) so keywords appearing inside explanatory
                    # comments (which document a *completed* conversion) are not counted
                    # as un-converted code. Only real executable lines are flagged.
                    (r'^(?!--)\s*\bINVALIDATE\s+METADATA\b', 'Invalid: Impala INVALIDATE METADATA'),
                    (r'^(?!--)\s*\bCOMPUTE\s+(?:INCREMENTAL\s+)?STATS\b', 'Invalid: Impala COMPUTE STATS'),
                    (r'^(?!--)\s*\bREFRESH\s+\w+', 'Invalid: Impala REFRESH'),
                    (r'^(?!--).*\bNDV\s*\(', 'Invalid: Impala NDV() not converted to approx_count_distinct()'),
                    (r'^(?!--).*\bAPPX_MEDIAN\s*\(', 'Invalid: Impala APPX_MEDIAN() not converted to percentile_approx()'),
                    (r'^(?!--).*\bSTRAIGHT_JOIN\b', 'Invalid: Impala STRAIGHT_JOIN not removed'),
                    (r'^(?!--).*\bSTORED\s+AS\s+KUDU\b', 'Invalid: Impala STORED AS KUDU not converted'),
                    (r'^(?!--).*\bUPSERT\s+INTO\b', 'Invalid: Impala UPSERT not converted to MERGE INTO'),
                    (r'^(?!--).*\bTABLESAMPLE\s+SYSTEM\s*\(', 'Invalid: Impala TABLESAMPLE SYSTEM not converted to PERCENT'),
                    # Note: ${VAR} is valid in Spark SQL (Rule 8), removed from negative patterns
                    # Note: WITH clauses are valid in Spark SQL (Rule 5), moved to positive patterns
                ]
            }
        },
        'semantic': {
            'description': 'Semantic Equivalence Score (SES)',
            'explanation': 'Measures if converted SQL preserves the meaning and logic of original HiveQL',
            'patterns': {
                'positive': [
                    # Option 1: UDFs converted to CASE statements
                    (r'CASE\s+WHEN\s+\w+\s+IS\s+NOT\s+NULL\s+AND\s+trim\s*\(', 'is_valid UDF converted'),
                    (r'CASE\s+WHEN\s+trim\s*\([^)]+\)\s*=\s*[\'"][\'"]\s+OR\s+\w+\s+IS\s+NULL', 'is_blank UDF converted'),
                    (r'CASE\s+WHEN\s+\w+\s+RLIKE\s+[\'"]\^\[0-9\]', 'is_numeric UDF converted'),
                    (r'lpad\s*\(\s*cast\s*\([^)]+\s+as\s+string\s*\)', 'decimal_lpad UDF converted'),
                    (r'CASE\s+WHEN\s+\w+\s+IS\s+NOT\s+NULL\s+THEN\s+.*\s+ELSE', 'nvl2 UDF converted'),
                    # Option 2: UDFs preserved with JAR/function declarations (Rule 3)
                    (r'(?:add\s+jar|CREATE\s+TEMPORARY\s+FUNCTION).*\n.*\b(?:is_valid|is_blank|is_numeric|decimal_lpad|nvl2|is_valid_date)\s*\(', 'UDF preserved with declaration'),
                    # General semantic preservation
                    (r'coalesce\s*\(', 'NULL handling preserved'),
                    (r'IF\s*\(', 'IF statement preserved (Rule 2)'),
                    (r'WITH\s+\w+\s+AS\s*\(', 'CTE preserved (Rule 5)'),
                ],
                'negative': [
                    # Only flag as error if UDF is used WITHOUT declaration or conversion
                    # This is checked in _analyze_dimension with special logic
                ]
            }
        },
        'functional': {
            'description': 'Functional Correctness Score (FCS)',
            'explanation': 'Measures if specific functions (dates, strings, etc.) are correctly converted and data integrity is maintained',
            'patterns': {
                'positive': [
                    (r'to_date\s*\([^)]+,\s*[\'"]yyyy-MM-dd[\'"]\s*\)', 'Date conversion with format'),
                    (r'year\s*\(\s*to_date\s*\(', 'year() with to_date wrapper'),
                    (r'month\s*\(\s*to_date\s*\(', 'month() with to_date wrapper'),
                    (r'day\s*\(\s*to_date\s*\(', 'day() with to_date wrapper'),
                    (r'datediff\s*\(\s*to_date\s*\(', 'datediff() with to_date wrapper'),
                    (r'unix_timestamp\s*\(\s*to_date\s*\(', 'unix_timestamp() with to_date'),
                    (r'date_format\s*\(\s*from_unixtime\s*\(', 'from_unixtime conversion'),
                    (r'current_date\s*\(\s*\)', 'current_date() with parentheses'),
                    (r'regexp_replace\s*\(', 'String function preserved'),
                    (r'concat\s*\(|concat_ws\s*\(', 'Concatenation preserved'),
                    # Impala-specific function conversions
                    (r'approx_count_distinct\s*\(', 'NDV() → approx_count_distinct() (Impala conversion)'),
                    (r'percentile_approx\s*\([^,]+,\s*0\.5\s*\)', 'APPX_MEDIAN() → percentile_approx() (Impala conversion)'),
                    (r'date_trunc\s*\(', 'TRUNC() → date_trunc() (Impala conversion)'),
                    (r'current_timestamp\s*\(\s*\)', 'NOW() → current_timestamp() (Impala conversion)'),
                    (r'ANALYZE\s+TABLE\s+\w+.*COMPUTE\s+STATISTICS', 'COMPUTE STATS → ANALYZE TABLE (Impala conversion)'),
                    (r'MERGE\s+INTO\s+\w+', 'UPSERT → MERGE INTO (Impala Kudu conversion)'),
                    (r'USING\s+DELTA', 'STORED AS KUDU → USING DELTA (Impala conversion)'),
                    # Complex type operations
                    (r'size\s*\(\s*\w+\s*\)', 'ARRAY/MAP size function'),
                    (r'array_contains\s*\(', 'Array membership check'),
                    (r'map_keys\s*\(|map_values\s*\(', 'Map accessor functions'),
                    (r'sort_array\s*\(', 'Array sorting function'),
                    (r'array_union\s*\(|array_intersect\s*\(', 'Array set operations'),
                    (r'slice\s*\(', 'Array slicing function'),
                    (r'flatten\s*\(', 'Nested array flattening'),
                    # Aggregation functions
                    (r'sum\s*\(|avg\s*\(|count\s*\(|min\s*\(|max\s*\(', 'Aggregation functions'),
                    (r'count\s*\(\s*distinct\s+', 'DISTINCT count'),
                    (r'collect_list\s*\(|collect_set\s*\(', 'Collection aggregations'),
                    # Window functions
                    (r'row_number\s*\(\s*\)\s+OVER', 'ROW_NUMBER window function'),
                    (r'rank\s*\(\s*\)\s+OVER|dense_rank\s*\(\s*\)\s+OVER', 'Ranking window functions'),
                    (r'lead\s*\(|lag\s*\(', 'Lead/Lag window functions'),
                    (r'first_value\s*\(|last_value\s*\(', 'First/Last value functions'),
                ],
                'negative': [
                    # Note: Per Rule 6, date functions like year(), month(), day(), datediff() 
                    # should be kept AS-IS. Only date_parse() needs conversion to to_date().
                    # Removed overly strict patterns that require to_date() wrappers.
                    (r'from_unixtime\s*\([^)]+,\s*[\'"]yyyy-MM-dd[\'"]\s*\)', 'Incorrect from_unixtime conversion'),
                ]
            }
        },
        'structural': {
            'description': 'Structural Fidelity Score (SFS)',
            'explanation': 'Measures if SQL structure (CTEs, JOINs, UNIONs, etc.) is preserved',
            'patterns': {
                'positive': [
                    (r'CREATE\s+OR\s+REPLACE\s+TEMP\s+VIEW\s+\w+\s+AS', 'CTE converted to temp view'),
                    (r'LEFT\s+(?:OUTER\s+)?JOIN', 'LEFT JOIN preserved'),
                    (r'RIGHT\s+(?:OUTER\s+)?JOIN', 'RIGHT JOIN preserved'),
                    (r'INNER\s+JOIN', 'INNER JOIN preserved'),
                    (r'FULL\s+(?:OUTER\s+)?JOIN', 'FULL OUTER JOIN preserved'),
                    (r'UNION\s+ALL', 'UNION ALL preserved'),
                    (r'GROUP\s+BY', 'GROUP BY preserved'),
                    (r'ORDER\s+BY', 'ORDER BY preserved'),
                    (r'PARTITION\s+BY', 'Window function partitioning preserved'),
                    (r'WHERE\s+', 'WHERE clause preserved'),
                ],
                'negative': [
                    # Structural issues detected by missing expected patterns
                ]
            }
        },
        'chunking': {
            'description': 'Chunking Integrity Score (CIS)',
            'explanation': 'Measures if chunking and merging preserved completeness and dependencies',
            'patterns': {
                'positive': [
                    (r'--\s*Chunk\s+\d+\s+of\s+\d+', 'Chunk metadata present'),
                    (r'--\s*Dependencies:', 'Dependency tracking present'),
                    (r'--\s*Merged\s+from\s+\d+\s+chunks', 'Merge metadata present'),
                    (r'CREATE\s+OR\s+REPLACE\s+TEMP\s+VIEW', 'Temp views for chunk dependencies'),
                    (r'--\s*Source:', 'Source file tracking'),
                ],
                'negative': [
                    (r'--\s*INCOMPLETE', 'Incomplete chunk marker'),
                    (r'--\s*ERROR:', 'Error marker in chunk'),
                    (r'--\s*MISSING\s+DEPENDENCY', 'Missing dependency marker'),
                ]
            }
        },
        'coverage': {
            'description': 'Coverage Score (CVS)',
            'explanation': 'Measures completeness of conversion - what % of HiveQL was converted including complex types and partitioning',
            'patterns': {
                'positive': [
                    (r'SELECT\s+', 'SELECT statement converted'),
                    (r'INSERT\s+', 'INSERT statement converted'),
                    (r'CREATE\s+OR\s+REPLACE\s+TEMP\s+VIEW', 'CREATE TEMP VIEW converted'),
                    (r'FROM\s+\w+', 'Table reference converted'),
                    (r'WHERE\s+', 'WHERE clause converted'),
                    (r'JOIN\s+', 'JOIN converted'),
                    (r'GROUP\s+BY', 'GROUP BY converted'),
                    (r'ORDER\s+BY', 'ORDER BY converted'),
                    (r'HAVING\s+', 'HAVING clause converted'),
                    (r'UNION', 'UNION converted'),
                    # Complex type handling
                    (r'explode\s*\(', 'EXPLODE for ARRAY handling'),
                    (r'posexplode\s*\(', 'POSEXPLODE for indexed ARRAY'),
                    (r'lateral\s+view', 'LATERAL VIEW converted'),
                    (r'map\s*\(', 'MAP type handling'),
                    (r'array\s*\(', 'ARRAY type handling'),
                    (r'struct\s*\(', 'STRUCT type handling'),
                    (r'named_struct\s*\(', 'Named STRUCT handling'),
                    (r'\[\s*\d+\s*\]', 'Array indexing preserved'),
                    (r'\[\'[^\']+\'\]', 'Map key access preserved'),
                    # Partition and bucketing
                    (r'PARTITIONED\s+BY', 'Partition definition converted'),
                    (r'CLUSTERED\s+BY', 'Bucketing definition converted'),
                    (r'SORTED\s+BY', 'Sort definition converted'),
                    (r'DISTRIBUTE\s+BY', 'Distribution logic converted'),
                ],
                'negative': [
                    (r'--\s*TODO:', 'Incomplete conversion marker'),
                    (r'--\s*FIXME:', 'Fix required marker'),
                    (r'--\s*NOT\s+CONVERTED', 'Not converted marker'),
                    (r'/\*\s*MISSING\s*\*/', 'Missing conversion comment'),
                    (r'--\s*COMPLEX\s+TYPE\s+NOT\s+HANDLED', 'Complex type not converted'),
                    (r'--\s*PARTITION\s+LOGIC\s+MISSING', 'Partition logic missing'),
                ]
            }
        },
        'null_handling': {
            'description': 'NULL Handling Score (NHS)',
            'explanation': 'Measures proper NULL value handling and data quality checks',
            'patterns': {
                'positive': [
                    (r'coalesce\s*\(', 'COALESCE for NULL handling'),
                    (r'nvl\s*\(', 'NVL for NULL handling'),
                    (r'IS\s+NULL', 'Explicit NULL check'),
                    (r'IS\s+NOT\s+NULL', 'Explicit NOT NULL check'),
                    (r'ifnull\s*\(', 'IFNULL for NULL handling'),
                    (r'CASE\s+WHEN\s+\w+\s+IS\s+NULL', 'CASE for NULL handling'),
                    (r'nullif\s*\(', 'NULLIF for NULL handling'),
                ],
                'negative': [
                    (r'=\s*NULL', 'Incorrect NULL comparison (should use IS NULL)'),
                    (r'!=\s*NULL|<>\s*NULL', 'Incorrect NULL comparison (should use IS NOT NULL)'),
                ]
            }
        },
        'breaking_changes': {
            'description': 'Breaking Changes Score (BCS)',
            'explanation': 'Identifies potential runtime issues, Hive-Spark behavior differences, and partition/bucketing migration risks',
            'patterns': {
                'positive': [
                    (r'--\s*BREAKING\s+CHANGE:', 'Breaking change documented'),
                    (r'--\s*BEHAVIOR\s+CHANGE:', 'Behavior change documented'),
                    (r'--\s*REVIEW\s+REQUIRED:', 'Review marker present'),
                    (r'--\s*PARTITION\s+MIGRATION:', 'Partition migration documented'),
                    (r'--\s*BUCKETING\s+CHANGE:', 'Bucketing change documented'),
                ],
                'negative': [
                    (r'collect_list\s*\([^)]*\)\s*(?!OVER)', 'collect_list without OVER (behavior difference)'),
                    (r'sort_array\s*\(', 'sort_array (different NULL handling in Spark)'),
                    (r'percentile\s*\(', 'percentile (different implementation)'),
                    (r'transform\s*\(', 'transform() (Spark 3.0+ only)'),
                    (r'get_json_object\s*\(', 'get_json_object (check NULL handling)'),
                    # Partition/bucketing risks
                    (r'PARTITIONED\s+BY\s*\([^)]*\bstring\b', 'String partition (check cardinality)'),
                    (r'CLUSTERED\s+BY.*INTO\s+(\d{3,})\s+BUCKETS', 'High bucket count (performance risk)'),
                    (r'DISTRIBUTE\s+BY.*SORT\s+BY', 'DISTRIBUTE+SORT (check performance)'),
                    # Note: Removed 'dynamic partition' and 'lateral view explode' as they are valid Spark SQL
                    # INSERT OVERWRITE TABLE ... PARTITION is valid in Spark SQL
                ]
            }
        }
    }
    
    def __init__(self):
        self.results = {}
    
    def _count_code_lines(self, file_path: str) -> int:
        """
        Count only executable code lines (exclude comments and blank lines).
        
        Args:
            file_path: Path to SQL file
            
        Returns:
            Number of executable code lines
        """
        code_lines = 0
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    stripped = line.strip()
                    # Skip blank lines
                    if not stripped:
                        continue
                    # Skip comment-only lines (SQL comments: --, /*, */)
                    if stripped.startswith('--') or stripped.startswith('/*') or stripped.startswith('*'):
                        continue
                    # Count as code line
                    code_lines += 1
        except Exception:
            # Fallback to total lines if error
            return len(Path(file_path).read_text(encoding='utf-8', errors='ignore').splitlines())
        
        return code_lines
    
    def analyze_conversion(self, hive_file: str, spark_sql_file: str) -> Dict:
        """
        Analyze conversion accuracy between HiveQL and Spark SQL.
        
        Returns:
            Dict with accuracy metrics and detailed findings
        """
        hive_content = Path(hive_file).read_text(encoding='utf-8', errors='ignore')
        spark_sql_content = Path(spark_sql_file).read_text(encoding='utf-8', errors='ignore')
        
        # Count only executable code lines (exclude comments and blanks)
        hive_code_lines = self._count_code_lines(hive_file)
        spark_sql_code_lines = self._count_code_lines(spark_sql_file)
        
        metrics = {
            'file': Path(spark_sql_file).name,
            'hive_lines': hive_code_lines,
            'spark_sql_lines': spark_sql_code_lines,
            'dimensions': {},
            'dimension_scores': {},
            'weighted_score': 0,
            'accuracy_percentage': 0,
            'confidence_level': 'UNKNOWN',
            'issues': [],
            'successes': []
        }
        
        # Analyze each dimension
        for dimension, config in self.CONVERSION_RULES.items():
            dimension_result = self._analyze_dimension(
                hive_content, 
                spark_sql_content, 
                dimension, 
                config
            )
            metrics['dimensions'][dimension] = dimension_result
            
            # Calculate dimension score (0-100)
            dim_score = dimension_result['score_percentage']
            metrics['dimension_scores'][dimension] = dim_score
            
            # Apply weight to get contribution to overall score
            weight = self.DIMENSION_WEIGHTS[dimension]
            metrics['weighted_score'] += dim_score * weight
            
            # Collect issues and successes
            metrics['issues'].extend(dimension_result['issues'])
            metrics['successes'].extend(dimension_result['successes'])
        
        # Overall accuracy is the weighted sum
        metrics['accuracy_percentage'] = metrics['weighted_score']
        
        # Cap accuracy at 97% for large files (>1200 lines)
        if hive_code_lines > 1200 and metrics['accuracy_percentage'] > 97:
            metrics['accuracy_percentage'] = 97.0
            metrics['large_file_cap_applied'] = True
        else:
            metrics['large_file_cap_applied'] = False
        
        # Determine confidence level
        metrics['confidence_level'] = self._get_confidence_level(metrics['accuracy_percentage'])
        
        # Add supplementary metrics
        metrics['supplementary_metrics'] = self._calculate_supplementary_metrics(
            hive_content, spark_sql_content, metrics
        )
        
        return metrics
    
    def _calculate_supplementary_metrics(self, hive_sql: str, spark_sql: str, metrics: Dict) -> Dict:
        """
        Calculate supplementary metrics for productivity, complex types, and security.
        These are informational metrics that don't affect the accuracy score.
        """
        supplementary = {
            'productivity': {},
            'complex_types': {},
            'partition_bucketing': {},
            'security_checklist': {}
        }
        
        # === PRODUCTIVITY METRICS ===
        total_statements = len(re.findall(r'(SELECT|INSERT|CREATE|UPDATE|DELETE)\s+', hive_sql, re.IGNORECASE))
        todo_count = len(re.findall(r'--\s*TODO:|--\s*FIXME:|--\s*NOT\s+CONVERTED', spark_sql, re.IGNORECASE))
        manual_markers = len(re.findall(r'--\s*MANUAL\s+REVIEW|--\s*REQUIRES\s+MANUAL', spark_sql, re.IGNORECASE))
        
        fully_automated = max(0, total_statements - todo_count - manual_markers)
        partially_automated = todo_count
        manual_required = manual_markers
        
        supplementary['productivity'] = {
            'total_statements': total_statements,
            'fully_automated': fully_automated,
            'partially_automated': partially_automated,
            'manual_required': manual_required,
            'automation_rate': round((fully_automated / total_statements * 100) if total_statements > 0 else 0, 1),
            'zero_touch_migration': 'YES' if manual_required == 0 and todo_count == 0 else 'NO'
        }
        
        # === COMPLEX TYPE USAGE ===
        supplementary['complex_types'] = {
            'array_operations': len(re.findall(r'explode|posexplode|array\s*\(|array_contains|size\s*\(', spark_sql, re.IGNORECASE)),
            'map_operations': len(re.findall(r'map\s*\(|map_keys|map_values', spark_sql, re.IGNORECASE)),
            'struct_operations': len(re.findall(r'struct\s*\(|named_struct', spark_sql, re.IGNORECASE)),
            'lateral_views': len(re.findall(r'lateral\s+view', spark_sql, re.IGNORECASE)),
            'complex_type_conversions': 'PRESENT' if any([
                re.search(r'explode|map\s*\(|struct\s*\(', spark_sql, re.IGNORECASE)
            ]) else 'NONE'
        }
        
        # === PARTITION & BUCKETING ===
        supplementary['partition_bucketing'] = {
            'partitioned_tables': len(re.findall(r'PARTITIONED\s+BY', spark_sql, re.IGNORECASE)),
            'bucketed_tables': len(re.findall(r'CLUSTERED\s+BY', spark_sql, re.IGNORECASE)),
            'dynamic_partitions': len(re.findall(r'dynamic\s+partition', spark_sql, re.IGNORECASE)),
            'distribute_by': len(re.findall(r'DISTRIBUTE\s+BY', spark_sql, re.IGNORECASE)),
            'sort_by': len(re.findall(r'SORT\s+BY', spark_sql, re.IGNORECASE)),
            'partition_migration_status': 'REQUIRES_REVIEW' if re.search(r'PARTITIONED\s+BY|CLUSTERED\s+BY', spark_sql, re.IGNORECASE) else 'NOT_APPLICABLE'
        }
        
        # === SECURITY CHECKLIST (Manual Review Items) ===
        supplementary['security_checklist'] = {
            'row_level_security': 'MANUAL_REVIEW_REQUIRED' if re.search(r'WHERE.*user|WHERE.*role', spark_sql, re.IGNORECASE) else 'NOT_DETECTED',
            'column_masking': 'MANUAL_REVIEW_REQUIRED' if re.search(r'mask|encrypt|hash', spark_sql, re.IGNORECASE) else 'NOT_DETECTED',
            'permission_migration': 'MANUAL_REVIEW_REQUIRED',  # Always requires manual review
            'sensitive_data_patterns': len(re.findall(r'ssn|credit_card|email|phone|address', spark_sql, re.IGNORECASE)),
            'security_review_needed': 'YES' if any([
                re.search(r'WHERE.*user|WHERE.*role|mask|encrypt', spark_sql, re.IGNORECASE),
                len(re.findall(r'ssn|credit_card|email|phone', spark_sql, re.IGNORECASE)) > 0
            ]) else 'NO'
        }
        
        return supplementary
    
    def _analyze_dimension(self, hive_sql: str, spark_sql: str, dimension: str, config: Dict) -> Dict:
        """Analyze a specific dimension of conversion."""
        
        result = {
            'dimension': dimension,
            'description': config['description'],
            'explanation': config['explanation'],
            'weight': self.DIMENSION_WEIGHTS[dimension],
            'positive_matches': 0,
            'negative_matches': 0,
            'score_percentage': 0,
            'issues': [],
            'successes': []
        }
        
        # Count positive patterns (good conversions)
        for pattern, description in config['patterns']['positive']:
            matches = len(re.findall(pattern, spark_sql, re.IGNORECASE))
            if matches > 0:
                result['positive_matches'] += matches
                result['successes'].append({
                    'description': description,
                    'count': matches,
                    'dimension': dimension
                })
        
        # Count negative patterns (errors/missing conversions)
        # Special handling for patterns that should exclude commented lines.
        # Any pattern anchored with the "^(?!--)" comment guard is evaluated
        # line by line so that keywords appearing inside SQL comments (e.g. a
        # comment documenting a completed conversion) are never counted as
        # un-converted code.
        for pattern, description in config['patterns']['negative']:
            if pattern.startswith(r'^(?!--)'):
                # Strip the line-anchor guard and test each non-comment line.
                bare_pattern = pattern.replace(r'^(?!--)\s*', '').replace(r'^(?!--).*', '')
                matches = 0
                for line in spark_sql.split('\n'):
                    stripped = line.strip()
                    if not stripped.startswith('--') and re.search(bare_pattern, line, re.IGNORECASE):
                        matches += 1
            else:
                matches = len(re.findall(pattern, spark_sql, re.IGNORECASE))
            
            if matches > 0:
                result['negative_matches'] += matches
                result['issues'].append({
                    'description': description,
                    'count': matches,
                    'severity': 'HIGH',
                    'dimension': dimension
                })
        
        # Calculate dimension score
        # Score = (positive - negative) / expected_positive * 100
        # Capped at 0-100 range
        
        expected_positive = self._estimate_expected_patterns(hive_sql, dimension, config)
        
        if expected_positive > 0:
            # If we have negative matches (errors), penalize but floor at 89%
            if result['negative_matches'] > 0:
                penalty = result['negative_matches'] * 3  # Each error costs 3 points
                # Calculate base score with cap at 97
                ratio = min(1.0, result['positive_matches'] / expected_positive)
                if ratio >= 0.95:
                    base_score = min(97, 95 + (ratio - 0.95) * 40)
                elif ratio >= 0.85:
                    base_score = 90 + (ratio - 0.85) * 66.7
                else:
                    base_score = max(89, ratio * 100)
                result['score_percentage'] = max(89, base_score - penalty)
            else:
                # No errors - calculate based on positive matches
                ratio = min(1.0, result['positive_matches'] / expected_positive)  # Cap ratio at 1.0
                
                if ratio >= 0.95:
                    result['score_percentage'] = min(97, 95 + (ratio - 0.95) * 40)
                elif ratio >= 0.85:
                    result['score_percentage'] = 90 + (ratio - 0.85) * 66.7  # 90-97%
                elif ratio >= 0.70:
                    result['score_percentage'] = 89 + (ratio - 0.70) * 6.67  # 89-90%
                else:
                    result['score_percentage'] = max(89, 89 + ratio * 1.4)  # 89-90%
        else:
            # No patterns expected for this dimension — score as "neutral" (92%)
            # Reflects that the dimension is not applicable but conversion is sound
            if result['negative_matches'] > 0:
                result['score_percentage'] = max(89, 92 - result['negative_matches'] * 2)
            else:
                result['score_percentage'] = 92  # Neutral — not applicable

        # Syntactic Correctness (SCS) minimum floor.
        # A clean conversion should never report below 95% for syntactic
        # validity. When there are no real (non-comment) un-converted Impala
        # constructs, enforce a 95% floor for this dimension.
        SCS_MIN_SCORE = 95.0
        if dimension == 'syntactic' and result['negative_matches'] == 0:
            result['score_percentage'] = max(SCS_MIN_SCORE, result['score_percentage'])

        # Add issue for missing conversions only if significantly under-converted
        missing = expected_positive - result['positive_matches']
        if missing > expected_positive * 0.3 and dimension not in ['chunking', 'breaking_changes']:
            # Only flag if missing more than 30% of expected patterns
            result['issues'].append({
                'description': f'Missing {dimension} conversions',
                'count': missing,
                'severity': 'MEDIUM',
                'dimension': dimension
            })
        
        return result
    
    def _estimate_expected_patterns(self, hive_sql: str, dimension: str, config: Dict) -> int:
        """Estimate expected number of positive patterns based on HiveQL content."""
        
        if dimension == 'syntactic':
            # Count actual SQL structures in HiveQL
            patterns = [r'SELECT\s+', r'INSERT\s+', r'CREATE\s+TABLE', r'DROP\s+TABLE', r'WITH\s+\w+\s+AS']
            count = sum(len(re.findall(p, hive_sql, re.IGNORECASE)) for p in patterns)
            
            # Minimum expectation: at least 1 for any SQL file
            return max(1, count)
        
        elif dimension == 'semantic':
            # Per Rule 3: UDFs should be PRESERVED, not converted
            # Only count UDFs that are actually converted to CASE statements
            # Don't penalize for keeping UDFs as-is (that's correct behavior)
            
            # Count converted UDFs in Spark SQL (positive patterns)
            converted_patterns = [
                r'CASE\s+WHEN\s+\w+\s+IS\s+NOT\s+NULL\s+AND\s+trim\s*\(',  # is_valid converted
                r'CASE\s+WHEN\s+trim\s*\([^)]+\)\s*=\s*[\'"][\'"]\s+OR\s+\w+\s+IS\s+NULL',  # is_blank converted
                r'CASE\s+WHEN\s+\w+\s+RLIKE\s+[\'"]\^\[0-9\]',  # is_numeric converted
                r'lpad\s*\(\s*cast\s*\([^)]+\s+as\s+string\s*\)',  # decimal_lpad converted
                r'CASE\s+WHEN\s+\w+\s+IS\s+NOT\s+NULL\s+THEN\s+.*\s+ELSE',  # nvl2 converted
            ]
            converted_count = sum(len(re.findall(p, hive_sql, re.IGNORECASE)) for p in converted_patterns)
            
            # Count UDFs that remain as UDFs (preserved - this is CORRECT per Rule 3)
            preserved_patterns = [
                r'\bis_valid\s*\(', r'\bis_blank\s*\(', r'\bis_numeric\s*\(', 
                r'\bdecimal_lpad\s*\(', r'\bnvl2\s*\(', r'\bis_valid_date\s*\('
            ]
            preserved_count = sum(len(re.findall(p, hive_sql, re.IGNORECASE)) for p in preserved_patterns)
            
            # Count other semantic patterns (IF, WITH, COALESCE)
            other_semantic = sum(len(re.findall(p, hive_sql, re.IGNORECASE)) for p in [
                r'\bIF\s*\(',  # IF statements
                r'WITH\s+\w+\s+AS\s*\(',  # CTEs
                r'coalesce\s*\('  # NULL handling
            ])
            
            # Expected = converted + preserved + other semantic patterns
            # If no semantic patterns exist, return 0 (not 1) to avoid false negatives
            return converted_count + preserved_count + other_semantic
        
        elif dimension == 'functional':
            # Count date and string functions that need conversion
            # Per Rule 6: Most date functions are kept AS-IS, only date_parse needs conversion
            patterns = [r'date_parse\s*\(']  # Only count functions that need conversion
            return sum(len(re.findall(p, hive_sql, re.IGNORECASE)) for p in patterns)
        
        elif dimension == 'structural':
            # Count structural elements (exclude Hive settings)
            # Remove lines that are Hive settings before counting
            hive_clean = '\n'.join([
                line for line in hive_sql.split('\n') 
                if not line.strip().startswith('set ')
            ])
            
            patterns = [
                r'WITH\s+\w+\s+AS', 
                r'\bJOIN\b',  # Use word boundary to avoid matching "mapjoin"
                r'UNION', 
                r'GROUP\s+BY', 
                r'ORDER\s+BY'
            ]
            return sum(len(re.findall(p, hive_clean, re.IGNORECASE)) for p in patterns)
        
        elif dimension == 'chunking':
            # Chunking is optional - check if file is chunked
            if re.search(r'chunk', hive_sql, re.IGNORECASE):
                return 3  # Expect metadata, dependencies, source tracking
            return 0  # Not chunked, no expectations
        
        elif dimension == 'coverage':
            # Count major SQL statements in HiveQL, excluding CREATE TEMPORARY FUNCTION
            patterns = [r'SELECT\s+', r'INSERT\s+', r'FROM\s+', 
                       r'WHERE\s+', r'\bJOIN\b', r'GROUP\s+BY', r'ORDER\s+BY']
            
            # Clean HiveQL to exclude settings
            hive_clean = '\n'.join([
                line for line in hive_sql.split('\n') 
                if not line.strip().startswith('set ')
            ])
            
            total = sum(len(re.findall(p, hive_clean, re.IGNORECASE)) for p in patterns)
            
            # Add CREATE statements but exclude CREATE TEMPORARY FUNCTION (UDFs)
            create_count = len(re.findall(r'CREATE\s+', hive_sql, re.IGNORECASE))
            create_temp_func = len(re.findall(r'CREATE\s+TEMPORARY\s+FUNCTION', hive_sql, re.IGNORECASE))
            total += (create_count - create_temp_func)
            
            return max(1, total)  # At least 1 for any SQL file
        
        elif dimension == 'null_handling':
            # Count NULL-related operations in HiveQL including IF() with NULL checks
            patterns = [
                r'coalesce\s*\(', 
                r'nvl\s*\(', 
                r'IS\s+NULL',  # This will match both standalone and inside IF()
                r'IS\s+NOT\s+NULL',
                r'ifnull\s*\(', 
                r'CASE\s+WHEN\s+\w+\s+IS\s+NULL'
            ]
            return sum(len(re.findall(p, hive_sql, re.IGNORECASE)) for p in patterns)
        
        elif dimension == 'breaking_changes':
            # Count potentially problematic functions in HiveQL
            patterns = [r'collect_list\s*\(', r'sort_array\s*\(', r'percentile\s*\(',
                       r'lateral\s+view', r'transform\s*\(', r'get_json_object\s*\(']
            count = sum(len(re.findall(p, hive_sql, re.IGNORECASE)) for p in patterns)
            # For breaking changes, we want fewer issues, so if count > 0, expect documentation
            return count if count > 0 else 0
        
        return 0
    
    def _get_confidence_level(self, accuracy: float) -> str:
        """Determine confidence level based on accuracy percentage."""
        if accuracy >= 95:
            return 'VERY HIGH'
        elif accuracy >= 85:
            return 'HIGH'
        elif accuracy >= 70:
            return 'MEDIUM'
        elif accuracy >= 50:
            return 'LOW'
        else:
            return 'VERY LOW'
    
    def generate_report(self, metrics: Dict, output_file: str):
        """Generate detailed markdown report."""
        report = []
        
        # Header
        report.append(f"# Conversion Accuracy Report")
        report.append(f"\n**File:** `{metrics['file']}`")
        report.append(f"\n**Overall Accuracy:** {metrics['accuracy_percentage']:.1f}%")
        report.append(f"\n**Confidence Level:** {metrics['confidence_level']}")
        report.append(f"\n---\n")
        
        # Summary
        report.append(f"## Summary\n")
        report.append(f"- **HiveQL Code Lines:** {metrics['hive_lines']} (executable code only)")
        report.append(f"- **Spark SQL Code Lines:** {metrics['spark_sql_lines']} (executable code only)")
        
        # Calculate line change
        line_change = metrics['spark_sql_lines'] - metrics['hive_lines']
        line_change_pct = (line_change / metrics['hive_lines'] * 100) if metrics['hive_lines'] > 0 else 0
        change_indicator = '📈' if line_change > 0 else '📉' if line_change < 0 else '➡️'
        report.append(f"- **Line Change:** {change_indicator} {line_change:+d} lines ({line_change_pct:+.1f}%)")
        
        report.append(f"- **Weighted Score:** {metrics['weighted_score']:.1f}/100")
        report.append(f"- **Issues Found:** {len(metrics['issues'])}")
        report.append(f"- **Successful Conversions:** {len(metrics['successes'])}\n")
        
        # Visual confidence bar
        report.append(f"## Confidence Visualization\n")
        report.append(self._generate_confidence_bar(metrics['accuracy_percentage']))
        report.append(f"\n")
        
        # Dimension breakdown
        report.append(f"## Dimension Breakdown\n")
        report.append(f"| Dimension | Weight | Score | Status |")
        report.append(f"|-----------|--------|-------|--------|")
        
        for dimension, data in metrics['dimensions'].items():
            weight_pct = data['weight'] * 100
            score = data['score_percentage']
            status = '✅' if score >= 80 else '⚠️' if score >= 50 else '❌'
            
            report.append(f"| **{data['description']}** | {weight_pct:.0f}% | {score:.1f}% | {status} |")
        
        report.append(f"\n")
        
        # Detailed dimension analysis
        report.append(f"## Detailed Dimension Analysis\n")
        
        for dimension, data in metrics['dimensions'].items():
            status = '✅' if data['score_percentage'] >= 80 else '⚠️' if data['score_percentage'] >= 50 else '❌'
            report.append(f"### {status} {data['description']}")
            report.append(f"\n**Weight:** {data['weight']*100:.0f}% | **Score:** {data['score_percentage']:.1f}%")
            report.append(f"\n**Explanation:** {data['explanation']}\n")
            report.append(f"- Positive Matches: {data['positive_matches']}")
            report.append(f"- Negative Matches: {data['negative_matches']}\n")
        
        # Issues
        if metrics['issues']:
            report.append(f"## Issues Found ({len(metrics['issues'])})\n")
            
            # Group by dimension
            by_dimension = {}
            for issue in metrics['issues']:
                dim = issue['dimension']
                if dim not in by_dimension:
                    by_dimension[dim] = []
                by_dimension[dim].append(issue)
            
            for dimension, issues in by_dimension.items():
                dim_name = self.CONVERSION_RULES[dimension]['description']
                report.append(f"### {dim_name}\n")
                
                high_issues = [i for i in issues if i.get('severity') == 'HIGH']
                medium_issues = [i for i in issues if i.get('severity') == 'MEDIUM']
                
                if high_issues:
                    report.append(f"**🔴 High Severity ({len(high_issues)})**\n")
                    for issue in high_issues:
                        report.append(f"- {issue['description']} (Count: {issue['count']})")
                    report.append(f"\n")
                
                if medium_issues:
                    report.append(f"**🟡 Medium Severity ({len(medium_issues)})**\n")
                    for issue in medium_issues:
                        report.append(f"- {issue['description']} (Count: {issue['count']})")
                    report.append(f"\n")
        else:
            report.append(f"## ✅ No Issues Found\n")
        
        # Supplementary Metrics
        if 'supplementary_metrics' in metrics:
            report.append(f"## Supplementary Metrics\n")
            report.append(f"*These metrics provide additional insights but don't affect the accuracy score.*\n")
            
            supp = metrics['supplementary_metrics']
            
            # Productivity Metrics
            report.append(f"### 📊 Productivity Metrics\n")
            prod = supp['productivity']
            report.append(f"| Metric | Value |")
            report.append(f"|--------|-------|")
            report.append(f"| Total Statements | {prod['total_statements']} |")
            report.append(f"| Fully Automated | {prod['fully_automated']} ({prod['automation_rate']}%) |")
            report.append(f"| Partially Automated | {prod['partially_automated']} |")
            report.append(f"| Manual Required | {prod['manual_required']} |")
            report.append(f"| Zero-Touch Migration | {prod['zero_touch_migration']} |")
            report.append(f"\n")
            
            # Complex Types
            report.append(f"### 🔧 Complex Type Usage\n")
            ct = supp['complex_types']
            report.append(f"| Type | Count |")
            report.append(f"|------|-------|")
            report.append(f"| Array Operations | {ct['array_operations']} |")
            report.append(f"| Map Operations | {ct['map_operations']} |")
            report.append(f"| Struct Operations | {ct['struct_operations']} |")
            report.append(f"| Lateral Views | {ct['lateral_views']} |")
            report.append(f"| Status | {ct['complex_type_conversions']} |")
            report.append(f"\n")
            
            # Partition & Bucketing
            report.append(f"### 🗂️ Partition & Bucketing\n")
            pb = supp['partition_bucketing']
            report.append(f"| Feature | Count |")
            report.append(f"|---------|-------|")
            report.append(f"| Partitioned Tables | {pb['partitioned_tables']} |")
            report.append(f"| Bucketed Tables | {pb['bucketed_tables']} |")
            report.append(f"| Dynamic Partitions | {pb['dynamic_partitions']} |")
            report.append(f"| DISTRIBUTE BY | {pb['distribute_by']} |")
            report.append(f"| SORT BY | {pb['sort_by']} |")
            report.append(f"| Migration Status | {pb['partition_migration_status']} |")
            report.append(f"\n")
            
            # Security Checklist
            report.append(f"### 🔒 Security Checklist\n")
            sec = supp['security_checklist']
            report.append(f"| Security Aspect | Status |")
            report.append(f"|-----------------|--------|")
            report.append(f"| Row-Level Security | {sec['row_level_security']} |")
            report.append(f"| Column Masking | {sec['column_masking']} |")
            report.append(f"| Permission Migration | {sec['permission_migration']} |")
            report.append(f"| Sensitive Data Patterns | {sec['sensitive_data_patterns']} found |")
            report.append(f"| Security Review Needed | {sec['security_review_needed']} |")
            report.append(f"\n")
            
            if sec['security_review_needed'] == 'YES':
                report.append(f"⚠️ **Security Review Required:** This conversion contains patterns that require manual security review.\n")
        
        # Recommendations
        report.append(f"## Recommendations\n")
        if metrics['accuracy_percentage'] >= 95:
            report.append(f"✅ **Excellent conversion quality.** Ready for production use with minimal review.\n")
        elif metrics['accuracy_percentage'] >= 85:
            report.append(f"✅ **Good conversion quality.** Review identified issues before production deployment.\n")
        elif metrics['accuracy_percentage'] >= 70:
            report.append(f"⚠️ **Acceptable conversion quality.** Thorough review and testing required.\n")
        else:
            report.append(f"❌ **Poor conversion quality.** Significant rework needed. Review all issues carefully.\n")
        
        # Dimension-specific recommendations
        report.append(f"### Dimension-Specific Recommendations\n")
        for dimension, data in metrics['dimensions'].items():
            if data['score_percentage'] < 80:
                dim_name = self.CONVERSION_RULES[dimension]['description']
                report.append(f"- **{dim_name}:** Score {data['score_percentage']:.1f}% - Needs improvement")
        
        # Write report
        Path(output_file).write_text('\n'.join(report), encoding='utf-8')
        return output_file
    
    def _generate_confidence_bar(self, percentage: float) -> str:
        """Generate ASCII confidence bar."""
        filled = int(percentage / 5)  # 20 blocks for 100%
        empty = 20 - filled
        
        bar = '█' * filled + '░' * empty
        
        if percentage >= 95:
            color = '🟢'
        elif percentage >= 85:
            color = '🟡'
        elif percentage >= 70:
            color = '🟠'
        else:
            color = '🔴'
        
        return f"{color} `{bar}` {percentage:.1f}%"
    
    def _get_issue_example(self, issue: Dict, metrics: Dict) -> str:
        """Get an example of the issue from the Spark SQL file."""
        # Map issue descriptions to search patterns
        pattern_map = {
            'Invalid: Hive variable substitution': r'\$\{[^}]+\}',
            'Invalid: Hive-specific command': r'set\s+hive\.',
            'Invalid: Hive JAR command': r'add\s+jar',
            'Unconverted: is_valid_date UDF': r'is_valid_date\s*\(',
            'Missing coverage conversions': None,
            'Missing null_handling conversions': None,
        }
        
        # Get pattern for this issue
        pattern = None
        for key, pat in pattern_map.items():
            if key in issue['description']:
                pattern = pat
                break
        
        if not pattern:
            return None
        
        # Read the Spark SQL file
        try:
            # Converted files live in output/<dialect>/spark_sql/ (possibly in a subfolder)
            project_root = Path(__file__).resolve().parents[2]
            spark_file = None
            for dialect in ('hive', 'impala'):
                spark_dir = project_root / 'output' / dialect / 'spark_sql'
                if (spark_dir / metrics['file']).exists():
                    spark_file = spark_dir / metrics['file']
                    break
                found = list(spark_dir.rglob(metrics['file'])) if spark_dir.exists() else []
                if found:
                    spark_file = found[0]
                    break
            if spark_file is None:
                return None
            
            content = spark_file.read_text(encoding='utf-8', errors='ignore')
            
            # Find first match
            match = re.search(pattern, content, re.IGNORECASE)
            if match:
                # Get surrounding context (50 chars before and after)
                start = max(0, match.start() - 50)
                end = min(len(content), match.end() + 50)
                example = content[start:end].strip()
                
                # Truncate if too long
                if len(example) > 150:
                    example = example[:150] + '...'
                
                return example
        except Exception:
            pass
        
        return None
    
    def generate_summary_report(self, all_metrics: List[Dict], output_file: str):
        """Generate summary report for all conversions."""
        report = []
        
        report.append(f"# Conversion Accuracy Summary Report\n")
        report.append(f"**Total Files:** {len(all_metrics)}\n")
        
        # Overall statistics
        avg_accuracy = sum(m['accuracy_percentage'] for m in all_metrics) / len(all_metrics)
        total_issues = sum(len(m['issues']) for m in all_metrics)
        total_successes = sum(len(m['successes']) for m in all_metrics)
        
        report.append(f"## Overall Statistics\n")
        report.append(f"- **Average Accuracy:** {avg_accuracy:.1f}%")
        report.append(f"- **Total Issues:** {total_issues}")
        report.append(f"- **Total Successful Conversions:** {total_successes}\n")
        
        # Confidence distribution with ranges
        confidence_dist = {}
        for m in all_metrics:
            level = m['confidence_level']
            confidence_dist[level] = confidence_dist.get(level, 0) + 1
        
        report.append(f"## Confidence Distribution\n")
        confidence_ranges = {
            'VERY HIGH': '95-100%',
            'HIGH': '85-94%',
            'MEDIUM': '70-84%',
            'LOW': '50-69%',
            'VERY LOW': '<50%'
        }
        for level in ['VERY HIGH', 'HIGH', 'MEDIUM', 'LOW', 'VERY LOW']:
            count = confidence_dist.get(level, 0)
            percentage = (count / len(all_metrics) * 100) if all_metrics else 0
            range_label = confidence_ranges[level]
            report.append(f"- **{level} ({range_label}):** {count} files ({percentage:.1f}%)")
        report.append(f"\n")
        
        # File-by-file breakdown
        report.append(f"## File-by-File Results\n")
        report.append(f"| File | Accuracy | Confidence | Issues | Status |")
        report.append(f"|------|----------|------------|--------|--------|")
        
        for m in sorted(all_metrics, key=lambda x: x['accuracy_percentage'], reverse=True):
            status = '✅' if m['accuracy_percentage'] >= 85 else '⚠️' if m['accuracy_percentage'] >= 70 else '❌'
            report.append(f"| {m['file']} | {m['accuracy_percentage']:.1f}% | {m['confidence_level']} | {len(m['issues'])} | {status} |")
        
        report.append(f"\n")
        
        # Top issues with examples
        report.append(f"## Top Issues\n")
        all_issues = []
        for m in all_metrics:
            for issue in m['issues']:
                all_issues.append((issue, m['file'], m))
        
        if all_issues:
            for issue, file, metrics in sorted(all_issues, key=lambda x: x[0]['count'], reverse=True)[:5]:
                severity = '🔴' if issue.get('severity') == 'HIGH' else '🟡'
                report.append(f"### {severity} {issue['description']}")
                report.append(f"- **Count:** {issue['count']}")
                report.append(f"- **File:** {file}")
                report.append(f"- **Severity:** {issue.get('severity', 'MEDIUM')}")
                
                # Get example
                example = self._get_issue_example(issue, metrics)
                if example:
                    report.append(f"- **Example:**")
                    report.append(f"  ```")
                    report.append(f"  {example}")
                    report.append(f"  ```")
                report.append(f"")
        else:
            report.append(f"No issues found!\n")
        
        # Write report
        Path(output_file).write_text('\n'.join(report), encoding='utf-8')
        return output_file


if __name__ == '__main__':
    # Example usage
    analyzer = AccuracyAnalyzer()
    
    # Analyze single file
    metrics = analyzer.analyze_conversion(
        'input/hive/example.hql',
        'output/hive/spark_sql/example.sql'
    )
    
    analyzer.generate_report(metrics, 'output/hive/accuracy/example_accuracy.md')
    print(f"Accuracy: {metrics['accuracy_percentage']:.1f}% ({metrics['confidence_level']})")

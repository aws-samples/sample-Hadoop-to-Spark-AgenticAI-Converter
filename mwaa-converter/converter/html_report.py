"""
HTML Report Generator for MWAA Converter.

Generates a professional HTML report matching the semantic validation report
look and feel:
- Left sidebar navigation with DAG list and confidence scores
- Per-DAG expandable sections with 8-dimension metric table
- Color-coded confidence badges (HIGH/MEDIUM/LOW)
- Issues list per DAG
- Executive summary statistics
- Job-to-task mapping table
- Airflow variables required
"""

import json
import re
from datetime import datetime
from html import escape
from pathlib import Path
from typing import Dict, List, Tuple


class HTMLReportGenerator:
    """Generate professional HTML conversion reports matching semantic validation report style."""

    def __init__(self, output_dir: Path):
        self.output_dir = Path(output_dir)

    # ── Public entry point (signature unchanged — merger.py calls this) ───────

    def generate_report(
        self,
        conversion_summary: Dict,
        validation_results: List[Dict],
        job_mappings: List[Dict],
        dag_files: List[Path],
        confidence_details: List[Dict],
        duration_seconds: float,
    ) -> Path:
        """Generate the complete HTML report. Returns path to written file."""
        dag_data = self._build_dag_data()
        if not dag_data:
            dag_data = self._build_dag_data_from_args(
                validation_results, confidence_details, job_mappings
            )

        total_dags = len(dag_data)
        total_jobs = sum(d["source_job_count"] for d in dag_data)
        total_tasks = sum(d["total_tasks"] for d in dag_data)
        high_count = sum(1 for d in dag_data if d["confidence"] >= 90)
        medium_count = sum(1 for d in dag_data if 70 <= d["confidence"] < 90)
        low_count = sum(1 for d in dag_data if d["confidence"] < 70)
        avg_confidence = (
            sum(d["confidence"] for d in dag_data) / total_dags if total_dags else 0.0
        )

        html = self._render_html(
            dag_data=dag_data,
            total_dags=total_dags,
            total_jobs=total_jobs,
            total_tasks=total_tasks,
            avg_confidence=avg_confidence,
            high_count=high_count,
            medium_count=medium_count,
            low_count=low_count,
            duration_seconds=duration_seconds,
        )

        report_path = self.output_dir / "conversion_report.html"
        report_path.write_text(html, encoding="utf-8")
        return report_path

    # ── Data builders ─────────────────────────────────────────────────────────

    def _live_syntax_check(self, chunk_id: str):
        """Re-validate DAG syntax against the current file on disk.

        Returns a dict shaped like the *_syntax.json artifact
        ({"is_valid": bool, "issues": [...]}), or None if the DAG file
        cannot be found (caller then falls back to the stored snapshot).
        """
        import ast

        dag_path = self.output_dir / "dags" / f"{chunk_id}.py"
        if not dag_path.exists():
            return None
        try:
            code = dag_path.read_text(encoding="utf-8")
            ast.parse(code)
            compile(code, str(dag_path), "exec")
            return {"is_valid": True, "dag_file": dag_path.name, "issues": []}
        except SyntaxError as e:
            return {
                "is_valid": False,
                "dag_file": dag_path.name,
                "issues": [{"category": "syntax",
                            "message": f"line {e.lineno}: {e.msg}"}],
            }
        except OSError:
            return None

    def _build_dag_data(self) -> List[Dict]:
        plans_dir = self.output_dir / "plans"
        val_dir = self.output_dir / "validation"
        if not plans_dir.exists():
            return []

        dag_data = []
        for plan_file in sorted(plans_dir.glob("*_plan.json")):
            try:
                with open(plan_file, encoding="utf-8") as f:
                    plan = json.load(f)
            except (OSError, json.JSONDecodeError):
                continue

            chunk_id = plan.get("chunk_id", plan_file.stem.replace("_plan", ""))
            dag_id = plan.get("dag_config", {}).get("dag_id") or chunk_id
            folder = plan.get("folder", chunk_id)
            source_type = plan.get("source_type", "control_m")
            tasks = plan.get("tasks", [])
            total_tasks = plan.get("total_tasks") or len(tasks)
            source_job_count = plan.get("source_job_count") or total_tasks
            summary = plan.get("conversion_summary", {})
            dep_graph = plan.get("dependency_graph", {})
            airflow_vars = plan.get("airflow_variables", {})
            schedule = plan.get("dag_config", {}).get("schedule")
            schedule_notes = plan.get("dag_config", {}).get("schedule_notes", "")

            def _read_val(suffix):
                p = val_dir / f"{chunk_id}_{suffix}.json"
                if p.exists():
                    try:
                        with open(p, encoding="utf-8") as f:
                            return json.load(f)
                    except (OSError, json.JSONDecodeError):
                        pass
                return {}

            v_syntax = _read_val("syntax")
            v_struct = _read_val("structure")
            v_sec = _read_val("security")
            v_dagbag = _read_val("dagbag")

            # The *_syntax.json snapshot is written during the first validation
            # pass, BEFORE fix_validation_errors auto-corrects the DAG. It is
            # never rewritten, so it can report a stale FAIL even though the
            # corrected DAG on disk parses cleanly (and DagBag loads it). Re-run
            # the syntax check against the current DAG file — the source of truth.
            live_syntax = self._live_syntax_check(chunk_id)
            if live_syntax is not None:
                v_syntax = live_syntax

            dag_code = self._read_dag_code(chunk_id)
            used_vars = self._variables_read_by_dag(dag_code, airflow_vars)
            source_has_schedule = self._source_has_schedule(chunk_id)
            triggered_by = self._dags_triggering(dag_id)

            dims, issues = self._compute_dimensions(
                plan=plan, tasks=tasks, summary=summary, dep_graph=dep_graph,
                airflow_vars=used_vars, schedule=schedule,
                schedule_notes=schedule_notes, v_syntax=v_syntax,
                v_struct=v_struct, v_sec=v_sec, v_dagbag=v_dagbag,
                source_has_schedule=source_has_schedule, triggered_by=triggered_by,
                source_var_count=len(airflow_vars),
            )

            weights = {
                "Job Parsing": 0.15, "Operator Mapping": 0.20,
                "Script Resolution": 0.15, "Dependency Wiring": 0.20,
                "Schedule Conversion": 0.10, "Variable Extraction": 0.05,
                "DAG Syntax": 0.10, "DagBag Load": 0.05,
            }
            confidence = sum(dims[n]["score"] * weights.get(n, 0) for n in dims)

            dag_data.append({
                "chunk_id": chunk_id, "dag_id": dag_id, "folder": folder,
                "source_type": source_type, "source_job_count": source_job_count,
                "total_tasks": total_tasks, "confidence": round(confidence, 1),
                "dimensions": dims, "issues": issues, "tasks": tasks,
                "airflow_vars": used_vars,
                "deploy_steps": self._deploy_steps(tasks, used_vars, dag_code, chunk_id),
                "operator_breakdown": summary.get("operator_breakdown", {}),
                "v_syntax": v_syntax, "v_struct": v_struct,
                "v_sec": v_sec, "v_dagbag": v_dagbag,
            })
        return dag_data

    # Why a mapping is below 100%: the source runtime is replaced, so behaviour
    # outside the script itself (e.g. Lambda wrapper logic) must be reviewed.
    _OP_REASON = {
        "emr_serverless": "AWS Lambda/OS job → EMR Serverless",
        "emr_add_steps": "EMR job → EMR step",
        "bash": "shell job → BashOperator on MWAA workers",
        "trigger_dag_run": "sub-folder → TriggerDagRunOperator",
    }

    _INFRA_VAR_DESCRIPTIONS = {
        "EMR_APP_ID": "EMR Serverless application ID",
        "EMR_EXEC_ROLE_ARN": "EMR Serverless job execution role ARN",
        "SCRIPTS_BUCKET": "S3 prefix (s3://bucket[/prefix]) holding spark_sql/ and scripts/",
        "LOGS_BUCKET": "S3 prefix for EMR Serverless job logs",
        "EMR_CLUSTER_ID": "EMR on EC2 cluster ID (j-...) for EMR step tasks",
    }

    def _read_dag_code(self, chunk_id: str) -> str:
        dag_file = self.output_dir / "dags" / f"{chunk_id}.py"
        try:
            return dag_file.read_text(encoding="utf-8")
        except OSError:
            return ""

    def _variables_read_by_dag(self, dag_code: str, plan_vars: Dict) -> Dict:
        """The Airflow Variables the generated DAG really reads, with descriptions.

        Taken from the DAG code (Variable.get(...) and {{ var.value.get(...) }}),
        so the list cannot drift from what MWAA will need.
        """
        names = re.findall(r"Variable\.get\(\s*[\"']([^\"']+)[\"']", dag_code)
        names += re.findall(r"var\.value\.get\(\s*[\"']([^\"']+)[\"']", dag_code)
        used: Dict = {}
        for name in dict.fromkeys(names):
            meta = plan_vars.get(name) if isinstance(plan_vars.get(name), dict) else None
            if meta:
                used[name] = meta
            elif name in self._INFRA_VAR_DESCRIPTIONS:
                used[name] = {"description": self._INFRA_VAR_DESCRIPTIONS[name],
                              "source": "infrastructure", "original_value": None}
            else:
                used[name] = {"description": "", "source": "", "undocumented": True}
        return used

    def _source_has_schedule(self, chunk_id: str) -> bool:
        ir_file = self.output_dir / "job_ir" / f"{chunk_id}.json"
        try:
            sched = (json.loads(ir_file.read_text(encoding="utf-8")).get("schedule") or {})
        except (OSError, ValueError):
            return True   # unknown: do not claim there was nothing to convert
        return bool(isinstance(sched, dict) and (sched.get("days") or sched.get("timefrom")))

    def _dags_triggering(self, dag_id: str) -> List[str]:
        """DAG ids whose generated code triggers dag_id."""
        out = []
        for dag_file in sorted((self.output_dir / "dags").glob("*.py")):
            code = dag_file.read_text(encoding="utf-8", errors="replace")
            if re.search(r'trigger_dag_id\s*=\s*["\']' + re.escape(dag_id) + r'["\']', code):
                m = re.search(r'dag_id\s*=\s*["\']([^"\']+)["\']', code)
                out.append(m.group(1) if m else dag_file.stem)
        return out

    @staticmethod
    def _mwaa_target(task: Dict) -> str:
        """Where the task's script lives on AWS, as used by the generated DAG."""
        op = (task.get("operator") or "").lower()
        src = (task.get("source_script") or "").strip()
        if not src:
            return "—"
        name = Path(src).name
        if "bash" in op:
            return f"MWAA dags/scripts/{name}"
        if "emr" in op:
            target = (task.get("target_script") or "").strip()
            if target.startswith("s3://"):
                parts = target.split("/", 3)
                return "<SCRIPTS_BUCKET>/" + parts[3] if len(parts) > 3 else target
            stem = Path(src).stem
            return f"<SCRIPTS_BUCKET>/spark_sql/{stem}.sql" if not src.endswith(".py") else f"<SCRIPTS_BUCKET>/{name}"
        return task.get("target_script") or "—"

    def _deploy_steps(self, tasks: List[Dict], used_vars: Dict, dag_code: str, chunk_id: str) -> List[str]:
        """Concrete steps to deploy this DAG on MWAA, derived from the generated code."""
        steps = [f"Upload <code>output/mwaa/dags/{escape(str(chunk_id))}.py</code> to the MWAA environment's <code>dags/</code> folder."]
        sql = sorted({self._mwaa_target(t) for t in tasks
                      if "emr" in (t.get("operator") or "").lower() and t.get("source_script")})
        if sql:
            steps.append("Upload the converted Spark SQL to " + ", ".join(f"<code>{escape(s)}</code>" for s in sql)
                         + " (convert the referenced Hive scripts first, e.g. with the hive_only pipeline).")
        if "scripts/run_spark_sql.py" in dag_code:
            steps.append("Upload <code>output/mwaa/scripts/run_spark_sql.py</code> to "
                         "<code>&lt;SCRIPTS_BUCKET&gt;/scripts/run_spark_sql.py</code> (runs the SQL files on EMR).")
        shell = sorted({Path(t["source_script"]).name for t in tasks
                        if "bash" in (t.get("operator") or "").lower() and t.get("source_script")})
        if shell:
            steps.append("Copy the shell scripts to the MWAA <code>dags/scripts/</code> folder: "
                         + ", ".join(f"<code>{escape(s)}</code>" for s in shell) + ".")
        if any(s.endswith(".ksh") for s in shell):
            steps.append("Install <code>ksh</code> on the MWAA workers (startup script: "
                         "<code>sudo yum install -y ksh</code>), or port the ksh scripts to bash.")
        if used_vars:
            steps.append(f"Create the {len(used_vars)} Airflow Variables listed above "
                         "(Admin → Variables, or import a JSON file).")
        if "ExternalTaskSensor(" in dag_code:
            steps.append("Deploy the upstream DAG it waits for in the same environment.")
        if "TriggerDagRunOperator(" in dag_code:
            steps.append("Deploy the DAG it triggers in the same environment.")
        return steps

    def _render_deploy_steps(self, steps: List[str]) -> str:
        if not steps:
            return ""
        items = "".join(f"<li>{s}</li>" for s in steps)   # steps contain escaped, trusted markup only
        return (f'<div class="sub-section"><div class="sub-title">Deploy checklist for MWAA</div>'
                f'<ol class="deploy-list">{items}</ol></div>')

    def _compute_dimensions(
        self, plan, tasks, summary, dep_graph, airflow_vars,
        schedule, schedule_notes, v_syntax, v_struct, v_sec, v_dagbag,
        source_has_schedule=True, triggered_by=None, source_var_count=None,
    ) -> Tuple[Dict, List[str]]:
        dims: Dict[str, Dict] = {}
        issues: List[str] = []

        # 1. Job Parsing
        has_tasks = len(tasks) > 0
        has_mapping = bool(plan.get("job_to_task_mapping"))
        parse_score = 100 if (has_tasks and has_mapping) else (50 if has_tasks else 0)
        if not has_tasks:
            issues.append("No jobs were parsed from the source file")
        dims["Job Parsing"] = {
            "score": parse_score,
            "detail": f"{len(tasks)} jobs parsed successfully" if has_tasks else "No jobs found",
            "justification": "All source jobs extracted and mapped to JobIR schema",
        }

        # 2. Operator Mapping
        avg_task_conf = sum(t.get("confidence_score", 0) for t in tasks) / len(tasks) if tasks else 0
        op_score = round(avg_task_conf * 100, 1)
        op_breakdown = summary.get("operator_breakdown", {})
        op_detail = ", ".join(f"{v}x {k}" for k, v in op_breakdown.items()) if op_breakdown else "N/A"
        low_conf = [t for t in tasks if t.get("confidence_score", 1) < 0.8]
        if low_conf:
            issues.append(f"{len(low_conf)} task(s) with operator confidence < 80%: "
                          + ", ".join(t.get("task_id", "") for t in low_conf))
        below = sorted({(t.get("operator", "?"), t.get("confidence_score", 0))
                        for t in tasks if t.get("confidence_score", 1) < 1})
        op_just = f"Average operator mapping confidence: {avg_task_conf:.0%}"
        if below:
            op_just += ". Below 100% because the target replaces the source runtime: " + ", ".join(
                f"{self._OP_REASON.get(op, op)} {conf:.0%}" for op, conf in below)
        dims["Operator Mapping"] = {
            "score": op_score, "detail": op_detail, "justification": op_just,
        }

        # 3. Script Resolution
        # Derive counts from the actual conversion plan tasks. A task "needs a
        # script" when it carries a source_script; it is "resolved" when the
        # planner produced a non-empty target_script (S3 URI) for it. The old
        # implementation read summary["script_mappings_resolved"/"unresolved"],
        # which are never populated, so it always reported 0/N.
        script_tasks = [t for t in tasks if (t.get("source_script") or "").strip()]
        resolved = sum(1 for t in script_tasks if (t.get("target_script") or "").strip())
        unresolved = len(script_tasks) - resolved
        total_scripts = len(script_tasks)
        script_score = 100 if total_scripts == 0 else round(resolved / total_scripts * 100, 1)
        if unresolved > 0:
            unresolved_srcs = ", ".join(
                (t.get("source_script") or "").strip()
                for t in script_tasks if not (t.get("target_script") or "").strip()
            )
            issues.append(
                f"{unresolved} script path(s) unresolved — add to spark_sql_mapping.json: {unresolved_srcs}"
            )
        mapping_file = self.output_dir.parent.parent / "input" / "mwaa" / "spark_sql_mapping.json"
        dims["Script Resolution"] = {
            "score": script_score,
            "detail": (f"{resolved}/{total_scripts} scripts resolved"
                       if total_scripts > 0 else "No scripts (sentinel jobs only)"),
            "justification": (
                "Source script paths mapped via input/mwaa/spark_sql_mapping.json"
                if mapping_file.exists() else
                "No spark_sql_mapping.json, so SQL scripts use the convention spark_sql/<name>.sql "
                "and shell scripts dags/scripts/<name>; see the deploy checklist"),
        }

        # 4. Dependency Wiring
        total_edges = sum(len(v) for v in dep_graph.values())
        struct_issues = v_struct.get("issues", [])
        missing_deps = [i for i in struct_issues if i.get("category") == "missing_dependency"]
        dep_score = 100 if not missing_deps else max(0, 100 - len(missing_deps) * 20)
        for md in missing_deps:
            issues.append(f"Missing dependency edge: {md.get('message', '')}")
        dims["Dependency Wiring"] = {
            "score": dep_score,
            "detail": f"{total_edges} dependency edge(s) wired as >> chains",
            "justification": "Control-M predecessor relationships preserved as Airflow >> operators",
        }

        # 5. Schedule Conversion
        if schedule is not None:
            sched_score, sched_detail = 100, f"Cron: {schedule}"
        elif not source_has_schedule:
            # Nothing to convert: the source folder is only ordered by another folder
            sched_score = 100
            sched_detail = "No schedule in source; schedule=None (runs when triggered)"
            if triggered_by:
                sched_detail += " by " + ", ".join(triggered_by)
        else:
            sched_score = 80
            sched_detail = "Manual trigger (schedule=None): source schedule not converted"
            if schedule_notes:
                sched_detail += f" — {schedule_notes[:80]}"
            issues.append("Schedule: the source folder has a schedule that was not converted")
        dims["Schedule Conversion"] = {
            "score": sched_score, "detail": sched_detail,
            "justification": "Control-M calendar converted to Airflow cron or documented as manual",
        }

        # 6. Variable Extraction
        infra_vars = [k for k, v in airflow_vars.items()
                      if isinstance(v, dict) and v.get("source") == "infrastructure"]
        job_vars = [k for k, v in airflow_vars.items()
                    if isinstance(v, dict) and v.get("source") != "infrastructure"]
        total_vars = len(airflow_vars)
        undocumented = [k for k, v in airflow_vars.items()
                        if isinstance(v, dict) and v.get("undocumented")]
        for k in undocumented:
            issues.append(f"Variable {k} is read by the DAG but not described in the plan")
        dims["Variable Extraction"] = {
            "score": 100 if not undocumented else max(0, 100 - 10 * len(undocumented)),
            "detail": f"{total_vars} variables read by the DAG ({len(infra_vars)} infra, {len(job_vars)} job-specific)",
            "justification": ("Every Airflow Variable the DAG reads is listed below; job variables are "
                              "passed to their task (shell env or SQL --var)"
                              + (f". {source_var_count - total_vars} other source value(s) were used during "
                                 "conversion only" if source_var_count and source_var_count > total_vars else "")),
        }

        # 7. DAG Syntax
        syntax_valid = v_syntax.get("is_valid", False)
        for si in v_syntax.get("issues", []):
            issues.append(f"Syntax: {si.get('message', str(si))}")
        dims["DAG Syntax"] = {
            "score": 100 if syntax_valid else 0,
            "detail": "Python AST parse passed" if syntax_valid else "Syntax errors found",
            "justification": "Generated DAG passes Python ast.parse() and compile()",
        }

        # 8. DagBag Load
        dagbag_valid = v_dagbag.get("is_valid", False)
        dagbag_status = v_dagbag.get("dagbag", "unknown")
        dagbag_msg = v_dagbag.get("message", "")
        if dagbag_status == "skipped":
            dagbag_score, dagbag_detail = 75, f"Skipped — {dagbag_msg[:60]}"
        elif dagbag_valid:
            dagbag_score, dagbag_detail = 100, dagbag_msg or "DAG loaded successfully"
        else:
            dagbag_score, dagbag_detail = 0, dagbag_msg or "DagBag load failed"
            issues.append(f"DagBag: {dagbag_msg}")
        dims["DagBag Load"] = {
            "score": dagbag_score, "detail": dagbag_detail,
            "justification": "Airflow DagBag can parse and load the generated DAG",
        }

        for si in v_sec.get("issues", []):
            if si.get("severity") == "CRITICAL":
                issues.append(f"Security CRITICAL: {si.get('message', '')}")

        return dims, issues

    def _build_dag_data_from_args(self, validation_results, confidence_details, job_mappings):
        conf_lookup = {d.get("dag_id", ""): d for d in confidence_details}
        dag_data = []
        for result in validation_results:
            dag_id = result.get("dag_id", "unknown")
            conf = conf_lookup.get(dag_id, {})
            score = conf.get("confidence_score", 0) * 100
            checks = result.get("checks", {})
            dims = {
                "Job Parsing":         {"score": 100, "detail": "N/A", "justification": ""},
                "Operator Mapping":    {"score": score, "detail": "N/A", "justification": ""},
                "Script Resolution":   {"score": 100, "detail": "N/A", "justification": ""},
                "Dependency Wiring":   {"score": 100 if checks.get("task_count") else 0, "detail": "N/A", "justification": ""},
                "Schedule Conversion": {"score": 80, "detail": "N/A", "justification": ""},
                "Variable Extraction": {"score": 100, "detail": "N/A", "justification": ""},
                "DAG Syntax":          {"score": 100 if checks.get("syntax") else 0, "detail": "N/A", "justification": ""},
                "DagBag Load":         {"score": 100 if str(checks.get("dagbag", "")).lower() == "true" else 75, "detail": "N/A", "justification": ""},
            }
            dag_data.append({
                "chunk_id": dag_id, "dag_id": dag_id, "folder": dag_id,
                "source_type": "control_m", "source_job_count": 0, "total_tasks": 0,
                "confidence": score, "dimensions": dims, "issues": result.get("issues", []),
                "tasks": [], "airflow_vars": {}, "operator_breakdown": {},
                "v_syntax": {}, "v_struct": {}, "v_sec": {}, "v_dagbag": {},
            })
        return dag_data

    def _enrich_job_mappings(self, job_mappings):
        plan_lookup: Dict[str, Dict] = {}
        plan_dir = self.output_dir / "plans"
        if plan_dir.exists():
            for plan_file in sorted(plan_dir.glob("*_plan.json")):
                try:
                    with open(plan_file, encoding="utf-8") as f:
                        plan = json.load(f)
                    dag_id = (plan.get("dag_config", {}).get("dag_id")
                              or plan_file.stem.replace("_plan", ""))
                    for task in plan.get("tasks", []):
                        job_id = task.get("source_job_id") or task.get("source_job_name", "")
                        if job_id:
                            plan_lookup[job_id] = {
                                "task_id": task.get("task_id", ""),
                                "operator": task.get("operator") or "Unknown",
                                "dag_id": dag_id,
                                "source_script": task.get("source_script") or "-",
                                "target_script": task.get("target_script") or "-",
                            }
                except (OSError, json.JSONDecodeError, KeyError):
                    pass
        enriched = []
        for m in job_mappings:
            job_id = m.get("source_job_id") or m.get("source_job", "")
            lk = plan_lookup.get(job_id, {})
            enriched.append({
                "source_job_id":  job_id,
                "target_task_id": lk.get("task_id") or m.get("target_task_id") or "N/A",
                "operator":       lk.get("operator") or m.get("operator_type") or "Unknown",
                "dag_id":         lk.get("dag_id") or m.get("target_dag_id") or "N/A",
                "source_script":  lk.get("source_script") or m.get("script_path") or "-",
                "target_script":  lk.get("target_script") or "-",
            })
        return enriched


    # ── HTML rendering ────────────────────────────────────────────────────────
    #
    # Every value that comes from the source workflow (Control-M XML / Airflow
    # DAG), the LLM-written plan files or validator output is HTML-escaped with
    # _e() before it is placed in the page. DAG selection uses data-dag-id
    # attributes + an event listener instead of inline onclick JavaScript, so a
    # crafted dag_id can never break out into script.

    @staticmethod
    def _e(value) -> str:
        """HTML-escape any value for safe use in element text or attributes."""
        return escape("" if value is None else str(value), quote=True)

    @staticmethod
    def _conf_class(score: float) -> str:
        if score >= 90:
            return "high"
        if score >= 70:
            return "medium"
        return "low"

    @staticmethod
    def _conf_label(score: float) -> str:
        if score >= 90:
            return "HIGH CONFIDENCE"
        if score >= 70:
            return "MEDIUM CONFIDENCE"
        return "LOW CONFIDENCE"

    @staticmethod
    def _score_bar(score: float) -> str:
        cls = "bar-high" if score >= 90 else "bar-medium" if score >= 70 else "bar-low"
        return (f'<div class="score-bar-wrap">'
                f'<div class="score-bar {cls}" style="width:{score:.0f}%"></div>'
                f'</div>'
                f'<span class="score-num">{score:.0f}%</span>')

    def _render_sidebar_items(self, dag_data: List[Dict]) -> str:
        items = []
        for d in dag_data:
            cls = self._conf_class(d["confidence"])
            items.append(
                f'<div class="sidebar-item" data-dag-id="{self._e(d["dag_id"])}" '
                f'id="nav-{self._e(d["dag_id"])}">'
                f'<span class="sidebar-name">{self._e(d["dag_id"])}</span>'
                f'<span class="conf-badge {cls}">{d["confidence"]:.0f}%</span>'
                f'</div>'
            )
        return "\n".join(items)

    def _render_dag_sections(self, dag_data: List[Dict]) -> str:
        sections = []
        for idx, d in enumerate(dag_data):
            cls = self._conf_class(d["confidence"])
            label = self._conf_label(d["confidence"])
            display = "block" if idx == 0 else "none"

            # Dimension rows
            dim_rows = []
            for dim_name, dim_info in d["dimensions"].items():
                score = dim_info["score"]
                dcls = self._conf_class(score)
                # Collect issues that belong to this dimension
                dim_issues = [iss for iss in d["issues"]
                              if dim_name.split()[0].lower() in iss.lower()
                              or dim_name.lower().replace(" ", "_") in iss.lower()]
                issue_marker = ""
                if score < 100:
                    issue_marker = f'<span class="dim-issue-count">{len(dim_issues) if dim_issues else "⚠"}</span>'
                dim_rows.append(
                    f'<tr>'
                    f'<td class="dim-name">{self._e(dim_name)} {issue_marker}</td>'
                    f'<td><span class="conf-badge {dcls}">{score:.0f}%</span></td>'
                    f'<td class="dim-bar">{self._score_bar(score)}</td>'
                    f'<td class="dim-detail">{self._e(dim_info["detail"])}</td>'
                    f'<td class="dim-just">{self._e(dim_info["justification"])}</td>'
                    f'</tr>'
                )
            dim_table = "\n".join(dim_rows)

            # Issues — prominent numbered list like reference report
            issues_html = ""
            if d["issues"]:
                li_items = "".join(
                    f'<li><span class="issue-num">{i+1}</span>{self._e(iss)}</li>'
                    for i, iss in enumerate(d["issues"])
                )
                issues_html = (
                    f'<div class="issues-box">'
                    f'<div class="issues-title">&#9888; Issues Found — {len(d["issues"])}</div>'
                    f'<ul class="issues-list">{li_items}</ul>'
                    f'</div>'
                )

            # Task mapping mini-table
            task_rows = []
            for t in d["tasks"]:
                op = t.get("operator", "unknown")
                op_badge = self._op_badge(op)
                src = t.get("source_script") or "—"
                tgt = self._mwaa_target(t)
                task_rows.append(
                    f'<tr>'
                    f'<td><code>{self._e(t.get("source_job_id",""))}</code></td>'
                    f'<td><code>{self._e(t.get("task_id",""))}</code></td>'
                    f'<td>{op_badge}</td>'
                    f'<td class="dim-detail">{self._e(src)}</td>'
                    f'<td class="dim-detail">{self._e(tgt)}</td>'
                    f'</tr>'
                )
            task_table_html = ""
            if task_rows:
                task_table_html = (
                    f'<div class="sub-section">'
                    f'<div class="sub-title">Job-to-Task Mapping ({self._e(d["source_job_count"])} jobs → {self._e(d["total_tasks"])} tasks)</div>'
                    f'<table class="inner-table">'
                    f'<thead><tr>'
                    f'<th>Source Job</th><th>Airflow Task</th><th>Operator</th>'
                    f'<th>Source Script</th><th>Location on AWS</th>'
                    f'</tr></thead>'
                    f'<tbody>{"".join(task_rows)}</tbody>'
                    f'</table>'
                    f'</div>'
                )

            # Variables mini-table
            var_rows = []
            for var_name, var_info in d["airflow_vars"].items():
                if isinstance(var_info, dict):
                    desc = var_info.get("description", "")
                    orig = var_info.get("original_value") or "—"
                    src = var_info.get("source", "")
                    var_rows.append(
                        f'<tr>'
                        f'<td><code>{self._e(var_name)}</code></td>'
                        f'<td class="dim-detail">{self._e(desc)}</td>'
                        f'<td class="dim-detail">{self._e(orig)}</td>'
                        f'<td class="dim-detail">{self._e(src)}</td>'
                        f'</tr>'
                    )
            var_table_html = ""
            if var_rows:
                var_table_html = (
                    f'<div class="sub-section">'
                    f'<div class="sub-title">Airflow Variables read by this DAG ({len(d["airflow_vars"])})</div>'
                    f'<table class="inner-table">'
                    f'<thead><tr>'
                    f'<th>Variable Name</th><th>Description</th>'
                    f'<th>Original Value</th><th>Source Job</th>'
                    f'</tr></thead>'
                    f'<tbody>{"".join(var_rows)}</tbody>'
                    f'</table>'
                    f'</div>'
                )

            source_label = "Control-M XML" if d["source_type"] == "control_m" else "Airflow DAG"

            sections.append(f'''
<div class="dag-section" id="dag-{self._e(d["dag_id"])}" style="display:{display}">
  <div class="dag-header">
    <div class="dag-title">
      <span class="dag-name">{self._e(d["dag_id"])}</span>
      <span class="conf-badge {cls} conf-large">{d["confidence"]:.1f}% avg</span>
      <span class="conf-label {cls}">{label}</span>
    </div>
    <div class="dag-meta">
      Source: <strong>{self._e(d["folder"])}</strong> ({source_label}) &nbsp;|&nbsp;
      {self._e(d["source_job_count"])} jobs &rarr; {self._e(d["total_tasks"])} tasks &nbsp;|&nbsp;
      Operators: {self._e(", ".join(f"{v}x {k}" for k,v in d["operator_breakdown"].items()) or "N/A")}
    </div>
  </div>

  <table class="dim-table">
    <thead>
      <tr>
        <th style="width:18%">Dimension</th>
        <th style="width:8%">Score</th>
        <th style="width:18%">Progress</th>
        <th style="width:28%">Detail</th>
        <th style="width:28%">Justification</th>
      </tr>
    </thead>
    <tbody>{dim_table}</tbody>
  </table>

  {issues_html}
  {task_table_html}
  {var_table_html}
  {self._render_deploy_steps(d.get("deploy_steps", []))}
</div>''')

        return "\n".join(sections)

    @staticmethod
    def _op_badge(op: str) -> str:
        op_l = op.lower()
        if "add_steps" in op_l or "addsteps" in op_l:
            return '<span class="op-badge op-emr">EMR Steps (EC2)</span>'
        if "emr" in op_l:
            return '<span class="op-badge op-emr">EMR Serverless</span>'
        if "empty" in op_l or "dummy" in op_l:
            return '<span class="op-badge op-empty">EmptyOperator</span>'
        if "bash" in op_l:
            return '<span class="op-badge op-bash">BashOperator</span>'
        if "trigger" in op_l:
            return '<span class="op-badge op-trigger">TriggerDagRun</span>'
        if "sensor" in op_l:
            return '<span class="op-badge op-sensor">ExternalSensor</span>'
        return f'<span class="op-badge op-other">{escape(str(op), quote=True)}</span>'

    def _render_html(
        self,
        dag_data: List[Dict],
        total_dags: int,
        total_jobs: int,
        total_tasks: int,
        avg_confidence: float,
        high_count: int,
        medium_count: int,
        low_count: int,
        duration_seconds: float,
    ) -> str:
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sidebar_items = self._render_sidebar_items(dag_data)
        dag_sections = self._render_dag_sections(dag_data)
        first_dag_id = dag_data[0]["dag_id"] if dag_data else ""

        # Summary table rows
        summary_rows = []
        for d in dag_data:
            cls = self._conf_class(d["confidence"])
            label = self._conf_label(d["confidence"])
            mapping_ok = d["source_job_count"] == d["total_tasks"] and d["source_job_count"] > 0
            mapping_badge = ('<span class="conf-badge high">1:1 ✓</span>'
                             if mapping_ok else
                             f'<span class="conf-badge low">{self._e(d["source_job_count"])}→{self._e(d["total_tasks"])}</span>')
            summary_rows.append(
                f'<tr data-dag-id="{self._e(d["dag_id"])}" style="cursor:pointer">'
                f'<td><strong>{self._e(d["dag_id"])}</strong></td>'
                f'<td><code>{self._e(d["folder"])}</code></td>'
                f'<td style="text-align:center">{self._e(d["source_job_count"])}</td>'
                f'<td style="text-align:center">{self._e(d["total_tasks"])}</td>'
                f'<td style="text-align:center">{mapping_badge}</td>'
                f'<td style="text-align:center">'
                f'<span class="conf-badge {cls}">{d["confidence"]:.1f}%</span></td>'
                f'<td><span class="conf-label {cls}">{label}</span></td>'
                f'</tr>'
            )
        summary_table = "\n".join(summary_rows)

        return f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>MWAA Conversion Report</title>
<style>
:root {{
  --high: #1e8e3e; --high-bg: #e6f4ea; --high-border: #a8d5b5;
  --medium: #b45309; --medium-bg: #fef3c7; --medium-border: #fcd34d;
  --low: #d93025; --low-bg: #fce8e6; --low-border: #f5a9a5;
  --primary: #232f3e; --secondary: #ff9900;
  --bg: #f5f7fa; --white: #ffffff;
  --text: #16191f; --text-light: #545b64;
  --border: #d5dbdb; --sidebar-w: 260px;
}}
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{ font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;
        background:var(--bg); color:var(--text); display:flex;
        flex-direction:column; min-height:100vh; }}
/* ── Top bar ── */
.topbar {{ background:linear-gradient(135deg,var(--primary) 0%,#37475a 100%);
           color:white; padding:16px 24px; display:flex;
           align-items:center; justify-content:space-between;
           box-shadow:0 2px 8px rgba(0,0,0,.2); position:sticky; top:0; z-index:100; }}
.topbar h1 {{ font-size:20px; display:flex; align-items:center; gap:10px; }}
.aws-badge {{ background:var(--secondary); color:var(--primary);
              padding:3px 10px; border-radius:4px; font-size:11px; font-weight:700; }}
.topbar-meta {{ font-size:12px; color:rgba(255,255,255,.7); }}
/* ── Layout ── */
.layout {{ display:flex; flex:1; }}
/* ── Sidebar ── */
.sidebar {{ width:var(--sidebar-w); background:var(--primary); color:white;
            padding:16px 0; overflow-y:auto; position:sticky;
            top:56px; height:calc(100vh - 56px); flex-shrink:0; }}
.sidebar-title {{ font-size:11px; text-transform:uppercase; letter-spacing:1px;
                  color:rgba(255,255,255,.5); padding:8px 16px 4px; }}
.sidebar-item {{ padding:10px 16px; cursor:pointer; display:flex;
                 align-items:center; justify-content:space-between;
                 border-left:3px solid transparent; transition:all .15s; }}
.sidebar-item:hover {{ background:rgba(255,255,255,.08); }}
.sidebar-item.active {{ background:rgba(255,153,0,.15);
                        border-left-color:var(--secondary); }}
.sidebar-name {{ font-size:13px; white-space:nowrap; overflow:hidden;
                 text-overflow:ellipsis; max-width:160px; }}
/* ── Main content ── */
.main {{ flex:1; padding:24px; overflow-y:auto; }}
/* ── Stats bar ── */
.stats-bar {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(130px,1fr));
              gap:12px; margin-bottom:24px; }}
.stat-card {{ background:var(--white); border-radius:10px; padding:16px;
              text-align:center; border:1px solid var(--border);
              box-shadow:0 1px 3px rgba(0,0,0,.05); }}
.stat-value {{ font-size:28px; font-weight:700; color:var(--primary); }}
.stat-value.high {{ color:var(--high); }}
.stat-value.medium {{ color:var(--medium); }}
.stat-value.low {{ color:var(--low); }}
.stat-label {{ font-size:11px; color:var(--text-light); text-transform:uppercase;
               letter-spacing:.5px; margin-top:2px; }}
/* ── Summary table ── */
.section {{ background:var(--white); border-radius:10px; padding:20px;
            margin-bottom:20px; border:1px solid var(--border);
            box-shadow:0 1px 3px rgba(0,0,0,.05); }}
.section-title {{ font-size:16px; font-weight:600; color:var(--primary);
                  margin-bottom:16px; display:flex; align-items:center; gap:8px; }}
table {{ width:100%; border-collapse:collapse; }}
th {{ background:var(--bg); padding:9px 12px; text-align:left; font-size:11px;
      text-transform:uppercase; letter-spacing:.5px; color:var(--text-light);
      border-bottom:2px solid var(--border); }}
td {{ padding:9px 12px; border-bottom:1px solid var(--border); font-size:13px; }}
tr:hover td {{ background:#fafbfc; }}
code {{ background:#f1f3f4; padding:2px 5px; border-radius:3px;
        font-family:monospace; font-size:12px; }}
/* ── Confidence badges ── */
.conf-badge {{ display:inline-block; padding:2px 8px; border-radius:12px;
               font-size:11px; font-weight:700; }}
.conf-badge.high {{ background:var(--high-bg); color:var(--high);
                    border:1px solid var(--high-border); }}
.conf-badge.medium {{ background:var(--medium-bg); color:var(--medium);
                      border:1px solid var(--medium-border); }}
.conf-badge.low {{ background:var(--low-bg); color:var(--low);
                   border:1px solid var(--low-border); }}
.conf-badge.conf-large {{ font-size:14px; padding:4px 12px; }}
.conf-label {{ font-size:11px; font-weight:600; }}
.conf-label.high {{ color:var(--high); }}
.conf-label.medium {{ color:var(--medium); }}
.conf-label.low {{ color:var(--low); }}
/* ── DAG sections ── */
.dag-section {{ background:var(--white); border-radius:10px; padding:20px;
                margin-bottom:20px; border:1px solid var(--border);
                box-shadow:0 1px 3px rgba(0,0,0,.05); }}
.dag-header {{ margin-bottom:16px; }}
.dag-title {{ display:flex; align-items:center; gap:12px; margin-bottom:6px; }}
.dag-name {{ font-size:18px; font-weight:700; color:var(--primary); }}
.dag-meta {{ font-size:13px; color:var(--text-light); }}
/* ── Dimension table ── */
.dim-table {{ width:100%; border-collapse:collapse; margin-bottom:16px; }}
.dim-table th {{ background:var(--bg); padding:8px 10px; font-size:11px;
                 text-transform:uppercase; letter-spacing:.5px;
                 color:var(--text-light); border-bottom:2px solid var(--border); }}
.dim-table td {{ padding:8px 10px; border-bottom:1px solid var(--border);
                 font-size:13px; vertical-align:middle; }}
.dim-name {{ font-weight:600; color:var(--primary); }}
.dim-detail {{ color:var(--text-light); font-size:12px; }}
.dim-just {{ color:var(--text-light); font-size:12px; font-style:italic; }}
.dim-bar {{ min-width:120px; }}
/* ── Score bar ── */
.score-bar-wrap {{ background:#e8eaed; border-radius:4px; height:8px;
                   width:100%; display:inline-block; vertical-align:middle;
                   margin-right:6px; }}
.score-bar {{ height:8px; border-radius:4px; }}
.bar-high {{ background:var(--high); }}
.bar-medium {{ background:var(--medium); }}
.bar-low {{ background:var(--low); }}
.score-num {{ font-size:12px; font-weight:600; vertical-align:middle; }}
/* ── Issues box ── */
.issues-box {{ background:var(--low-bg); border:1px solid var(--low-border);
               border-radius:8px; padding:12px 16px; margin-bottom:16px; }}
.issues-title {{ font-size:13px; font-weight:700; color:var(--low);
                 margin-bottom:10px; display:flex; align-items:center; gap:6px; }}
.issues-list {{ padding-left:0; list-style:none; }}
.issues-list li {{ font-size:13px; color:var(--text); margin-bottom:6px;
                   display:flex; align-items:flex-start; gap:8px; }}
.issue-num {{ background:var(--low); color:white; border-radius:50%;
              width:18px; height:18px; display:inline-flex; align-items:center;
              justify-content:center; font-size:10px; font-weight:700;
              flex-shrink:0; margin-top:1px; }}
.dim-issue-count {{ background:var(--low); color:white; border-radius:10px;
                    padding:1px 6px; font-size:10px; font-weight:700;
                    margin-left:4px; vertical-align:middle; }}
/* ── Sub-sections ── */
.sub-section {{ margin-top:16px; }}
.sub-title {{ font-size:13px; font-weight:600; color:var(--primary);
              margin-bottom:10px; padding-bottom:6px;
              border-bottom:1px solid var(--border); }}
.inner-table {{ width:100%; border-collapse:collapse; font-size:12px; }}
.inner-table th {{ background:var(--bg); padding:7px 10px; font-size:10px;
                   text-transform:uppercase; letter-spacing:.5px;
                   color:var(--text-light); border-bottom:1px solid var(--border); }}
.inner-table td {{ padding:7px 10px; border-bottom:1px solid var(--border); }}
/* ── Operator badges ── */
.op-badge {{ display:inline-block; padding:2px 7px; border-radius:4px;
             font-size:11px; font-weight:600; }}
.op-emr {{ background:#e8f0fe; color:#1967d2; }}
.op-empty {{ background:#f1f3f4; color:#5f6368; }}
.op-bash {{ background:#e6f4ea; color:var(--high); }}
.op-trigger {{ background:var(--medium-bg); color:var(--medium); }}
.op-sensor {{ background:#fce8e6; color:var(--low); }}
.op-other {{ background:#f1f3f4; color:#5f6368; }}
/* ── Footer ── */
.footer {{ text-align:center; padding:20px; color:var(--text-light);
           font-size:12px; background:var(--white);
           border-top:1px solid var(--border); }}
</style>
</head>
<body>

<!-- Top bar -->
<div class="topbar">
  <h1>MWAA Conversion Report <span class="aws-badge">Amazon MWAA</span></h1>
  <div class="topbar-meta">Generated: {timestamp}{f" &nbsp;|&nbsp; Duration: {duration_seconds/60:.1f} min" if duration_seconds > 0 else ""}</div>
</div>

<div class="layout">

  <!-- Sidebar -->
  <div class="sidebar">
    <div class="sidebar-title">DAGs ({total_dags})</div>
    {sidebar_items}
  </div>

  <!-- Main -->
  <div class="main">

    <!-- Stats bar -->
    <div class="stats-bar">
      <div class="stat-card">
        <div class="stat-value">{total_dags}</div>
        <div class="stat-label">DAGs</div>
      </div>
      <div class="stat-card">
        <div class="stat-value">{total_jobs}</div>
        <div class="stat-label">Source Jobs</div>
      </div>
      <div class="stat-card">
        <div class="stat-value">{total_tasks}</div>
        <div class="stat-label">Airflow Tasks</div>
      </div>
      <div class="stat-card">
        <div class="stat-value {'high' if avg_confidence >= 90 else 'medium' if avg_confidence >= 70 else 'low'}">{avg_confidence:.1f}%</div>
        <div class="stat-label">Avg Confidence</div>
      </div>
      <div class="stat-card">
        <div class="stat-value high">{high_count}</div>
        <div class="stat-label">High (&ge;90%)</div>
      </div>
      <div class="stat-card">
        <div class="stat-value medium">{medium_count}</div>
        <div class="stat-label">Medium</div>
      </div>
      <div class="stat-card">
        <div class="stat-value low">{low_count}</div>
        <div class="stat-label">Low</div>
      </div>
    </div>

    <!-- Summary table -->
    <div class="section">
      <div class="section-title">&#128203; Conversion Summary</div>
      <table>
        <thead>
          <tr>
            <th>DAG ID</th><th>Source Folder</th>
            <th style="text-align:center">Jobs</th>
            <th style="text-align:center">Tasks</th>
            <th style="text-align:center">Mapping</th>
            <th style="text-align:center">Confidence</th>
            <th>Level</th>
          </tr>
        </thead>
        <tbody>{summary_table}</tbody>
      </table>
    </div>

    <!-- Per-DAG sections -->
    {dag_sections}

  </div><!-- /main -->
</div><!-- /layout -->

<div class="footer">
  Generated by <strong>MWAA Converter</strong> &nbsp;|&nbsp;
  <a href="https://aws.amazon.com/mwaa/" style="color:var(--secondary)">Amazon MWAA</a>
</div>

<script>
function showDAG(dagId) {{
  document.querySelectorAll('.dag-section').forEach(s => s.style.display = 'none');
  document.querySelectorAll('.sidebar-item').forEach(s => s.classList.remove('active'));
  var sec = document.getElementById('dag-' + dagId);
  if (sec) sec.style.display = 'block';
  var nav = document.getElementById('nav-' + dagId);
  if (nav) nav.classList.add('active');
}}
// Wire DAG selection (data-dag-id attributes; no inline JS) and activate first item
document.addEventListener('DOMContentLoaded', function() {{
  document.querySelectorAll('[data-dag-id]').forEach(function(el) {{
    el.addEventListener('click', function() {{ showDAG(el.dataset.dagId); }});
  }});
  var first = document.querySelector('.sidebar-item');
  if (first) first.classList.add('active');
}});
</script>
</body>
</html>'''

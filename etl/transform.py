import json
from decimal import Decimal, InvalidOperation


def _json(value):
    return json.dumps(value, ensure_ascii=False, separators=(",", ":")) if value is not None else None


def _duration(value):
    if value is None or value == "":
        return None
    try:
        return float(Decimal(str(value)))
    except (InvalidOperation, ValueError, TypeError):
        return None


def transform_run(run):
    meta = run.get("metadata") or {}
    run_id = run.get("_id") or meta.get("id")
    missing = [name for name, value in (("run_id", run_id),
                ("repository_name", run.get("repository_name")), ("workflow_path", run.get("workflow_path"))) if value is None]
    if missing:
        raise ValueError("Missing required fields: " + ", ".join(missing))
    run_row = {
        "run_id": str(run_id), "repository_name": run.get("repository_name"),
        "workflow_path": run.get("workflow_path"), "run_number": run.get("run_number"),
        "run_attempt": run.get("run_attempt"), "workflow_name": meta.get("name"),
        "branch": meta.get("branch"), "commit_sha": meta.get("sha"), "event": meta.get("event"),
        "status": meta.get("status"), "conclusion": meta.get("conclusion"),
        "created_at": meta.get("created_at"), "updated_at": meta.get("updated_at"),
        "run_started_at": meta.get("run_started_at"), "total_logs_size": run.get("total_logs_size"),
    }
    jobs, steps = [], []
    for ji, log in enumerate(run.get("log_insights") or []):
        if not isinstance(log, dict):
            continue
        job_id = f"{run_id}_{ji}"
        log_name = log.get("file")
        job_name = log_name.rsplit("/", 1)[-1] if isinstance(log_name, str) else None
        job_steps = log.get("steps") or []
        jobs.append({"job_id": job_id, "run_id": str(run_id), "job_name": job_name,
                     "job_index": ji, "log_file": log_name, "total_lines": log.get("total_lines"),
                     "log_size": log.get("log_size"), "runner_image": log.get("image"),
                     "runner_image_version": log.get("image_version"), "step_count": len(job_steps),
                     "token_permissions_json": _json(log.get("token_permissions")),
                     "extra_json": _json({k: v for k, v in log.items() if k not in {"file", "total_lines", "log_size", "image", "image_version", "steps", "token_permissions"}})})
        for si, step in enumerate(job_steps):
            if not isinstance(step, dict):
                continue
            steps.append({"step_id": f"{job_id}_{si}", "job_id": job_id, "step_index": si,
                "step_type": step.get("type"), "action_repository": step.get("repository"),
                "action_name": step.get("action"), "action_version": step.get("version"),
                "action_sha": step.get("sha"), "code": step.get("code"),
                "start_date": step.get("start_date"), "duration_sec": _duration(step.get("duration_sec")),
                "environment_json": _json(step.get("env")), "with_json": _json(step.get("with")),
                "commands_json": _json(step.get("commands")),
                "parser_error_json": _json({k: step[k] for k in ("error", "errors") if k in step}) or None,
                "extra_json": _json({k: v for k, v in step.items() if k not in {"type", "repository", "action", "version", "sha", "code", "start_date", "duration_sec", "env", "with", "commands", "error", "errors"}})})
    return run_row, jobs, steps

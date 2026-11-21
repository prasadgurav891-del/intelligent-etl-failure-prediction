import json


RUN_COLS = "run_id,repository_name,workflow_path,run_number,run_attempt,workflow_name,branch,commit_sha,event,status,conclusion,created_at,updated_at,run_started_at,total_logs_size"
JOB_COLS = "job_id,run_id,job_name,job_index,log_file,total_lines,log_size,runner_image,runner_image_version,step_count,token_permissions_json,extra_json"
STEP_COLS = "step_id,job_id,step_index,step_type,action_repository,action_name,action_version,action_sha,code,start_date,duration_sec,environment_json,with_json,commands_json,parser_error_json,extra_json"


def _insert(conn, table, columns, rows, json_columns=()):
    if not rows:
        return
    from psycopg2.extras import execute_values
    cols = columns.split(",")
    values = [[row[c] for c in cols] for row in rows]
    casts = {c: "::jsonb" for c in json_columns}
    template = "(" + ",".join("%s" + casts.get(c, "") for c in cols) + ")"
    updates = ",".join(f"{c}=EXCLUDED.{c}" for c in cols[1:])
    with conn.cursor() as cur:
        execute_values(cur, f"INSERT INTO {table} ({columns}) VALUES %s ON CONFLICT ({cols[0]}) DO UPDATE SET {updates}", values, template=template)


def load_batch(conn, runs, jobs, steps, simulate_failure=False):
    """Atomically upsert one bounded batch; callers commit only after all three tables."""
    try:
        _insert(conn, "workflow_runs", RUN_COLS, runs)
        _insert(conn, "jobs", JOB_COLS, jobs, ("token_permissions_json", "extra_json"))
        _insert(conn, "steps", STEP_COLS, steps, ("environment_json", "with_json", "commands_json", "parser_error_json", "extra_json"))
        if simulate_failure:
            # Raise a PostgreSQL error after writes but before commit to exercise rollback.
            with conn.cursor() as cur:
                cur.execute("SELECT 1 / 0")
        conn.commit()
    except Exception:
        conn.rollback()
        raise

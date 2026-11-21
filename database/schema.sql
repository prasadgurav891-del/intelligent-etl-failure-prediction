CREATE TABLE IF NOT EXISTS workflow_runs (
 run_id TEXT PRIMARY KEY, repository_name TEXT NOT NULL, workflow_path TEXT NOT NULL,
 run_number BIGINT, run_attempt INTEGER, workflow_name TEXT, branch TEXT, commit_sha TEXT,
 event TEXT, status TEXT, conclusion TEXT, created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ,
 run_started_at TIMESTAMPTZ, total_logs_size BIGINT
);
CREATE TABLE IF NOT EXISTS jobs (
 job_id TEXT PRIMARY KEY, run_id TEXT NOT NULL REFERENCES workflow_runs(run_id) ON DELETE CASCADE,
 job_name TEXT, job_index INTEGER NOT NULL CHECK(job_index >= 0), log_file TEXT,
 total_lines BIGINT, log_size BIGINT, runner_image TEXT, runner_image_version TEXT, step_count INTEGER,
 token_permissions_json JSONB, extra_json JSONB
);
CREATE TABLE IF NOT EXISTS steps (
 step_id TEXT PRIMARY KEY, job_id TEXT NOT NULL REFERENCES jobs(job_id) ON DELETE CASCADE,
 step_index INTEGER NOT NULL CHECK(step_index >= 0), step_type TEXT, action_repository TEXT,
 action_name TEXT, action_version TEXT, action_sha TEXT, code TEXT, start_date TIMESTAMPTZ,
 duration_sec DOUBLE PRECISION, environment_json JSONB, with_json JSONB, commands_json JSONB,
 parser_error_json JSONB, extra_json JSONB
);
CREATE TABLE IF NOT EXISTS etl_executions (
 etl_run_id UUID PRIMARY KEY, scenario TEXT NOT NULL, started_at TIMESTAMPTZ NOT NULL,
 ended_at TIMESTAMPTZ, overall_status TEXT NOT NULL, error_type TEXT, error_message TEXT,
 random_seed BIGINT, source_size_bytes BIGINT, requested_record_limit BIGINT,
 effective_record_limit BIGINT, batch_size INTEGER, resource_memory_limit_bytes BIGINT,
 task_count INTEGER NOT NULL DEFAULT 3
);
CREATE TABLE IF NOT EXISTS etl_task_metadata (
 metadata_id BIGSERIAL PRIMARY KEY, etl_run_id UUID NOT NULL REFERENCES etl_executions(etl_run_id),
 task_name TEXT NOT NULL, task_position INTEGER NOT NULL, start_time TIMESTAMPTZ NOT NULL,
 end_time TIMESTAMPTZ NOT NULL, duration_sec DOUBLE PRECISION NOT NULL,
 records_read BIGINT NOT NULL DEFAULT 0, records_written BIGINT NOT NULL DEFAULT 0,
 records_failed BIGINT NOT NULL DEFAULT 0, status TEXT NOT NULL, error_type TEXT, error_message TEXT,
 cpu_percent DOUBLE PRECISION, memory_percent DOUBLE PRECISION, input_records BIGINT NOT NULL DEFAULT 0,
 output_records BIGINT NOT NULL DEFAULT 0, throughput DOUBLE PRECISION NOT NULL DEFAULT 0,
 retry_count INTEGER NOT NULL DEFAULT 0, failure_origin TEXT,
 resource_metrics_error TEXT, UNIQUE(etl_run_id, task_name)
);
CREATE INDEX IF NOT EXISTS etl_task_metadata_run_idx ON etl_task_metadata(etl_run_id, task_position);
CREATE INDEX IF NOT EXISTS etl_executions_ended_idx ON etl_executions(ended_at);
DROP VIEW IF EXISTS ml_training_data;
CREATE VIEW ml_training_data AS
SELECT m.etl_run_id,
       m.task_name,
       e.scenario,
       m.failure_origin,
       'ETL_TASK_STATUS'::TEXT AS label_source,
       e.source_size_bytes,
       e.requested_record_limit,
       e.effective_record_limit,
       e.batch_size,
       e.resource_memory_limit_bytes,
       m.task_position,
       e.task_count,
       h.historical_task_execution_count,
       h.historical_task_failure_rate,
       h.historical_avg_duration_sec,
       h.historical_avg_cpu_percent,
       h.historical_avg_memory_percent,
       CASE m.status WHEN 'SUCCESS' THEN 0 WHEN 'FAILURE' THEN 1 END AS task_failure
FROM etl_task_metadata m
JOIN etl_executions e USING (etl_run_id)
LEFT JOIN LATERAL (
    SELECT COUNT(*)::BIGINT AS historical_task_execution_count,
           AVG(CASE p.status WHEN 'FAILURE' THEN 1.0 WHEN 'SUCCESS' THEN 0.0 END) AS historical_task_failure_rate,
           AVG(p.duration_sec) AS historical_avg_duration_sec,
           AVG(p.cpu_percent) AS historical_avg_cpu_percent,
           AVG(p.memory_percent) AS historical_avg_memory_percent
    FROM etl_task_metadata p
    JOIN etl_executions prior USING (etl_run_id)
    WHERE p.task_name = m.task_name
      AND p.status IN ('SUCCESS', 'FAILURE')
      AND prior.ended_at IS NOT NULL
      AND prior.ended_at < e.started_at
) h ON TRUE
WHERE m.status IN ('SUCCESS', 'FAILURE');

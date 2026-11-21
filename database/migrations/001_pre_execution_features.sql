-- Apply once to databases created with the initial ETL schema.
BEGIN;

ALTER TABLE etl_executions
    ADD COLUMN IF NOT EXISTS source_size_bytes BIGINT,
    ADD COLUMN IF NOT EXISTS requested_record_limit BIGINT,
    ADD COLUMN IF NOT EXISTS effective_record_limit BIGINT,
    ADD COLUMN IF NOT EXISTS batch_size INTEGER,
    ADD COLUMN IF NOT EXISTS resource_memory_limit_bytes BIGINT,
    ADD COLUMN IF NOT EXISTS task_count INTEGER NOT NULL DEFAULT 3;

ALTER TABLE etl_task_metadata
    ADD COLUMN IF NOT EXISTS failure_origin TEXT,
    ADD COLUMN IF NOT EXISTS resource_metrics_error TEXT;

UPDATE etl_task_metadata m
SET failure_origin = CASE
    WHEN m.status <> 'FAILURE' THEN NULL
    WHEN e.scenario IN ('INVALID_DATA', 'TRANSFORMATION_FAILURE',
                        'DATABASE_FAILURE_SIMULATION', 'RESOURCE_THRESHOLD_FAILURE') THEN 'INJECTED'
    ELSE 'OBSERVED'
END
FROM etl_executions e
WHERE e.etl_run_id = m.etl_run_id
  AND m.failure_origin IS NULL;

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

COMMIT;

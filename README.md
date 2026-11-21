# Intelligent ETL Pipeline Monitoring and Failure Prediction

This first research-project version streams the GHALogs workflow-run JSONL dataset into PostgreSQL and records metadata about each ETL execution and its extract, transform, and load tasks. It does not train a model. Workflow conclusions are retained as dataset fields; step `error` and `errors` are stored as parser observations and never used as ETL failure labels.

## Dataset and architecture

The source is the GHALogs `runs.json.gz` file (approximately 580k workflow objects). `etl.extract` reads gzip or plain JSON Lines one line at a time, skips and reports malformed JSON, and supports limits. `etl.transform` creates deterministic run, job, and step records while retaining nested fields as JSON. `etl.load` uses PostgreSQL batch upserts with a transaction per batch. `etl.pipeline` coordinates the stages and records task metadata. `etl.experiment_runner` creates repeated executions with controlled scenarios. `etl.export_ml_data` exports the database view.

The relational tables are `workflow_runs`, `jobs`, and `steps`. Execution history is append-only in `etl_executions` and `etl_task_metadata`. `ml_training_data` contains configuration known before execution and historical averages from executions that ended before the current run started. It excludes current-run duration, output counts, CPU/memory readings, throughput, and previous-task status from the model feature columns. Scenario, run UUID, failure origin, and label source are exported as audit columns and should not be used as model predictors. The binary target is based only on each ETL task's own `SUCCESS` or `FAILURE` status; skipped tasks are not training rows. GHALogs workflow conclusions stay in `workflow_runs` and are never joined into task labels.

## Install and configure

Use Python 3.10 or newer and PostgreSQL 14 or newer. From the repository root in PowerShell:

```powershell
py -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
Copy-Item .env.example .env
```

Edit `.env` with PostgreSQL credentials and the dataset path. `python-dotenv` loads it automatically when the ETL starts. Alternatively set the variables individually with `$env:DB_HOST`, `$env:DB_PORT`, `$env:DB_NAME`, `$env:DB_USER`, `$env:DB_PASSWORD`, and `$env:GHALOGS_PATH`. `psutil` supplies process CPU and memory measures. If unavailable, those measures are null.

## PostgreSQL setup

Create the database (substitute the configured PostgreSQL administrator if needed):

```powershell
createdb -h localhost -p 5432 -U postgres etl_failure_prediction
```

For a new database, install the schema:

```powershell
psql -h localhost -p 5432 -U postgres -d etl_failure_prediction -f database/schema.sql
```

If the database already uses the earlier project schema, apply the migration before running the updated pipeline:

```powershell
psql -h localhost -p 5432 -U postgres -d etl_failure_prediction -f database/migrations/001_pre_execution_features.sql
```

## Run

Process and validate a 1,000-run pilot (PostgreSQL must be running):

```powershell
python -m etl.pipeline --limit 1000
```

For a pilot experiment set, scenario pinning makes a run reproducible:

```powershell
python -m etl.experiment_runner --runs 6 --seed 42 --limit 1000
python -m etl.experiment_runner --runs 100 --seed 42
```

The experiment runner deterministically selects from NORMAL, LARGE_DATA, INVALID_DATA, TRANSFORMATION_FAILURE, DATABASE_FAILURE_SIMULATION, RESOURCE_STRESS, and RESOURCE_THRESHOLD_FAILURE for a given seed. Pass `--scenario NORMAL` to pin all runs. INVALID_DATA rejects one deterministic input record. TRANSFORMATION_FAILURE injects a transform exception. DATABASE_FAILURE_SIMULATION writes a batch inside a transaction, triggers a PostgreSQL division-by-zero error before commit, and verifies rollback before recording the failed load. RESOURCE_STRESS performs bounded CPU work and touches an 8 MiB buffer. RESOURCE_THRESHOLD_FAILURE exercises a deterministic memory-budget guard (requests at least 64 MiB and one byte more than `RESOURCE_MEMORY_LIMIT_BYTES`, 32 MiB by default) without allocating that memory. LARGE_DATA processes up to five times `--limit` in one pass, or the full source without a limit. Every repeated execution starts at the beginning of the source: repeated runs therefore revisit the same prefix, while deterministic IDs upsert source rows and each run keeps separate metadata. Use different source shards or offsets in a future experiment design if distinct subsets are needed. Tasks that did not execute are marked `SKIPPED` and omitted from the labeled training view.

Run all 580k+ source records once with:

```powershell
python -m etl.pipeline
```

Export model-ready features to `data/processed/ml_training_data.csv`. Scenario, run ID, label source, and failure origin are written separately to `data/processed/ml_training_data_audit.csv`, so those outcome-revealing fields are not accidentally fed to the model:

```powershell
python -m etl.export_ml_data
```

## Verify and tests

Use psql to inspect inserted rows and task labels:

```sql
SELECT count(*) FROM workflow_runs;
SELECT count(*) FROM jobs;
SELECT count(*) FROM steps;
SELECT etl_run_id, scenario, overall_status FROM etl_executions ORDER BY started_at DESC LIMIT 10;
SELECT task_name, failure_origin, task_failure, count(*) FROM ml_training_data GROUP BY task_name, failure_origin, task_failure;
SELECT task_name, status, records_failed, error_type FROM etl_task_metadata ORDER BY metadata_id DESC LIMIT 12;
```

Run the local unit tests:

```powershell
python -m unittest discover -s tests -v
```

## Metadata, limitations, and next research steps

Each pipeline execution gets a UUID. Per-task counters, duration, status, exception information, CPU and memory are stored separately; throughput and retry count are included for later extension. CPU utilization is process CPU time divided by the measured active operation time (it can exceed 100% for multithreaded processes); memory is peak process RSS as a percentage of physical RAM. These are process-level measures, not container-wide resource accounting. `psutil` is required. If its process/system counters are unavailable, the values remain NULL and `resource_metrics_error` records why; no zeros are fabricated. A real loss of database connectivity can prevent PostgreSQL from receiving metadata and must be recovered externally. Retries are currently zero because automatic retry behavior is not implemented.

The scenarios are controlled and useful for checking data collection, but they are not a substitute for naturally observed production failure conditions. Later work can add XGBoost and SHAP after enough representative task outcomes have been collected, then Kafka, Spark, and a Streamlit interface if justified. Those systems are intentionally outside this version.

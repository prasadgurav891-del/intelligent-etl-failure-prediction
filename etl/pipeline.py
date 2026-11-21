import argparse
import os
import time
import uuid
from datetime import datetime, timezone

import psycopg2

from etl.config import Config
from etl.config import effective_record_limit
from etl.extract import iter_runs
from etl.load import load_batch
from etl.transform import transform_run
from metadata.collector import TaskMeasurement
from metadata.features import classify_failure_origin


def _now():
    return datetime.now(timezone.utc)


def _insert_execution(conn, run_id, scenario, seed, config, requested_limit, effective_limit):
    with conn.cursor() as cur:
        cur.execute("""INSERT INTO etl_executions
          (etl_run_id,scenario,started_at,overall_status,random_seed,source_size_bytes,
           requested_record_limit,effective_record_limit,batch_size,resource_memory_limit_bytes,task_count)
          VALUES (%s,%s,%s,'RUNNING',%s,%s,%s,%s,%s,%s,3)""",
                    (str(run_id), scenario, _now(), seed, os.path.getsize(config.source_path),
                     requested_limit, effective_limit, config.batch_size, config.resource_memory_limit_bytes))
    conn.commit()


def _save_task(conn, run_id, name, position, started, ended, duration, cpu, memory, read, written, failed, status, error, failure_origin, metrics_error):
    with conn.cursor() as cur:
        cur.execute("""INSERT INTO etl_task_metadata
          (etl_run_id,task_name,task_position,start_time,end_time,duration_sec,records_read,records_written,records_failed,status,error_type,error_message,cpu_percent,memory_percent,input_records,output_records,throughput,failure_origin,resource_metrics_error)
          VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
          (str(run_id), name, position, started, ended, duration, read, written, failed, status,
           type(error).__name__ if error else None, str(error)[:4000] if error else None, cpu, memory,
           read, written, written / duration if duration > 0 else 0.0, failure_origin, metrics_error))
    conn.commit()


def _record_task(conn, run_id, name, position, task, read, written, failed, error=None, status=None, failure_origin=None):
    duration, cpu, memory, started, ended = task.finish()
    actual_status = status or ("FAILURE" if error else "SUCCESS")
    origin = (failure_origin if actual_status == "FAILURE" else None)
    _save_task(conn, run_id, name, position, started, ended, duration, cpu, memory, read, written, failed,
               actual_status, error, origin, task.resource_metrics_error)


def run_pipeline(config=None, limit=None, scenario="NORMAL", seed=None, run_id=None):
    if limit is not None and limit < 1:
        raise ValueError("limit must be at least 1")
    config = config or Config.from_env()
    run_id = run_id or uuid.uuid4()
    started_wall = time.perf_counter()
    conn = psycopg2.connect(**config.db_kwargs())
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT to_regclass('public.workflow_runs')")
            schema_installed = cur.fetchone()[0] is not None
            if not schema_installed:
                cur.execute(open("database/schema.sql", encoding="utf-8").read())
        conn.commit()
        source_limit = effective_record_limit(limit, scenario)
        _insert_execution(conn, run_id, scenario, seed, config, limit, source_limit)
        stage_stats = {n: {"read": 0, "written": 0, "failed": 0} for n in ("extract", "transform", "load")}
        errors = []
        extraction = TaskMeasurement()
        transformation = TaskMeasurement()
        loading = TaskMeasurement()
        run_batch, job_batch, step_batch = [], [], []
        totals = {"runs": 0, "jobs": 0, "steps": 0}
        malformed = {"n": 0}
        invalid_injected = {"done": False}
        load_failure_injected = {"done": False}

        def on_bad(line_no, exc):
            malformed["n"] += 1
            if len(errors) < 100:
                errors.append(f"line {line_no}: {type(exc).__name__}: {exc}")
            extraction.records_failed += 1
            stage_stats["extract"]["failed"] += 1

        def before_extract(line_no, line):
            extraction.records_read += 1
            stage_stats["extract"]["read"] += 1

        def timed_runs():
            iterator = iter_runs(config.source_path, source_limit, on_bad, before_extract)
            while True:
                active_task = "extract"
                token = extraction.begin()
                try:
                    item = next(iterator)
                except StopIteration:
                    return
                finally:
                    extraction.end(token)
                yield item

        def flush():
            inject_load_failure = scenario == "DATABASE_FAILURE_SIMULATION" and not load_failure_injected["done"]
            if not run_batch and not inject_load_failure:
                return
            try:
                load_batch(conn, run_batch, job_batch, step_batch, simulate_failure=inject_load_failure)
                load_failure_injected["done"] = load_failure_injected["done"] or inject_load_failure
                loading.records_written += len(run_batch) + len(job_batch) + len(step_batch)
                stage_stats["load"]["written"] += len(run_batch) + len(job_batch) + len(step_batch)
                totals["runs"] += len(run_batch); totals["jobs"] += len(job_batch); totals["steps"] += len(step_batch)
            except Exception:
                if scenario == "DATABASE_FAILURE_SIMULATION" and not load_failure_injected["done"]:
                    load_failure_injected["done"] = True
                loading.records_failed += len(run_batch) + len(job_batch) + len(step_batch)
                stage_stats["load"]["failed"] += len(run_batch) + len(job_batch) + len(step_batch)
                raise
            finally:
                run_batch.clear(); job_batch.clear(); step_batch.clear()
                print(f"Processed runs: {totals['runs']}", flush=True)

        pipeline_error = None
        active_task = "extract"
        failed_task = None
        try:
            if scenario == "RESOURCE_THRESHOLD_FAILURE":
                active_task = "transform"
                requested_bytes = max(64 * 1024 * 1024, config.resource_memory_limit_bytes + 1)
                token = transformation.begin()
                try:
                    if requested_bytes > config.resource_memory_limit_bytes:
                        raise MemoryError(
                            f"Resource budget exceeded: requested {requested_bytes} bytes, "
                            f"configured limit {config.resource_memory_limit_bytes} bytes"
                        )
                finally:
                    transformation.end(token)
            if scenario == "RESOURCE_STRESS":
                active_task = "transform"
                token = transformation.begin()
                value = 1
                for stress_i in range(200_000):
                    value = (value * 33 + stress_i) % 1_000_003
                sample_buffer = bytearray(8 * 1024 * 1024)
                for stress_i in range(0, len(sample_buffer), 4096):
                    sample_buffer[stress_i] = value % 256
                transformation.end(token)
                del sample_buffer
            for run in timed_runs():
                if scenario == "INVALID_DATA" and not invalid_injected["done"]:
                    invalid_injected["done"] = True
                    invalid_token = transformation.begin()
                    transformation.end(invalid_token)
                    transformation.records_read += 1
                    transformation.records_failed += 1
                    stage_stats["transform"]["read"] += 1
                    stage_stats["transform"]["failed"] += 1
                    if len(errors) < 100:
                        errors.append("Deterministic INVALID_DATA scenario record rejected during transform")
                    continue
                transformation.records_read += 1
                stage_stats["transform"]["read"] += 1
                active_task = "transform"
                if scenario == "TRANSFORMATION_FAILURE" and totals["runs"] == 0:
                    failure_token = transformation.begin()
                    try:
                        raise ValueError("Deterministic simulated transformation failure")
                    finally:
                        transformation.end(failure_token)
                transform_token = transformation.begin()
                try:
                    r, j, s = transform_run(run)
                except Exception as exc:
                    transformation.end(transform_token)
                    transformation.records_failed += 1
                    stage_stats["transform"]["failed"] += 1
                    if len(errors) < 100:
                        errors.append(str(exc))
                    continue
                transformation.end(transform_token)
                active_task = "extract"
                run_batch.append(r); job_batch.extend(j); step_batch.extend(s)
                transformation.records_written += 1 + len(j) + len(s)
                stage_stats["transform"]["written"] += 1 + len(j) + len(s)
                loading.records_read += 1 + len(j) + len(s)
                stage_stats["load"]["read"] += 1 + len(j) + len(s)
                if len(run_batch) >= config.batch_size:
                    active_task = "load"
                    load_token = loading.begin()
                    try:
                        flush()
                    finally:
                        loading.end(load_token)
                    active_task = "extract"
                if totals["runs"] and totals["runs"] % config.progress_every == 0:
                    print(f"Runs processed: {totals['runs']}; jobs: {totals['jobs']}; steps: {totals['steps']}")
            active_task = "load"
            load_token = loading.begin()
            try:
                flush()
            finally:
                loading.end(load_token)
        except Exception as exc:
            pipeline_error = exc
            failed_task = active_task
            failed_counter = {"extract": (extraction, "extract"),
                              "transform": (transformation, "transform"),
                              "load": (loading, "load")}.get(active_task)
            if failed_counter and not stage_stats[failed_counter[1]]["failed"]:
                failed_counter[0].records_failed += 1
                stage_stats[failed_counter[1]]["failed"] += 1
        finally:
            # Persist one row per stage even when an injected/operational task failure occurs.
            for pos, name in enumerate(("extract", "transform", "load"), 1):
                task = {"extract": extraction, "transform": transformation, "load": loading}[name]
                task_error = pipeline_error if pipeline_error and name == failed_task else None
                failed_position = {"extract": 1, "transform": 2, "load": 3}.get(failed_task)
                task_status = "SKIPPED" if (scenario == "RESOURCE_THRESHOLD_FAILURE" and name == "extract" or failed_position and pos > failed_position) else None
                failure_status = bool(task_error or stage_stats[name]["failed"])
                failure_origin = classify_failure_origin(scenario, name, "FAILURE" if failure_status else "SUCCESS")
                try:
                    if not task_error and not task_status and stage_stats[name]["failed"]:
                        task_error = ValueError(f"{stage_stats[name]['failed']} record(s) failed in {name}")
                    _record_task(conn, run_id, name, pos, task, stage_stats[name]["read"], stage_stats[name]["written"], stage_stats[name]["failed"], task_error, task_status, failure_origin)
                except Exception:
                    conn.rollback()
                    if not pipeline_error:
                        raise
            final = "FAILURE" if pipeline_error or errors or malformed["n"] else "SUCCESS"
            with conn.cursor() as cur:
                cur.execute("UPDATE etl_executions SET ended_at=%s,overall_status=%s,error_type=%s,error_message=%s WHERE etl_run_id=%s",
                    (_now(), final, type(pipeline_error).__name__ if pipeline_error else ("MalformedInput" if errors else None),
                     (str(pipeline_error) if pipeline_error else "; ".join(errors[:5])[:4000] if errors else None), str(run_id)))
            conn.commit()
        if pipeline_error:
            raise pipeline_error
        print(f"Runs: {totals['runs']}; jobs: {totals['jobs']}; steps: {totals['steps']}; duration: {time.perf_counter()-started_wall:.2f}s; etl_run_id: {run_id}")
        return {"etl_run_id": str(run_id), "status": "FAILURE" if errors else "SUCCESS", **totals}
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Stream GHALogs JSONL into PostgreSQL")
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()
    run_pipeline(limit=args.limit)


if __name__ == "__main__":
    main()

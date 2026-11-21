"""Pure helpers defining safe, pre-execution training features and task labels."""

PRE_EXECUTION_FEATURE_COLUMNS = (
    "source_size_bytes", "requested_record_limit", "effective_record_limit",
    "batch_size", "resource_memory_limit_bytes", "task_name", "task_position", "task_count",
    "historical_task_execution_count", "historical_task_failure_rate",
    "historical_avg_duration_sec", "historical_avg_cpu_percent",
    "historical_avg_memory_percent",
)
AUDIT_COLUMNS = ("etl_run_id", "scenario", "failure_origin", "label_source")
TARGET_COLUMN = "task_failure"
INJECTED_FAILURE_TASK = {
    "INVALID_DATA": "transform",
    "TRANSFORMATION_FAILURE": "transform",
    "DATABASE_FAILURE_SIMULATION": "load",
    "RESOURCE_THRESHOLD_FAILURE": "transform",
}


def task_failure_label(status):
    """Return a binary label only for tasks that actually ran."""
    if status == "SUCCESS":
        return 0
    if status == "FAILURE":
        return 1
    if status == "SKIPPED":
        return None
    raise ValueError(f"Unsupported task status: {status}")


def classify_failure_origin(scenario, task_name, status):
    if status != "FAILURE":
        return None
    if INJECTED_FAILURE_TASK.get(scenario) == task_name:
        return "INJECTED"
    return "OBSERVED"


def pre_execution_features(*, source_size_bytes, requested_limit, effective_limit,
                           batch_size, resource_memory_limit_bytes, task_name, task_position, task_count,
                           scenario, execution_started_at, history):
    """Build feature values using only config and executions completed before start.

    Each history item must provide task_name, execution_ended_at, task_failure,
    duration_sec, cpu_percent, and memory_percent. Current/future or still-running
    executions are excluded by the strict end-time cutoff.
    """
    prior = [row for row in history
             if row["task_name"] == task_name
             and row["execution_ended_at"] is not None
             and row["execution_ended_at"] < execution_started_at
             and row.get("task_failure") in (0, 1)]

    def average(field):
        values = [row[field] for row in prior if row.get(field) is not None]
        return sum(values) / len(values) if values else None

    return {
        "source_size_bytes": source_size_bytes,
        "requested_record_limit": requested_limit,
        "effective_record_limit": effective_limit,
        "batch_size": batch_size,
        "resource_memory_limit_bytes": resource_memory_limit_bytes,
        "task_name": task_name,
        "task_position": task_position,
        "task_count": task_count,
        "historical_task_execution_count": len(prior),
        "historical_task_failure_rate": (sum(row["task_failure"] for row in prior) / len(prior)) if prior else None,
        "historical_avg_duration_sec": average("duration_sec"),
        "historical_avg_cpu_percent": average("cpu_percent"),
        "historical_avg_memory_percent": average("memory_percent"),
        # Scenario and execution start are audit context, not model features.
        "scenario": scenario,
        "execution_started_at": execution_started_at,
    }

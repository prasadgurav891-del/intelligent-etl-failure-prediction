import gzip
import json
import os
import tempfile
import unittest
import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import patch

from etl.extract import iter_runs
from etl.config import effective_record_limit
from etl.transform import transform_run
from metadata.collector import TaskMeasurement
from metadata.features import (AUDIT_COLUMNS, PRE_EXECUTION_FEATURE_COLUMNS,
                               classify_failure_origin, pre_execution_features,
                               task_failure_label)


class EtlUnitTests(unittest.TestCase):
    def test_stream_extracts_jsonl(self):
        with tempfile.NamedTemporaryFile(suffix=".gz", delete=False) as f:
            path = f.name
        try:
            with gzip.open(path, "wt", encoding="utf-8") as f:
                f.write(json.dumps({"_id": "x"}) + "\n")
                f.write(json.dumps({"_id": "y"}) + "\n")
            self.assertEqual(["x", "y"], [r["_id"] for r in iter_runs(path, limit=2)])
        finally:
            os.unlink(path)

    def test_malformed_record_is_reported_and_skipped(self):
        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", delete=False) as f:
            f.write('{"_id":"ok"}\n{bad json}\n{"_id":"next"}\n')
            path = f.name
        failures = []
        try:
            self.assertEqual(["ok", "next"], [r["_id"] for r in iter_runs(path, on_error=lambda n, e: failures.append(n))])
            self.assertEqual([2], failures)
        finally:
            os.unlink(path)

    def test_transform_and_reproducible_ids(self):
        run = {"_id": "r1", "repository_name": "org/repo", "workflow_path": ".github/a.yml",
               "metadata": {"conclusion": "failure"}, "log_insights": [{"file": "job.log", "steps": [{"type": "shell", "duration_sec": "1.5", "error": "parser"}]}]}
        transformed = transform_run(run)
        self.assertEqual(transformed, transform_run(run))
        self.assertEqual("r1_0", transformed[1][0]["job_id"])
        self.assertEqual("r1_0_0", transformed[2][0]["step_id"])
        self.assertEqual(1.5, transformed[2][0]["duration_sec"])
        self.assertIn("parser", transformed[2][0]["parser_error_json"])

    def test_metadata_measurement(self):
        measurement = TaskMeasurement()
        measurement.elapsed = 0.01
        duration, cpu, memory, started, ended = measurement.finish()
        self.assertGreaterEqual(duration, 0.01)
        self.assertTrue(cpu is None or cpu >= 0)
        self.assertTrue(memory is None or memory >= 0)
        self.assertLessEqual(started, ended)

    def test_task_failure_label_tracks_own_task_status(self):
        self.assertEqual(1, task_failure_label("FAILURE"))
        self.assertEqual(0, task_failure_label("SUCCESS"))
        self.assertIsNone(task_failure_label("SKIPPED"))
        # A prior failed transform is not the load task's label.
        self.assertEqual(0, task_failure_label("SUCCESS"))

    def test_injected_failures_are_distinguished_from_observed(self):
        self.assertEqual("INJECTED", classify_failure_origin("INVALID_DATA", "transform", "FAILURE"))
        self.assertEqual("INJECTED", classify_failure_origin("DATABASE_FAILURE_SIMULATION", "load", "FAILURE"))
        self.assertEqual("OBSERVED", classify_failure_origin("NORMAL", "transform", "FAILURE"))
        self.assertIsNone(classify_failure_origin("INVALID_DATA", "load", "SUCCESS"))

    def test_historical_features_use_only_completed_earlier_executions(self):
        start = datetime(2026, 11, 11, tzinfo=timezone.utc)
        history = [
            {"task_name": "transform", "execution_ended_at": start - timedelta(days=1),
             "task_failure": 1, "duration_sec": 5.0, "cpu_percent": 25.0, "memory_percent": 4.0},
            {"task_name": "transform", "execution_ended_at": start,
             "task_failure": 0, "duration_sec": 9.0, "cpu_percent": 80.0, "memory_percent": 9.0},
            {"task_name": "load", "execution_ended_at": start - timedelta(days=2),
             "task_failure": 0, "duration_sec": 3.0, "cpu_percent": 10.0, "memory_percent": 2.0},
            {"task_name": "transform", "execution_ended_at": None,
             "task_failure": 1, "duration_sec": 20.0, "cpu_percent": 90.0, "memory_percent": 20.0},
        ]
        features = pre_execution_features(source_size_bytes=1234, requested_limit=100,
            effective_limit=100, batch_size=10, resource_memory_limit_bytes=1024,
            task_name="transform", task_position=2,
            task_count=3, scenario="NORMAL", execution_started_at=start, history=history)
        self.assertEqual(1, features["historical_task_execution_count"])
        self.assertEqual(1.0, features["historical_task_failure_rate"])
        self.assertEqual(5.0, features["historical_avg_duration_sec"])
        self.assertEqual(25.0, features["historical_avg_cpu_percent"])
        self.assertEqual(set(PRE_EXECUTION_FEATURE_COLUMNS), set(PRE_EXECUTION_FEATURE_COLUMNS) & set(features))
        self.assertNotIn("previous_task_status", PRE_EXECUTION_FEATURE_COLUMNS)
        self.assertNotIn("duration_sec", PRE_EXECUTION_FEATURE_COLUMNS)
        self.assertNotIn("output_records", PRE_EXECUTION_FEATURE_COLUMNS)
        self.assertIn("scenario", AUDIT_COLUMNS)

    def test_large_data_limit_is_a_single_five_times_pass(self):
        self.assertEqual(5000, effective_record_limit(1000, "LARGE_DATA"))
        self.assertEqual(1000, effective_record_limit(1000, "NORMAL"))
        self.assertIsNone(effective_record_limit(None, "LARGE_DATA"))

    def test_training_view_has_historical_cutoff_and_no_post_run_leakage(self):
        sql = Path("database/schema.sql").read_text(encoding="utf-8").lower()
        view_sql = sql.split("create view ml_training_data as", 1)[1]
        self.assertIn("prior.ended_at < e.started_at", view_sql)
        self.assertIn("case m.status when 'success' then 0 when 'failure' then 1", view_sql)
        self.assertNotIn("previous_task_status", view_sql)
        self.assertNotIn("m.output_records", view_sql)
        self.assertNotIn("m.duration_sec,", view_sql)
        self.assertNotIn("workflow_runs", view_sql)

    def test_resource_measurement_uses_process_cpu_and_rss(self):
        class FakeProcess:
            cpu = 0
            def cpu_times(self):
                self.cpu += 0.02
                return SimpleNamespace(user=self.cpu, system=0.0)
            def memory_info(self):
                return SimpleNamespace(rss=100)
        fake_psutil = SimpleNamespace(Process=FakeProcess,
                                      virtual_memory=lambda: SimpleNamespace(total=1000))
        with patch.dict(sys.modules, {"psutil": fake_psutil}):
            measurement = TaskMeasurement()
            token = measurement.begin()
            measurement.end(token)
            duration, cpu, memory, _, _ = measurement.finish()
        self.assertGreater(duration, 0)
        self.assertGreater(cpu, 0)
        self.assertEqual(10.0, memory)


if __name__ == "__main__":
    unittest.main()

"""Task-scoped process resource measurements.

CPU is process CPU time divided by active operation wall time (so a process using
two cores may exceed 100%). Memory is peak process RSS as a percentage of physical
RAM. Missing psutil/OS counters are reported explicitly instead of silently guessed.
"""
import time
from datetime import datetime, timezone


class TaskMeasurement:
    def __init__(self):
        self.elapsed = 0.0
        self.started_at = None
        self.ended_at = None
        self._cpu_seconds = 0.0
        self._memory_percent_peak = None
        self.resource_metrics_error = None
        try:
            import psutil
            self._psutil = psutil
            self._process = psutil.Process()
            self._physical_memory = psutil.virtual_memory().total
        except (ImportError, OSError, AttributeError) as exc:
            self._psutil = self._process = None
            self._physical_memory = None
            self.resource_metrics_error = f"{type(exc).__name__}: {exc}"

    def begin(self):
        """Start sampling one bounded operation belonging to this task."""
        wall = time.perf_counter()
        if self.started_at is None:
            self.started_at = datetime.now(timezone.utc)
        if self._process is None:
            return wall, None, None
        try:
            cpu = self._process.cpu_times()
            return wall, cpu.user + cpu.system, None
        except (OSError, AttributeError) as exc:
            self.resource_metrics_error = f"{type(exc).__name__}: {exc}"
            return wall, None, None

    def end(self, token):
        """Finish a sampled operation and accumulate task-scoped measurements."""
        wall_started, cpu_started, _ = token
        wall_ended = time.perf_counter()
        self.ended_at = datetime.now(timezone.utc)
        elapsed = max(0.0, wall_ended - wall_started)
        self.elapsed += elapsed
        if self._process is None:
            return
        try:
            cpu = self._process.cpu_times()
            if cpu_started is not None and elapsed > 0:
                self._cpu_seconds += max(0.0, cpu.user + cpu.system - cpu_started)
            rss = self._process.memory_info().rss
            if self._physical_memory:
                pct = 100.0 * rss / self._physical_memory
                self._memory_percent_peak = max(self._memory_percent_peak or 0.0, pct)
        except (OSError, AttributeError) as exc:
            self.resource_metrics_error = f"{type(exc).__name__}: {exc}"

    def finish(self):
        duration = max(0.0, self.elapsed)
        cpu = (100.0 * self._cpu_seconds / duration
               if self._process is not None and duration > 0 else None)
        memory = self._memory_percent_peak
        if self.started_at is None:
            self.started_at = self.ended_at = datetime.now(timezone.utc)
        return duration, cpu, memory, self.started_at, self.ended_at

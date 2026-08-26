import json
import sys
import time
import threading
from collections import deque


class Metrics:
    """
    Tracks per-request latency and running throughput, and emits
    structured (JSON-lines) log entries to stdout.

    Design choice: keep this in-memory and simple (a rolling window
    for throughput calculation) rather than wiring up Prometheus/Grafana.
    That's a deliberate scope cut — for a portfolio project, "I logged
    structured data and computed my own percentiles" is a more defensible
    interview story than a half-configured Prometheus setup you can't
    fully explain.
    """

    def __init__(self, window_seconds: int = 10):
        self._lock = threading.Lock()
        self._window_seconds = window_seconds
        # Rolling deque of request timestamps, used to compute a
        # trailing ops/sec figure without storing every request forever.
        self._recent_timestamps: deque[float] = deque()

    def _prune_old(self, now: float) -> None:
        cutoff = now - self._window_seconds
        while self._recent_timestamps and self._recent_timestamps[0] < cutoff:
            self._recent_timestamps.popleft()

    def record_request(
        self,
        *,
        method: str,
        path: str,
        key: str | None,
        node_id: str,
        role: str,          # "coordinator" or "internal"
        status_code: int,
        latency_ms: float,
        replication_ms: float | None = None,
        preference_list: list[str] | None = None,
        acked_by: list[str] | None = None,
        failed_nodes: list[str] | None = None,
    ) -> None:
        now = time.time()
        with self._lock:
            self._recent_timestamps.append(now)
            self._prune_old(now)
            current_throughput = len(self._recent_timestamps) / self._window_seconds

        log_entry = {
            "ts": now,
            "node": node_id,
            "method": method,
            "path": path,
            "key": key,
            "role": role,
            "status_code": status_code,
            "latency_ms": round(latency_ms, 3),
            "throughput_ops_sec": round(current_throughput, 2),
        }
        # Replication-specific fields only present on coordinator writes
        if replication_ms is not None:
            log_entry["replication_ms"] = round(replication_ms, 3)
        if preference_list is not None:
            log_entry["preference_list"] = preference_list
        if acked_by is not None:
            log_entry["acked_by"] = acked_by
        if failed_nodes is not None:
            log_entry["failed_nodes"] = failed_nodes

        # One JSON object per line -> trivially parseable later with
        # `[json.loads(line) for line in open("logfile")]`
        print(json.dumps(log_entry), file=sys.stdout, flush=True)


metrics = Metrics()
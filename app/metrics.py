from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from threading import Lock
from typing import Deque, Dict, Tuple


@dataclass
class MetricSnapshot:
    events_last_minute: int
    errors_last_minute: int
    avg_latency_ms: float


class IngestionMetrics:
    def __init__(self, window_seconds: int = 60) -> None:
        self.window = timedelta(seconds=window_seconds)
        self.events: Deque[datetime] = deque()
        self.errors: Deque[datetime] = deque()
        self.latencies: Deque[Tuple[datetime, float]] = deque()
        self.lock = Lock()

    def _trim(self, now: datetime) -> None:
        threshold = now - self.window
        while self.events and self.events[0] < threshold:
            self.events.popleft()
        while self.errors and self.errors[0] < threshold:
            self.errors.popleft()
        while self.latencies and self.latencies[0][0] < threshold:
            self.latencies.popleft()

    def record_event(self, latency_ms: float) -> None:
        now = datetime.now(timezone.utc)
        with self.lock:
            self.events.append(now)
            self.latencies.append((now, latency_ms))
            self._trim(now)

    def record_error(self) -> None:
        now = datetime.now(timezone.utc)
        with self.lock:
            self.errors.append(now)
            self._trim(now)

    def snapshot(self) -> MetricSnapshot:
        with self.lock:
            now = datetime.now(timezone.utc)
            self._trim(now)
            events_count = len(self.events)
            errors_count = len(self.errors)
            avg_latency = (
                sum(lat for _, lat in self.latencies) / len(self.latencies)
                if self.latencies
                else 0.0
            )
        return MetricSnapshot(events_count, errors_count, avg_latency)

    def as_dict(self) -> Dict[str, float]:
        snap = self.snapshot()
        return {
            "events_per_minute": snap.events_last_minute,
            "errors_per_minute": snap.errors_last_minute,
            "avg_latency_ms": round(snap.avg_latency_ms, 2),
        }


__all__ = ["IngestionMetrics", "MetricSnapshot"]

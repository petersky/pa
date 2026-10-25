"""Bounded numeric evidence for a test deadline; no GC policy changes."""
from collections import deque
from functools import wraps
import gc
import json
import os
from pathlib import Path
import platform
import re
import threading
import time
from uuid import uuid4


class DeadlineEvidence:
    def __init__(self, artifact_name, *, capacity=32, minimum_pause_ms=1):
        if not re.fullmatch(r"[a-z0-9-]{1,64}", artifact_name):
            raise ValueError("Use a fixed artifact name, never request data")
        if capacity < 1 or minimum_pause_ms < 0:
            raise ValueError("Invalid evidence bounds")
        self.artifact_name = artifact_name
        self.capacity = capacity
        self.minimum_pause_ms = minimum_pause_ms
        self.gc_events = deque(maxlen=capacity)
        self.worker_events = deque(maxlen=capacity)
        self.gc_count = self.worker_count = self.worker_started_count = 0
        self.pending_gc = {}
        self.started = self.finished = self.loop_cpu_started = self.loop_cpu_ms = None
        self.loop_thread = threading.get_ident()
        self.gc_thresholds = gc.get_threshold()

    def __enter__(self):
        gc.callbacks.append(self._gc)
        return self

    def __exit__(self, kind, error, traceback):
        gc.callbacks.remove(self._gc)
        try:
            self.publish()
        except Exception:
            if error is None:
                raise
            error.add_note("Deadline evidence publication failed; original failure retained.")

    def _gc(self, phase, info):
        key = (threading.get_ident(), info["generation"])
        if phase == "start":
            self.pending_gc[key] = (time.monotonic(), time.thread_time())
        elif key in self.pending_gc:
            started, cpu = self.pending_gc.pop(key)
            finished = time.monotonic()
            wall_ms = (finished - started) * 1000
            if wall_ms >= self.minimum_pause_ms:
                self.gc_count += 1
                self.gc_events.append(dict(
                    started=started, finished=finished, wall_ms=wall_ms,
                    thread_cpu_ms=(time.thread_time() - cpu) * 1000,
                    thread_id=key[0], generation=key[1],
                    collected=info["collected"], uncollectable=info["uncollectable"],
                ))

    def begin(self, started):
        self.started = started
        self.loop_cpu_started = time.thread_time()

    def finish(self, finished):
        self.finished = finished
        self.loop_cpu_ms = (time.thread_time() - self.loop_cpu_started) * 1000

    def wrap(self, call):
        @wraps(call)
        def observed(*args, **kwargs):
            started, cpu = time.monotonic(), time.thread_time()
            event = dict(started=started, finished=None, wall_ms=None,
                         thread_cpu_ms=None, thread_id=threading.get_ident())
            self.worker_started_count += 1
            self.worker_events.append(event)
            try:
                return call(*args, **kwargs)
            finally:
                finished = time.monotonic()
                event.update(finished=finished, wall_ms=(finished - started) * 1000,
                             thread_cpu_ms=(time.thread_time() - cpu) * 1000)
                self.worker_count += 1
        return observed

    def summary(self):
        def relative(event):
            return {**{k: v for k, v in event.items() if k not in {"started", "finished"}},
                    "start_relative_ms": (event["started"] - self.started) * 1000,
                    "end_relative_ms": (event["finished"] - self.started) * 1000 if event["finished"] is not None else None}

        window = self.started is not None and self.finished is not None
        overlaps = [relative(e) for e in tuple(self.gc_events) if window
                    and e["started"] < self.finished and e["finished"] > self.started]
        sha = os.environ.get("GITHUB_SHA", "")
        return dict(
            contract="pa.test-deadline-evidence/v1", python=platform.python_version(),
            platform=platform.system(), source_sha=sha if re.fullmatch(r"[0-9a-f]{40}", sha) else None,
            gc_thresholds=self.gc_thresholds, loop_thread_id=self.loop_thread,
            request_wall_ms=(self.finished - self.started) * 1000 if window else None,
            request_loop_thread_cpu_ms=self.loop_cpu_ms,
            gc_events_total=self.gc_count, gc_events_retained=len(self.gc_events),
            gc_events_dropped=max(0, self.gc_count - self.capacity),
            request_gc_overlaps=overlaps,
            worker_calls_started=self.worker_started_count, worker_calls_total=self.worker_count,
            worker_calls_incomplete=self.worker_started_count - self.worker_count,
            worker_events_dropped=max(0, self.worker_started_count - self.capacity),
            worker_intervals=[relative(e) for e in tuple(self.worker_events)] if window else [],
            scope="Numeric test-only observations include observer cost. GC intervals can overlap worker/loop time. Wall minus CPU is unclassified; no physical-holder or disk attribution. No arguments, results, exception messages, frames, SQL or request data retained. No deadline or GC setting changed.",
        )

    def describe(self):
        return "deadline_evidence=" + json.dumps(self.summary(), separators=(",", ":"))

    def publish(self):
        directory = os.environ.get("PA_TEST_TIMING_ARTIFACT_DIR")
        if not directory:
            return
        root = Path(directory)
        root.mkdir(parents=True, exist_ok=True)
        path = root / f"{self.artifact_name}-{uuid4().hex}.json"
        with path.open("x") as stream:
            json.dump(self.summary(), stream, indent=2)
            stream.write("\n")

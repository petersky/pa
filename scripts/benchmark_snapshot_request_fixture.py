"""Compare archived and explicit-context snapshot test fixtures without PA storage."""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import math
import os
from pathlib import Path
import subprocess
import tempfile
import time
import unittest
from unittest.mock import MagicMock, patch


def distribution(values):
    values = sorted(values)
    return {"samples": len(values), **{
        name: round(values[max(0, math.ceil(len(values) * p) - 1)], 6)
        for name, p in (("p50", .5), ("p95", .95), ("p99", .99), ("max", 1))
    }}


def measure(revision):
    path = "tests/test_agent_snapshot_responsiveness.py"
    baseline = subprocess.check_output(["git", "show", f"{revision}:{path}"], text=True)
    current = Path(path).read_text()
    with tempfile.TemporaryDirectory(prefix="snapshot-fixture-", dir=".dev") as directory:
        root = Path(directory).resolve()
        os.environ["PA_DATA_DIR"] = str(root / "unused-data")
        os.environ["PA_WORKSPACE_ROOT"] = str(root / "unused-workspaces")
        from pa.repository.workspace import WorkspaceManager
        from pa.domain.projection import CardProjection
        from pa.execution.dispatch import DispatchStore
        from pa.sync.object_store import ObjectStore
        from pa.modules import agent_chat

        def deny(*args, **kwargs):
            raise AssertionError("No PA workspace/store construction in snapshot fixture comparison")

        namespaces = {}
        for kind, source in (("baseline", baseline), ("explicit_context", current)):
            namespace = {"__name__": "snapshot_fixture_" + kind, "__file__": path}
            exec(compile(source, revision + ":" + path if kind == "baseline" else path, "exec"), namespace)
            namespaces[kind] = namespace

        def run(kind, *, count_children=False):
            observed = {"mock_child_constructions": 0}
            original_request = namespaces[kind]["get_session_snapshot"]
            original_worker = agent_chat._session_snapshot
            original_child = MagicMock._get_child_mock

            async def request(*args, **kwargs):
                started = time.perf_counter()
                try:
                    return await original_request(*args, **kwargs)
                finally:
                    observed["request_wall_ms"] = (time.perf_counter() - started) * 1000

            def worker(*args, **kwargs):
                started, cpu = time.perf_counter(), time.thread_time()
                try:
                    return original_worker(*args, **kwargs)
                finally:
                    observed["worker_wall_ms"] = (time.perf_counter() - started) * 1000
                    observed["worker_thread_cpu_ms"] = (time.thread_time() - cpu) * 1000

            def child(self, *args, **kwargs):
                observed["mock_child_constructions"] += 1
                return original_child(self, *args, **kwargs)

            namespaces[kind]["get_session_snapshot"] = request
            try:
                with patch.object(agent_chat, "_session_snapshot", new=worker):
                    capture = io.StringIO()
                    test = namespaces[kind]["AgentSnapshotResponsivenessTests"](
                        "test_live_metadata_does_not_flush_or_read_large_transcript"
                    )
                    started = time.perf_counter()
                    if count_children:
                        with patch.object(MagicMock, "_get_child_mock", new=child):
                            result = unittest.TextTestRunner(stream=capture).run(test)
                    else:
                        result = unittest.TextTestRunner(stream=capture).run(test)
                    observed["case_wall_ms"] = (time.perf_counter() - started) * 1000
                    observed["passed"] = result.wasSuccessful()
                    if not observed["passed"]:
                        observed["failure"] = capture.getvalue()
            finally:
                namespaces[kind]["get_session_snapshot"] = original_request
            return observed

        with (
            patch.object(WorkspaceManager, "__init__", new=deny),
            patch.object(CardProjection, "__init__", new=deny),
            patch.object(DispatchStore, "__init__", new=deny),
            patch.object(ObjectStore, "__init__", new=deny),
        ):
            counts = {kind: run(kind, count_children=True) for kind in namespaces}
            pairs = []
            for index in range(10):
                pair = {"order": ["baseline", "explicit_context"] if index % 2 == 0 else ["explicit_context", "baseline"]}
                for kind in pair["order"]:
                    pair[kind] = run(kind)
                pairs.append(pair)
    metrics = ("request_wall_ms", "worker_wall_ms", "worker_thread_cpu_ms", "case_wall_ms")
    return {
        "contract": "pa.snapshot-fixture-comparison/v1", "baseline_revision": revision,
        "baseline_test_sha256": hashlib.sha256(baseline.encode()).hexdigest(),
        "source_test_sha256": hashlib.sha256(current.encode()).hexdigest(),
        "mock_counts_outside_timing_comparison": counts, "pairs": pairs,
        "latency_ms": {kind: {metric: distribution([p[kind][metric] for p in pairs]) for metric in metrics} for kind in namespaces},
        "scope": "10 alternating warm-host pairs; each unittest retains its own cold default executor and unchanged200ms wait/assertion. Child construction counted separately. No PA store/workspace construction, Kernel, service workload, activation or performance acceptance. Uncontrolled host; success does not resolve CI scheduling/CPU cause.",
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-revision", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    result = measure(args.baseline_revision)
    with args.output.open("x") as output:
        json.dump(result, output, indent=2)
        output.write("\n")

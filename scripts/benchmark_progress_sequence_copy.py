"""Compare durable scalar sequence allocation against an archived implementation."""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
import tempfile
import time
from datetime import UTC, datetime
from types import MethodType

from pa.core.operation_budget import measured_lock
from pa.execution.dispatch import DispatchRecord, DispatchStore
from pa.execution.progress import DispatchProgressEventV1, ProgressPhase


def distribution(values):
    values = sorted(values)
    return {"samples": len(values), **{key: round(values[max(0, math.ceil(len(values)*fraction)-1)], 6)
            for key, fraction in (("p50", .5), ("p95", .95), ("p99", .99), ("max", 1))}}


def measure(revision):
    path = "src/pa/execution/dispatch.py"
    archived = subprocess.check_output(["git", "show", f"{revision}:{path}"], text=True)
    cls = next(n for n in ast.parse(archived).body if isinstance(n, ast.ClassDef) and n.name == "DispatchStore")
    current = next(n for n in ast.parse(Path(path).read_text()).body if isinstance(n, ast.ClassDef) and n.name == "DispatchStore")
    for name in ("_save", "_publish_records_locked", "_core_payload", "_transaction", "_persist_record_conn", "_snapshot"):
        before = next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == name)
        after = next(n for n in current.body if isinstance(n, ast.FunctionDef) and n.name == name)
        assert ast.dump(before) == ast.dump(after), f"Shared baseline dependency changed: {name}"
    method = next(n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == "allocate_progress_sequence")
    namespace = {"measured_lock": measured_lock, "datetime": datetime, "UTC": UTC}
    exec(compile(ast.Module(body=[method], type_ignores=[]), f"{revision}:{path}", "exec"), namespace)
    started = datetime.now(UTC).isoformat()
    scenarios = {}
    with tempfile.TemporaryDirectory(prefix="progress-sequence-copy-", dir=".dev") as directory:
        root = Path(directory).resolve()
        os.environ["PA_DATA_DIR"] = str(root / "unused-data")
        os.environ["PA_WORKSPACE_ROOT"] = str(root / "unused-workspaces")
        for count in (0, 1000):
            record = DispatchRecord(mutation_id="fixture", authority_instance_id="fixture", authority_url="https://fixture.invalid", target_instance_id="fixture",
                materialization_plan={"steps": [{"proof": [index, "x" * 256]} for index in range(count)]})
            record.progress_events = [DispatchProgressEventV1(card_id="fixture", dispatch_id=record.dispatch_id, acp_session_id="fixture",
                originating_instance_id="fixture", authority_instance_id="fixture", sequence=index+1, idempotency_key=f"fixture-{index}",
                phase=ProgressPhase.TURN_ENDED, summary="Retained fixture " + "x" * 256) for index in range(count)]
            record.progress_next_sequence = count + 1
            stores = {}
            for kind in ("baseline", "source"):
                location = root / str(count) / kind
                location.mkdir(parents=True)
                (location / "dispatch_mutations.json").write_text(json.dumps({record.dispatch_id: record.model_dump(mode="json")}))
                stores[kind] = DispatchStore(location)
                assert stores[kind].db_path.resolve().is_relative_to(root)
            stores["baseline"].allocate_progress_sequence = MethodType(namespace["allocate_progress_sequence"], stores["baseline"])
            pairs = []
            try:
                for iteration in range(20):
                    pair = {"order": ["baseline", "source"] if iteration % 2 == 0 else ["source", "baseline"]}
                    for kind in pair["order"]:
                        wall, cpu = time.perf_counter(), time.thread_time()
                        sequence = stores[kind].allocate_progress_sequence(record.dispatch_id)
                        pair[kind] = {"wall_ms": (time.perf_counter()-wall)*1000, "thread_cpu_ms": (time.thread_time()-cpu)*1000}
                        assert sequence == count + iteration + 1
                    pairs.append(pair)
                retained = {}
                for kind, store in stores.items():
                    observed = store.get(record.dispatch_id)
                    assert observed.progress_events == record.progress_events
                    assert observed.materialization_plan == record.materialization_plan
                    assert observed.progress_next_sequence == count + 21
                    retained[kind] = {"events": len(observed.progress_events), "allocation_commits": 20, "core_bytes": len(store._core_payload(observed).encode())}
                scenarios[str(count)] = {"retained_events": count, "retained_plan_steps": count, "pairs": pairs, "verified": retained,
                    "latency_ms": {kind: {metric: distribution([p[kind][metric] for p in pairs]) for metric in ("wall_ms", "thread_cpu_ms")} for kind in stores}}
            finally:
                for store in stores.values():
                    store.close()
    return {"contract": "pa.progress-sequence-copy-comparison/v1", "started_at": started, "finished_at": datetime.now(UTC).isoformat(),
        "baseline_revision": revision, "baseline_module_sha256": hashlib.sha256(archived.encode()).hexdigest(),
        "source_module_sha256": hashlib.sha256(Path(path).read_bytes()).hexdigest(), "scenarios": scenarios,
        "percentile_method": "Nearest rank, ceil(fraction * sample count) - 1; 20 samples per implementation per scenario.",
        "scope": "20 alternating warm pairs per case; real disposable SQLite commits and unchanged publication deep copy. Uncontrolled host. Core bytes are encoded payload bytes, not physical I/O. No Kernel, production state, service workload, backup action, activation or deployed acceptance."}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline-revision", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    with args.output.open("x") as output:
        json.dump(measure(args.baseline_revision), output, indent=2)
        output.write("\n")

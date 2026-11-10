"""Private fixed-size leaf comparison; aggregate stdout, no retained fixtures.

This isolates the existing indexed reader, excluding SQL hydration, durable
seeding and governance/network actions. It does not reproduce historical tails.
"""
from datetime import UTC, datetime, timedelta
import hashlib
import json
import math
from pathlib import Path
import shutil
import tempfile
import time
from unittest.mock import patch

from pa.core.operation_budget import measured_lock
from pa.execution.dispatch import DispatchRecord, DispatchStore, GoalDispatchProvenance


def original_selection(store, authority, *, limit=100):
    """Pre-repair algorithm, retained only as a bounded comparison oracle."""
    store._require_readable()
    store._yield_to_index_writer()
    with measured_lock(store._index_lock):
        records = [store._snapshot(row) for row in store._goal_lifecycle_records.values()
                   if row.authority_instance_id == authority and row.goal_provenance is not None
                   and row.goal_provenance.authority_instance_id == authority]
    records.sort(key=lambda row: row.updated_at)
    return records[:max(1, min(limit, 1000))]


def quantiles(samples):
    ordered = sorted(samples)
    return {"samples": len(samples), **{name: round(ordered[math.ceil(len(ordered)*fraction)-1], 3)
            for name, fraction in (("p50_ms", .5), ("p95_ms", .95), ("p99_ms", .99), ("max_ms", 1))}}


def benchmark():
    modes = {"before": [], "after": []}
    copies = {}
    root_path = None
    with tempfile.TemporaryDirectory(prefix="pa-goal-lifecycle-") as temporary:
        root = Path(temporary)
        root_path = root
        if shutil.disk_usage(root).free < 4*1024**3:
            raise RuntimeError("At least4GiB free required before seeding")
        store = DispatchStore(root)
        try:
            records = []
            for index in reversed(range(1100)):
                authority = "local" if index < 1000 else "other"
                records.append(DispatchRecord(
                    dispatch_id=f"d-{index}", mutation_id=f"m-{index}",
                    authority_instance_id=authority, authority_url="http://fixture",
                    target_instance_id="remote", state="running",
                    updated_at=datetime(2026,1,1,tzinfo=UTC)+timedelta(seconds=index//2),
                    goal_provenance=GoalDispatchProvenance(
                        goal_id="goal", goal_version=1, policy_revision=1,
                        authority_instance_id=authority, fencing_token=1,
                        action_reservation_id=f"reservation-{index}", actor_principal="user:fixture"),
                    request_payload={"proof": [{"sequence": node, "values": [index,node,"x"*128]}
                                               for node in range(256)]}))
            # Publication only: no fake production workload, persistence timing
            # or SQLite/write-amplification claim from this index-only fixture.
            with store._lock:
                store._publish_records_locked(records)
            expected_ids = [f"d-{i}" for pair in range(50) for i in (2*pair+1,2*pair)]
            readers = {"before": lambda: original_selection(store,"local"),
                       "after": lambda: store.pending_goal_lifecycle("local")}
            for pair in range(5):
                for mode in (("before","after") if pair%2==0 else ("after","before")):
                    started = time.perf_counter()
                    result = readers[mode]()
                    elapsed = (time.perf_counter()-started)*1000
                    assert [row.dispatch_id for row in result] == expected_ids
                    modes[mode].append(elapsed)
                    del result
            # Separate untimed detachment/count oracle, not mixed into timers.
            expected = readers["before"]()
            for mode, read in readers.items():
                with patch.object(store,"_snapshot",wraps=store._snapshot) as copied:
                    result = read()
                copies[mode] = copied.call_count
                assert result == expected
                result[0].request_payload["proof"][0]["values"].append("mutated")
                assert store.pending_goal_lifecycle("local") == expected
            source = Path(__file__).resolve().parents[1] / "src/pa/execution/dispatch.py"
            report = {"contract":"pa.goal-lifecycle-selection-benchmark/v1",
                      "fixture":{"pending_local":1000,"pending_other":100,"batch":100,
                                 "nested_proof_nodes_per_record":256,"alternating_pairs":5},
                      "latency":{key:quantiles(value) for key,value in modes.items()},
                      "full_record_copies_per_call":copies,
                      "ordering_full_fields_authority_detachment":"pass",
                      "scope":"private index leaf; no historical-cause, lock-contention, SQL, physical-IO, deployed or whole-PA acceptance",
                      "source_sha256":hashlib.sha256(source.read_bytes()).hexdigest()}
        finally:
            store.close()
    report["fixture_removed"] = not root_path.exists()
    return report


if __name__ == "__main__":
    print(json.dumps(benchmark(),sort_keys=True))

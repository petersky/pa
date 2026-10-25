"""Compare lifecycle watch inputs in private stores; --output must be new."""
import argparse
from datetime import UTC, datetime
import hashlib
import json
import math
import os
from pathlib import Path
import tempfile
import time
from unittest.mock import patch


def distribution(values):
    values = sorted(values)
    return dict(samples=len(values), **{
        name: values[math.ceil(q * len(values)) - 1]
        for name, q in [("p50", .5), ("p95", .95), ("p99", .99), ("max", 1)]
    })


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    assert not args.output.exists(), "Preserve earlier runs; choose a new path"
    out = dict(contract="pa.lifecycle-watch-links-benchmark/v1",
               started_at=datetime.now(UTC).isoformat(), samples=[], complete=False)
    clock = time.time() - time.monotonic()
    with tempfile.TemporaryDirectory(prefix="pa-lifecycle-watch-links-") as tmp:
        root = Path(tmp)
        os.environ.update(PA_DATA_DIR=str(root / "data"),
                          PA_WORKSPACE_ROOT=str(root / "workspaces"), PA_AGENT_ENABLED="false")
        from pa.pr_supervisor import store as module
        from pa.pr_supervisor.models import PRWatch, PRWatchStatus
        store = module.PRSupervisorStore(root / "fixture.db")
        out.update(module_path=module.__file__,
                   module_sha256=hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest())
        for i in range(100):
            store.upsert_watch(PRWatch(
                id=f"w-{i}", repository="fixture/repo", pr_number=i + 1,
                pr_url="https://example.invalid/pr", realm_id="other" if i % 2 else "default",
                originating_session_id=f"session-{i % 10}", card_id=f"card-{i % 7}",
                status=list(PRWatchStatus)[i % len(PRWatchStatus)],
                retired_at=datetime.now(UTC) if i % 3 else None,
                state={"proof": "x" * 65536},
            ))
        expected = sorted((w.originating_session_id, w.card_id, w.status)
                          for w in store.list_watches(include_retired=True))
        variants = dict(full=lambda: store.list_watches(include_retired=True),
                        links=store.lifecycle_watch_links)
        original = store._row_to_watch
        stats = {}

        def counted(row):
            stats["hydrated_models"] += 1
            stats["materialized_json_bytes"] += sum(
                len(row[k].encode()) for k in ("state_json", "policy_json", "required_capabilities_json")
            )
            return original(row)

        try:
            with patch.object(store, "_row_to_watch", counted):
                for pair in range(30):
                    for variant in (["full", "links"] if pair % 2 == 0 else ["links", "full"]):
                        stats = dict(hydrated_models=0, materialized_json_bytes=0)
                        start, cpu = time.perf_counter_ns(), time.thread_time_ns()
                        watches = variants[variant]()
                        wall_ms = (time.perf_counter_ns() - start) / 1e6
                        thread_cpu_ms = (time.thread_time_ns() - cpu) / 1e6
                        actual = sorted((w.originating_session_id, w.card_id, w.status) for w in watches)
                        assert actual == expected
                        if variant == "links":
                            assert stats == dict(hydrated_models=0, materialized_json_bytes=0)
                        out["samples"].append(dict(pair=pair, variant=variant, wall_ms=wall_ms,
                                                   thread_cpu_ms=thread_cpu_ms, **stats))
                        del watches
            out.update(complete=True, watches=100, pairs=30, proof_bytes_each=65536,
                       decision_fields_equal=True,
                       latency_ms={v: distribution([s["wall_ms"] for s in out["samples"]
                                                   if s["variant"] == v]) for v in variants})
        finally:
            out.update(finished_at=datetime.now(UTC).isoformat(), clock_offset_before=clock,
                       clock_offset_after=time.time() - time.monotonic(),
                       scope="Thirty alternating pairs in one private synthetic store. Every status, realm and "
                       "retired/terminal link is retained. Timers cover only the read, excluding comparison and "
                       "observer persistence. Materialized JSON bytes are application copy work, not physical disk "
                       "traffic. No provider closure, production workload, cadence/threshold change or whole-path "
                       "speedup/acceptance is inferred. Both roots are private before PA imports.")
            args.output.write_text(json.dumps(out, indent=2) + "\n")
    print(json.dumps({k: v for k, v in out.items() if k != "samples"}))


if __name__ == "__main__":
    main()

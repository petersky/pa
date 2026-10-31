"""Compare installed or source lifecycle decisions using private dispatch links."""
import argparse
import asyncio
from datetime import UTC, datetime
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time
from types import SimpleNamespace


async def run(output):
    from pa.domain.models import AgentSession
    from pa.execution.dispatch import DispatchRecord
    from pa.instance import session_lifecycle as module

    now = datetime.now(UTC)
    sessions = [AgentSession(id=f"s-{i}", dispatch_id=f"d-{i}", agent_name="fixture",
                             purpose="chat") for i in range(1000)]
    records = [DispatchRecord(dispatch_id=f"d-{i}", session_id=f"s-{i}", mutation_id=f"m-{i}",
        authority_instance_id="private", authority_url="https://example.invalid",
        target_instance_id="private", card_id=f"c-{i}", state="running" if i % 2 else "completed",
        acknowledged_at=now, completion_delivery_class="acknowledged",
        reconciliation_state="not_required") for i in range(1000)]
    records.extend([records[0].model_copy(), records[1].model_copy(update={"session_id": None})])
    index = module._DispatchLinkIndex(records)
    for session in sessions:
        expected = [r for r in records if r.session_id == session.id
                    or (session.dispatch_id and r.dispatch_id == session.dispatch_id)]
        assert [id(r) for r in index.for_session(session)] == [id(r) for r in expected]
    # This sparse-link fixture excludes receipt SQL, while preserving the
    # manager interface used by the optional continuation capability check.
    policy = module.SessionLifecyclePolicy(SimpleNamespace(get=lambda _: None, store=SimpleNamespace(),
        settings=SimpleNamespace(agent_session_idle_retention_hours=24)), {})
    expected = [("retained", "dispatch_active" if i % 2 else "conversation_available")
                for i in range(1000)]
    samples = []
    try:
        for pair in range(5):
            for variant in (["scan", "index"] if pair % 2 == 0 else ["index", "scan"]):
                wall, cpu = time.perf_counter_ns(), time.thread_time_ns()
                dispatches = module._DispatchLinkIndex(records) if variant == "index" else records
                actual = [await policy._decision(s, sessions=sessions, dispatches=dispatches,
                    watches=[], leases=[], now=now) for s in sessions]
                samples.append(dict(pair=pair, variant=variant,
                    wall_ms=(time.perf_counter_ns()-wall)/1e6,
                    thread_cpu_ms=(time.thread_time_ns()-cpu)/1e6))
                assert actual == expected
    finally:
        await policy.close()
    value = dict(contract="pa.lifecycle-dispatch-matching-benchmark/v1",
        observed_at=datetime.now(UTC).isoformat(), sessions=1000, dispatch_records=len(records),
        pairs=5, samples=samples, decisions_equal=True, ordered_occurrences_equal=True,
        source_module=module.__file__, source_sha256=hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest(),
        scope="Five alternating private sparse-link sweeps of the actual decision method. Index construction "
              "and lookup at the original guard point are included. Every decision and ordered OR occurrence "
              "matches, including both-key and equal distinct rows. No SQL, durable closure, real provider, "
              "service state or whole-sweep/historical cause/acceptance claim. Imported path/hash distinguishes "
              "source from installed code. Host load and normal GC remain uncontrolled. "
              "The fixture has no receipt-read capability; native pending-continuation "
              "protection is covered separately by lifecycle contracts.")
    output.write_text(json.dumps(value, indent=2)+"\n")
    print(json.dumps(value))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    assert not args.output.exists(), "Preserve earlier evidence; choose a new path"
    with tempfile.TemporaryDirectory(prefix="pa-dispatch-matching-") as tmp:
        os.environ.update(PA_DATA_DIR=str(Path(tmp)/"data"),
                          PA_WORKSPACE_ROOT=str(Path(tmp)/"workspaces"), PA_AGENT_ENABLED="false")
        asyncio.run(run(args.output))

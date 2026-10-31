"""Compare lifecycle lease matching with private, already copied lease models."""
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
    from pa.instance import session_lifecycle as module

    now = datetime.now(UTC)
    sessions = [AgentSession(id=f"s-{i}", agent_name="fixture", purpose="chat")
                for i in range(1000)]
    # Cleaned leases remain in the copied inventory and still participate in
    # matching. Workspace probes are excluded from this sparse-link comparison.
    leases = [SimpleNamespace(session_id=f"s-{i}", state="cleaned") for i in range(1000)]
    leases.extend([SimpleNamespace(**vars(leases[0])), SimpleNamespace(session_id=None, state="cleaned")])
    index = module._LeaseLinkIndex(leases)
    for session in sessions:
        expected = [r for r in leases if r.session_id == session.id]
        assert [id(r) for r in index.for_session(session)] == [id(r) for r in expected]
    # Sparse matching excludes native receipt reads; retain the manager shape
    # expected by the optional pending-continuation capability check.
    policy = module.SessionLifecyclePolicy(SimpleNamespace(get=lambda _: None, store=SimpleNamespace(),
        settings=SimpleNamespace(agent_session_idle_retention_hours=24)), {})
    expected = [("retained", "conversation_available")] * len(sessions)
    samples = []
    try:
        for pair in range(5):
            for variant in (["scan", "index"] if pair % 2 == 0 else ["index", "scan"]):
                wall, cpu = time.perf_counter_ns(), time.thread_time_ns()
                candidates = module._LeaseLinkIndex(leases) if variant == "index" else leases
                actual = [await policy._decision(s, sessions=sessions, dispatches=[],
                    watches=[], leases=candidates, now=now) for s in sessions]
                samples.append(dict(pair=pair, variant=variant,
                    wall_ms=(time.perf_counter_ns()-wall)/1e6,
                    thread_cpu_ms=(time.thread_time_ns()-cpu)/1e6))
                assert actual == expected
    finally:
        await policy.close()
    value = dict(contract="pa.lifecycle-lease-matching-benchmark/v1",
        observed_at=datetime.now(UTC).isoformat(), sessions=len(sessions), lease_records=len(leases),
        pairs=5, samples=samples, decisions_equal=True, ordered_occurrences_equal=True,
        source_module=module.__file__, source_sha256=hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest(),
        scope="Five alternating private sparse-link sweeps of the actual decision method. Index construction "
              "and lookup at the original guard point are included. Cleaned leases, ordered duplicates and "
              "every decision match. SQL enumeration, fresh model copying, git status probes, durable closure, "
              "real provider and service state are excluded. Not whole-sweep or historical cause/acceptance "
              "evidence. Imported path/hash identifies source versus installed code. Host load and normal GC "
              "remain uncontrolled. The fixture has no receipt-read capability; native pending-"
              "continuation protection is covered separately by the lifecycle contracts.")
    output.write_text(json.dumps(value, indent=2)+"\n")
    print(json.dumps(value))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    assert not args.output.exists(), "Preserve earlier evidence; choose a new path"
    with tempfile.TemporaryDirectory(prefix="pa-lease-matching-") as tmp:
        os.environ.update(PA_DATA_DIR=str(Path(tmp)/"data"),
                          PA_WORKSPACE_ROOT=str(Path(tmp)/"workspaces"), PA_AGENT_ENABLED="false")
        asyncio.run(run(args.output))

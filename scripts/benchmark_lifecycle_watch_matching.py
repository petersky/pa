import asyncio, hashlib, json, os, tempfile, time
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import NamedTuple

import argparse
parser = argparse.ArgumentParser(description='Compare actual lifecycle decisions in private roots, including fresh index construction.')
parser.add_argument('--output', type=Path, required=True)
out = parser.parse_args().output
assert not out.exists()

class Link(NamedTuple):
    originating_session_id: str | None
    card_id: str | None
    status: object

async def run():
    from pa.instance import session_lifecycle as module
    Index = module._WatchLinkIndex
    from pa.domain.models import AgentSession
    from pa.pr_supervisor.models import PRWatchStatus
    manager = SimpleNamespace(get=lambda _: None,
        settings=SimpleNamespace(agent_session_idle_retention_hours=24))
    policy = module.SessionLifecyclePolicy(manager, {})
    sessions = [AgentSession(id=f's-{i}', agent_name='fixture', purpose='chat', card_id=f'c-{i}') for i in range(1000)]
    watches = [Link(s.id, s.card_id, PRWatchStatus.ACTIVE if i % 2 else PRWatchStatus.MERGED) for i, s in enumerate(sessions)]
    check_index = Index(watches)
    for session in sessions:
        assert check_index.for_session(session) == [w for w in watches if w.originating_session_id == session.id or (session.card_id and w.card_id == session.card_id)]
    rows = []
    try:
        for pair in range(5):
            for variant in (['full', 'index'] if pair % 2 == 0 else ['index', 'full']):
                start, cpu = time.perf_counter_ns(), time.thread_time_ns()
                index = Index(watches) if variant == 'index' else None
                decisions = [await policy._decision(session, sessions=sessions, dispatches=[],
                    watches=index if index else watches, leases=[], now=datetime.now(UTC)) for session in sessions]
                elapsed = (time.perf_counter_ns()-start)/1e6
                rows.append(dict(pair=pair, variant=variant, wall_ms=elapsed, thread_cpu_ms=(time.thread_time_ns()-cpu)/1e6))
                assert decisions == [('retained', 'actionable_pr_watch' if i % 2 else 'conversation_available') for i in range(1000)]
    finally:
        await policy.close()
    value = dict(contract='pa.lifecycle-watch-matching-benchmark/v1', observed_at=datetime.now(UTC).isoformat(),
        sessions=1000, watches=1000, pairs=5, samples=rows, decisions_equal=True,
        source_module=module.__file__, source_sha256=hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest(),
        scope='Private sparse-link comparison: five alternating actual decision sweeps, sparse immutable metadata links, candidate index construction included. No durable close, SQL, provider, service policy or PA store mutation. Does not measure the whole lifecycle sweep or establish historical cause. Uses the actual imported candidate index and actual decision method. Source or installed module path/hash is retained; the caller must distinguish these scopes.')
    out.write_text(json.dumps(value, indent=2)+'\n')
    print(json.dumps(value))

with tempfile.TemporaryDirectory(prefix='pa-watch-matching-') as tmp:
    os.environ.update(PA_DATA_DIR=str(Path(tmp)/'data'), PA_WORKSPACE_ROOT=str(Path(tmp)/'workspaces'), PA_AGENT_ENABLED='false')
    asyncio.run(run())

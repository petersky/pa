"""Small no-store diagnostic overhead comparison; never a service acceptance run.

Run from the checkout with --baseline-revision naming a local immutable commit.
The baseline method is extracted from that commit, not imported from a live PA
installation. No Kernel, Journal, Store or workspace is constructed.
"""
import argparse
import ast
import asyncio
import hashlib
import json
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from types import MethodType, SimpleNamespace

from pa.core.async_runtime import AsyncRuntime
from pa.core.performance import distribution
from pa.health_journal.service import HealthService


async def measure(revision):
    path = 'src/pa/health_journal/service.py'
    baseline = subprocess.check_output(['git', 'show', f'{revision}:{path}'], text=True)
    cls = next(n for n in ast.parse(baseline).body if isinstance(n, ast.ClassDef) and n.name == 'HealthService')
    method = next(n for n in cls.body if isinstance(n, ast.AsyncFunctionDef) and n.name == 'canonical_call')
    scope = {'asyncio': asyncio}
    exec(compile(ast.Module(body=[method], type_ignores=[]), f'{revision}:{path}', 'exec'), scope)
    runtime = AsyncRuntime(max_workers=1)
    health = object.__new__(HealthService)
    health.ctx = SimpleNamespace(services={'async_runtime': runtime})
    old = MethodType(scope['canonical_call'], health)
    sentinel = object()
    def call():
        return sentinel
    async def batch(kind):
        start = time.perf_counter()
        for _ in range(20):
            result = await (old(call) if kind == 'baseline' else health.canonical_call(call, diagnostic_phase='card_get'))
            assert result is sentinel
        return round((time.perf_counter() - start) * 1000, 6)
    started = datetime.now(UTC).isoformat()
    try:
        await batch('baseline'); await batch('source')
        pairs = []
        for index in range(30):
            values = {'order': ['baseline', 'source'] if index % 2 == 0 else ['source', 'baseline']}
            for kind in values['order']:
                values[kind] = await batch(kind)
            pairs.append(values)
        return {'contract': 'pa.health-canonical-diagnostic-overhead/v1',
                'started_at': started, 'finished_at': datetime.now(UTC).isoformat(),
                'baseline_revision': revision,
                'baseline_module_sha256': hashlib.sha256(baseline.encode()).hexdigest(),
                'source_module_sha256': hashlib.sha256(Path(path).read_bytes()).hexdigest(),
                'pairs': pairs, 'calls_per_batch': 20, 'warmup_calls_per_variant': 20,
                'batch_wall_ms': {kind: distribution([p[kind] for p in pairs]) for kind in ('baseline', 'source')},
                'runtime': runtime.snapshot(),
                'scope': 'Warm no-op diagnostic overhead on an uncontrolled host. Aggregate worker metrics unchanged; leaf worker spans exclude queue/completion delivery, sqlite-scope cleanup after return and diagnostic recording. Not a speedup, live cause attribution, physical I/O measurement or performance acceptance.'}
    finally:
        await runtime.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline-revision', required=True)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    with args.output.open('x') as output:
        json.dump(asyncio.run(measure(args.baseline_revision)), output, indent=2)
        output.write('\n')

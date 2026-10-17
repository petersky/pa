"""Paired indexed-parent amplification against the integrated PR495 baseline.

Runs only on a disposable immutable DAG, with no provider/peer traffic.
"""
from __future__ import annotations

import argparse
import ast
import json
import subprocess
import sys
import tempfile
import time
import types
from datetime import UTC, datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pa.core.performance import OperationObservation, current_observation, distribution
from pa.core.sqlite_metrics import sqlite_connection_scope
from pa.domain.models import SyncCommit
import pa.sync.event_log as event_log
from tests.test_realm_convergence import _Node

BASELINE = 'f36d4b38cc362024579cbf2ae917c011f88a148c'


def baseline_method():
    # This is trusted tracked repository code, never a log/comment or network
    # payload. Keep the exact baseline revision in the result.
    source = subprocess.check_output(
        ['git', 'show', BASELINE + ':src/pa/sync/event_log.py'],
        cwd=Path(__file__).resolve().parents[1], text=True,
    )
    cls = next(n for n in ast.parse(source).body
               if isinstance(n, ast.ClassDef) and n.name == 'EventLog')
    method = next(n for n in cls.body
                  if isinstance(n, ast.FunctionDef) and n.name == 'is_ancestor')
    namespace = dict(vars(event_log))
    exec(compile(ast.Module(body=[method], type_ignores=[]),
                 '<tracked-baseline-parent-proof>', 'exec'), namespace)
    return namespace['is_ancestor']


def run(root: Path, commits: int, iterations: int) -> dict:
    node = _Node(root, 'scale', 'Scale')
    parents = []
    stamp = datetime(2026, 1, 1, tzinfo=UTC)
    for _ in range(commits):
        head = node.objects.put_json(SyncCommit(
            hash='', realm_id='default', instance_id='scale',
            parent_hashes=parents, event_hashes=[], timestamp=stamp,
        ).model_dump(mode='json'))
        parents = [head]
    node.log.ensure_indexed('default', head)
    child = node.objects.put_json(SyncCommit(
        hash='', realm_id='default', instance_id='scale',
        parent_hashes=[head], event_hashes=[], timestamp=stamp,
    ).model_dump(mode='json'))
    methods = {'baseline': types.MethodType(baseline_method(), node.log),
               'source': node.log.is_ancestor}
    observations = {key: OperationObservation() for key in methods}
    samples = {key: [] for key in methods}
    for iteration in range(iterations):
        order = list(methods) if iteration % 2 else list(reversed(methods))
        for key in order:
            token = current_observation.set(observations[key])
            started = time.perf_counter()
            try:
                with sqlite_connection_scope():
                    assert methods[key](head, child)
            finally:
                current_observation.reset(token)
            samples[key].append((time.perf_counter() - started) * 1000)
    return {
        'scope': 'Alternating checks over one ready parent index; empty events isolate topology; no refs/live/network writes',
        'baseline_commit': BASELINE, 'commits': commits, 'iterations_per_version': iterations,
        'comparison': 'Only the tracked baseline is_ancestor method differs; supporting source is shared',
        'results': {key: {
            'latency_ms': distribution(samples[key]),
            'aggregate_items': observations[key].sqlite_aggregate_items,
            'aggregate_bytes': observations[key].sqlite_aggregate_bytes,
            'sqlite_calls': observations[key].sqlite_calls,
        } for key in methods},
    }


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--commits', type=int, default=10000, choices=range(1, 20001), metavar='1..20000')
    parser.add_argument('--iterations', type=int, default=30, choices=range(1, 101), metavar='1..100')
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix='pa-direct-parent-') as temporary:
        result = run(Path(temporary), args.commits, args.iterations)
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result))

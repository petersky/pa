"""Compare disposable Fleet dispatch reads; never open installed PA state."""
from __future__ import annotations

import argparse
import hashlib
import json
import platform
import tempfile
import time
from pathlib import Path
from unittest.mock import patch

from pa.core.performance import distribution
from pa.execution.dispatch import (
    DispatchEvent, DispatchRecord, DispatchStore, TERMINAL_DISPATCH_STATES,
)


def run(root: Path, *, terminal: int, active: int, events: int, iterations: int):
    source = Path(__import__('pa.execution.dispatch', fromlist=['']).__file__)
    source_hash = hashlib.sha256(source.read_bytes()).hexdigest()
    store = DispatchStore(root)
    ids = set()
    input_bytes = 0
    try:
        for i in range(terminal + active):
            card = f'fixture-card-{i}'
            ids.add(card)
            record = DispatchRecord(mutation_id=f'fixture-{i}', card_id=card,
                authority_instance_id='fixture', target_instance_id='fixture',
                authority_url='http://fixture', session_id=f'fixture-session-{i}',
                state='completed' if i < terminal else 'running',
                placement_decision={'candidates': [
                    {'id': str(j), 'values': {'proof': 'x' * 256, 'slots': [1, 2, 3]}}
                    for j in range(256)]},
                events=[DispatchEvent(seq=j + 1, state='running', message='x' * 256)
                        for j in range(events)])
            input_bytes += len(record.model_dump_json().encode())
            store.put(record)

        def legacy():
            return {key: row.public_dict() for key, row in store.latest_by_card(ids).items()
                    if row.state not in TERMINAL_DISPATCH_STATES}

        expected = legacy()
        samples = {'legacy': [], 'source': []}
        original = store._snapshot
        copies = 0

        def snapshot(record):
            nonlocal copies
            copies += 1
            return original(record)

        with patch.object(store, '_snapshot', snapshot):
            for i in range(iterations):
                for mode in (['legacy', 'source'] if i % 2 else ['source', 'legacy']):
                    copies = 0
                    start = time.perf_counter()
                    cpu = time.thread_time()
                    value = legacy() if mode == 'legacy' else store.active_public_by_card(ids)
                    row = {'wall_ms': (time.perf_counter() - start) * 1000,
                           'thread_cpu_ms': (time.thread_time() - cpu) * 1000,
                           'full_model_snapshots': copies}
                    assert value == expected
                    samples[mode].append(row)
        if expected:
            detached = store.active_public_by_card(ids)
            first = next(iter(detached))
            detached[first]['placement_decision']['candidates'][0]['values']['slots'].append(99)
            assert store.active_public_by_card(ids) == expected
        assert source_hash == hashlib.sha256(source.read_bytes()).hexdigest()
        return {'scope': 'Disposable normal durable put/read, alternating legacy full '
                'snapshot/filter/public path and active-first detached projection. '
                'Normal GC/fsync/locks unchanged. Isolates retained-history copy CPU; '
                'not a deployed or complete Fleet causal comparison.',
                'platform': platform.platform(), 'python': platform.python_version(),
                'fixture': {'terminal': terminal, 'active': active, 'events_per_record': events,
                            'placement_candidates_per_record': 256,
                            'serialized_input_bytes': input_bytes, 'iterations': iterations},
                'exact_public_projection_parity': True, 'detached_mutation_guard': True,
                'dispatch_source_sha256': source_hash, 'samples': samples,
                'latency_ms': {k: distribution([r['wall_ms'] for r in v]) for k, v in samples.items()},
                'thread_cpu_ms': {k: distribution([r['thread_cpu_ms'] for r in v]) for k, v in samples.items()},
                'full_model_snapshots': {k: sum(r['full_model_snapshots'] for r in v)
                                         for k, v in samples.items()}}
    finally:
        store.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--terminal', type=int, default=100)
    parser.add_argument('--active', type=int, default=5)
    parser.add_argument('--events', type=int, default=500)
    parser.add_argument('--iterations', type=int, default=20)
    args = parser.parse_args()
    if min(args.terminal, args.active, args.events) < 0 or args.iterations < 1:
        parser.error('counts must be nonnegative and iterations positive')
    with tempfile.TemporaryDirectory(prefix='pa-fleet-dispatch-projection-') as tmp:
        result = run(Path(tmp), terminal=args.terminal, active=args.active,
                     events=args.events, iterations=args.iterations)
    args.output.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps({k: v for k, v in result.items() if k != 'samples'}, indent=2))

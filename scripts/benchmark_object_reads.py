"""Alternate old/new immutable reads in disposable state; never live PA data."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
import time
from pathlib import Path
from unittest.mock import patch

from pa.core.performance import OperationObservation, current_observation, distribution
from pa.sync.object_store import ObjectStore


def run(root: Path, iterations: int, size: int) -> dict:
    store = ObjectStore(root)
    expected = {store.put(i.to_bytes(4, 'big') + b'x' * (size - 4)):
                i.to_bytes(4, 'big') + b'x' * (size - 4) for i in range(512)}

    def baseline(h):
        observation = current_observation.get()
        start = time.perf_counter()
        observation.object_get_calls += 1
        try:
            path = store._path_for(h)
            if not path.exists():
                return None
            raw = path.read_bytes()
            observation.object_get_bytes += len(raw)
            return raw
        finally:
            observation.object_get_ms += (time.perf_counter() - start) * 1000

    methods = {'baseline': baseline, 'source': store.get}
    samples = {k: [] for k in methods}
    cpu = {k: [] for k in methods}
    stat_counts = {k: 0 for k in methods}
    observations = {k: OperationObservation() for k in methods}
    stat = os.stat
    mode = ''

    def counted(*args, **kwargs):
        stat_counts[mode] += 1
        return stat(*args, **kwargs)

    with patch('os.stat', counted):
        for iteration in range(iterations):
            for mode in (list(methods) if iteration % 2 else list(reversed(methods))):
                token = current_observation.set(observations[mode])
                try:
                    start, thread = time.perf_counter(), time.thread_time()
                    actual = {h: methods[mode](h) for h in expected}
                    samples[mode].append((time.perf_counter() - start) * 1000)
                    cpu[mode].append((time.thread_time() - thread) * 1000)
                    assert actual == expected
                finally:
                    current_observation.reset(token)
    return {'objects_per_batch': 512, 'bytes_per_object': size,
            'logical_bytes_per_batch': 512 * size,
            'latency_ms': {k: distribution(v) for k, v in samples.items()},
            'thread_cpu_ms': {k: distribution(v) for k, v in cpu.items()},
            'samples_ms': samples, 'python_os_stat_calls': stat_counts,
            'observations': {k: {'calls': v.object_get_calls, 'bytes': v.object_get_bytes}
                             for k, v in observations.items()}}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', required=True, type=Path)
    parser.add_argument('--iterations', type=int, default=30)
    args = parser.parse_args()
    if args.iterations < 1:
        parser.error('iterations must be positive')
    with tempfile.TemporaryDirectory(prefix='pa-object-reads-') as tmp:
        out = {'scope': 'Alternating 512 immutable object reads, 512B and16KiB payloads, '
               'temporary state, exact byte parity, normal GC retained. Python os.stat '
               'counts exclude native open/fstat/disk work. Warm filesystem, no concurrent '
               'writers/provider/peer; no deployed or mixed-load acceptance claim.',
               'source_sha256': hashlib.sha256(Path(__import__('pa.sync.object_store', fromlist=['']).__file__).read_bytes()).hexdigest(),
               'stages': [run(Path(tmp) / str(size), args.iterations, size) for size in (512, 16384)]}
    args.output.write_text(json.dumps(out, indent=2) + '\n')
    print(json.dumps({k: out[k] for k in ('scope', 'source_sha256')}))
    print(json.dumps([{k: stage[k] for k in ('bytes_per_object', 'latency_ms', 'python_os_stat_calls')}
                      for stage in out['stages']]))

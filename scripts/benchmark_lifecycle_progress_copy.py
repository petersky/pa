"""Compare lifecycle put copies on paired, guarded disposable durable stores."""
import argparse
import ast
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
import hashlib
import json
import math
import os
from pathlib import Path
import subprocess
import tempfile
import time
from types import MethodType
from unittest.mock import patch

import pa.execution.dispatch as module
from pa.core.performance import OperationObservation, current_observation
from pa.execution.dispatch import DispatchRecord, DispatchStore
from pa.execution.progress import DispatchProgressEventV1, DispatchProgressHeartbeatV1, ProgressPhase
from pa.repository.workspace import WorkspaceManager

parser = argparse.ArgumentParser()
parser.add_argument('--baseline-revision', required=True)
parser.add_argument('--output', type=Path, required=True)
args = parser.parse_args()
assert not args.output.exists()
source = Path('src/pa/execution/dispatch.py').read_text()
baseline = subprocess.check_output(['git', 'show', args.baseline_revision + ':src/pa/execution/dispatch.py'], text=True)
old_tree, new_tree = ast.parse(baseline), ast.parse(source)
classes = [next(n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == 'DispatchStore') for tree in (old_tree, new_tree)]
old_method = next(n for n in classes[0].body if isinstance(n, ast.FunctionDef) and n.name == 'put')
for cls in classes:
    cls.body = [n for n in cls.body if getattr(n, 'name', None) != 'put']
assert ast.dump(old_tree) == ast.dump(new_tree), 'Shared application changed; reassess comparison'

class FixedNow:
    stamp = datetime.now(UTC)
    @classmethod
    def now(cls, tz=None):
        return cls.stamp

namespace = dict(vars(module))
namespace['datetime'] = FixedNow
exec(compile(ast.Module(body=[old_method], type_ignores=[]), 'archived-lifecycle-put', 'exec'), namespace)

def distribution(values):
    values = sorted(values)
    return {'samples': len(values), **{name: round(values[max(0, math.ceil(p * len(values)) - 1)], 6)
            for name, p in [('p50', .5), ('p95', .95), ('p99', .99), ('max', 1)]}}

out = {'contract': 'pa.lifecycle-progress-copy-comparison/v1', 'started_at': datetime.now(UTC).isoformat(),
       'baseline_revision': args.baseline_revision, 'baseline_source_sha256': hashlib.sha256(baseline.encode()).hexdigest(),
       'source_sha256': hashlib.sha256(source.encode()).hexdigest(), 'all_other_application_ast_equal': True, 'scenarios': {}}
with tempfile.TemporaryDirectory(prefix='lifecycle-progress-copy-', dir='.dev') as directory:
    root = Path(directory).resolve()
    os.environ['PA_DATA_DIR'] = str(root / 'unused-data')
    os.environ['PA_WORKSPACE_ROOT'] = str(root / 'unused-workspaces')
    def deny(*a, **k):
        raise AssertionError('No workspace construction')
    WorkspaceManager.__init__ = deny
    for retained in (0, 128, 1000):
        for caller_count in (0, 128, 1000):
            record = DispatchRecord(dispatch_id='dispatch', mutation_id='fixture', authority_instance_id='authority',
                authority_url='https://fixture.invalid', target_instance_id='origin', session_id='session', card_id='card',
                materialization_plan={'steps': [{'proof': ['x' * 256]} for _ in range(128)]})
            fields = dict(card_id='card', dispatch_id='dispatch', acp_session_id='session', originating_instance_id='origin',
                          authority_instance_id='authority', occurred_at=record.created_at, phase=ProgressPhase.TURN_ENDED)
            record.progress_events = [DispatchProgressEventV1(**fields, sequence=100+i, idempotency_key=f'event-{i}',
                summary=f'retained {i}', delivery_attempts=i % 3) for i in range(retained)]
            record.progress_heartbeat = DispatchProgressHeartbeatV1(**fields, sequence=1100, idempotency_key='heartbeat', summary='current')
            record.progress_next_sequence = 1101
            stores = {}
            for kind in ('baseline', 'source'):
                location = root / f'{retained}-{caller_count}' / kind
                location.mkdir(parents=True)
                assert location.is_relative_to(root)
                (location / 'dispatch_mutations.json').write_text(json.dumps({'dispatch': record.model_dump(mode='json')}))
                stores[kind] = DispatchStore(location)
            stores['baseline'].put = MethodType(namespace['put'], stores['baseline'])
            pairs = []
            try:
                for i in range(10):
                    FixedNow.stamp = record.created_at + timedelta(milliseconds=100*(i+1))
                    incoming = stores['source'].get('dispatch')
                    incoming.progress_events = [DispatchProgressEventV1(**fields, sequence=1+j, idempotency_key=f'stale-{j}', summary='discarded') for j in range(caller_count)]
                    incoming.progress_heartbeat.summary = 'stale heartbeat'
                    incoming.progress_seen_keys = ['discarded-key']
                    incoming.progress_next_sequence = 1
                    incoming.last_error = f'lifecycle {i}'
                    before = incoming.model_dump()
                    pair = {'order': ['baseline', 'source'] if i % 2 == 0 else ['source', 'baseline']}
                    for kind in pair['order']:
                        store = stores[kind]
                        original_snapshot = store._snapshot
                        copies, counts = [], {'events': 0, 'heartbeats': 0}
                        def counted(value):
                            wall, cpu = time.perf_counter(), time.thread_time()
                            result = original_snapshot(value)
                            copies.append({'events': len(value.progress_events), 'wall_ms': (time.perf_counter()-wall)*1000,
                                           'thread_cpu_ms': (time.thread_time()-cpu)*1000})
                            return result
                        def count_copy(cls, key):
                            original = cls.__deepcopy__
                            def wrapped(value, *a, **k):
                                counts[key] += 1
                                return original(value, *a, **k)
                            return wrapped
                        observation = OperationObservation()
                        token = current_observation.set(observation)
                        try:
                            with patch.object(store, '_snapshot', counted), patch.object(module, 'datetime', FixedNow), \
                                 patch.object(DispatchProgressEventV1, '__deepcopy__', count_copy(DispatchProgressEventV1, 'events')), \
                                 patch.object(DispatchProgressHeartbeatV1, '__deepcopy__', count_copy(DispatchProgressHeartbeatV1, 'heartbeats')):
                                wall, cpu = time.perf_counter(), time.thread_time()
                                result = store.put(incoming)
                                elapsed = (time.perf_counter()-wall)*1000
                                cpu_ms = (time.thread_time()-cpu)*1000
                        finally:
                            current_observation.reset(token)
                        assert counts == {'events': (caller_count if kind == 'baseline' else 0) + 3*retained,
                                          'heartbeats': 4 if kind == 'baseline' else 3}
                        assert len(copies) == 3
                        assert incoming.model_dump() == before
                        pair[kind] = {'wall_ms': elapsed, 'thread_cpu_ms': cpu_ms, 'full_snapshots': copies,
                                      'deepcopy_counts': counts, 'observation': asdict(observation)}
                        assert result.progress_events == record.progress_events
                        assert result.progress_heartbeat == record.progress_heartbeat
                    assert stores['baseline'].get('dispatch') == stores['source'].get('dispatch')
                    pairs.append(pair)
                verified = {}
                for kind, store in stores.items():
                    visible = store.get('dispatch')
                    verified[kind] = {'retained_events': len(visible.progress_events), 'retained_plan_steps': 128,
                        'core_bytes': len(store._core_payload(visible).encode()), 'write_rows': store._write_rows,
                        'sqlite_total_changes': store._conn.total_changes}
                    store.close()
                    stores[kind] = DispatchStore(root / f'{retained}-{caller_count}' / kind)
                    assert stores[kind].get('dispatch') == visible
                assert verified['baseline'] == verified['source']
                out['scenarios'][f'{retained}:{caller_count}'] = {'pairs': pairs, 'verified': verified,
                    'exact_state_delivery_and_reload_equal': True,
                    'latency_ms': {kind: {metric: distribution([p[kind][metric] for p in pairs]) for metric in ('wall_ms', 'thread_cpu_ms')} for kind in stores}}
            finally:
                for store in stores.values():
                    store.close()
out['finished_at'] = datetime.now(UTC).isoformat()
out['scope'] = ('Ten alternating pairs per current0/128/1000event and caller0/128/1000event shape; constant128plan steps, current/caller heartbeat present. '
    'All180samples retained. Only put differs in application AST. Full durability, pinned timestamps, complete state/delivery, unchanged caller, '
    'logical writes/SQLitechanges/core bytes and reload parity. Both instrumented with snapshot wall/CPU, model deepcopy counts and native SQL observations. '
    'Three full snapshots remain on both paths; phase observations overlap total time. Disposable fixture seed/migration and caller construction excluded. '
    'No physical disk/fsync/successful-busy/GC partition, controlled hardware/statistical bound, service/provider/network workload or deployed acceptance. '
    'At10samples nearest-rankp95/p99=max; do not pool shapes.')
args.output.write_text(json.dumps(out, indent=2)+'\n')
print(json.dumps({name: item['latency_ms'] for name, item in out['scenarios'].items()}))

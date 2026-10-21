"""Compare pinned progress ingestion on guarded disposable durable stores."""
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
from pa.execution.progress import DispatchProgressEventV1, ProgressPhase
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
old_method = next(n for n in classes[0].body if isinstance(n, ast.FunctionDef) and n.name == 'ingest_progress')
for cls in classes:
    cls.body = [n for n in cls.body if getattr(n, 'name', None) != 'ingest_progress']
assert ast.dump(old_tree) == ast.dump(new_tree), 'Shared application changed; reassess comparison'

class FixedNow:
    stamp = datetime.now(UTC)
    @classmethod
    def now(cls, tz=None):
        return cls.stamp

namespace = dict(vars(module))
namespace['datetime'] = FixedNow
exec(compile(ast.Module(body=[old_method], type_ignores=[]), 'archived-ingest-progress', 'exec'), namespace)

def distribution(values):
    values = sorted(values)
    return {'samples': len(values), **{name: round(values[max(0, math.ceil(p * len(values)) - 1)], 6)
            for name, p in [('p50', .5), ('p95', .95), ('p99', .99), ('max', 1)]}}

out = {'contract': 'pa.progress-ingest-copy-comparison/v1', 'started_at': datetime.now(UTC).isoformat(),
       'baseline_revision': args.baseline_revision, 'baseline_source_sha256': hashlib.sha256(baseline.encode()).hexdigest(),
       'source_sha256': hashlib.sha256(source.encode()).hexdigest(), 'all_other_application_ast_equal': True, 'scenarios': {}}
with tempfile.TemporaryDirectory(prefix='progress-ingest-copy-', dir='.dev') as directory:
    root = Path(directory).resolve()
    os.environ['PA_DATA_DIR'] = str(root / 'unused-data')
    os.environ['PA_WORKSPACE_ROOT'] = str(root / 'unused-workspaces')
    def deny(*a, **k):
        raise AssertionError('No workspace construction')
    WorkspaceManager.__init__ = deny
    for count in (1, 128, 1000):
        for branch in ('accepted', 'late', 'conflict', 'coalesced'):
            record = DispatchRecord(dispatch_id='dispatch', mutation_id='fixture', authority_instance_id='authority',
                authority_url='https://fixture.invalid', target_instance_id='origin', session_id='session', card_id='card',
                materialization_plan={'steps': [{'proof': ['x' * 256]} for _ in range(count)]})
            fields = dict(card_id='card', dispatch_id='dispatch', acp_session_id='session', originating_instance_id='origin',
                          authority_instance_id='authority', occurred_at=record.created_at)
            record.progress_events = [DispatchProgressEventV1(**fields, sequence=100+i, idempotency_key=f'event-{i}',
                phase=ProgressPhase.TURN_ENDED, summary=f'retained {i}') for i in range(count)]
            if branch == 'coalesced':
                record.progress_events[-1].phase = ProgressPhase.IMPLEMENTING
            record.progress_highest_accepted_sequence = 99 + count
            record.progress_next_sequence = 100 + count
            record.progress_accepted_ranges = [[100, 99+count]]
            stores = {}
            for kind in ('baseline', 'source'):
                location = root / str(count) / branch / kind
                location.mkdir(parents=True)
                assert location.is_relative_to(root)
                (location / 'dispatch_mutations.json').write_text(json.dumps({'dispatch': record.model_dump(mode='json')}))
                stores[kind] = DispatchStore(location)
            stores['baseline'].ingest_progress = MethodType(namespace['ingest_progress'], stores['baseline'])
            pairs = []
            try:
                for i in range(10):
                    FixedNow.stamp = record.created_at + timedelta(milliseconds=100*(i+1))
                    seq = 100 if branch == 'conflict' else 10+i if branch == 'late' else 100+count+i
                    incoming = DispatchProgressEventV1(**fields, sequence=seq, idempotency_key=f'incoming-{i}',
                        phase=ProgressPhase.IMPLEMENTING if branch == 'coalesced' else ProgressPhase.TURN_ENDED,
                        summary=record.progress_events[-1].summary if branch == 'coalesced' else f'incoming {i}')
                    pair = {'order': ['baseline', 'source'] if i % 2 == 0 else ['source', 'baseline']}
                    for kind in pair['order']:
                        store = stores[kind]
                        original_snapshot = store._snapshot
                        original_dump = DispatchProgressEventV1.model_dump_json
                        copies = []
                        serialized = {'calls': 0, 'bytes': 0, 'wall_ms': 0.0, 'thread_cpu_ms': 0.0}
                        def counted(value):
                            wall, cpu = time.perf_counter(), time.thread_time()
                            result = original_snapshot(value)
                            copies.append({'events': len(value.progress_events), 'plan_steps': len(value.materialization_plan['steps']),
                                'wall_ms': (time.perf_counter()-wall)*1000, 'thread_cpu_ms': (time.thread_time()-cpu)*1000})
                            return result
                        def dumped(value, *a, **k):
                            wall, cpu = time.perf_counter(), time.thread_time()
                            result = original_dump(value, *a, **k)
                            serialized['wall_ms'] += (time.perf_counter()-wall)*1000
                            serialized['thread_cpu_ms'] += (time.thread_time()-cpu)*1000
                            serialized['calls'] += 1
                            serialized['bytes'] += len(result.encode())
                            return result
                        observation = OperationObservation()
                        token = current_observation.set(observation)
                        try:
                            with patch.object(store, '_snapshot', counted), patch.object(module, 'datetime', FixedNow), patch.object(DispatchProgressEventV1, 'model_dump_json', dumped):
                                wall, cpu = time.perf_counter(), time.thread_time()
                                result = store.ingest_progress(incoming, delivered=True)
                                elapsed = (time.perf_counter()-wall)*1000
                                cpu_ms = (time.thread_time()-cpu)*1000
                        finally:
                            current_observation.reset(token)
                        assert result.status == branch
                        pair[kind] = {'wall_ms': elapsed, 'thread_cpu_ms': cpu_ms, 'full_snapshots': copies,
                                      'event_json': serialized, 'observation': asdict(observation), 'result': result.model_dump(mode='json')}
                    assert pair['baseline']['result'] == pair['source']['result']
                    assert stores['baseline'].get('dispatch') == stores['source'].get('dispatch')
                    pairs.append(pair)
                verified = {}
                for kind, store in stores.items():
                    visible = store.get('dispatch')
                    verified[kind] = {'retained_events': len(visible.progress_events), 'retained_plan_steps': count,
                        'core_bytes': len(store._core_payload(visible).encode()), 'write_rows': store._write_rows,
                        'sqlite_total_changes': store._conn.total_changes}
                    store.close()
                    stores[kind] = DispatchStore(root / str(count) / branch / kind)
                    assert stores[kind].get('dispatch') == visible
                assert verified['baseline'] == verified['source']
                out['scenarios'][f'{count}:{branch}'] = {'pairs': pairs, 'verified': verified, 'exact_state_receipts_and_reload_equal': True,
                    'latency_ms': {kind: {metric: distribution([p[kind][metric] for p in pairs]) for metric in ('wall_ms', 'thread_cpu_ms')} for kind in stores}}
            finally:
                for store in stores.values():
                    store.close()
out['finished_at'] = datetime.now(UTC).isoformat()
out['scope'] = ('Ten alternating pairs per1/128/1000retained-event/plan-step shape and accepted/late/conflict/coalesced branch, all samples retained. '
    'Only ingest_progress differs in application AST. Same pinned timestamps, complete state/results, logical writes/SQLitechanges/core bytes and reload parity. '
    'Snapshot/SQL/event model_dump_json instrumentation active in both; event JSON wall/CPU observations overlap total operation time and do not cover all serialization. '
    'No physical disk/fsync/native successful-busy partition, controlled hardware/statistical bound, service/provider/network workload, activation/restart or deployed acceptance. '
    'At10samples nearest-rankp95/p99=max; do not pool branches/shapes.')
args.output.write_text(json.dumps(out, indent=2)+'\n')
print(json.dumps({name: item['latency_ms'] for name, item in out['scenarios'].items()}))

"""Compare per-row migration read setup with one batch-owned handle.

Uses only private synthetic stores. Run with --output NEW_PATH; the archived
baseline is read from Git, never checked out or installed.
"""
import argparse
import ast
import hashlib
import json
import math
import os
import subprocess
import tempfile
import textwrap
import time
from contextlib import contextmanager, nullcontext
from datetime import UTC, datetime
from pathlib import Path
from types import MethodType
from unittest.mock import patch


def distribution(values):
    ordered = sorted(values)
    return dict(samples=len(ordered), **{
        key: ordered[math.ceil(q * len(ordered)) - 1]
        for key, q in [('p50_ms', .5), ('p95_ms', .95), ('p99_ms', .99), ('max_ms', 1)]})


def archived_method(revision, path, cls, method, namespace):
    raw = subprocess.check_output(['git', 'show', f'{revision}:{path}'], text=True)
    node = next(n for n in ast.walk(ast.parse(raw)) if isinstance(n, ast.ClassDef) and n.name == cls)
    method_node = next(n for n in node.body if isinstance(n, ast.FunctionDef) and n.name == method)
    leaf = textwrap.dedent(ast.get_source_segment(raw, method_node))
    copied = dict(namespace)
    exec(leaf, copied)
    return copied[method], hashlib.sha256(leaf.encode()).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--include-constructor', action='store_true', help='Also measure reopening the seeded projection through its normal constructor')
    args = parser.parse_args()
    assert not args.output.exists(), 'Preserve earlier evidence; choose a new output path'
    baseline = '8ca8d6860aa548d4babac02b0bbb646010e2f9ab'
    started = datetime.now(UTC).isoformat()
    samples, work, constructors = [], [], []
    with tempfile.TemporaryDirectory(prefix='pa-migration-read-scope-') as tmp:
        root = Path(tmp)
        os.environ.update(PA_DATA_DIR=str(root / 'data'), PA_WORKSPACE_ROOT=str(root / 'workspaces'), PA_AGENT_ENABLED='false')
        from pa.core.sqlite_metrics import ObservedConnection, sqlite_connection_scope
        from pa.domain import projection, transcript_storage
        from pa.domain.models import TranscriptEvent
        repo = Path(__file__).resolve().parents[1]
        assert Path(projection.__file__).resolve() == repo / 'src/pa/domain/projection.py'
        assert Path(transcript_storage.__file__).resolve() == repo / 'src/pa/domain/transcript_storage.py'
        old_migrate, migrate_sha = archived_method(baseline, 'src/pa/domain/projection.py', 'CardProjection', '_migrate_legacy_transcripts', vars(projection))
        old_range, range_sha = archived_method(baseline, 'src/pa/domain/transcript_storage.py', 'TranscriptStorage', 'list_range', vars(transcript_storage))
        store = projection.CardProjection(root / 'fixture.db')
        expected = [TranscriptEvent(id=f'e-{i}', session_id='session', seq=i, event_type='chunk', payload={},
                    created_at=datetime(2026, 10, 25, tzinfo=UTC)) for i in range(1, 501)]
        with store._conn() as conn:
            conn.executemany('INSERT INTO agent_transcript_events VALUES(?,?,?,?,?,?)',
                [(e.id, e.session_id, e.seq, e.event_type, '{}', e.created_at.isoformat()) for e in expected])
        store.transcripts.append(expected)
        candidate_migrate, candidate_range = store._migrate_legacy_transcripts, store.transcripts.list_range

        def select(label):
            store._migrate_legacy_transcripts = MethodType(old_migrate, store) if label == 'baseline' else candidate_migrate
            store.transcripts.list_range = MethodType(old_range, store.transcripts) if label == 'baseline' else candidate_range

        def reset():
            store.transcripts.record_operation('legacy-v1', cursor=json.dumps(['', -1]), state='running', examined=0, changed=0)
            return store.transcripts.operation('legacy-v1')

        for scoped in (False, True):
            for pair in range(5):
                for label in (['baseline', 'candidate'] if pair % 2 == 0 else ['candidate', 'baseline']):
                    select(label)
                    with sqlite_connection_scope() if scoped else nullcontext():
                        before = reset()
                        wall, cpu = time.perf_counter_ns(), time.thread_time_ns()
                        store._migrate_legacy_transcripts(batch_size=500)
                        timing = dict(wall_ms=(time.perf_counter_ns() - wall) / 1e6,
                                      thread_cpu_ms=(time.thread_time_ns() - cpu) / 1e6)
                        after = store.transcripts.operation('legacy-v1')
                    assert json.loads(after['cursor']) == ['session', 500]
                    assert after['examined'] - before['examined'] == 500
                    assert after['changed'] == before['changed']
                    samples.append(dict(scoped=scoped, pair=pair, variant=label, **timing))
            for label in ['baseline', 'candidate']:
                select(label)
                counts = dict(canonical_borrows=0, new_canonical_connections=0, canonical_lookups=0, hydrated_events=0)
                original_conn, original_new, original_event = store.transcripts._conn, store.transcripts._new_connection, store.transcripts._event
                original_execute = ObservedConnection.execute
                @contextmanager
                def counted_conn():
                    counts['canonical_borrows'] += 1
                    with original_conn() as conn:
                        yield conn
                def counted_new():
                    counts['new_canonical_connections'] += 1
                    return original_new()
                def counted_event(row):
                    counts['hydrated_events'] += 1
                    return original_event(row)
                def counted_execute(conn, sql, parameters=()):
                    if sql.startswith('SELECT * FROM transcript_events WHERE session_id=?'):
                        counts['canonical_lookups'] += 1
                    return original_execute(conn, sql, parameters)
                with sqlite_connection_scope() if scoped else nullcontext():
                    reset()
                    with patch.object(store.transcripts, '_conn', counted_conn), patch.object(store.transcripts, '_new_connection', counted_new), patch.object(store.transcripts, '_event', counted_event), patch.object(ObservedConnection, 'execute', counted_execute):
                        store._migrate_legacy_transcripts(batch_size=500)
                assert counts['canonical_lookups'] == counts['hydrated_events'] == 500
                work.append(dict(scoped=scoped, variant=label, **counts))
        select('candidate')
        if args.include_constructor:
            candidate_class_migrate = projection.CardProjection._migrate_legacy_transcripts
            candidate_class_range = transcript_storage.TranscriptStorage.list_range
            for pair in range(5):
                for label in (['baseline', 'candidate'] if pair % 2 == 0 else ['candidate', 'baseline']):
                    before = reset()
                    with patch.object(projection.CardProjection, '_migrate_legacy_transcripts', old_migrate if label == 'baseline' else candidate_class_migrate), patch.object(transcript_storage.TranscriptStorage, 'list_range', old_range if label == 'baseline' else candidate_class_range):
                        wall, cpu = time.perf_counter_ns(), time.thread_time_ns()
                        reopened = projection.CardProjection(root / 'fixture.db')
                        timing = dict(wall_ms=(time.perf_counter_ns() - wall) / 1e6,
                                      thread_cpu_ms=(time.thread_time_ns() - cpu) / 1e6)
                        after = reopened.transcripts.operation('legacy-v1')
                        assert json.loads(after['cursor']) == ['session', 500]
                        assert after['examined'] - before['examined'] == 500 and after['changed'] == before['changed']
                        assert reopened.transcripts.list_range('session') == expected
                    constructors.append(dict(pair=pair, variant=label, **timing))
        assert store.transcripts.list_range('session') == expected
        with store._conn() as conn:
            assert conn.execute('SELECT COUNT(*) FROM agent_transcript_events').fetchone()[0] == 500
        hashes = {name: hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()
                  for name, module in [('pa.domain.projection', projection), ('pa.domain.transcript_storage', transcript_storage)]}
    out = dict(contract='pa.migration-read-scope-benchmark/v1', started_at=started, finished_at=datetime.now(UTC).isoformat(),
        baseline=baseline, archived_method_sha256=dict(migration=migrate_sha, list_range=range_sha), source_module_sha256=hashes,
        samples=samples, work_counts=work, constructor_samples=constructors, constructor_summaries={label: distribution([s['wall_ms'] for s in constructors if s['variant'] == label]) for label in ['baseline', 'candidate']} if constructors else {}, summaries={str(scoped): {label: distribution([s['wall_ms'] for s in samples if s['scoped'] == scoped and s['variant'] == label]) for label in ['baseline', 'candidate']} for scoped in [False, True]},
        scope='Five alternating pairs per direct/scoped mode, private 500-row already-canonical hot-payload fixture. Every timed method performs the same 500 SQL lookups and full hydrations; order/cursor/retention asserted. Work counts are a separate instrumented pass excluded from timing. Scoped mode reuses handles exactly through sqlite_connection_scope, but does not model executor queue, operation budgets, cold files, production load, health or whole maintenance. Optional constructor phase reopens the same private 500-row store through CardProjection.__init__, including its unchanged schema and closure-migration work. It is not full Kernel or real provider startup. No deployment or statistical whole-path speedup claim.')
    with args.output.open('x') as stream:
        json.dump(out, stream, indent=2)
        stream.write('\n')
    print(json.dumps(dict(output=str(args.output), sha256=hashlib.sha256(args.output.read_bytes()).hexdigest(), summaries=out['summaries'], constructor_summaries=out['constructor_summaries'], work_counts=work)))


if __name__ == '__main__':
    main()

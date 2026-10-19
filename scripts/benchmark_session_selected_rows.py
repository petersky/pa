"""Compare exact pre-selection source readers in a small disposable SQLite DB.

Run from this checkout with a local pre-selector --baseline-revision. Query
timings exclude model decoding and numeric transfer accounting. All selected
models and candidate metadata must match. This is not a live endpoint workload.
"""
import argparse
import ast
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import tempfile
import time
from datetime import UTC, datetime, timedelta
from types import MethodType
from unittest.mock import patch

from pa.core.performance import distribution
from pa.core.sqlite_metrics import ObservedCursor
from pa.domain.models import AgentSession
from pa.domain.projection import (
    CardProjection, SessionListMetadata, SessionListSnapshot, _SESSION_COLUMNS_WITHOUT_METRICS,
)


def measure(revision):
    path = 'src/pa/domain/projection.py'
    archived = subprocess.check_output(['git', 'show', f'{revision}:{path}'], text=True)
    old_class = next(n for n in ast.parse(archived).body if isinstance(n, ast.ClassDef) and n.name == 'CardProjection')
    methods = {n.name: n for n in old_class.body if isinstance(n, ast.FunctionDef)}
    assert 'select_ids' not in [a.arg for a in methods['session_list_snapshot'].args.kwonlyargs], 'Use a pre-selector baseline'
    current = next(n for n in ast.parse(Path(path).read_text()).body if isinstance(n, ast.ClassDef) and n.name == 'CardProjection')
    decoder = next(n for n in current.body if isinstance(n, ast.FunctionDef) and n.name == '_row_to_session')
    assert ast.dump(methods['_row_to_session']) == ast.dump(decoder), 'Decoder changed; review baseline mapping'
    namespace = {'sqlite3': sqlite3, 'datetime': datetime, 'SessionListSnapshot': SessionListSnapshot,
                 'SessionListMetadata': SessionListMetadata, '_SESSION_COLUMNS_WITHOUT_METRICS': _SESSION_COLUMNS_WITHOUT_METRICS}
    functions = [methods[name] for name in ('_list_session_rows', 'session_list_snapshot')]
    exec(compile(ast.Module(body=functions, type_ignores=[]), f'{revision}:{path}', 'exec'), namespace)
    started = datetime.now(UTC).isoformat()
    with tempfile.TemporaryDirectory(prefix='selected-session-benchmark-', dir='.dev') as directory:
        root = Path(directory).resolve()
        os.environ['PA_DATA_DIR'] = str(root / 'data')
        os.environ['PA_WORKSPACE_ROOT'] = str(root / 'workspaces')
        source = CardProjection(root / 'fixture.db')
        baseline = CardProjection(root / 'fixture.db')
        assert all(store.db_path.resolve().is_relative_to(root) for store in (source, baseline))
        for name in ('_list_session_rows', 'session_list_snapshot'):
            setattr(baseline, name, MethodType(namespace[name], baseline))
        now = datetime.now(UTC)
        for index in range(64):
            source.save_session(AgentSession(id=f'fixture-{index:03d}', agent_name='fixture', purpose='chat', status='idle',
                updated_at=now - timedelta(seconds=index), config_json={'selection_proof': 'c' * 8192},
                metrics_json={'retained': 'm' * 8192}))
        result = {}
        try:
            for count in (2, 32, 64, 0):
                def choose(candidates):
                    ids = [row.id for row in candidates[:count]]
                    if 0 < count < len(candidates):
                        ids[-1] = candidates[-1].id  # existing selected-row replacement shape
                    return ids
                def read(kind):
                    return (baseline.session_list_snapshot() if kind == 'baseline'
                            else source.session_list_snapshot(select_ids=choose))
                before, after = read('baseline'), read('source')
                ids = choose(before.candidates)
                assert before.candidates == after.candidates
                assert [before.hydrate(key).model_dump() for key in ids] == [after.hydrate(key).model_dump() for key in ids]
                samples = []
                for index in range(20):
                    pair = {'order': ['baseline', 'source'] if index % 2 == 0 else ['source', 'baseline']}
                    for kind in pair['order']:
                        clock, cpu = time.perf_counter(), time.thread_time()
                        snapshot = read(kind)
                        pair[kind] = {'wall_ms': round((time.perf_counter() - clock) * 1000, 6),
                                      'thread_cpu_ms': round((time.thread_time() - cpu) * 1000, 6)}
                        assert snapshot.candidates == before.candidates
                    samples.append(pair)
                transfers = {}
                for kind in ('baseline', 'source'):
                    totals = Counter()
                    original_fetch = ObservedCursor.fetchall
                    original_execute = ObservedCursor.execute
                    def fetch(cursor):
                        rows = original_fetch(cursor)
                        totals['fetch_calls'] += 1
                        totals['rows'] += len(rows)
                        totals['returned_text_bytes'] += sum(len(v.encode()) for row in rows for v in row if isinstance(v, str))
                        return rows
                    def execute(cursor, sql, parameters=()):
                        totals['execute_calls'] += 1
                        totals['command_' + sql.lstrip().split()[0].lower()] += 1
                        return original_execute(cursor, sql, parameters)
                    with patch.object(ObservedCursor, 'fetchall', new=fetch), patch.object(ObservedCursor, 'execute', new=execute):
                        snapshot = read(kind)
                    transfers[kind] = dict(totals) | {'retained_complete_rows': len(snapshot._rows)}
                result[str(count)] = {'selected': count, 'metadata_and_complete_selected_models_equal': True,
                    'pairs': samples, 'latency': {kind: {phase: distribution([pair[kind][phase] for pair in samples])
                        for phase in ('wall_ms', 'thread_cpu_ms')} for kind in ('baseline', 'source')},
                    'transfer_observation_outside_timing': transfers}
        finally:
            source.close_thread_connection()
            baseline.close_thread_connection()
    return {'contract': 'pa.session-selected-source-read-comparison/v1', 'started_at': started,
            'finished_at': datetime.now(UTC).isoformat(), 'baseline_revision': revision, 'rows': 64,
            'baseline_module_sha256': hashlib.sha256(archived.encode()).hexdigest(),
            'source_module_sha256': hashlib.sha256(Path(path).read_bytes()).hexdigest(),
            'baseline_reader_mapping': 'Exact archived snapshot and row-query methods bound to an isolated reader; selected decoder AST unchanged.',
            'scenarios': result,
            'scope': 'Warm uncontrolled host, query-span helper timings. Byte accounting outside timing measures returned text, not physical I/O or HTTP response bytes. Every-row and zero-durable-row cases retained, not only favorable bounded selection. No Kernel, production Store/EventLog/refs, service workload, backup action, restart or deployed acceptance.'}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--baseline-revision', required=True)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    with args.output.open('x') as output:
        json.dump(measure(args.baseline_revision), output, indent=2)
        output.write('\n')

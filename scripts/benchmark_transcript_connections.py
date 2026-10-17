"""Disposable exact-baseline worker connection and message parity benchmark."""
import ast, hashlib, json, subprocess, tempfile, time, types
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from pa.acp.transcript_page import complete_page
from pa.core.performance import OperationObservation, current_observation, distribution
from pa.core.operation_budget import OperationBudget, OperationDeadline, current_operation
from pa.core.sqlite_metrics import sqlite_connection_scope
from pa.domain.models import TranscriptEvent
import pa.domain.transcript_storage as module

def run(output: Path):
    """Exact baseline-vs-worker-cache message page parity and connection work."""
    baseline = '646aff549ad17e177577ba3d1f46e0595e4f0bc6'
    source = subprocess.check_output(['git', 'show', baseline + ':src/pa/domain/transcript_storage.py'], text=True)
    cls = next((n for n in ast.parse(source).body if isinstance(n, ast.ClassDef) and n.name == 'TranscriptStorage'))
    fn = next((n for n in cls.body if isinstance(n, ast.FunctionDef) and n.name == '_conn'))
    namespace = dict(vars(module))
    exec(compile(ast.Module(body=[fn], type_ignores=[]), '<tracked-baseline-transcript-connection>', 'exec'), namespace)
    with tempfile.TemporaryDirectory(prefix='pa-transcript-connection-') as tmp:
        store = module.TranscriptStorage(Path(tmp) / 'fixture.db')
        stamp = datetime(2026, 1, 1, tzinfo=UTC)
        for offset in range(0, 3200, 32):
            store.append([TranscriptEvent(id=f'event-{i}', session_id='fixture', seq=i + 1, event_type='agent_message_chunk', created_at=stamp, payload={'text': 'fixture ' * 100, 'message_id': 'stable'}) for i in range(offset, offset + 32)])
        page = store.list_before('fixture', limit=64)
        adapter = SimpleNamespace(list_transcript_events_before=store.list_before, list_transcript_events=store.list)
        methods = {'baseline': types.MethodType(namespace['_conn'], store), 'source': store._conn}
        observations = {k: OperationObservation() for k in methods}
        samples = {k: [] for k in methods}
        connections = Counter()
        connect = module.sqlite3.connect
        case = None
        expected = None

        def counted(*a, **kw):
            connections[case] += 1
            return connect(*a, **kw)
        with patch.object(module.sqlite3, 'connect', counted):
            for iteration in range(30):
                for key in list(methods) if iteration % 2 else list(reversed(methods)):
                    case = key
                    store._conn = methods[key]
                    deadline = OperationDeadline(OperationBudget(), time.monotonic())
                    deadline.execution_started(time.monotonic())
                    op = current_operation.set(deadline)
                    obs = current_observation.set(observations[key])
                    started = time.perf_counter()
                    try:
                        with sqlite_connection_scope():
                            result = complete_page(adapter, 'fixture', page, forward=False)
                    finally:
                        current_observation.reset(obs)
                        current_operation.reset(op)
                    samples[key].append((time.perf_counter() - started) * 1000)
                    if expected is None:
                        expected = result
                    assert result == expected and result[1:] == (1, 3200, False, False)
        out = {'scope': 'Alternating30complete-message pages/version,3200retained800character hot chunks, initial64events. Exact integrated646aff54 _conn baseline; shared payload/compaction/model/source functions. Same operation budget/progress callback and complete canonical result; disposable data, no provider/network or live state.', 'connections': dict(connections), 'latency_ms': {k: distribution(v) for (k, v) in samples.items()}, 'sqlite_calls': {k: observations[k].sqlite_calls for k in methods}, 'canonical_output_sha256': hashlib.sha256(json.dumps([v.model_dump(mode='json') for v in expected[0]], sort_keys=True).encode()).hexdigest(), 'output_events': len(expected[0]), 'reconstructed_characters': len(expected[0][0].payload['text'])}
        output.write_text(json.dumps(out, indent=2) + '\n')
        print(json.dumps(out))

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    run(parser.parse_args().output)

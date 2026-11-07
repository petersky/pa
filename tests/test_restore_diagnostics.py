import asyncio
import json
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

from acp.exceptions import RequestError

from pa.acp.client import _resolve_session_load_target
from pa.acp.restore_diagnostics import record_restore_attempt, safe_restore_attempts
from pa.domain.models import TranscriptEvent
from pa.execution.observability import diagnostic_timeline


def test_listing_absence_and_listing_failure_remain_distinct():
    async def run():
        rows = []
        conn = SimpleNamespace(list_sessions=AsyncMock(return_value={'sessions': []}))
        assert await _resolve_session_load_target(
            conn, session_id='private-id', cwd='/private/path', diagnostics=rows,
        ) is None
        assert rows[0]['outcome'] == 'not_listed'
        conn.list_sessions.side_effect = RequestError(-32602, 'private payload')
        assert await _resolve_session_load_target(
            conn, session_id='private-id', cwd='/private/path', diagnostics=rows,
        ) == ('private-id', '/private/path')
        assert rows[1]['outcome'] == 'failed'
        assert rows[1]['rpc_code'] == -32602
        assert 'private' not in json.dumps(rows)
    asyncio.run(run())


def test_public_timeline_revalidates_and_bounds_persisted_restore_metadata():
    row = {'method': 'session/resume', 'outcome': 'failed', 'duration_ms': 12.5,
           'category': 'rpc_error', 'rpc_code': -32603, 'message': 'private answer',
           'cwd': '/private/path', 'session_id': 'private-id'}
    timeline = diagnostic_timeline([TranscriptEvent(
        session_id='pa-session', seq=1, event_type='session_admission_failed',
        payload={'error': 'private error', 'configuration': {'secret': 'private'},
                 'restore_attempts': [row] * 100},
    )])
    rows = timeline[0]['payload']['restore_attempts']
    assert len(rows) == 3
    assert rows[0] == {k: v for k, v in row.items()
                       if k in {'method', 'outcome', 'duration_ms', 'category', 'rpc_code'}}
    assert 'private' not in json.dumps(timeline)


def test_malformed_persisted_attempts_do_not_break_diagnostics_or_leak_fields():
    for value in [None, 'private', {}, [None], [True],
                  [{'method': [], 'outcome': {}, 'duration_ms': True}],
                  [{'method': 'session/load', 'outcome': 'failed', 'duration_ms': 10 ** 1000}],
                  [{'method': 'session/load', 'outcome': 'failed', 'duration_ms': float('nan')}]]:
        assert safe_restore_attempts(value) == []
    assert safe_restore_attempts([{'method': 'session/load', 'outcome': 'failed',
                                  'duration_ms': 3, 'category': 'private',
                                  'rpc_code': -32603}]) == [
        {'method': 'session/load', 'outcome': 'failed', 'duration_ms': 3}]


def test_recording_never_formats_error_message_or_data_and_keeps_three_attempts():
    class PrivateError(Exception):
        def __str__(self):
            raise AssertionError('must not inspect provider data')
    rows = []
    for method in ['session/resume', 'session/list', 'session/load', 'session/load']:
        record_restore_attempt(rows, method, time.perf_counter(), 'failed', PrivateError())
    assert len(rows) == 3
    assert all(row['category'] == 'exception' for row in rows)

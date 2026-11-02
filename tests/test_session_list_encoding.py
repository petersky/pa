"""Session-list wire size changes preserve complete JSON and extension behavior."""
import asyncio
import json
import threading
from datetime import UTC, datetime
from types import SimpleNamespace

import pytest

from pa.core.async_runtime import AsyncRuntime
from pa.modules import agent_chat


@pytest.mark.asyncio
@pytest.mark.parametrize('detail', ['summary', 'full'])
@pytest.mark.parametrize('payload', [
    'ASCII text with spaces ' * 128,
    '界éλ' * 1024,
    '\ud800middle\udc00literal\\ud800',
    '😃e\u0301é\u2028\u2029</script>',
    {'array': [False, None, 3.5, {'when': datetime(2026, 1, 1, tzinfo=UTC)}]},
])
async def test_complete_wire_values_order_headers_and_forwarded_selection(
    monkeypatch, detail, payload
):
    runtime = AsyncRuntime(max_workers=1, max_queue=1)
    manager = SimpleNamespace(async_runtime=runtime)
    request = object()
    items = [dict(id=str(i), project=dict(id='p', title='Fixture'),
                  config_json=dict(retained=payload), metrics_json={'retained': [i, i+1]})
             for i in range(3)]
    expected = json.loads(json.dumps(items, default=str))
    main_thread = threading.get_ident()
    calls = []
    def read(*args):
        assert threading.get_ident() != main_thread
        calls.append(args)
        return items
    monkeypatch.setattr(agent_chat, '_manager', lambda _: manager)
    monkeypatch.setattr(agent_chat, 'list_agent_sessions', read)
    try:
        response = await agent_chat.list_agent_sessions_http(
            request, view='all', archived=True, activity_filter='running', limit=10,
            selected_session_id='selected', detail=detail,
        )
        assert calls == [(request, 'all', True, 'running', 10, 'selected', detail)]
        assert response.status_code == 200
        assert response.headers['content-type'] == 'application/json'
        assert int(response.headers['content-length']) == len(response.body)
        actual = json.loads(response.body)
        assert actual == expected
        assert [list(row) for row in actual] == [list(row) for row in expected]
        assert [row['metrics_json']['retained'] for row in actual] == [[0,1],[1,2],[2,3]]
        response.body.decode('utf-8')
        metrics = runtime.work_metrics.snapshot()['metrics']
        assert metrics['http.session_list.response_bytes']['max'] == len(response.body)
        assert metrics['http.session_list.rows_returned']['max'] == len(items)
        assert metrics['http.session_list.serialization_ms']['samples'] == 1
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_ten_full_unicode_sessions_fit_bound_without_losing_values(monkeypatch):
    items = [dict(id=str(i), project=dict(id='p', title='Fixture'),
                  config_json={'retained': '界é' * 32768}) for i in range(10)]
    legacy = json.dumps(items, default=str).encode()
    assert len(legacy) > 2097152
    monkeypatch.setattr(agent_chat, '_manager', lambda _: SimpleNamespace())
    monkeypatch.setattr(agent_chat, 'list_agent_sessions', lambda *args: items)
    response = await agent_chat.list_agent_sessions_http(object(), view='all', detail='full', limit=10)
    assert len(response.body) <= 2097152
    assert json.loads(response.body) == json.loads(legacy)
    assert len(json.loads(response.body)) == 10


@pytest.mark.asyncio
async def test_custom_default_runs_once_per_occurrence_with_surrogate(monkeypatch):
    seen = []
    class Custom:
        def __str__(self):
            seen.append(threading.get_ident())
            return 'retained\ud800界'
    items = [{'first': Custom(), 'second': Custom()}]
    monkeypatch.setattr(agent_chat, '_manager', lambda _: SimpleNamespace())
    monkeypatch.setattr(agent_chat, 'list_agent_sessions', lambda *args: items)
    main_thread = threading.get_ident()
    response = await agent_chat.list_agent_sessions_http(object())
    assert len(seen) == 2 and all(thread != main_thread for thread in seen)
    assert json.loads(response.body) == [{'first': 'retained\ud800界', 'second': 'retained\ud800界'}]


@pytest.mark.asyncio
async def test_empty_list_remains_complete(monkeypatch):
    monkeypatch.setattr(agent_chat, '_manager', lambda _: SimpleNamespace())
    monkeypatch.setattr(agent_chat, 'list_agent_sessions', lambda *args: [])
    response = await agent_chat.list_agent_sessions_http(object())
    assert response.body == b'[]' and response.headers['content-length'] == '2'


@pytest.mark.asyncio
@pytest.mark.parametrize('kind', ['cycle', 'key', 'default'])
async def test_original_encoding_errors_propagate_without_second_read(monkeypatch, kind):
    class Broken:
        def __str__(self):raise RuntimeError('fixture formatter failure')
    items = [{}]
    if kind == 'cycle':items[0]['cycle'] = items
    elif kind == 'key':items[0][('unsupported',)] = True
    else:items[0]['value'] = Broken()
    try:
        json.dumps(items, default=str).encode()
    except Exception as original:
        expected_type, expected_message = type(original), str(original)
    else:raise AssertionError('Invalid baseline fixture')
    calls = []
    def read(*args):calls.append(True);return items
    monkeypatch.setattr(agent_chat, '_manager', lambda _: SimpleNamespace())
    monkeypatch.setattr(agent_chat, 'list_agent_sessions', read)
    with pytest.raises(expected_type) as caught:
        await agent_chat.list_agent_sessions_http(object())
    assert str(caught.value) == expected_message and calls == [True]

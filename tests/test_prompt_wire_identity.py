"""Exercise shipping prompt records through the existing metadata readers."""
import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from pa.acp.client import AgentConnection, WireJsonlLogger
from pa.config import Settings
from pa.domain.models import AgentSession
from pa.instance.agent_session import AgentSessionRuntime
from pa.instance.quiesce import QueuedPrompt
from pa.instance.wire_metadata import read_wire_metadata
from pa.instance.wire_metadata_pages import read_wire_metadata_page

SID = '1b436b1d-2a0c-41d4-a778-6954d22be645'
NATIVE = '01a1433b-d51c-7a21-9e94-840dabca18c9'
HANDOFF = 'c1b1b358-4f8e-59cd-8b4c-6287c5e11294'
PROMPT = 'restart-handoff:' + HANDOFF


def test_real_prompt_records_link_without_changing_provider_request(tmp_path):
    store = MagicMock()
    connection = AgentConnection(Settings(data_dir=tmp_path / "data", workspace_root=tmp_path / "workspaces"), store)
    connection.session = AgentSession(id=SID, external_session_id=NATIVE, agent_name='test')
    connection._conn = SimpleNamespace(prompt=AsyncMock(return_value=SimpleNamespace(stop_reason='end_turn')))
    connection._await_turn = lambda call: call
    wire = WireJsonlLogger(tmp_path / 'sessions' / SID / 'wire.jsonl')
    connection._wire_log = wire.log
    assert asyncio.run(connection.prompt('PRIVATE', queued_prompt_id=PROMPT, restart_attempt=0)) == 'end_turn'
    sent = connection._conn.prompt.call_args.kwargs
    assert set(sent) == {'session_id', 'prompt', 'message_id'}
    rows = [json.loads(line) for line in wire.path.read_text().splitlines()]
    assert rows[0]['params']['message_id'] == rows[1]['message_id'] == sent['message_id']
    assert rows[1]['session_id'] == NATIVE
    for result in (
        read_wire_metadata(tmp_path, session_id=SID, native_id=NATIVE, prompt_id=PROMPT, attempt=0),
        read_wire_metadata_page(tmp_path, session_id=SID, native_id=NATIVE, prompt_id=PROMPT,
                                attempt=0, scope='fixed', secret='test-only'),
    ):
        assert result['effect'] == 'completed'
        assert result['historical_coverage'] == 'unknown' and result['delivery_allowed'] is False
        assert 'PRIVATE' not in json.dumps(result)
    # Correlation does not waive the saved attempt fence or manufacture old links.
    assert read_wire_metadata(tmp_path, session_id=SID, native_id=NATIVE, prompt_id=PROMPT,
                              attempt=1)['effect'] == 'unknown'
    assert store.update_restart_handoff.call_count == 0


@pytest.mark.parametrize('change', ['session', 'prompt', 'owner', 'message', 'status', 'absent', 'ordinary', 'exact'])
def test_runtime_attempt_comes_only_from_exact_saved_receipt(change):
    session = AgentSession(id=SID, agent_name='test', principal_id='user:fixture', cwd='/fixture')
    item = QueuedPrompt(id=PROMPT, session_id=SID, message='PRIVATE',
                        source=PROMPT, principal_id=session.principal_id, cwd=session.cwd)
    receipt = SimpleNamespace(id=HANDOFF, session_id=SID, continuation_prompt_id=PROMPT,
        continuation_prompt=item.message, status='continuation_queued', delivered_at=None,
        attempts=3, instance_id='owner', card_id=None, project_id=None, execution_binding={})
    if change == 'session': receipt.session_id = 'other'
    if change == 'prompt': receipt.continuation_prompt_id = 'other'
    if change == 'owner': receipt.instance_id = 'other'
    if change == 'message': receipt.continuation_prompt = 'other'
    if change == 'status': receipt.status = 'failed'
    if change == 'ordinary': item.source = 'api'
    runtime = SimpleNamespace(session_id=SID, session=session, store=MagicMock(),
        _offload=AsyncMock(), _merged_agent_env=lambda _: {},
        _restart_receipts={} if change == 'absent' else {PROMPT:receipt},
        settings=SimpleNamespace(instance_id='owner'))
    runtime._restart_continuation_receipt = lambda item: AgentSessionRuntime._restart_continuation_receipt(runtime, item)
    result = AgentSessionRuntime._prompt_wire_identity(runtime, item)
    assert result == ({'queued_prompt_id':PROMPT, 'restart_attempt':3} if change == 'exact'
                      else {'queued_prompt_id':PROMPT})
    runtime._offload.assert_not_called()
    runtime.store.get_restart_handoff.assert_not_called()
    runtime.store.update_restart_handoff.assert_not_called()


def test_failed_provider_turn_never_writes_completion_identity(tmp_path):
    connection = AgentConnection(Settings(data_dir=tmp_path / "data", workspace_root=tmp_path / "workspaces"), MagicMock())
    connection.session = AgentSession(id=SID, external_session_id=NATIVE, agent_name='test')
    connection._conn = SimpleNamespace(prompt=AsyncMock(side_effect=ValueError('fixture rejection')))
    connection._await_turn = lambda call: call
    connection._transport_alive = lambda: True
    wire = WireJsonlLogger(tmp_path / 'sessions' / SID / 'wire.jsonl')
    connection._wire_log = wire.log
    with pytest.raises(ValueError, match='fixture rejection'):
        asyncio.run(connection.prompt('PRIVATE', queued_prompt_id=PROMPT, restart_attempt=0))
    result = read_wire_metadata(tmp_path, session_id=SID, native_id=NATIVE, prompt_id=PROMPT, attempt=0)
    assert result['effect'] == 'unknown' and result['delivery_allowed'] is False
    assert len(result['records']) == 1 and 'stop_reason' not in result['records'][0]

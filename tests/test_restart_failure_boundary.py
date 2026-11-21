import asyncio
import sqlite3
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from pydantic import ValidationError

from pa.config import Settings
from pa.domain.models import AgentSession, RestartHandoff
from pa.domain.projection import CardProjection
from pa.domain.transcript_storage import TranscriptStorage
from pa.instance.agent_session import AgentSessionManager
from pa.instance.restart_lifecycle import RestartTransitionConflict
from pa.modules.agent_chat import RestartHandoffRetryBody, retry_restart_handoff


@pytest.fixture
def store(tmp_path):
    projection = CardProjection(tmp_path / 'cards.db')
    try:
        yield projection
    finally:
        projection.close_thread_connection()


@pytest.mark.parametrize('failure', ['quiesce', 'host'])
def test_worker_records_actual_pre_dispatch_boundary_without_settling_effect(tmp_path, store, failure):
    async def run():
        session = store.save_session(AgentSession(id='exact', agent_name='codex'))
        manager = AgentSessionManager(Settings(data_dir=tmp_path), store)
        # asyncio.run owns a disposable executor here. Close the projection's
        # cached connection in the thread that used it before that thread exits.
        async def offload(operation, call, *args, timeout=None, **kwargs):
            kwargs.pop('wait_for_completion', None)
            def invoke():
                try:
                    return call(*args, **kwargs)
                finally:
                    store.close_thread_connection()
            return await asyncio.to_thread(invoke)
        manager._offload = offload
        manager.complete_startup()
        manager.quiesce = AsyncMock(side_effect=TimeoutError('turn still active') if failure == 'quiesce' else None)
        with patch('pa.cli.service.request_restart', side_effect=TimeoutError('host response lost')) as host:
            receipt = await manager.request_restart_handoff(
                session_id=session.id, continuation_prompt='Continue once', idempotency_key='once',
            )
            await manager._restart_handoff_tasks[receipt.id]
        saved = store.get_restart_handoff(receipt.id)
        assert saved.status == 'failed'
        assert saved.observation['effect_state'] == 'unknown'
        assert saved.delivered_at is None
        assert saved.continuation_prompt_id == receipt.continuation_prompt_id
        if failure == 'quiesce':
            host.assert_not_called()
            assert saved.attempts == 0
            assert saved.reason_code == 'failed_before_host_restart_request'
            assert saved.failure_stage == 'quiescing'
        else:
            host.assert_called_once()
            assert saved.attempts == 1
            assert saved.reason_code == 'restart_operation_failed'
            assert saved.failure_stage == 'restarting'
    asyncio.run(run())


def test_legacy_failure_stays_unknown_and_retry_cas_rejects_stale_version(store):
    old = store.create_restart_handoff(RestartHandoff(
        session_id='exact', instance_id='owner', idempotency_key='once',
        continuation_prompt='Continue', continuation_prompt_id='exact-prompt',
        status='failed', failure_stage='quiescing', error='old timeout',
        reason_code='restart_operation_failed',
    ))
    assert old.observation['effect_state'] == 'unknown'
    with pytest.raises(RestartTransitionConflict):
        store.retry_restart_handoff(old.id, session_id='exact', expected_version=1)
    assert store.get_restart_handoff(old.id) == old
    current = store.retry_restart_handoff(old.id, session_id='exact', expected_version=0)
    assert current.id == old.id
    assert current.continuation_prompt_id == old.continuation_prompt_id
    assert current.transition_history[0]['reason_code'] == 'restart_operation_failed'
    assert current.transition_history[0]['error'] == 'old timeout'
    with pytest.raises(RestartTransitionConflict):
        store.retry_restart_handoff(old.id, session_id='exact', expected_version=0)
    assert store.get_restart_handoff(old.id) == current


def test_retry_route_passes_version_fence_and_preserves_authorization(store):
    session = store.save_session(AgentSession(id='exact', agent_name='codex', principal_id='user:owner'))
    receipt = store.create_restart_handoff(RestartHandoff(session_id=session.id, idempotency_key='once', continuation_prompt_id='exact-prompt', status='failed'))
    manager = SimpleNamespace(store=store, get=lambda _: None, retry_restart_handoff=AsyncMock(return_value=receipt))
    request = SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(ctx=SimpleNamespace(settings=SimpleNamespace(auth_required=True)))),
        state=SimpleNamespace(user=SimpleNamespace(role='member')),
    )
    with patch('pa.modules.agent_chat._require_session_traffic_ready', return_value=manager), patch('pa.modules.agent_chat.get_principal_id', return_value='user:owner'):
        asyncio.run(retry_restart_handoff(request, session.id, receipt.id, RestartHandoffRetryBody(expected_version=3)))
    manager.retry_restart_handoff.assert_awaited_once_with(session_id=session.id, handoff_id=receipt.id, expected_version=3)
    manager.retry_restart_handoff.reset_mock()
    with patch('pa.modules.agent_chat._require_session_traffic_ready', return_value=manager), patch('pa.modules.agent_chat.get_principal_id', return_value='user:other'), pytest.raises(HTTPException) as exc:
        asyncio.run(retry_restart_handoff(request, session.id, receipt.id, RestartHandoffRetryBody(expected_version=3)))
    assert exc.value.status_code == 403
    manager.retry_restart_handoff.assert_not_called()
    manager.retry_restart_handoff.side_effect = RestartTransitionConflict('receipt advanced')
    with patch('pa.modules.agent_chat._require_session_traffic_ready', return_value=manager), patch('pa.modules.agent_chat.get_principal_id', return_value='user:owner'), pytest.raises(HTTPException) as stale:
        asyncio.run(retry_restart_handoff(request, session.id, receipt.id, RestartHandoffRetryBody(expected_version=3)))
    assert stale.value.status_code == 409
    assert stale.value.detail['code'] == 'stale_restart_phase'
    assert stale.value.detail['committed'] is False


def test_retry_body_rejects_negative_versions_and_unrecognized_proof():
    with pytest.raises(ValidationError):
        RestartHandoffRetryBody(expected_version=-1)
    with pytest.raises(ValidationError):
        RestartHandoffRetryBody(expected_version=3, effect='none')


def test_transcript_constructor_closes_every_initialization_connection(tmp_path, monkeypatch):
    connect = sqlite3.connect
    connections = []
    def tracked(*args, **kwargs):
        connection = connect(*args, **kwargs)
        connections.append(connection)
        return connection
    monkeypatch.setattr(sqlite3, 'connect', tracked)
    TranscriptStorage(tmp_path / 'metadata.db')
    assert connections
    for connection in connections:
        with pytest.raises(sqlite3.ProgrammingError, match='closed'):
            connection.execute('SELECT 1')

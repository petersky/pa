from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from pa.acp.client import AgentConnection
from pa.config import Settings
from pa.domain.models import AgentSession
from pa.domain.projection import CardProjection
from pa.instance.agent_session import AgentSessionManager, AgentSessionRuntime, WorkspaceBindingMismatch
from pa.modules.agent_chat import list_agent_sessions


@pytest.mark.parametrize('view', ['chats', 'all', 'activity'])
def test_session_listing_limits_hydration_and_retains_selection(view):
    now = datetime.now(UTC)
    sessions = [AgentSession(id=f's-{i}', agent_name='codex', purpose='chat',
                            updated_at=now - timedelta(seconds=i)) for i in range(1000)]
    if view == 'activity':
        for session in sessions:
            session.purpose = 'automated_run'
    manager = SimpleNamespace(store=SimpleNamespace(list_sessions=lambda: sessions), list_runtimes=lambda: [])
    hydrated = []

    def item(request, session, **kwargs):
        assert kwargs['include_diagnostics'] is False
        hydrated.append(session.id)
        return {'id': session.id}

    with patch('pa.modules.agent_chat._require_session_traffic_ready', return_value=manager), \
         patch('pa.modules.agent_chat._session_list_item', side_effect=item), \
         patch('pa.modules.agent_chat._presentation', return_value={'workflow': {'state': 'active'}}):
        result = list_agent_sessions(None, view=view, limit=3, selected_session_id='s-999')
    assert [item['id'] for item in result] == ['s-0', 's-1', 's-999']
    assert hydrated == ['s-0', 's-1', 's-999']


@pytest.mark.asyncio
async def test_archive_connected_chat_survives_stale_connection_teardown(tmp_path):
    store = CardProjection(tmp_path / 'pa.db')
    settings = Settings(data_dir=tmp_path)
    session = AgentSession(id='chat', agent_name='codex', purpose='chat', control_mode='human', status='idle')
    store.save_session(session)
    manager = AgentSessionManager(settings, store)
    runtime = AgentSessionRuntime(manager, session)
    connection = AgentConnection(settings, store)
    connection.session = session.model_copy(deep=True)
    runtime.connection = connection
    manager._runtimes[session.id] = runtime
    await manager.archive_session(session.id)
    persisted = store.get_session(session.id)
    assert persisted.archived_at is not None
    assert persisted.archive_reason == 'user_archive'
    assert manager.get(session.id) is None
    assert any(e.event_type == 'provider_released' for e in store.list_transcript_events(session.id))


@pytest.mark.asyncio
@pytest.mark.parametrize('status', ['idle', 'closed', 'quiesced'])
async def test_disconnect_preserves_newer_metadata_and_terminal_fences(tmp_path, status):
    store = CardProjection(tmp_path / 'pa.db')
    session = AgentSession(id='chat', agent_name='codex', status='idle')
    store.save_session(session)
    connection = AgentConnection(Settings(data_dir=tmp_path), store)
    connection.session = session.model_copy(deep=True)
    session.pinned_at = datetime.now(UTC)
    session.archived_at = datetime.now(UTC)
    session.control_mode = 'human'
    session.status = status
    store.save_session(session)
    await connection.disconnect()
    persisted = store.get_session(session.id)
    assert persisted.pinned_at == session.pinned_at
    assert persisted.archived_at == session.archived_at
    assert persisted.control_mode == 'human'
    assert persisted.status == ('disconnected' if status == 'idle' else status)


@pytest.mark.asyncio
async def test_binding_mismatch_blocks_without_exhausting_retries(tmp_path):
    store = CardProjection(tmp_path / 'pa.db')
    session = AgentSession(id='run', agent_name='codex', status='recoverable_interrupted',
                           execution_binding={'version': 1, 'cwd': '/original'})
    store.save_session(session)
    manager = AgentSessionManager(Settings(data_dir=tmp_path), store)
    result = await manager._mark_recovery_interrupted(
        manager._snapshot_from_persisted(session), WorkspaceBindingMismatch('wrong workspace'))
    persisted = store.get_session(session.id)
    assert result == 'recovery_blocked'
    assert persisted.recovery_json['attempts'] == 1
    assert persisted.recovery_json['next_retry_at'] is None
    assert 'original' in persisted.recovery_json['remedy']
    assert persisted.execution_binding == session.execution_binding


def test_sidebar_does_not_read_transcript_bodies_or_build_diagnostics():
    from unittest.mock import MagicMock
    session = AgentSession(id='chat', agent_name='codex', purpose='chat')
    manager = MagicMock()
    manager.list_runtimes.return_value = []
    manager.store.list_sessions.return_value = [session]
    request = MagicMock()
    request.app.state.ctx.store.list_transcript_events_before.side_effect = AssertionError('transcript hydration')
    with patch('pa.modules.agent_chat._require_session_traffic_ready', return_value=manager), \
         patch('pa.modules.agent_chat._manager', return_value=manager), \
         patch('pa.modules.agent_chat._observability', side_effect=AssertionError('diagnostic hydration')):
        result = list_agent_sessions(request, view='chats')
    assert result[0]['id'] == session.id
    assert result[0]['observability'] is None
    assert result[0]['provider_attempts'] is None


@pytest.mark.asyncio
@pytest.mark.parametrize('different_branch', [False, True])
async def test_workspace_recovery_checks_materialized_facts_without_retargeting(tmp_path, different_branch):
    from unittest.mock import MagicMock
    store = CardProjection(tmp_path / 'pa.db')
    settings = Settings(data_dir=tmp_path / 'data', workspace_root=tmp_path / 'workspaces')
    cwd = tmp_path / 'workspaces' / 'original'
    cwd.mkdir(parents=True)
    binding = {'version': 1, 'origin_instance_id': settings.instance_id,
               'execution_card_id': None, 'execution_project_id': None,
               'repository_ids': ['repo'], 'worktree_paths': [str(cwd)],
               'lease_ids': ['lease'], 'branch': 'original', 'base_sha': 'original-base'}
    session = AgentSession(id='run', agent_name='codex', origin_instance_id=settings.instance_id,
                           execution_binding=binding)
    store.save_session(session)
    manager = AgentSessionManager(settings, store)
    context = {'cwd': str(cwd), 'repositories': [{'repository_id': 'repo', 'worktree_path': str(cwd),
                'lease_id': 'lease', 'branch': 'different' if different_branch else 'original',
                'base_sha': 'original-base'}]}
    workspace = SimpleNamespace(cwd=str(cwd), execution_context=lambda *_: context)
    manager.workspace_manager.scratch_workspace = MagicMock(return_value=workspace)
    if different_branch:
        with pytest.raises(WorkspaceBindingMismatch):
            await manager._prepare_workspace(session, requested_cwd=None, provider_id='codex')
        assert store.get_session(session.id).execution_binding == binding
    else:
        await manager._prepare_workspace(session, requested_cwd=None, provider_id='codex')
        assert store.get_session(session.id).execution_binding == {**binding, 'cwd': str(cwd)}


@pytest.mark.asyncio
async def test_pin_survives_idle_runtime_release(tmp_path):
    store = CardProjection(tmp_path / 'pa.db')
    settings = Settings(data_dir=tmp_path)
    session = AgentSession(id='chat', agent_name='codex', purpose='chat', status='idle')
    store.save_session(session)
    manager = AgentSessionManager(settings, store)
    runtime = AgentSessionRuntime(manager, session)
    connection = AgentConnection(settings, store)
    connection.session = session.model_copy(deep=True)
    runtime.connection = connection
    manager._runtimes[session.id] = runtime
    pinned = await manager.pin_session(session.id, pinned=True)
    await manager.release_session_process(session.id, reason='idle')
    assert store.get_session(session.id).pinned_at == pinned.pinned_at

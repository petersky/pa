import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from pa.domain.models import AgentSession
from pa.instance.agent_session import AgentSessionManager


def retained(**recovery):
    return AgentSession(id='retained',agent_name='codex',status='recovery_blocked',
        external_session_id='original-native',model_id='saved-model',mode_id='saved-mode',
        execution_binding={'base_sha':'a'*40,'lease_ids':['original-lease']},
        recovery_json={'blocked':True,'attempts':129,**recovery},
        config_json={'durable_runtime':{'in_flight':None,'queued_prompts':[{'id':'q1'},{'id':'q2'}]}})


@pytest.mark.parametrize('recovery',[{'code':'workspace_binding_mismatch','exhausted':True},
    {'code':'workspace_binding_mismatch','exhausted':False},
    {'code':'provider_start_failed','exhausted':True}])
def test_queue_does_not_override_durable_binding_or_exhaustion_block(recovery):
    manager=AgentSessionManager.__new__(AgentSessionManager)
    manager._project_recovery_available=AsyncMock(side_effect=AssertionError('must not probe admission'))
    session=retained(**recovery);before=session.model_dump()
    assert asyncio.run(manager._automatic_recovery_eligibility(session)) is None
    assert session.model_dump()==before
    manager._project_recovery_available.assert_not_called()


def test_ordinary_interrupted_queue_remains_eligible():
    manager=AgentSessionManager.__new__(AgentSessionManager)
    session=retained(code='provider_start_failed',exhausted=False)
    session.status='recoverable_interrupted';session.recovery_json['blocked']=False
    assert asyncio.run(manager._automatic_recovery_eligibility(session)) is not None


def test_repaired_project_availability_policy_is_preserved():
    manager=AgentSessionManager.__new__(AgentSessionManager)
    manager._project_recovery_available=AsyncMock(return_value=True)
    session=retained(code='project_unavailable',exhausted=False)
    session.config_json={'durable_runtime':{'in_flight':None,'queued_prompts':[]}}
    assert asyncio.run(manager._automatic_recovery_eligibility(session))=='project_available'
    manager._project_recovery_available.assert_awaited_once_with(session)


def test_two_service_starts_preserve_block_and_accepted_queue(tmp_path):
    from pa.config import Settings
    from pa.domain.projection import CardProjection
    settings=Settings(data_dir=tmp_path/'data',workspace_root=tmp_path/'trees')
    store=CardProjection(tmp_path/'pa.db')
    saved=store.save_session(retained(code='workspace_binding_mismatch',exhausted=True))
    for _ in range(2):
        manager=AgentSessionManager(settings,store)
        manager.workspace_manager.reconcile_terminal_state=MagicMock(return_value={})
        manager.workspace_manager.collect_garbage=MagicMock(return_value={})
        manager._resume_from_snapshot=AsyncMock(side_effect=AssertionError('known incompatible retry'))
        manager.attach_default=AsyncMock(side_effect=AssertionError('provider admission'))
        manager.request_recovery=MagicMock()
        asyncio.run(manager.start(resume=True))
        manager._resume_from_snapshot.assert_not_called()
        assert store.get_session(saved.id)==saved
        assert manager._startup_blocked==1

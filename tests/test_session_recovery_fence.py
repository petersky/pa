import asyncio
import hashlib
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pa.acp.client import AgentConnection
from pa.config import Settings
from pa.domain.projection import CardProjection
from pa.domain.models import AgentSession
from pa.instance.agent_session import AgentSessionManager, AgentSessionRuntime, SessionAdmissionInProgress
from pa.instance.quiesce import QueuedPrompt
from tests.test_restart_handoff_reconcile import dismissed


def fixture(tmp_path):
    store=CardProjection(tmp_path/'pa.db');receipt=dismissed(store)
    session=store.save_session(AgentSession(id=receipt.session_id,agent_name='codex',status='closed',
        authority_instance_id='owner',principal_id='user:owner',external_session_id='saved-native',
        model_id='saved-model',config_json={'configuration':{'reasoning_effort':'high'}},
        cwd='/saved/worktree',execution_binding=dict(lease_ids=['retained'],worktree_paths=['/saved/worktree'],
            branch='saved-branch',base_sha='saved-base',cwd='/saved/worktree')))
    args=dict(session_id=session.id,expected_version=receipt.phase_version,owner_instance_id='owner',
        principal_id='user:owner',idempotency_key='seal-once',expected_session_version=session.updated_at,
        expected_binding_hash=hashlib.sha256(json.dumps(session.execution_binding,sort_keys=True,separators=(',',':')).encode()).hexdigest(),
        expected_settings_hash=hashlib.sha256(json.dumps([session.agent_name,session.model_id,session.mode_id,session.config_json],sort_keys=True).encode()).hexdigest(),
        expected_native_id=session.external_session_id,lease_expectations=[dict(lease_id='retained',fencing_token=403)])
    manager = AgentSessionManager(Settings(data_dir=tmp_path/'data',workspace_root=tmp_path/'workspaces',instance_id='owner'),store)
    lease = SimpleNamespace(id='retained',session_id=session.id,state='ready',fencing_token=403,
        expires_at=datetime.now(UTC)+timedelta(days=1),worktree_path='/saved/worktree',branch='saved-branch',base_sha='saved-base')
    manager.workspace_manager = MagicMock();manager.workspace_manager.get_by_id.return_value=lease
    return store,receipt,session,args,manager,lease


def seal(receipt,args,manager):
    return asyncio.run(manager.prepare_recovery_fence(receipt.id,**args))


def test_prepare_is_a_durable_seal_not_restore_or_close_disposition(tmp_path):
    store,r,s,a,m,lease=fixture(tmp_path)
    with patch('pa.instance.agent_session.AgentConnection',side_effect=AssertionError('provider load')):
        result=seal(r,a,m)
    assert result['state']=='sealed_provider_load_unsupported'
    assert result['effect']=='unknown' and not result['delivery_allowed'] and not result['restored']
    assert result['close_disposition']=='required_not_granted'
    assert store.get_session(s.id)==s and store.get_restart_handoff(r.id)==r
    m.workspace_manager.renew_session.assert_not_called()
    m.workspace_manager.provision_project.assert_not_called()
    assert not store.pending_restart_continuation(session_id=s.id)
    reopened=CardProjection(tmp_path/'pa.db')
    assert reopened.session_recovery_fence(s.id)==result
    with pytest.raises(ValueError):reopened.require_session_delivery_allowed(s.id)


def test_unknown_acknowledgement_retry_repairs_only_audit_mirror(tmp_path):
    store,r,s,a,m,_=fixture(tmp_path)
    with patch.object(store,'append_transcript_events',side_effect=OSError('mirror failed')):
        with pytest.raises(OSError):seal(r,a,m)
    durable=store.session_recovery_fence(s.id)
    assert durable and not durable['delivery_allowed']
    with pytest.raises(ValueError):store.require_session_delivery_allowed(s.id)
    assert seal(r,a,m)==durable
    store.transcripts.prune([s.id])
    assert seal(r,a,m)==durable
    with pytest.raises(ValueError):seal(r,a|{'expected_settings_hash':'0'*64},m)
    with pytest.raises(ValueError):seal(r,a|{'lease_expectations':[dict(lease_id='retained',fencing_token=404)]},m)
    assert store.get_session(s.id)==s and store.get_restart_handoff(r.id)==r


@pytest.mark.parametrize('change',['principal','owner','native','version','session','settings','binding','lease','workspace','active','admitting'])
def test_stale_missing_foreign_and_concurrent_activation_preserve_state(tmp_path,change):
    store,r,s,a,m,lease=fixture(tmp_path)
    if change=='principal':a['principal_id']='foreign'
    if change=='owner':a['owner_instance_id']='foreign'
    if change=='native':a['expected_native_id']='foreign'
    if change=='version':a['expected_version']+=1
    if change=='session':a['expected_session_version']-=timedelta(seconds=1)
    if change=='settings':a['expected_settings_hash']='0'*64
    if change=='binding':a['expected_binding_hash']='0'*64
    if change=='lease':lease.fencing_token+=1
    if change=='workspace':lease.worktree_path='/another/worktree'
    if change=='active':m._runtimes[s.id]=SimpleNamespace(_closed=False)
    if change=='admitting':m._admitting_sessions.add(s.id)
    with pytest.raises((ValueError,SessionAdmissionInProgress)):seal(r,a,m)
    assert not store.session_recovery_fence(s.id)
    assert store.get_session(s.id)==s and store.get_restart_handoff(r.id)==r


@pytest.mark.parametrize('source',['api','manual','dispatch','pr-supervisor','reconciliation','post-turn','restart-handoff:original'])
def test_all_prompt_sources_and_retained_queues_are_sealed(tmp_path,source):
    store,r,s,a,m,_=fixture(tmp_path);seal(r,a,m)
    runtime=AgentSessionRuntime(m,s,initial_transcript_seq=0)
    runtime.connection=SimpleNamespace(connected=True,prompt=AsyncMock())
    queued=QueuedPrompt(message='retained private content',session_id=s.id,source=source)
    runtime._queue=[queued]
    with pytest.raises(ValueError):runtime.enqueue('another',source=source)
    with pytest.raises(ValueError):asyncio.run(runtime.prompt('another',source=source))
    with pytest.raises(ValueError):asyncio.run(runtime._run_prompt(queued))
    with pytest.raises(ValueError):runtime.resume_queue()
    runtime._start_drain();assert runtime._drain_task is None
    asyncio.run(runtime._drain_queue())
    assert runtime._queue==[queued]
    runtime.connection.prompt.assert_not_awaited()
    assert store.get_session(s.id)==s and store.get_restart_handoff(r.id)==r
    assert not store.pending_restart_continuation(session_id=s.id)


def test_no_startup_recovery_publish_or_low_level_provider_native_load(tmp_path):
    store,r,s,a,m,_=fixture(tmp_path);seal(r,a,m)
    rt=AgentSessionRuntime(m,s,initial_transcript_seq=0)
    with patch('pa.instance.agent_session.AgentConnection',side_effect=AssertionError('spawn')):
        with pytest.raises(ValueError):asyncio.run(rt.start(resume_external_id=s.external_session_id,require_restore=True))
        with pytest.raises(ValueError):asyncio.run(m.create_session(existing=s))
        with pytest.raises(ValueError):asyncio.run(m.create_session(context_source_session_id=s.id))
        with pytest.raises(ValueError):asyncio.run(m.create_session(initiating_workflow={
            'kind':'conversation_branch','strategy':'copy','source_session_id':s.id}))
        with pytest.raises(ValueError):asyncio.run(m.recover_session(s.id))
        with pytest.raises(ValueError):m.enqueue_prompt('x',session_id=s.id)
        with pytest.raises(ValueError):asyncio.run(m.prompt('x',session_id=s.id))
    rt.close=AsyncMock()
    with pytest.raises(ValueError):asyncio.run(m._publish_runtime(rt))
    rt.close.assert_awaited_once_with(reason='recovery_delivery_fenced',reconcile_workspace=False)
    assert m.get(s.id) is None
    conn=AgentConnection(m.settings,store,agent_name='codex')
    with patch.object(conn,'_resolved_spec',side_effect=AssertionError('provider launch')):
        with pytest.raises(ValueError):asyncio.run(conn.connect(existing_session=s,resume_external_id=s.external_session_id,require_restore=True))
        target=s.model_copy(update={'id':'new-target','initiating_workflow':{
            'kind':'conversation_branch','strategy':'native','source_session_id':s.id}})
        with pytest.raises(ValueError):asyncio.run(conn.connect(existing_session=target))
        alias=target.model_copy(update={'initiating_workflow':{'kind':'conversation_branch','strategy':'native',
            'source_session_id':'unsealed-alias','source_external_id':s.external_session_id}})
        with pytest.raises(ValueError):asyncio.run(conn.connect(existing_session=alias))
    conn.session=s;conn._conn=SimpleNamespace(prompt=AsyncMock())
    with pytest.raises(ValueError):asyncio.run(conn.prompt('must not send'))
    conn.session=s.model_copy(update={'id':'new-alias'})
    with pytest.raises(ValueError):asyncio.run(conn.prompt('alias must not send'))
    with pytest.raises(ValueError):asyncio.run(conn.connect(resume_external_id=s.external_session_id))
    conn._conn.prompt.assert_not_awaited()
    assert store.get_session(s.id)==s and store.get_restart_handoff(r.id)==r


def test_admission_reservation_excludes_start_during_fence_commit(tmp_path):
    store,r,s,a,m,_=fixture(tmp_path)
    async def run():
        real=m._offload
        async def interleave(operation,func,*args,**kwargs):
            if operation=='sqlite.recovery_fence_prepare':
                # Isolate the existing admission reservation from the additional
                # closed+dismissed fallback, which independently denies startup.
                with patch.object(store,'require_session_delivery_allowed'):
                    with pytest.raises(SessionAdmissionInProgress):
                        await m.create_session(existing=s)
            return await real(operation,func,*args,**kwargs)
        with patch.object(m,'_offload',side_effect=interleave):
            return await m.prepare_recovery_fence(r.id,**a)
    assert asyncio.run(run())['delivery_allowed'] is False
    assert s.id not in m._admitting_sessions


def test_guard_read_failure_is_not_permission(tmp_path):
    store,r,s,a,m,_=fixture(tmp_path);seal(r,a,m)
    rt=AgentSessionRuntime(m,s,initial_transcript_seq=0)
    with patch.object(store,'session_recovery_fence',side_effect=OSError('unavailable')):
        with pytest.raises(OSError):rt.enqueue('x')
        with pytest.raises(OSError):asyncio.run(rt.start())


def test_missing_preparation_receipt_never_authorizes_closed_dismissed_history(tmp_path):
    store,r,s,a,m,_=fixture(tmp_path)
    assert store.session_recovery_fence(s.id) is None
    with pytest.raises(ValueError,match='explicit audited close disposition'):
        asyncio.run(m.recover_session(s.id))
    # Even a future/unknown preparation failure before a durable receipt exists
    # cannot turn the absence of that receipt into provider-load permission.
    with patch.object(store,'prepare_session_recovery_fence',side_effect=OSError('commit unavailable')):
        with pytest.raises(OSError):seal(r,a,m)
    restarted=CardProjection(tmp_path/'pa.db')
    with pytest.raises(ValueError):restarted.require_session_delivery_allowed(s.id)
    assert restarted.get_restart_handoff(r.id)==r


def test_snapshot_recovery_bypasses_no_guard_and_retains_queue(tmp_path):
    store,r,s,a,m,_=fixture(tmp_path);seal(r,a,m)
    snap=SimpleNamespace(session_id=s.id)
    with patch('pa.instance.agent_session.AgentConnection',side_effect=AssertionError('native load')):
        with pytest.raises(ValueError):asyncio.run(m._resume_from_snapshot(snap,None))
    assert not m._admitting_sessions and store.get_session(s.id)==s


def test_prepare_and_read_routes_bind_principal_and_reject_fake_load_choice(tmp_path):
    from fastapi import HTTPException
    from pydantic import ValidationError
    from pa.modules.agent_chat import (SessionRecoveryFenceBody,
        prepare_session_recovery_fence,get_session_recovery_fence)
    store,r,s,a,m,_=fixture(tmp_path)
    fields={k:v for k,v in a.items() if k not in {'session_id','principal_id','owner_instance_id'}}
    with pytest.raises(ValidationError):SessionRecoveryFenceBody(**fields,load=True)
    body=SessionRecoveryFenceBody(**fields)
    req=MagicMock();req.app.state.ctx.settings.instance_id='owner'
    with patch('pa.modules.agent_chat._require_session_traffic_ready',return_value=m),patch('pa.modules.agent_chat.get_principal_id',return_value='foreign'):
        with pytest.raises(HTTPException):asyncio.run(prepare_session_recovery_fence(req,s.id,r.id,body))
    assert store.session_recovery_fence(s.id) is None
    with patch('pa.modules.agent_chat._require_session_traffic_ready',return_value=m),patch('pa.modules.agent_chat.get_principal_id',return_value='user:owner'):
        result=asyncio.run(prepare_session_recovery_fence(req,s.id,r.id,body))
        assert asyncio.run(get_session_recovery_fence(req,s.id,r.id))==result
    with patch('pa.modules.agent_chat._require_session_traffic_ready',return_value=m),patch('pa.modules.agent_chat.get_principal_id',return_value='foreign'):
        with pytest.raises(HTTPException):asyncio.run(get_session_recovery_fence(req,s.id,r.id))


def test_completion_reconciliation_does_not_dispose_historical_close(tmp_path):
    from pa.domain.models import TranscriptEvent
    store,r,s,a,m,_=fixture(tmp_path)
    store.append_transcript_events([TranscriptEvent(session_id=s.id,
        seq=store.next_transcript_seq(s.id),event_type='turn_completed',
        payload={'queued_prompt_id':r.continuation_prompt_id})])
    observation=store.reconcile_restart_handoff(r.id,session_id=s.id,expected_version=r.phase_version,
        owner_instance_id='owner',principal_id='user:owner')
    assert observation['effect']=='confirmed'
    assert store.session_recovery_fence(s.id) is None
    with pytest.raises(ValueError):asyncio.run(m.recover_session(s.id))
    assert store.get_session(s.id)==s


def test_native_seal_survives_missing_session_projection_and_invalid_receipt(tmp_path):
    store,r,s,a,m,_=fixture(tmp_path);seal(r,a,m)
    # Disposable corruption/retention simulation; never an operational repair.
    with store._conn() as conn:
        conn.execute('DELETE FROM agent_sessions WHERE id=?',(s.id,))
        conn.execute("UPDATE session_recovery_fences SET receipt_json='null' WHERE session_id=?",(s.id,))
    restarted=CardProjection(tmp_path/'pa.db')
    with pytest.raises(ValueError):restarted.require_session_delivery_allowed(s.id)
    with pytest.raises(ValueError):restarted.require_native_history_delivery_allowed(s.external_session_id)


def test_active_native_alias_prevents_preparation(tmp_path):
    store,r,s,a,m,_=fixture(tmp_path)
    store.save_session(s.model_copy(update={'id':'native-alias'}))
    m._runtimes['native-alias']=SimpleNamespace(_closed=False)
    with pytest.raises(SessionAdmissionInProgress):seal(r,a,m)
    assert store.session_recovery_fence(s.id) is None

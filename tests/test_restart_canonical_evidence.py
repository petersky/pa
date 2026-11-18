import json
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from pa.domain.models import AgentSession, TranscriptEvent
from pa.domain.projection import CardProjection
from pa.instance.restart_evidence import inspect_restart_evidence
from tests.test_restart_transition_cas import receipt


def setup(tmp_path, *, dismissed=False):
    store = CardProjection(tmp_path/'pa.db')
    session = store.save_session(AgentSession(id='exact-session', agent_name='codex',
        principal_id='principal', authority_instance_id='owner', status='idle',
        external_session_id='native', execution_binding={'base_sha':'original'},
        model_id='saved-model', mode_id='saved-mode',
        config_json={'secret':'PRIVATE_CONFIG','durable_runtime':{
            'queued_prompts':[{'id':'queued-original','text':'PRIVATE_PROMPT'}]}}))
    handoff = receipt(store, status='failed', failure_stage='quiescing', attempts=0)
    if dismissed:
        handoff = store.dismiss_restart_handoff(handoff.id, session_id=handoff.session_id)
    # Disposable legacy fixture: production now prevents closing a session with
    # a pending restart, but retained historical rows still require inspection.
    with store._conn() as conn:
        conn.execute("UPDATE agent_sessions SET status='closed' WHERE id=?",(session.id,))
    session=store.get_session(session.id)
    args = dict(owner_instance_id='owner', principal_id='principal', is_admin=False,
        expected_version=handoff.phase_version, expected_attempt=0,
        expected_session_version=session.updated_at, expected_native_id='native')
    return store, handoff, session, args


def read(store, handoff, args, **changes):
    return inspect_restart_evidence(store, handoff.session_id, handoff.id, **(args|changes))


def event(store, handoff, seq, kind='turn_completed', **payload):
    store.append_transcript_events([TranscriptEvent(session_id=handoff.session_id, seq=seq,
        event_type=kind, payload=payload)])


@pytest.mark.parametrize('dismissed', [False,True])
def test_read_preserves_closed_session_receipt_queue_and_audit(tmp_path,dismissed):
    store,h,s,args=setup(tmp_path,dismissed=dismissed)
    event(store,h,0,queued_prompt_id=h.continuation_prompt_id,private='PRIVATE_ANSWER')
    before=store.list_transcript_events(s.id)
    with patch.object(store,'get_session',side_effect=AssertionError('private config read')), \
         patch.object(store,'get_restart_handoff',side_effect=AssertionError('private prompt read')), \
         patch.object(store.transcripts,'_event',side_effect=AssertionError('body hydration')):
        result=read(store,h,args)
    assert result['evidence']['prompt_completion_observed']
    assert not result['evidence']['attempt_completion_observed']
    assert result['historical_effect']=='unknown'
    assert all(result[k] is False for k in ('receipt_changed','audit_written','restored','delivery_allowed','replayed'))
    assert 'PRIVATE' not in json.dumps(result)
    assert store.get_session(s.id)==s and store.get_restart_handoff(h.id)==h
    assert store.list_transcript_events(s.id)==before


@pytest.mark.parametrize('payload,expected', [
    ({},False), ({'attempt':0},False), ({'handoff_id':'other','attempt':0},False),
    ({'attempt':1,'handoff_id':'HANDOFF'},False),
    ({'attempt':False,'handoff_id':'HANDOFF'},False),
    ({'attempt':0,'handoff_id':'HANDOFF'},True),
])
def test_prompt_completion_and_attempt_provenance_are_separate(tmp_path,payload,expected):
    store,h,_,args=setup(tmp_path)
    if payload.get('handoff_id')=='HANDOFF':payload=dict(payload,handoff_id=h.id)
    event(store,h,1,queued_prompt_id=h.continuation_prompt_id,**payload)
    evidence=read(store,h,args)['evidence']
    assert evidence['prompt_completion_observed']
    assert evidence['attempt_completion_observed'] is expected


def test_wrong_prompt_and_acceptance_never_become_completion(tmp_path):
    store,h,_,args=setup(tmp_path)
    event(store,h,0,queued_prompt_id='other',handoff_id=h.id,attempt=0)
    event(store,h,1,'queue_enqueued',id=h.continuation_prompt_id,text='PRIVATE_PROMPT')
    event(store,h,2,id=h.continuation_prompt_id)  # wrong identity field for completion
    r=read(store,h,args)
    assert not r['evidence']['prompt_completion_observed']
    assert r['evidence']['events'][0]['event_type']=='queue_enqueued'
    assert r['evidence']['absence_means']=='unknown'


def test_cold_malformed_and_oversized_payloads_stay_unknown_without_body_access(tmp_path):
    store,h,_,args=setup(tmp_path)
    event(store,h,0,queued_prompt_id=h.continuation_prompt_id,private='PRIVATE'*10000)
    event(store,h,1,queued_prompt_id='other')
    event(store,h,2,queued_prompt_id='other')
    with store.transcripts._conn() as conn:
        conn.execute("UPDATE transcript_events SET hot_payload='{' WHERE seq=1")
        conn.execute('UPDATE transcript_events SET hot_payload=? WHERE seq=2',('PRIVATE'*10000,))
    with patch.object(store.transcripts,'_event',side_effect=AssertionError('cold read')):
        r=read(store,h,args)
    assert r['evidence']['uninspectable_rows']==3
    assert not r['evidence']['events'] and r['historical_effect']=='unknown'
    assert r['retention_complete']=='unknown' and 'PRIVATE' not in json.dumps(r)


def test_bounded_pages_include_sequence_zero_and_reject_changed_population(tmp_path):
    store,h,_,args=setup(tmp_path)
    for i in range(4):event(store,h,i,'queue_enqueued',id='other')
    event(store,h,4,queued_prompt_id=h.continuation_prompt_id)
    first=read(store,h,args,limit=2)['evidence']
    assert first['rows_examined']==2 and first['next_after_seq']==1
    with pytest.raises(ValueError):read(store,h,args,after_seq=1,limit=2)
    second=read(store,h,args,after_seq=1,limit=2,expected_snapshot=first['snapshot'])['evidence']
    third=read(store,h,args,after_seq=second['next_after_seq'],limit=2,expected_snapshot=first['snapshot'])['evidence']
    assert third['prompt_completion_observed'] and not third['truncated']
    event(store,h,5,'queue_enqueued',id='other')
    with pytest.raises(ValueError,match='snapshot changed'):
        read(store,h,args,after_seq=1,expected_snapshot=first['snapshot'])


@pytest.mark.parametrize('change,error', [
    ({'principal_id':'foreign'},PermissionError),
    ({'principal_id':'','is_admin':True},PermissionError),
    ({'owner_instance_id':'foreign'},ValueError),
    ({'expected_version':999},ValueError), ({'expected_attempt':1},ValueError),
    ({'expected_native_id':''},ValueError),
    ({'expected_session_version':datetime(2000,1,1,tzinfo=UTC)},ValueError),
    ({'limit':129},ValueError), ({'after_seq':-2},ValueError),
    ({'after_seq':2**63,'expected_snapshot':'0'*64},ValueError),
])
def test_identity_auth_and_capacity_fences(tmp_path,change,error):
    store,h,s,args=setup(tmp_path)
    with pytest.raises(error):read(store,h,args,**change)
    assert store.get_session(s.id)==s and store.get_restart_handoff(h.id)==h


def test_identity_race_fails_closed(tmp_path):
    store,h,_,args=setup(tmp_path)
    original=store.restart_evidence_identity
    calls=0
    def changed(*a):
        nonlocal calls
        calls+=1
        r=original(*a)
        return r if calls==1 else dict(r,attempts=7)
    with patch.object(store,'restart_evidence_identity',side_effect=changed):
        with pytest.raises(ValueError,match='changed during read'):read(store,h,args)


def test_http_is_authenticated_bounded_no_store_and_sanitizes_unavailable(tmp_path):
    from pa.modules.agent_chat import router
    store,h,s,args=setup(tmp_path)
    manager=SimpleNamespace(store=store)
    app=FastAPI();app.state.ctx=SimpleNamespace(settings=SimpleNamespace(instance_id='owner'))
    app.include_router(router)
    url=f'/agent/observability/v1/sessions/{s.id}/restart-handoffs/{h.id}/canonical-evidence'
    params={k:v for k,v in args.items()if k.startswith('expected_')}
    params.update(expected_authority_instance_id='owner',expected_session_version=s.updated_at.isoformat())
    with patch('pa.modules.agent_chat._manager',return_value=manager),patch('pa.modules.agent_chat.get_principal_id',return_value='principal'),TestClient(app) as client:
        assert client.get(url).status_code==422
        assert client.get(url,params=params|{'limit':129}).status_code==422
        response=client.get(url,params=params)
        assert response.status_code==200 and response.headers['cache-control']=='private, no-store'
        with patch.object(store,'restart_evidence_identity',side_effect=OSError('PRIVATE')):
            failure=client.get(url,params=params)
        assert failure.status_code==503 and 'PRIVATE' not in failure.text
        assert failure.headers['cache-control']=='private, no-store'
        with patch('pa.modules.agent_chat.get_principal_id',return_value='foreign'):
            assert client.get(url,params=params).status_code==403


def test_mcp_proxy_is_get_only_and_forwards_exact_empty_native():
    from tests.test_instance_mcp import FakeMcp
    from pa.mcp.tools.agent_chat import register_mcp
    mcp=FakeMcp()
    with patch('pa.mcp.local_api.request_local_pa',return_value={'historical_effect':'unknown'})as call:
        register_mcp(mcp,SimpleNamespace(settings='settings'))
        mcp.functions['get_restart_canonical_evidence']('s/path','h/path','owner',6,1,'version','')
    assert call.call_args.args[1]=='GET'
    assert 's%2Fpath/restart-handoffs/h%2Fpath/canonical-evidence' in call.call_args.args[2]
    assert call.call_args.kwargs['params']['expected_native_id']==''
    assert call.call_args.kwargs['max_response_bytes']==128*1024

import asyncio
import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from fastapi import HTTPException
from pa.domain.models import AgentSession
from pa.domain.projection import CardProjection
from pa.instance.binding_inspection import inspect_binding, fingerprint, binding_metadata


def fixture(tmp_path, *, native_id='native', base='a'*40):
    store = CardProjection(tmp_path / 'pa.db')
    binding = {'version': 1, 'repository_ids': ['repo'], 'lease_ids': ['lease'],
        'worktree_paths': ['/original'], 'branch': 'pa/original', 'base_sha': base,
        'unknown_private': 'DO_NOT_EMIT'}
    session = store.save_session(AgentSession(id='s', agent_name='codex',
        principal_id='owner', authority_instance_id='instance', external_session_id=native_id,
        execution_binding=binding, status='recovery_blocked', model_id='saved-model',
        mode_id='saved-mode', config_json={'durable_runtime': {
            'in_flight': None, 'queued_prompts': [{'id': 'q1', 'text': 'PRIVATE_PROMPT'}, {'id': 'q2'}]}}))
    leases = [dict(id='lease', session_id='s', repository_id='repo', card_id='card',
        project_id='project', worktree_path='/original', branch='pa/original',
        base_sha='b'*40, fencing_token=373, state='ready', updated_at='version')]
    manager = SimpleNamespace(store=store, workspace_manager=SimpleNamespace(
        binding_inspection_leases=MagicMock(side_effect=lambda _: [dict(l) for l in leases])))
    args = dict(owner_instance_id='instance', principal_id='owner', is_admin=False,
        expected_session_version=session.updated_at, expected_native_id='native',
        expected_binding_hash=fingerprint(binding), expected_lease_hash=fingerprint(leases))
    return manager, session, leases, args


def test_exact_comparison_is_not_restore_or_delivery_authority(tmp_path):
    mgr, s, leases, args = fixture(tmp_path)
    with patch.object(mgr.store, 'save_session', side_effect=AssertionError('mutation')),patch.object(mgr.store, 'get_session', side_effect=AssertionError('private config read')):
        result = inspect_binding(mgr, 's', **args)
    assert result['different_fields'] == ['base_sha']
    assert result['restore_plan']['state'] == 'unsupported'
    assert result['historical_effect'] == 'unknown' and result['effect'] == 'none'
    assert not result['restored'] and not result['delivery_allowed'] and not result['mutation_authorized']
    assert 'DO_NOT_EMIT' not in json.dumps(result) and 'PRIVATE_PROMPT' not in json.dumps(result)
    assert mgr.store.get_session('s') == s
    assert mgr.store.list_transcript_events('s') == []


@pytest.mark.parametrize('change,error', [({'principal_id':'foreign'},PermissionError),
    ({'principal_id':None,'is_admin':True},PermissionError),
    ({'owner_instance_id':'foreign'},ValueError),
    ({'expected_native_id':''},ValueError),({'expected_binding_hash':'0'*64},ValueError),
    ({'expected_lease_hash':'0'*64},ValueError),
    ({'expected_session_version':datetime(2000,1,1,tzinfo=UTC)},ValueError),
    ({'history_limit':33},ValueError),({'history_offset':4097},ValueError)])
def test_preconditions_fail_closed(tmp_path, change, error):
    mgr,s,_,args=fixture(tmp_path)
    with pytest.raises(error):inspect_binding(mgr,'s',**(args|change))
    assert mgr.store.get_session('s')==s


@pytest.mark.parametrize('race', ['session','fence','new_lease','history'])
def test_concurrent_snapshot_change_rejects(tmp_path,race):
    mgr,s,leases,args=fixture(tmp_path)
    original=mgr.store.binding_inspection_history
    def read(*a,**k):
        result=original(*a,**k)
        if race=='session':
            changed=s.model_copy(update={'updated_at':s.updated_at+timedelta(seconds=1)});mgr.store.save_session(changed)
        elif race=='fence':leases[0]['fencing_token']+=1
        elif race=='new_lease':leases.append(dict(leases[0],id='another'))
        else:result['snapshot_count']+=mgr.workspace_manager.binding_inspection_leases.call_count
        return result
    with patch.object(mgr.store,'binding_inspection_history',side_effect=read):
        with pytest.raises(ValueError,match='snapshot changed'):inspect_binding(mgr,'s',**args)


def test_missing_native_cleaned_original_and_repository_alias_remain_distinct(tmp_path):
    mgr,s,leases,args=fixture(tmp_path,native_id=None,base='pending')
    leases[0]['state']='cleaned'
    leases.append(dict(leases[0],id='new',repository_id='different',state='ready',worktree_path='/new',branch='pa/new'))
    args.update(expected_session_version=s.updated_at,expected_native_id='',
        expected_binding_hash=fingerprint(s.execution_binding),expected_lease_hash=fingerprint(leases))
    result=inspect_binding(mgr,'s',**args)
    assert {'native_identity_missing','original_lease_cleaned','original_base_unresolved'} <= set(result['restore_plan']['gaps'])
    assert len(result['leases'])==2 and not result['delivery_allowed']
    assert len(result['different_fields'])==5 and result['comparison_lease_id']=='new'
    assert result['original_lease_ids_observed']==['lease']


def test_history_paging_and_malformed_oversized_rows_are_explicit(tmp_path):
    mgr,s,_,_=fixture(tmp_path)
    # Disposable DB only: insert adversarial retained history without production mutation.
    with mgr.store._conn() as c:
        for n in range(35):
            raw='[' if n==0 else json.dumps({'base_sha':'secret'*4000}) if n==1 else json.dumps({'base_sha':'a'*40,'private':'DO_NOT_EMIT'})
            c.execute('INSERT INTO agent_execution_binding_history VALUES(?,?,?,?,?,?)',
                (str(n).zfill(3),'s','untrusted private reason',raw,raw,'2026-01-01'))
    page=mgr.store.binding_inspection_history('s',limit=32,offset=0)
    assert len(page['items'])==32 and page['next_offset']==32 and page['truncated']
    assert page['items'][0]['binding']['state']=='malformed'
    assert page['items'][1]['binding']['state']=='oversized'
    assert page['retention_complete']=='unknown'
    assert 'DO_NOT_EMIT' not in json.dumps(page) and 'private reason' not in json.dumps(page)
    assert len(mgr.store.binding_inspection_history('s',limit=32,offset=32)['items'])==3


def test_binding_shape_is_bounded_and_allowlisted():
    from pa.instance.binding_inspection import decode_binding
    assert decode_binding('['*33+']'*33)['state']=='depth_limit'
    assert binding_metadata({'base_sha':{'answer':'secret'}})['state']=='unsupported_shape'
    assert binding_metadata({'lease_ids':['a']*33})['state']=='unsupported_shape'
    assert binding_metadata({'private':'x'*20000})['state']=='unsupported_shape'


def test_multiple_live_leases_are_not_arbitrarily_joined(tmp_path):
    mgr,s,leases,args=fixture(tmp_path)
    leases.append(dict(leases[0],id='other',repository_id='other'))
    args['expected_lease_hash']=fingerprint(leases)
    result=inspect_binding(mgr,'s',**args)
    assert result['comparison_lease_id'] is None
    assert 'comparison_lease_ambiguous' in result['restore_plan']['gaps']
    assert result['original_lease_ids_observed']==['lease']


def test_route_identity_and_errors_never_load_runtime(tmp_path):
    from pa.modules.agent_chat import inspect_session_workspace_binding
    mgr,s,leases,args=fixture(tmp_path)
    request=MagicMock();request.app.state.ctx.settings.instance_id='instance'
    request.state.user=SimpleNamespace(role='user')
    call=dict(expected_authority_instance_id='instance',
        expected_session_version=s.updated_at,expected_native_id='native',
        expected_binding_hash=args['expected_binding_hash'],expected_lease_hash=args['expected_lease_hash'],
        history_limit=16,history_offset=0)
    async def offload(manager,label,fn,*a,**kw):
        kw.pop('timeout',None);return fn(*a,**kw)
    with patch('pa.modules.agent_chat._manager',return_value=mgr),patch('pa.modules.agent_chat._offload',side_effect=offload),patch('pa.modules.agent_chat.get_principal_id',return_value='foreign'):
        with pytest.raises(HTTPException) as e:asyncio.run(inspect_session_workspace_binding(request,'s',**call))
    assert e.value.status_code==403 and mgr.store.get_session('s')==s


def test_http_preconditions_privacy_and_no_store(tmp_path):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from pa.modules.agent_chat import router
    mgr,s,leases,args=fixture(tmp_path)
    app=FastAPI();app.state.ctx=SimpleNamespace(settings=SimpleNamespace(instance_id='instance'))
    app.include_router(router)
    params={'expected_authority_instance_id':'instance','expected_session_version':s.updated_at.isoformat(),
        'expected_native_id':'native','expected_binding_hash':args['expected_binding_hash'],
        'expected_lease_hash':args['expected_lease_hash']}
    with patch('pa.modules.agent_chat._manager',return_value=mgr),patch('pa.modules.agent_chat.get_principal_id',return_value='owner'),TestClient(app) as client:
        url='/agent/observability/v1/sessions/s/workspace-binding'
        assert client.get(url).status_code==422
        assert client.get(url,params=params|{'history_limit':33}).status_code==422
        assert client.get(url,params=params|{'expected_authority_instance_id':'other'}).status_code==409
        assert client.get(url,params=params|{'expected_lease_hash':'0'*64}).status_code==409
        response=client.get(url,params=params)
        with patch.object(mgr.store,'binding_inspection_history',side_effect=OSError('PRIVATE_ERROR')):
            unavailable=client.get(url,params=params)
    assert response.status_code==200
    assert response.headers['cache-control']=='private, no-store'
    assert response.json()['different_fields']==['base_sha']
    assert 'PRIVATE_PROMPT' not in response.text and 'DO_NOT_EMIT' not in response.text
    assert mgr.store.get_session('s')==s
    assert unavailable.status_code==503 and 'unknown' in unavailable.text
    assert 'PRIVATE_ERROR' not in unavailable.text


def test_lease_reader_limits_before_decoding_unrelated_proofs(tmp_path):
    from pa.config import Settings
    from pa.repository.workspace import WorkspaceManager, WorkspaceLease
    manager=WorkspaceManager(Settings(data_dir=tmp_path/'data',workspace_root=tmp_path/'trees'),MagicMock())
    lease=WorkspaceLease(id='lease',repository_id='r',repository_url='test',session_id='s',
        cache_path='/cache',worktree_path='/tree',branch='pa/test',base_sha='a'*40,fencing_token=1,
        expires_at=datetime.now(UTC)+timedelta(days=1))
    for i in range(32):manager._save(lease.model_copy(update={'id':str(i),'repository_id':str(i)}))
    rows=manager.binding_inspection_leases('s')
    assert len(rows)==32 and 'cleanup_evidence' not in rows[0]
    assert manager.binding_inspection_leases('other')==[]
    manager._save(lease.model_copy(update={'id':'33','repository_id':'33'}))
    with pytest.raises(ValueError,match='capacity'):manager.binding_inspection_leases('s')


def test_mcp_forwards_exact_read_preconditions_without_mutation():
    from pa.mcp.tools.agent_chat import register_mcp
    from tests.test_instance_mcp import FakeMcp
    mcp=FakeMcp()
    with patch('pa.mcp.local_api.request_local_pa',return_value={'effect':'none'}) as call:
        register_mcp(mcp,SimpleNamespace(settings='settings'))
        result=mcp.functions['get_agent_session_workspace_binding']('s/path','owner','version','', 'a'*64,'b'*64,8,16)
    assert result=={'effect':'none'}
    assert call.call_args.args[:2]==('settings','GET')
    assert 's%2Fpath/workspace-binding' in call.call_args.args[2]
    assert call.call_args.kwargs['params']['expected_native_id']==''
    assert call.call_args.kwargs['params']['history_offset']==16

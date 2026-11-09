import asyncio
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from fastapi import HTTPException

from pa.instance import wire_metadata as wire
from pa.modules.agent_chat import session_wire_metadata

SID = '1eb4393e-26a0-471b-9b01-0b96763d2c01'
NATIVE = '01a1433b-d51c-7a21-9e94-840dabca18c9'
PID = 'restart-handoff:be010d09-11db-54e2-8415-262746e3cc84'
MESSAGE = '49cf1266-51ed-45bb-9865-256bd0e5e48a'


def inspect(root):
    return wire.read_wire_metadata(root, session_id=SID, native_id=NATIVE, prompt_id=PID, attempt=1)


def artifact(root, rows, name='wire.jsonl'):
    directory = root / 'sessions' / SID
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / name
    path.write_bytes(b''.join(json.dumps(r).encode()+b'\n' for r in rows))
    return path


def request(**extra):
    return {'method':'session/prompt', 'direction':'out', 'ts':'2026-11-08T14:30:16Z',
        'params':{'session_id':NATIVE, 'message_id':MESSAGE, 'message':'SECRET PROMPT', **extra}}


def completed():
    return {'method':'session/prompt', 'direction':'in', 'ts':'2026-11-08T14:30:17Z',
        'session_id':NATIVE, 'message_id':MESSAGE,
        'result':{'stop_reason':'end_turn', 'message':'SECRET ANSWER', 'usage':{'secret':'SECRET'}}}


def test_explicit_exact_attempt_link_and_privacy(tmp_path):
    path = artifact(tmp_path, [request(queued_prompt_id=PID, attempt=1), completed()])
    result = inspect(tmp_path)
    assert result['effect'] == 'completed' and result['correlation'] == 'exact'
    assert result['historical_coverage'] == 'unknown' and not result['delivery_allowed']
    assert result['coverage'] == 'observed_fixed_files_complete'
    assert result['artifacts'][0]['sha256'] == hashlib.sha256(path.read_bytes()).hexdigest()
    assert 'SECRET' not in json.dumps(result)


@pytest.mark.parametrize('fields', [{}, {'queued_prompt_id':PID}, {'queued_prompt_id':PID, 'attempt':2}])
def test_missing_or_foreign_attempt_link_never_inferred(tmp_path, fields):
    artifact(tmp_path, [request(**fields), completed()])
    assert inspect(tmp_path)['effect'] == 'unknown'


def test_outgoing_request_is_not_submission_and_explicit_ack_is(tmp_path):
    rows = [request(queued_prompt_id=PID, attempt=1)]
    artifact(tmp_path, rows)
    assert inspect(tmp_path)['effect'] == 'unknown'
    ack = completed(); ack['result'] = {'acknowledged':True}
    artifact(tmp_path, rows+[ack])
    assert inspect(tmp_path)['effect'] == 'submitted'


def test_foreign_native_records_do_not_settle_target(tmp_path):
    row = request(queued_prompt_id=PID, attempt=1)
    row['params']['session_id'] = str(uuid4())
    artifact(tmp_path, [row, completed()])
    assert inspect(tmp_path)['effect'] == 'unknown'


def test_missing_and_partial_and_malformed_coverage(tmp_path):
    assert inspect(tmp_path)['coverage'] == 'missing'
    path = artifact(tmp_path, [request()])
    with path.open('ab') as f:
        f.write(b'{bad}\n'+b'x'*(wire.MAX_LINE+1)+b'\n'+b'{"partial":')
    r = inspect(tmp_path)
    assert r['coverage'] == 'partial'
    assert r['malformed_records'] == 1 and r['unparsed_records'] == 2
    assert r['effect'] == 'unknown'


def test_adversarial_json_shapes_are_redacted(tmp_path):
    artifact(tmp_path, [{'method':[]}, {'method':'session/prompt','direction':[]},
        {'method':'session/prompt','direction':'in','result':{'stop_reason':[]}},
        request(message_id='sk-secret-token', queued_prompt_id='private answer',
                headers={'Authorization':'SECRET'})])
    r = inspect(tmp_path)
    assert 'SECRET' not in json.dumps(r) and 'sk-secret' not in json.dumps(r)
    assert r['effect'] == 'unknown'


@pytest.mark.parametrize('part', ['file', 'session', 'sessions', 'root'])
def test_symlinks_never_followed(tmp_path, part):
    actual = tmp_path / 'actual'; actual.mkdir()
    artifact(actual, [request(queued_prompt_id=PID,attempt=1), completed()])
    root = tmp_path / 'selected'; root.mkdir()
    if part == 'root':
        root.rmdir(); root.symlink_to(actual, target_is_directory=True)
    elif part == 'sessions':
        (root/'sessions').symlink_to(actual/'sessions', target_is_directory=True)
    elif part == 'session':
        (root/'sessions').mkdir(); (root/'sessions'/SID).symlink_to(actual/'sessions'/SID, target_is_directory=True)
    else:
        (root/'sessions'/SID).mkdir(parents=True)
        (root/'sessions'/SID/'wire.jsonl').symlink_to(actual/'sessions'/SID/'wire.jsonl')
    r=inspect(root)
    assert r['coverage'] == 'unavailable' and r['effect'] == 'unknown'


def test_path_attack_and_nonregular_file(tmp_path):
    with pytest.raises(ValueError):
        wire.read_wire_metadata(tmp_path, session_id='../escape', native_id=NATIVE, prompt_id=PID, attempt=1)
    (tmp_path/'sessions'/SID/'wire.jsonl').mkdir(parents=True)
    assert inspect(tmp_path)['coverage'] == 'unavailable'


def test_byte_record_output_limits_and_rotation(tmp_path, monkeypatch):
    monkeypatch.setattr(wire,'MAX_BYTES',200)
    artifact(tmp_path,[request()]*30)
    r=inspect(tmp_path)
    assert r['coverage']=='partial' and sum(a.get('bytes_read',0) for a in r['artifacts']) <= 200
    monkeypatch.setattr(wire,'MAX_BYTES',4*1024*1024)
    monkeypatch.setattr(wire,'MAX_OUTPUT',1)
    artifact(tmp_path,[request(),request()], 'wire.jsonl.1')
    r=inspect(tmp_path)
    assert r['output_truncated'] and len(r['records'])==1
    assert r['historical_coverage']=='unknown'
    monkeypatch.setattr(wire,'MAX_RECORDS',1)
    assert inspect(tmp_path)['coverage']=='partial'


@pytest.mark.parametrize('paged',[False,True])
@pytest.mark.parametrize('assess_unparsed',[False,True])
@pytest.mark.parametrize('change,status', [('principal',403),('owner',409),('native',409),('version',409),('attempt',409)])
def test_route_fences_before_artifact_access(tmp_path,change,status,paged,assess_unparsed):
    session=SimpleNamespace(id=SID,principal_id='user:owner',authority_instance_id='owner',external_session_id=NATIVE)
    receipt=SimpleNamespace(id='receipt',session_id=SID,instance_id='owner',phase_version=6,attempts=1,continuation_prompt_id=PID,status='dismissed')
    req=MagicMock();req.state.user.role='member'
    req.app.state.ctx.settings.instance_id='owner';req.app.state.ctx.settings.data_dir=tmp_path
    mgr=MagicMock();mgr.store.get_session.return_value=session;mgr.store.get_restart_handoff.return_value=receipt
    principal='user:other' if change=='principal' else 'user:owner'
    args=dict(expected_native_id=NATIVE,expected_version=6,expected_attempt=1,paged=paged,
              assess_unparsed=assess_unparsed)
    if change=='owner':receipt.instance_id='foreign'
    if change=='native':args['expected_native_id']='foreign'
    if change=='version':args['expected_version']=7
    if change=='attempt':args['expected_attempt']=2
    with patch('pa.modules.agent_chat._manager',return_value=mgr),patch('pa.modules.agent_chat.get_principal_id',return_value=principal),patch.object(wire,'read_wire_metadata') as read:
        with pytest.raises(HTTPException) as e:
            asyncio.run(session_wire_metadata(req,SID,'receipt',**args))
        assert e.value.status_code==status
        read.assert_not_called()


def test_tail_ranges_and_partial_line_are_honest(tmp_path, monkeypatch):
    monkeypatch.setattr(wire,'MAX_ARTIFACT_BYTES',512)
    path=artifact(tmp_path,[request()]*50+[request(queued_prompt_id=PID,attempt=1),completed()])
    result=inspect(tmp_path)
    a=result['artifacts'][0]
    assert a['byte_start']>0 and a['byte_end']==path.stat().st_size
    assert a['sha256']==hashlib.sha256(path.read_bytes()[a['byte_start']:a['byte_end']]).hexdigest()
    assert result['coverage']=='partial' and result['historical_coverage']=='unknown'


def test_unavailable_artifact_open_and_mutation_are_reported(tmp_path,monkeypatch):
    path=artifact(tmp_path,[request()])
    original=wire.os.fstat
    calls=0
    def changed(fd):
        nonlocal calls
        calls+=1
        s=original(fd)
        if calls==2:
            from types import SimpleNamespace
            return SimpleNamespace(st_size=s.st_size+1,st_mtime_ns=s.st_mtime_ns,st_ino=s.st_ino)
        return s
    monkeypatch.setattr(wire.os,'fstat',changed)
    assert inspect(tmp_path)['artifacts'][0]['changed_during_read']


def test_mcp_uses_get_fixed_route_and_bounded_response():
    from tests.test_agent_chat_mcp import FakeMcp
    from pa.mcp.tools.agent_chat import register_mcp
    mcp=FakeMcp()
    with patch('pa.mcp.local_api.request_local_pa',return_value={}) as api:
        register_mcp(mcp,SimpleNamespace(settings=SimpleNamespace()))
        mcp.functions['get_agent_session_wire_metadata'](SID,'receipt',NATIVE,6,1)
    assert api.call_args.args[1]=='GET'
    assert api.call_args.kwargs['max_response_bytes']==512*1024
    assert api.call_args.kwargs['params']['expected_attempt']==1


def test_authorized_route_observes_without_mutation_and_revalidates(tmp_path):
    from pa.domain.models import AgentSession, RestartHandoff
    session=AgentSession(id=SID,agent_name='codex',status='closed',principal_id='user:owner',
        authority_instance_id='owner',external_session_id=NATIVE)
    receipt=RestartHandoff(id='receipt',session_id=SID,idempotency_key='key',instance_id='owner',
        phase_version=6,attempts=1,continuation_prompt_id=PID,status='dismissed')
    req=MagicMock();req.state.user.role='member'
    req.app.state.ctx.settings.instance_id='owner';req.app.state.ctx.settings.data_dir=tmp_path
    mgr=MagicMock();mgr.store.get_session.return_value=session;mgr.store.get_restart_handoff.return_value=receipt
    with patch('pa.modules.agent_chat._manager',return_value=mgr),patch('pa.modules.agent_chat.get_principal_id',return_value='user:owner'):
        result=asyncio.run(session_wire_metadata(req,SID,'receipt',NATIVE,6,1))
        assert result['coverage']=='missing' and result['effect']=='unknown'
        assert result['receipt_status']=='dismissed'
        mgr.store.save_session.assert_not_called()
        mgr.store.update_restart_handoff.assert_not_called()
        mgr.create_session.assert_not_called()
        mgr.store.get_session.side_effect=[session,session.model_copy(update={'principal_id':'foreign'})]
        with pytest.raises(HTTPException) as e:
            asyncio.run(session_wire_metadata(req,SID,'receipt',NATIVE,6,1))
        assert e.value.status_code==409


def test_native_update_turn_identity_without_tool_content(tmp_path):
    artifact(tmp_path,[{'method':'session/update','direction':'in',
        'params':{'session_id':NATIVE,'update':{'sessionUpdate':'tool_call',
        'turnId':'turn_abc123','rawInput':{'private':'SECRET'},'content':'SECRET'}}}])
    result=inspect(tmp_path)
    assert result['records'][0]['turn_id']=='turn_abc123'
    assert result['records'][0]['update_type']=='tool_call'
    assert 'SECRET' not in json.dumps(result)
    assert result['effect']=='unknown'

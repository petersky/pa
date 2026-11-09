import json
from unittest.mock import patch

import pytest

from pa.instance import wire_metadata_pages as pages
from tests.test_wire_metadata import SID,NATIVE,PID,MESSAGE,artifact,request,completed


def read(root, cursor=None, **changes):
    return pages.read_wire_metadata_page(root,session_id=SID,native_id=NATIVE,
        prompt_id=PID,attempt=1,scope='owner/receipt/v6',secret='test-only-secret',
        cursor=cursor,**changes)


def walk(root, **kwargs):
    result=[]; cursor=None
    for _ in range(10000):
        page=read(root,cursor,**kwargs)
        result.append(page)
        if not page['next_cursor']:return result
        assert page['next_cursor'] != cursor
        cursor=page['next_cursor']
    raise AssertionError('cursor did not exhaust')


def test_all_output_paged_and_replay_exact(tmp_path,monkeypatch):
    monkeypatch.setattr(pages,'MAX_OUTPUT',2)
    path=artifact(tmp_path,[request() for _ in range(11)])
    first=read(tmp_path)
    replay=read(tmp_path,first['next_cursor'])
    assert read(tmp_path,first['next_cursor'])==replay
    result=walk(tmp_path)
    rows=[r for p in result for r in p['records']]
    assert len(rows)==11
    assert len({r['byte_start'] for r in rows})==11
    assert [r['byte_start'] for r in rows]==sorted(r['byte_start'] for r in rows)
    assert result[-1]['totals']['scanned']==path.stat().st_size
    assert result[-1]['totals']['emitted']==11
    assert result[-1]['snapshot_exhausted'] and result[-1]['historical_coverage']=='unknown'
    assert len({p['snapshot_id'] for p in result})==1
    assert 'SECRET' not in json.dumps(result)


def test_earlier_bytes_boundaries_and_exact_byte_coverage(tmp_path,monkeypatch):
    monkeypatch.setattr(pages,'MAX_ARTIFACT_BYTES',512)
    path=artifact(tmp_path,[request() for _ in range(31)])
    expected=path.read_bytes()
    result=walk(tmp_path)
    ranges=[r for p in result for r in p['scanned_ranges']]
    import hashlib
    position=0
    for r in ranges:
        assert r['byte_start']==position
        assert r['sha256']==hashlib.sha256(expected[r['byte_start']:r['byte_end']]).hexdigest()
        position=r['byte_end']
    assert position==len(expected)
    assert sum(p['records_examined'] for p in result)==31
    assert sum(len(p['records']) for p in result)==31
    assert all(p['bytes_read']<=512 for p in result)
    assert any(p['lookahead_bytes'] for p in result)


def test_oversize_line_carries_no_content_across_cursor(tmp_path,monkeypatch):
    monkeypatch.setattr(pages,'MAX_ARTIFACT_BYTES',512)
    monkeypatch.setattr(pages,'MAX_LINE',300)
    path=artifact(tmp_path,[])
    path.write_bytes(b'{"private":"'+b'SECRET'*1000+b'"}\n'+json.dumps(request()).encode()+b'\n')
    result=walk(tmp_path)
    assert result[-1]['totals']['unparsed']==1
    assert result[-1]['totals']['scanned']==path.stat().st_size
    assert sum(len(p['records']) for p in result)==1
    assert 'SECRET' not in json.dumps(result)
    import base64
    for p in result:
        if p['next_cursor']:
            body=p['next_cursor'].split('.')[0]
            assert b'SECRET' not in base64.urlsafe_b64decode(body+'='*(-len(body)%4))


@pytest.mark.parametrize('change',['grow','truncate','replace','symlink','rotation','rewrite'])
@pytest.mark.parametrize('assess',[False,True])
def test_snapshot_changes_rejected(tmp_path,monkeypatch,change,assess):
    monkeypatch.setattr(pages,'MAX_OUTPUT',1)
    path=artifact(tmp_path,[request()]*3)
    page=read(tmp_path)
    if change=='grow':path.write_bytes(path.read_bytes()+b'{}\n')
    if change=='truncate':path.write_bytes(b'')
    if change=='replace':
        data=path.read_bytes();path.unlink();path.write_bytes(data)
    if change=='symlink':
        other=tmp_path/'other';other.write_bytes(path.read_bytes());path.unlink();path.symlink_to(other)
    if change=='rotation':artifact(tmp_path,[], 'wire.jsonl.1')
    if change=='rewrite':
        data=path.read_bytes();path.write_bytes(data.replace(b'SECRET',b'PUBLIC'))
    with pytest.raises((ValueError,OSError)):
        read(tmp_path,page['next_cursor'],assess_unparsed=assess)


def test_cursor_scope_tamper_and_selection_fences(tmp_path,monkeypatch):
    monkeypatch.setattr(pages,'MAX_OUTPUT',1)
    artifact(tmp_path,[request()]*3)
    page=read(tmp_path);cursor=page['next_cursor']
    with pytest.raises(ValueError):read(tmp_path,cursor+'bad')
    with pytest.raises(ValueError):read(tmp_path,cursor,selection='outcomes')
    with pytest.raises(ValueError):
        pages.read_wire_metadata_page(tmp_path,session_id=SID,native_id=NATIVE,prompt_id=PID,
            attempt=2,scope='owner/receipt/v6',secret='test-only-secret',cursor=cursor)
    with pytest.raises(ValueError):
        pages.read_wire_metadata_page(tmp_path,session_id=SID,native_id=NATIVE,prompt_id=PID,
            attempt=1,scope='foreign/receipt/v6',secret='test-only-secret',cursor=cursor)


def test_known_link_crosses_output_pages_no_false_legacy_link(tmp_path,monkeypatch):
    monkeypatch.setattr(pages,'MAX_OUTPUT',1)
    artifact(tmp_path,[request(queued_prompt_id=PID,attempt=1),completed()])
    result=walk(tmp_path)
    assert result[-1]['effect']=='completed'
    assert result[1]['records'][0]['correlation']=='exact'
    artifact(tmp_path,[request(),completed()])
    assert walk(tmp_path)[-1]['effect']=='unknown'


def test_missing_rotations_malformed_eof_and_filtered_scope(tmp_path):
    path=artifact(tmp_path,[request()])
    with path.open('ab') as f:f.write(b'bad\n{"unfinished":')
    result=walk(tmp_path)[-1]
    assert result['totals']['malformed']==1 and result['totals']['unparsed']==1
    assert result['artifacts'][1]['state']=='missing'
    assert result['coverage']=='snapshot_exhausted' and result['effect']=='unknown'
    assert result['historical_coverage']=='unknown'


def test_outcomes_selection_scan_accounts_omitted_updates(tmp_path):
    rows=[{'method':'session/update','direction':'in','params':{'session_id':NATIVE,
        'update':{'sessionUpdate':'agent_message_chunk','content':'SECRET'}}} for _ in range(10)]
    path=artifact(tmp_path,rows+[request(),completed()])
    result=walk(tmp_path,selection='outcomes')[-1]
    assert result['totals']['selection_filtered']==10
    assert result['totals']['emitted']==2
    assert result['totals']['scanned']==path.stat().st_size
    assert result['output_coverage']=='all_selected_metadata_in_scanned_ranges'
    assert 'SECRET' not in json.dumps(result)


def test_record_budget_makes_progress(tmp_path,monkeypatch):
    monkeypatch.setattr(pages,'MAX_RECORDS',2)
    artifact(tmp_path,[request()]*11)
    result=walk(tmp_path)
    assert all(p['records_examined']<=2 for p in result)
    assert result[-1]['totals']['examined']==11


def test_change_during_page_rejected(tmp_path,monkeypatch):
    path=artifact(tmp_path,[request()])
    original=pages.os.pread
    def replace(fd,size,offset):
        value=original(fd,size,offset)
        path.write_bytes(path.read_bytes()+b'{}\n')
        return value
    monkeypatch.setattr(pages.os,'pread',replace)
    with pytest.raises(ValueError):read(tmp_path)


def test_conflicting_prompt_identity_cannot_reuse_provider_link(tmp_path,monkeypatch):
    monkeypatch.setattr(pages,'MAX_OUTPUT',1)
    artifact(tmp_path,[request(queued_prompt_id=PID,attempt=1),
        request(queued_prompt_id='restart-handoff:11111111-1111-1111-1111-111111111111',attempt=2),completed()])
    result=walk(tmp_path)
    assert result[-1]['effect']=='unknown'
    assert result[-1]['conflicting_message_ids']==1
    assert result[-1]['records'][0]['correlation']=='unknown'


def test_readable_empty_snapshot_is_exhausted_without_historical_claim(tmp_path):
    artifact(tmp_path,[])
    result=read(tmp_path)
    assert result['snapshot_exhausted'] and not result['next_cursor']
    assert result['totals']['scanned']==0 and result['historical_coverage']=='unknown'


def test_route_pagination_reuses_auth_and_rejects_stale_receipt(tmp_path):
    from types import SimpleNamespace
    from unittest.mock import MagicMock
    from pa.domain.models import AgentSession,RestartHandoff
    from pa.modules.agent_chat import session_wire_metadata
    from fastapi import HTTPException
    import asyncio
    artifact(tmp_path,[request()]*201)
    session=AgentSession(id=SID,agent_name='codex',status='closed',principal_id='user:owner',
        authority_instance_id='owner',external_session_id=NATIVE)
    receipt=RestartHandoff(id='receipt',session_id=SID,idempotency_key='key',instance_id='owner',
        phase_version=6,attempts=1,continuation_prompt_id=PID,status='dismissed')
    req=MagicMock();req.state.user.role='member'
    req.app.state.ctx.settings=SimpleNamespace(instance_id='owner',data_dir=tmp_path,session_secret='test-only')
    mgr=MagicMock();mgr.store.get_session.return_value=session;mgr.store.get_restart_handoff.return_value=receipt
    with patch('pa.modules.agent_chat._manager',return_value=mgr),patch('pa.modules.agent_chat.get_principal_id',return_value='user:owner'):
        first=asyncio.run(session_wire_metadata(req,SID,'receipt',NATIVE,6,1,paged=True))
        assert first['next_cursor']
        second=asyncio.run(session_wire_metadata(req,SID,'receipt',NATIVE,6,1,paged=True,cursor=first['next_cursor'],assess_unparsed=True))
        assert second['unparsed_assessment']['gaps']==[]
        assert second['snapshot_exhausted']
        assert second['totals']['emitted']==201
        mgr.store.get_restart_handoff.return_value=receipt.model_copy(update={'phase_version':7})
        with pytest.raises(HTTPException) as e:
            asyncio.run(session_wire_metadata(req,SID,'receipt',NATIVE,6,1,paged=True,cursor=first['next_cursor'],assess_unparsed=True))
        assert e.value.status_code==409
        mgr.store.save_session.assert_not_called();mgr.store.update_restart_handoff.assert_not_called()

"""Loops share the durable ledger, including exact session and delivery lifecycle."""
import json
from datetime import UTC, datetime

import pytest
from pa.scheduling.store import Ledger, Schedule
from tests.test_scheduling_api import client  # noqa: F401


def spec(kind='interval', **kwargs):
    trigger = {'kind':kind, **({'seconds':600} if kind=='interval' else
               {'at':'2030-01-01T00:00:00Z'} if kind=='once' else
               {'event_type':'card.updated'} if kind=='event' else {})}
    return Schedule(name=kind, trigger=trigger, payload='Reminder',
                    destination={'kind':'inbox','target':'owner'}, **kwargs)


def test_session_query_lifecycle_and_owner_isolation(client):
    ledger = client.app.state.ctx.services['scheduling'].ledger
    records = [ledger.create('owner',kind,spec(kind,context_refs={'session':'session-a'}),0)
               for kind in ['once','interval','event']]
    extra = ledger.create('owner','extra',spec(additional_destinations=[{'kind':'agent_session','target':'session-a'}]),0)
    ledger.create('owner','different',spec(context_refs={'session':'session-ab'}),0)
    ledger.create('other','private',spec(context_refs={'session':'session-a'}),0)
    def pending(session='session-a', **params):
        r=client.get('/api/schedules',headers={'test-user':'owner'},params={
            'session_id':session,'pending_only':True,**params})
        assert r.status_code==200
        return r.json()
    assert pending('empty')['schedules']==[]
    ids=set();cursor=None
    while True:
        page=pending(limit=1,**({'cursor':cursor} if cursor else {}))
        ids.update(r['id'] for r in page['schedules'])
        cursor=page['next_cursor']
        if not cursor: break
    assert ids=={r['id'] for r in records}|{extra['id']}
    assert page['provider_discovery']['status']=='unsupported'
    record=records[0]
    for action,version,count in [('pause',1,3),('resume',2,4),('cancel',3,3)]:
        result=client.post('/api/schedules',headers={'test-user':'owner'},json={
            'action':action,'schedule_id':record['id'],'version':version,'idempotency_key':action})
        assert result.status_code==200
        assert len(pending()['schedules'])==count
    assert client.post('/api/schedules',headers={'test-user':'owner'},json={
        'action':'resume','schedule_id':record['id'],'version':4,'idempotency_key':'terminal'}).status_code==409
    assert ledger.get('owner',record['id'])['cancelled']


def test_completed_once_expiry_limit_and_failed_delivery(tmp_path):
    ledger=Ledger(tmp_path/'ledger.db')
    once=ledger.create('owner','once',spec('once',context_refs={'session':'s'}),0)
    due=datetime(2030,1,1,tzinfo=UTC).timestamp()
    ledger.tick(due)
    record=ledger.list_page('owner',session_id='s',pending_only=True,now=due)['schedules'][0]
    assert record['state']=='pending'
    oid=ledger.pending(due)[0]['id']
    for i in range(8): ledger.finish(oid,due+i,error='unavailable')
    assert ledger.list_page('owner',pending_only=True,now=due)['schedules'][0]['state']=='failed'
    ledger.retry('owner',once['id'],oid,'retry',due+9)
    ledger.finish(oid,due+10,accepted=True)
    assert ledger.list_page('owner',pending_only=True,now=due+10)['schedules']==[]
    assert ledger.list_page('owner',now=due+10)['schedules'][0]['state']=='completed'
    expired=ledger.create('owner','expired',spec(expires_at=datetime(2020,1,1,tzinfo=UTC)),0)
    assert ledger.lifecycle(expired,now=due)=='expired'
    limited=ledger.create('owner','limited',spec(max_occurrences=1),0)
    oid=ledger.fire('owner',limited['id'],'fire',1)['occurrence_id']
    ledger.finish(oid,2,accepted=True)
    assert ledger.lifecycle(ledger.get('owner',limited['id']),3)=='completed'


def test_multiple_deliveries_independent_retry_correction_restart_cancel(tmp_path):
    path=tmp_path/'ledger.db';ledger=Ledger(path)
    original=spec(max_occurrences=1,additional_destinations=[{'kind':'agent_session','target':'s'}])
    record=ledger.create('owner','create',original,0)
    first=ledger.fire('owner',record['id'],'fire',1)
    assert ledger.fire('owner',record['id'],'fire',1)==first
    deliveries=ledger.pending(1)
    assert len(deliveries)==2 and len({d['id'] for d in deliveries})==2
    assert ledger.get('owner',record['id'])['occurrence_count']==1
    accepted=deliveries[0];other=deliveries[1]
    ledger.finish(accepted['id'],2,accepted=True)
    ledger.finish(other['id'],2,error='unavailable')
    corrected=original.model_copy(update={'additional_destinations':[]})
    ledger.update('owner',record['id'],'correct',corrected,1,3)
    ledger=Ledger(path)
    assert ledger.pending(100)==[]
    history=ledger.history('owner',record['id'])
    assert {r['state'] for r in history}=={'delivered','cancelled'}
    assert json.loads(next(r['envelope'] for r in history if r['state']=='delivered'))['occurrence_id']==accepted['id']


def test_multitarget_coalescing_counts_occurrences_once(tmp_path):
    ledger=Ledger(tmp_path/'ledger.db')
    record=ledger.create('owner','create',spec(coalesce=True,additional_destinations=[{'kind':'inbox','target':'second'}]),0)
    ledger.fire('owner',record['id'],'one',1)
    ledger.fire('owner',record['id'],'two',2)
    assert [json.loads(r['envelope'])['coalesced_count'] for r in ledger.pending(2)]==[1,1]


@pytest.mark.asyncio
async def test_mcp_records_bound_source_session(monkeypatch):
    from types import SimpleNamespace
    from mcp.server.mcpserver import MCPServer
    from pa.mcp.tools.scheduling import register_mcp
    calls=[]
    monkeypatch.setenv('PA_BROWSER_SESSION_ID','bound-session')
    monkeypatch.setattr('pa.mcp.local_api.request_local_pa',lambda *a,**kw:calls.append(kw) or {})
    mcp=MCPServer('test');register_mcp(mcp,SimpleNamespace(settings=None))
    await mcp.call_tool('schedule_command',{'action':'create','idempotency_key':'create','schedule':spec().model_dump(mode='json')})
    assert calls[0]['json']['schedule']['context_refs']['session']=='bound-session'


@pytest.mark.asyncio
async def test_fanout_cycle_retries_only_unaccepted_destination(tmp_path):
    from pa.scheduling.errors import DeliveryError
    from pa.scheduling.service import Scheduler
    ledger=Ledger(tmp_path/'ledger.db')
    ledger.create('owner','create',spec('once',additional_destinations=[{'kind':'inbox','target':'second'}]),0)
    calls=[]
    async def deliver(item):
        destination=json.loads(item['destination'])['target']
        calls.append(destination)
        if destination=='second' and calls.count('second')==1:
            raise DeliveryError('fixture unavailable')
        return {'accepted':True}
    service=Scheduler(None,None,ledger,deliver=deliver)
    due=datetime(2030,1,1,tzinfo=UTC).timestamp()
    await service.cycle(due)
    await service.cycle(due+3)
    assert calls.count('owner')==1 and calls.count('second')==2
    assert ledger.list_page('owner',now=due+3)['schedules'][0]['state']=='completed'


def test_legacy_ledger_migration_preserves_existing_spec(tmp_path):
    import sqlite3
    path=tmp_path/'ledger.db';ledger=Ledger(path)
    record=ledger.create('owner','create',spec(),0)
    with sqlite3.connect(path) as db:
        legacy=dict(record)
        data={k:v for k,v in legacy.items() if k in Schedule.model_fields and k!='additional_destinations'}
        db.execute('UPDATE schedules SET spec=?',(json.dumps(data),))
        db.execute('ALTER TABLE schedules DROP COLUMN cancelled')
    ledger=Ledger(path)
    assert not ledger.get('owner',record['id'])['cancelled']
    ledger.tick(600)
    assert len(ledger.pending(600))==1
    ledger.update('owner',record['id'],'cancel',spec(),1,601,cancel=True)
    assert ledger.pending(602)==[]

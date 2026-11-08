from types import SimpleNamespace
from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest
from pa.modules.scheduling import router
from pa.scheduling.service import Scheduler
from pa.scheduling.store import Ledger


@pytest.fixture
def client(tmp_path):
    app=FastAPI()
    ledger=Ledger(tmp_path/'s.db')
    service=Scheduler(None,None,ledger)
    app.state.ctx=SimpleNamespace(services={'scheduling':service})
    @app.middleware('http')
    async def auth(request, next):
        if request.headers.get('test-user'):
            request.state.user=SimpleNamespace(role=request.headers.get('test-role','editor'))
            request.state.principal_id=request.headers['test-user']
        return await next(request)
    app.include_router(router,prefix='/api')
    return TestClient(app)


def create(client):
    return client.post('/api/schedules',headers={'test-user':'owner'},json={
        'action':'create','idempotency_key':'create','schedule':{
            'name':'Reminder','payload':'Report current state',
            'trigger':{'kind':'manual'},'destination':{'kind':'inbox','target':'owner'}}})


def test_auth_crud_and_replay(client):
    assert client.get('/api/schedules').status_code==401
    record=create(client).json()
    assert record['enabled'] and record['next_due'] is None
    assert len(client.get('/api/schedules',headers={'test-user':'owner'}).json()['schedules'])==1
    assert client.get('/api/schedules',headers={'test-user':'other'}).json()['schedules']==[]
    body={'action':'delete','schedule_id':record['id'],'version':1,'idempotency_key':'delete'}
    first=client.post('/api/schedules',headers={'test-user':'owner'},json=body)
    assert first.status_code==200
    assert client.post('/api/schedules',headers={'test-user':'owner'},json=body).json()==first.json()


def test_webhook_requires_auth_and_matching_trigger(client):
    record=create(client).json()
    command={'action':'webhook','schedule_id':record['id'],'idempotency_key':'event'}
    assert client.post('/api/schedules',json=command).status_code==401
    assert client.post('/api/schedules',headers={'test-user':'owner'},json=command).status_code==409
    command['action']='fire'
    first=client.post('/api/schedules',headers={'test-user':'owner'},json=command)
    assert first.status_code==200
    assert client.post('/api/schedules',headers={'test-user':'owner'},json=command).json()==first.json()
    assert len(client.get(f"/api/schedules/{record['id']}/history",headers={'test-user':'owner'}).json())==1


def test_pause_receipt_replays_after_intervening_update(client):
    record=create(client).json()
    body={'action':'pause','schedule_id':record['id'],'version':1,'idempotency_key':'pause'}
    first=client.post('/api/schedules',headers={'test-user':'owner'},json=body)
    assert first.status_code==200
    resume={**body,'action':'resume','version':2,'idempotency_key':'resume'}
    assert client.post('/api/schedules',headers={'test-user':'owner'},json=resume).status_code==200
    assert client.post('/api/schedules',headers={'test-user':'owner'},json=body).json()==first.json()


def test_control_page_and_script(client):
    from pa.modules.scheduling import ui
    client.app.include_router(ui)
    page=client.get('/schedules',headers={'test-user':'owner'})
    assert page.status_code==200
    assert 'Recipients choose actions, reporting and completion' in page.text
    assert '<form id="editor">' in page.text
    script=client.get('/schedules-controls.js',headers={'test-user':'owner'})
    assert script.status_code==200
    assert 'X-CSRF-Token' in script.text and 'idempotency_key' in script.text


@pytest.mark.asyncio
@pytest.mark.parametrize('payload',[{'command':'check','options':{'count':3,'enabled':True}},['check',{'items':[1,None]}]])
async def test_structured_payload_roundtrip_through_mcp_and_api(client,monkeypatch,payload):
    import json
    from mcp.server.mcpserver import MCPServer
    from pa.mcp.tools.scheduling import register_mcp
    responses=[]
    def proxy(settings,method,path,**kwargs):
        response=client.request(method,path,headers={'test-user':'owner'},json=kwargs.get('json'),params=kwargs.get('params'))
        response.raise_for_status()
        responses.append(response.json())
        return response.json()
    monkeypatch.setattr('pa.mcp.local_api.request_local_pa',proxy)
    mcp=MCPServer('scheduling-test')
    register_mcp(mcp,SimpleNamespace(settings=None))
    await mcp.call_tool('schedule_command',{'action':'create','idempotency_key':'structured-create','schedule':{
        'name':'Structured','trigger':{'kind':'manual'},'destination':{'kind':'inbox','target':'owner'},'payload':payload}})
    record=responses[-1]
    assert record['payload']==payload and type(record['payload']) is type(payload)
    await mcp.call_tool('schedule_command',{'action':'fire','idempotency_key':'structured-fire','schedule_id':record['id']})
    envelope=json.loads(client.get(f"/api/schedules/{record['id']}/history",headers={'test-user':'owner'}).json()[0]['envelope'])
    assert envelope['payload']==payload and type(envelope['payload']) is type(payload)
    await mcp.call_tool('list_schedules',{'limit':1})
    assert responses[-1]['schedules'][0]['payload']==payload


def test_pagination_discovers_201st_schedule_and_uses_direct_summary(client,monkeypatch):
    import json
    from pa.scheduling.store import Schedule
    ledger=client.app.state.ctx.services['scheduling'].ledger
    for i in range(201):
        ledger.create('owner',f'create-{i}',Schedule(name=f'Schedule {i}',trigger={'kind':'manual'},
            destination={'kind':'inbox','target':'owner'},payload={'index':i}),0)
    first=ledger.list_page('owner',limit=1)['schedules'][0]
    ledger.fire('owner',first['id'],'fire',1)
    def no_history(*args):
        raise AssertionError('list must select a summary directly')
    monkeypatch.setattr(ledger,'history',no_history)
    ids=set();cursor=None;pages=0
    while True:
        params={'limit':50,**({'cursor':cursor} if cursor else {})}
        page=client.get('/api/schedules',headers={'test-user':'owner'},params=params).json()
        assert len(page['schedules'])<=50
        ids.update(record['id'] for record in page['schedules'])
        if pages==0:
            summary=page['schedules'][0]['last_occurrence']
            assert summary['state']=='pending' and 'envelope' not in summary
        pages+=1
        cursor=page['next_cursor']
        if cursor is None:
            break
    assert len(ids)==201 and pages==5
    # Follow the same opaque cursor through the actual MCP handler and API proxy.
    import asyncio
    from mcp.server.mcpserver import MCPServer
    from pa.mcp.tools.scheduling import register_mcp
    replies=[]
    def proxy(settings,method,path,**kwargs):
        response=client.request(method,path,headers={'test-user':'owner'},params=kwargs.get('params'))
        response.raise_for_status()
        replies.append(response.json())
        return response.json()
    monkeypatch.setattr('pa.mcp.local_api.request_local_pa',proxy)
    async def mcp_pages():
        mcp=MCPServer('scheduling-pagination')
        register_mcp(mcp,SimpleNamespace(settings=None))
        await mcp.call_tool('list_schedules',{'limit':200})
        await mcp.call_tool('list_schedules',{'limit':200,'cursor':replies[-1]['next_cursor']})
    asyncio.run(mcp_pages())
    assert [len(page['schedules']) for page in replies]==[200,1]
    assert replies[-1]['next_cursor'] is None
    assert client.get('/api/schedules',headers={'test-user':'other'},params={'cursor':first['id']}).json()['schedules']==[]
    assert client.get('/api/schedules',headers={'test-user':'owner'},params={'limit':201}).status_code==422


def test_json_payload_bound_is_encoded_bytes_and_finite():
    from pydantic import ValidationError
    from pa.scheduling.store import Schedule
    base={'name':'bounded','trigger':{'kind':'manual'},'destination':{'kind':'inbox','target':'owner'}}
    for payload in [{'text':'x'*16000},{'number':float('inf')}]:
        with pytest.raises(ValidationError):
            Schedule(**base,payload=payload)
    assert Schedule(**base,payload={'text':'é'*100}).payload=={'text':'é'*100}


def test_overdue_delivery_health_survives_fresh_ticks_and_backoff(client, monkeypatch):
    import asyncio
    from pa.scheduling.errors import DeliveryError
    service = client.app.state.ctx.services['scheduling']
    ledger = service.ledger
    monkeypatch.setattr('pa.modules.scheduling.time.time', lambda: 1000)
    # A real cycle and disposable unavailable adapter reproduce a healthy worker
    # alongside a past-due delivery; no model or production recipient is used.
    async def unavailable(item):
        raise DeliveryError('fixture_target_unavailable')
    service.deliver = unavailable
    service.task = SimpleNamespace(done=lambda: False)
    record = create(client).json()
    oid = ledger.fire('owner', record['id'], 'due', 880)['occurrence_id']
    asyncio.run(service.cycle(now=1000))

    def health(user='owner', **params):
        response = client.get('/api/schedules', headers={'test-user':user}, params=params)
        assert response.status_code == 200
        return response.json()['health']

    result = health()
    assert result['running'] and not result['worker_overdue'] and result['overdue']
    assert result['overdue_delivery_count'] == 1
    assert result['oldest_overdue_due_at'] == 880
    occurrence = ledger.history('owner', record['id'])[0]
    assert occurrence['next_retry'] > 1000 and occurrence['error'] == 'fixture_target_unavailable'
    assert not health('other')['overdue']
    assert health('other')['oldest_overdue_due_at'] is None
    # Aggregate is independent of list pagination, even beyond the last page.
    assert health(cursor='z')['overdue_delivery_count'] == 1

    claimed = ledger.claim(oid, 1002)
    assert claimed and health()['overdue_delivery_count'] == 1
    ledger.finish(oid, 1002, accepted=True)
    assert not health()['overdue']
    assert health()['oldest_overdue_due_at'] is None
    # Future and exactly-due notifications are not yet overdue.
    ledger.fire('owner', record['id'], 'future', 1001)
    ledger.fire('owner', record['id'], 'exact', 1000)
    assert health()['overdue_delivery_count'] == 0


def test_exhausted_overdue_health_pause_resume_delete_and_coalesce(client, monkeypatch):
    from pa.scheduling.store import Schedule
    service = client.app.state.ctx.services['scheduling']
    ledger = service.ledger
    monkeypatch.setattr('pa.modules.scheduling.time.time', lambda: 1000)
    service.last_tick = 1000
    spec = Schedule(name='health', trigger={'kind':'manual'}, coalesce=True,
                    destination={'kind':'inbox','target':'owner'}, payload='Check current state')
    record = ledger.create('owner', 'health-create', spec, 0)
    sid = record['id']
    ledger.fire('owner', sid, 'coalesced', 1)
    oid = ledger.fire('owner', sid, 'latest', 2)['occurrence_id']
    for attempt in range(8):
        ledger.finish(oid, 10 + attempt, error='fixture_target_unavailable')
    assert ledger.history('owner', sid)[0]['state'] == 'failed'

    def health():
        return client.get('/api/schedules', headers={'test-user':'owner'}).json()['health']

    assert health()['overdue_delivery_count'] == 1
    assert health()['oldest_overdue_due_at'] == 2
    paused = spec.model_copy(update={'enabled':False})
    ledger.update('owner', sid, 'pause-health', paused, 1, 1000)
    assert not health()['overdue']
    assert ledger.history('owner', sid)[0]['error'] == 'fixture_target_unavailable'
    ledger.update('owner', sid, 'resume-health', spec, 2, 1000)
    assert health()['overdue']
    ledger.retry('owner', sid, oid, 'retry-health', 1000)
    assert health()['overdue_delivery_count'] == 1
    ledger.update('owner', sid, 'delete-health', spec, 3, 1000, delete=True)
    assert not health()['overdue']
    assert health()['oldest_overdue_due_at'] is None


@pytest.mark.parametrize('last_tick,overdue', [(None,True),(940,False),(939.99,True),(1000,False)])
def test_worker_health_threshold_is_preserved(client, monkeypatch, last_tick, overdue):
    monkeypatch.setattr('pa.modules.scheduling.time.time', lambda: 1000)
    client.app.state.ctx.services['scheduling'].last_tick = last_tick
    health = client.get('/api/schedules', headers={'test-user':'owner'}).json()['health']
    assert health['worker_overdue'] is overdue and health['overdue'] is overdue
    assert health['overdue_delivery_count'] == 0

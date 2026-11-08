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

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

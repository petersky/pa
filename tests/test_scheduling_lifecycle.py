import time
from uuid import uuid4
from fastapi.testclient import TestClient
from pa.config import Settings,reset_settings
from pa.core.kernel import Kernel
from pa.domain.store import reset_store
from pa.instance.agent_session import reset_instance_agent


def test_server_owns_ledger_and_inbox_acceptance(tmp_path,monkeypatch):
    from pa.instance.session_lifecycle import SessionLifecyclePolicy
    from pa.domain.instance_config import update_instance_config
    from pa.auth.users import UserDirectory
    reset_settings();reset_store();reset_instance_agent()
    monkeypatch.setattr(SessionLifecyclePolicy,'start',lambda self:None)
    settings=Settings(data_dir=tmp_path/'data',workspace_root=tmp_path/'workspaces',
        instance_id=str(uuid4()),agent_enabled=False,auth_required=True,peers=[],subscribed_realms=['default'])
    update_instance_config(settings.data_dir,session_secret=settings.session_secret,instance_id=settings.instance_id)
    kernel=Kernel.boot(settings=settings)
    try:
        with TestClient(kernel.build_app()) as client:
            user=UserDirectory(settings.data_dir).list_users()[0]
            client.headers['Authorization']='Bearer '+user.cli_token
            record=client.post('/api/schedules',json={'action':'create','idempotency_key':'create','schedule':{
                'name':'Fixture notification','payload':'Report current state','trigger':{'kind':'manual'},
                'destination':{'kind':'inbox','target':'user:'+user.id}}})
            assert record.status_code==200,record.text
            sid=record.json()['id']
            assert client.post('/api/schedules',json={'action':'fire','idempotency_key':'fire','schedule_id':sid}).status_code==200
            for _ in range(50):
                history=client.get(f'/api/schedules/{sid}/history').json()
                if history and history[0]['state']=='delivered':
                    break
                time.sleep(.1)
            assert history[0]['state']=='delivered',history
            assert client.get('/api/schedules').json()['health']['running']
    finally:
        reset_settings();reset_store();reset_instance_agent()

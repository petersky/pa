import json
from types import SimpleNamespace
from unittest.mock import AsyncMock,Mock
import pytest
from pa.auth.users import UserDirectory
from pa.scheduling.service import Scheduler
from pa.scheduling.store import Ledger,Schedule


@pytest.mark.asyncio
@pytest.mark.parametrize('busy,interrupt',[(False,False),(True,False),(True,True)])
async def test_agent_delivery_uses_exact_target_normal_input(tmp_path,monkeypatch,busy,interrupt):
    user=UserDirectory(tmp_path).ensure_default_user()
    ledger=Ledger(tmp_path/'schedule.db')
    record=ledger.create('user:'+user.id,'create',Schedule(name='monitor',payload='Check and report',
        trigger={'kind':'manual'},destination={'kind':'agent_session','target':'exact-target','interrupt':interrupt}),0)
    oid=ledger.fire('user:'+user.id,record['id'],'fire',1)['occurrence_id']
    store=SimpleNamespace(get_session=Mock(return_value=SimpleNamespace(dispatch_id=None,status='busy' if busy else 'idle')))
    ctx=SimpleNamespace(settings=SimpleNamespace(data_dir=tmp_path,subscribed_realms=[]),services={'instance_agent':SimpleNamespace(store=store)})
    ctx.require_service=ctx.services.__getitem__
    app=SimpleNamespace(state=SimpleNamespace(ctx=ctx))
    submit=AsyncMock(return_value={'accepted':True,'queued':busy})
    monkeypatch.setattr('pa.modules.agent_chat.session_prompt',submit)
    scheduler=Scheduler(ctx,app,ledger)
    await scheduler.cycle(1)
    store.get_session.assert_called_once_with('exact-target')
    request,target,body=submit.call_args.args
    assert target=='exact-target'
    assert body.action==('interrupt' if interrupt else 'append')
    assert body.client_prompt_id==oid
    assert json.loads(body.message)['occurrence_id']==oid
    assert ledger.history('user:'+user.id,record['id'])[0]['state']=='delivered'


@pytest.mark.asyncio
async def test_missing_target_visible_retained(tmp_path):
    user=UserDirectory(tmp_path).ensure_default_user()
    ledger=Ledger(tmp_path/'schedule.db')
    record=ledger.create('user:'+user.id,'create',Schedule(name='monitor',payload='Check and report',
        trigger={'kind':'manual'},destination={'kind':'agent_session','target':'missing'}),0)
    ledger.fire('user:'+user.id,record['id'],'fire',1)
    ctx=SimpleNamespace(settings=SimpleNamespace(data_dir=tmp_path,subscribed_realms=[]),services={'instance_agent':SimpleNamespace(store=SimpleNamespace(get_session=lambda _:None))})
    ctx.require_service=ctx.services.__getitem__
    scheduler=Scheduler(ctx,None,ledger)
    await scheduler.cycle(1)
    history=ledger.history('user:'+user.id,record['id'])
    assert history[0]['state']=='pending'
    assert history[0]['error']=='target_session_missing'
    assert history[0]['next_retry']==3


# Reuse the disposable real prompt ledger/runtime fixture; no external model or
# production session. This exercises durable queue acceptance, not agent action.
from tests.test_dispatch_followup_identity import env


@pytest.mark.asyncio
async def test_schedule_accepted_into_real_agent_queue(env,tmp_path,monkeypatch):
    user=UserDirectory(env.manager.settings.data_dir).ensure_default_user()
    ledger=Ledger(tmp_path/'schedule.db')
    record=ledger.create('user:'+user.id,'create',Schedule(name='monitor',payload='Check current state and report',
        trigger={'kind':'manual'},destination={'kind':'agent_session','target':env.record.session_id}),0)
    oid=ledger.fire('user:'+user.id,record['id'],'fire',1)['occurrence_id']
    ctx=env.request.app.state.ctx
    scheduler=Scheduler(ctx,env.request.app,ledger)
    from pa.modules.agent_chat import session_prompt, PromptBody, dispatch_prompt_capabilities, get_prompt_acceptance_status
    async def peer(request,instance,method,path,**kwargs):
        if path=='prompt-capabilities':
            return dispatch_prompt_capabilities()
        if method=='POST':
            return await session_prompt(env.request,env.record.session_id,PromptBody(**kwargs['body']))
        return await get_prompt_acceptance_status(env.request,env.record.session_id,path.rsplit('/',1)[1])
    monkeypatch.setattr('pa.modules.fleet._peer_agent_json',peer)
    await scheduler.cycle(1)
    history=ledger.history('user:'+user.id,record['id'])
    assert (history[0]['state'],history[0]['error'])==('delivered',None)
    assert len(env.runtime._queue)==1
    assert env.runtime._queue[0].message==json.dumps(json.loads(history[0]['envelope']),sort_keys=True)
    await scheduler.cycle(2)
    assert len(env.runtime._queue)==1

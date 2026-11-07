"""Opt-in actual-clock acceptance into PA's durable agent queue.

This takes twenty minutes. It uses the real authority/target prompt paths with a
disposable transport and quiesced recipient runtime, not a live model/provider.
Ordinary CI skips it; compressed tests are kept separately.
"""
import asyncio
import hashlib
import json
import os
from pathlib import Path
import subprocess
import time

import pytest
from pa.auth.users import UserDirectory
from pa.modules.agent_chat import session_prompt, PromptBody, dispatch_prompt_capabilities, get_prompt_acceptance_status
from pa.scheduling.service import Scheduler
from pa.scheduling.store import Ledger, Schedule
from tests.test_dispatch_followup_identity import env


@pytest.mark.skipif(os.environ.get('PA_SCHEDULING_REAL_CLOCK') != '1',reason='opt-in two actual ten-minute intervals')
@pytest.mark.asyncio
async def test_two_actual_ten_minute_deliveries_to_durable_agent_queue(env,tmp_path,monkeypatch):
    root=Path(__file__).resolve().parents[1]
    user=UserDirectory(env.manager.settings.data_dir).ensure_default_user()
    ledger=Ledger(tmp_path/'schedule.db')
    started=time.time()
    monotonic=time.monotonic()
    record=ledger.create('user:'+user.id,'create',Schedule(name='Actual ten-minute queue fixture',
        trigger={'kind':'interval','seconds':600},destination={'kind':'agent_session','target':env.record.session_id},
        payload='Check the current state and report a concise update.'),started)
    async def peer(request,instance,method,path,**kwargs):
        if path=='prompt-capabilities':
            return dispatch_prompt_capabilities()
        if method=='POST':
            return await session_prompt(env.request,env.record.session_id,PromptBody(**kwargs['body']))
        return await get_prompt_acceptance_status(env.request,env.record.session_id,path.rsplit('/',1)[1])
    monkeypatch.setattr('pa.modules.fleet._peer_agent_json',peer)
    scheduler=Scheduler(env.request.app.state.ctx,env.request.app,ledger)
    output=Path(os.environ.get('PA_SCHEDULING_RUNTIME_EVIDENCE',str(tmp_path/'runtime-evidence.json')))
    evidence={'commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=root,text=True).strip(),
        'source_hashes':{str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted((root/'src/pa/scheduling').glob('*.py'))},
        'scope':'Actual clock and real PA durable agent queue; disposable authority/target transport and quiesced recipient. No live model, production data or deployed acceptance.',
        'compressed_time':False,'interval_seconds':600,'started_at':started,'deliveries':[]}
    while time.monotonic()-monotonic<1230:
        await scheduler.cycle()
        history=ledger.history('user:'+user.id,record['id'])
        evidence['deliveries']=[{'occurrence_id':r['id'],'state':r['state'],'accepted_at':r['delivered_at'],
                                'envelope':json.loads(r['envelope'])} for r in reversed(history)]
        evidence['queue_count']=len(env.runtime._queue)
        evidence['elapsed_monotonic_seconds']=time.monotonic()-monotonic
        output.parent.mkdir(parents=True,exist_ok=True)
        output.write_text(json.dumps(evidence,indent=2)+'\n')
        if len(history)>=2 and all(r['state']=='delivered' for r in history):
            break
        await asyncio.sleep(1)
    assert len(evidence['deliveries'])==2 and len(env.runtime._queue)==2,evidence
    assert evidence['elapsed_monotonic_seconds']>=1199
    envelopes=[json.loads(item.message) for item in env.runtime._queue]
    assert len({item['occurrence_id'] for item in envelopes})==2
    assert all(item['missed_count']==0 for item in envelopes)
    assert envelopes[1]['due_at']-envelopes[0]['due_at']==600
    await scheduler.cycle()
    assert len(env.runtime._queue)==2
    evidence['passed']=True
    output.write_text(json.dumps(evidence,indent=2)+'\n')

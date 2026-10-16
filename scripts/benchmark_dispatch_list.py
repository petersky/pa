"""Disposable dispatch-list response and responsiveness fixture.

Fifty retained records each contain 400k characters of synthetic plans/proofs.
The loop probe includes same-process ASGI client JSON decoding; it is not a
production server-only loop SLO. Run without competing validation processes.
"""
import argparse,asyncio,json,math,tempfile,time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import httpx
from fastapi import FastAPI
from pa.core.async_runtime import AsyncRuntime
from pa.execution.dispatch import DispatchRecord,DispatchStore
from pa.modules.fleet import list_dispatches

def dist(v):
 x=sorted(v);return {'samples':len(x),**{f'p{p}_ms':round(x[math.ceil(len(x)*p/100)-1],3) for p in (50,95,99)},'max_ms':round(x[-1],3)}
async def run(root):
 store=DispatchStore(root);runtime=AsyncRuntime();await runtime.start()
 proof='fixture '*25000
 for i in range(50):
  store.put(DispatchRecord(mutation_id=f'm-{i}',authority_instance_id='local',authority_url='http://fixture',target_instance_id='target',state='running',session_id=f's-{i}',placement_decision={'proof':proof,'tie_breaking_reason':'fixture'},materialization_plan={'proof':proof,'summary':'fixture'}))
 services={'dispatch_store':store,'async_runtime':runtime};ctx=SimpleNamespace(services=services,settings=SimpleNamespace(instance_id='local'),require_service=services.__getitem__)
 app=FastAPI();app.state.ctx=ctx;app.add_api_route('/jobs',list_dispatches)
 times=[];sizes=[];probes=[];stop=False
 async def probe():
  while not stop:
   start=time.perf_counter();await asyncio.sleep(.005);probes.append(max(0,(time.perf_counter()-start)*1000-5))
 task=asyncio.create_task(probe())
 try:
  with patch('pa.modules.fleet.require_user'):
   async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://fixture') as client:
    for _ in range(20):
     start=time.perf_counter();r=await client.get('/jobs');r.raise_for_status();times.append((time.perf_counter()-start)*1000);sizes.append(len(r.content));assert len(r.json())==50
     await asyncio.sleep(.005)
  stop=True;await task
  return {'source_module':list_dispatches.__module__,'fixture':{'records':50,'proof_characters_per_record':len(proof)*2,'requests':20,'scope':'isolated temp dispatch store and ASGI API; no installed PA mutation'},'latency':dist(times),'bytes':{'min':min(sizes),'max':max(sizes)},'probe_lag':dist(probes),'runtime':runtime.snapshot()}
 finally:
  stop=True;await task;await runtime.close();store.close()
if __name__ == '__main__':
 parser=argparse.ArgumentParser();parser.add_argument('--output',type=Path,required=True);args=parser.parse_args()
 with tempfile.TemporaryDirectory(prefix='pa-dispatch-list-') as tmp:r=asyncio.run(run(Path(tmp)))
 args.output.write_text(json.dumps(r,indent=2)+'\n');print({k:r[k] for k in ('latency','bytes','probe_lag')})

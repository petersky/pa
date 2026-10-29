"""Complete private duplicate-receipt A/B; no mode/provider effects."""
import asyncio,ast,hashlib,json,os,subprocess,tempfile,threading,time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from datetime import datetime,timezone
import argparse
_parser=argparse.ArgumentParser(description=__doc__);_parser.add_argument('--output',type=Path,required=True);OUT=_parser.parse_args().output;assert not OUT.exists()
async def run(root):
 from pa.collaboration import service as module
 from pa.collaboration.models import ModeTransitionRequest,ModeTransitionResult,ExecuteCommandRequest,CommandResult
 from pa.core.async_runtime import AsyncRuntime
 baseline='7d4052fda87864e775cdabca53778f48bb658599';source=subprocess.check_output(['git','show',baseline+':src/pa/collaboration/service.py'],text=True)
 cls=next(n for n in ast.parse(source).body if isinstance(n,ast.ClassDef) and n.name=='CollaborationService');ns=dict(vars(module))
 for n in cls.body:
  if getattr(n,'name',None) in ['request_transition','execute_command']:exec(compile(ast.Module(body=[n],type_ignores=[]),'<archived615-collaboration>','exec'),ns)
 rows=[];snapshots=[]
 for bounded in [True,False]:
  path=root/str(bounded);path.mkdir();service=module.CollaborationService(SimpleNamespace(data_dir=path,instance_id='fixture'),SimpleNamespace());executor=AsyncRuntime(max_workers=1)
  if bounded:service.bind_runtime(SimpleNamespace(async_runtime=executor))
  session=SimpleNamespace(id='session')
  req=ModeTransitionRequest(session_id='session',requested_mode='default',purpose='Private duplicate receipt',intended_next_action='Observe read',idempotency_key='transition')
  service.store.save_mode_request(req,ModeTransitionResult(status='approved_applied',requested_mode='default',effective_mode='default',reason='Private already applied'))
  command=ExecuteCommandRequest(session_id='session',name='pa:status',idempotency_key='command');service.store.save_command_result(command.model_dump(mode='json',exclude={'idempotency_key'}),CommandResult(session_id='session',command_name='pa:status',status='applied',reason='Private already applied'),idempotency_key='command')
  try:
   for family,request,method,callname in [('transition',req,'get_mode_request','request_transition'),('command',command,'get_command_result','execute_command')]:
    original=getattr(service.store,method);expected=None
    for pair in range(3):
     for variant in (['baseline','candidate'] if pair%2==0 else ['candidate','baseline']):
      done=False;ticks=[];threads=[];owner=threading.get_ident()
      def delayed(*a,**k):threads.append(threading.get_ident());time.sleep(.2);return original(*a,**k)
      async def beat():
       last=time.perf_counter()
       while not done:
        await asyncio.sleep(.005);now=time.perf_counter();ticks.append((now-last)*1000);last=now
      pulse=asyncio.create_task(beat());await asyncio.sleep(0)
      with patch.object(service.store,method,side_effect=delayed):
       start=time.perf_counter();result=await (ns[callname](service,session,request) if variant=='baseline' else getattr(service,callname)(session,request));elapsed=(time.perf_counter()-start)*1000
      await asyncio.sleep(.01);done=True;await pulse
      body=result.model_dump_json();expected=expected or body;assert body==expected and result.duplicate and len(threads)==1 and (threads[0]==owner)==(variant=='baseline')
      assert service._receipt_guards=={}
      rows.append(dict(bounded=bounded,family=family,pair=pair,variant=variant,request_ms=elapsed,max_loop_gap_ms=max(ticks),read_on_loop=threads[0]==owner,result_sha256=hashlib.sha256(body.encode()).hexdigest(),guard_entries_after_call=0))
    snapshots.append(dict(bounded=bounded,after_family=family,executor_snapshot=executor.snapshot(),scope='Cumulative executor counters for this private fixture; only candidate calls use executor. Deliberate200msread included. Fallback uses asyncio thread pool and has no native operation metrics.'))
  finally:await executor.close()
 return dict(baseline=baseline,baseline_sha256=hashlib.sha256(source.encode()).hexdigest(),candidate_source_sha256=hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest(),rows=rows,candidate_cumulative_executor_snapshots=snapshots)
with tempfile.TemporaryDirectory(prefix='pa616-collab-pairs-') as tmp:
 root=Path(tmp);os.environ.update(PA_DATA_DIR=str(root/'data'),PA_WORKSPACE_ROOT=str(root/'workspaces'),PA_AGENT_ENABLED='false');v=asyncio.run(run(root))
v.update(observed_at=datetime.now(timezone.utc).isoformat(),scope='Twelve alternating private pairs:3perfamily/executor; complete native receipt duplicate results match. Deliberate200msread preserved, request latency not removed, loop progress compared. No provider/configuration/admission/notification effects, production mutation, policy/default/GC/deadline change, physicalIO/historicalcause/hardware/wholePAacceptance. Bothrootsprivatebeforeimports/children. Sourcecandidateuninstalled.')
OUT.write_text(json.dumps(v,indent=2)+'\n');print(json.dumps({'rows':len(v['rows']),'loop_max_ms':{variant:max(r['max_loop_gap_ms'] for r in v['rows'] if r['variant']==variant) for variant in ['baseline','candidate']}}))

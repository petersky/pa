"""Complete private prior-intent ensure comparison; no verifier admission.

Pre-session dispatches retain identity/control/readiness facts. Session-bearing
records retain full lifecycle snapshots. Logical copy counts are not disk I/O.
"""
import argparse,ast,asyncio,hashlib,json,os,sys,subprocess,tempfile,time
from datetime import datetime,timezone
from pathlib import Path
from unittest.mock import patch
BASELINE='841a352fc6e032e284d1e6bcd6f9529701e3911c'

def scope():
    hashes={};old_method=None
    for path in ['src/pa/execution/dispatch.py','src/pa/execution/verification.py']:
        before=subprocess.check_output(['git','show',BASELINE+':'+path],text=True);after=Path(path).read_text()
        hashes[path]={'baseline':hashlib.sha256(before.encode()).hexdigest(),'candidate':hashlib.sha256(after.encode()).hexdigest()}
        trees=[ast.parse(s) for s in [before,after]]
        for index,tree in enumerate(trees):
            if path.endswith('/dispatch.py'):
                tree.body=[n for n in tree.body if getattr(n,'name',None)!='VerificationPriorIntent']
                cls=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='DispatchStore')
                cls.body=[n for n in cls.body if getattr(n,'name',None) not in {'_verification_prior_intent','verification_prior_intent','verification_prior_intent_by_authority'}]
            else:
                cls=next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='VerificationService')
                if index==0:old_method=next(n for n in cls.body if getattr(n,'name',None)=='ensure')
                cls.body=[n for n in cls.body if getattr(n,'name',None)!='ensure']
                tree.body=[n for n in tree.body if getattr(n,'name',None)!='_prior_intent_reader']
                tree.body=[n for n in tree.body if not (isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='_PRIOR_INTENT_NATIVE_READERS' for t in n.targets))]
        assert ast.dump(trees[0])==ast.dump(trees[1]),'Unexpected shared application change: '+path
    return hashes,old_method

def main():
    p=argparse.ArgumentParser();p.add_argument('--output',type=Path,required=True);a=p.parse_args();assert not a.output.exists()
    with tempfile.TemporaryDirectory(prefix='pa-prior-intent-pairs-') as tmp:
        os.environ.update(PA_DATA_DIR=str(Path(tmp)/'data'),PA_WORKSPACE_ROOT=str(Path(tmp)/'workspaces'),PA_AGENT_ENABLED='false');sys.path.insert(0,str(Path.cwd()))
        from tests.test_autonomous_verification import system
        from tests.test_verification_prior_intent_inputs import older
        from pa.execution.dispatch import DispatchRecord
        from pa.execution import verification
        hashes,node=scope();namespace=dict(vars(verification));exec(compile(ast.Module(body=[node],type_ignores=[]),'<archived613-ensure>','exec'),namespace)
        value=dict(contract='pa.verification-prior-intent-comparison/v1',started_at=datetime.now(timezone.utc).isoformat(),baseline=BASELINE,source_hashes=hashes,other_application_ast_equal_after_explicit_scope=True,scenarios={})
        def save():a.output.write_text(json.dumps(value,indent=2)+'\n')
        async def run():
            for name,steps,session in [('pre_session1000',1000,None),('pre_session10000',10000,None),('session_full1000',1000,'prior-verifier')]:
                directory=Path(tmp)/name;directory.mkdir();fixture=system.__wrapped__(directory);target=next(fixture)
                row=dict(retained_steps=steps,session_bearing=session is not None,rows=[]);value['scenarios'][name]=row;save()
                try:
                    card,old,record=older(target,session_id=session,materialization_plan={'steps':[{'index':i,'retained':['x'*64]} for i in range(steps)]})
                    original=DispatchRecord.model_copy
                    for pair in range(30):
                        expected=None;expected_old=None
                        for variant in (['baseline','candidate'] if pair%2==0 else ['candidate','baseline']):
                            target.service.store.update(old['id'],'dispatched',dispatch_id=record.dispatch_id);copies=[]
                            def counted(r,**kwargs):
                                if r.dispatch_id==record.dispatch_id:copies.append(len((r.materialization_plan or {}).get('steps',[])))
                                return original(r,**kwargs)
                            call=namespace['ensure'] if variant=='baseline' else verification.VerificationService.ensure
                            with patch.object(DispatchRecord,'model_copy',counted):
                                wall,cpu=time.perf_counter(),time.process_time();result=await call(target.service,card);ms,cpu_ms=(time.perf_counter()-wall)*1000,(time.process_time()-cpu)*1000
                            decision=target.service.store.get(old['id']);decision={k:v for k,v in decision.items() if k not in ['created_at','updated_at']}
                            if expected is None:expected=result;expected_old=decision
                            assert result==expected and decision==expected_old and not target.calls
                            row['rows'].append(dict(pair=pair,variant=variant,wall_ms=ms,process_cpu_ms=cpu_ms,full_snapshots=len(copies),logical_copied_steps=sum(copies),complete_result_sha256=hashlib.sha256(json.dumps(result,sort_keys=True).encode()).hexdigest()));save()
                    def dist(xs):
                        xs=sorted(xs);return dict(samples=len(xs),p50_ms=xs[int((len(xs)-1)*.5)],p95_ms=xs[int((len(xs)-1)*.95)],p99_ms=xs[int((len(xs)-1)*.99)],max_ms=max(xs))
                    row.update(complete_result_and_prior_decision_equal=True,no_admission=True,variants={v:dict(latency_ms=dist([r['wall_ms'] for r in row['rows'] if r['variant']==v]),full_snapshots=sum(r['full_snapshots'] for r in row['rows'] if r['variant']==v),logical_copied_steps=sum(r['logical_copied_steps'] for r in row['rows'] if r['variant']==v)) for v in ['baseline','candidate']});save()
                finally:
                    target.ctx.store.close_thread_connection()
                    try:next(fixture)
                    except StopIteration:pass
        asyncio.run(run());value.update(finished_at=datetime.now(timezone.utc).isoformat(),scope=__doc__+' Thirty alternating pairs per valid integrated prior scope, complete ensure result/prior decision parity, same native private store and canonical job identity. Session-bearing full fallback deliberately retains copies. Lowerempirical percentiles/all raw/maxima; no provider/network/production mutation/policy/default/deadline/GC change, physicalIO/historicalcause/hardwarebound/deployment/everybranch/wholePA acceptance.');save();print(json.dumps({k:r['variants'] for k,r in value['scenarios'].items()}))
if __name__=='__main__':main()

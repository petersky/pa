"""Private old/new ledger-read parity and observation-overhead comparison."""
import argparse,hashlib,json,os,subprocess,tempfile,time,types
from dataclasses import asdict
from datetime import datetime,timezone
from pathlib import Path

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args();assert not args.output.exists()
    with tempfile.TemporaryDirectory(prefix='pa-collaboration-observation-pairs-')as tmp:
        root=Path(tmp)
        os.environ.update(PA_DATA_DIR=str(root/'data'),PA_WORKSPACE_ROOT=str(root/'workspaces'),PA_AGENT_ENABLED='false')
        from pa.collaboration.models import ModeTransitionRequest,ModeTransitionResult,ExecuteCommandRequest,CommandResult
        from pa.collaboration.store import CollaborationStore
        from pa.core.performance import OperationObservation,current_observation,distribution
        baseline='acd4e88bbd080bea190f77803458b57efb119ac4'
        source=subprocess.check_output(['git','show',baseline+':src/pa/collaboration/store.py'],text=True)
        module=types.ModuleType('pa.collaboration._archived_observation_baseline')
        exec(compile(source,'<archived617-collaboration-store>','exec'),module.__dict__)
        stores={name:cls(root/name)for name,cls in [('baseline',module.CollaborationStore),('candidate',CollaborationStore)]}
        request=ModeTransitionRequest(session_id='fixture-session',requested_mode='default',purpose='Private parity',intended_next_action='Read receipt',idempotency_key='fixture-mode')
        result=ModeTransitionResult(status='approved_applied',requested_mode='default',effective_mode='default',reason='Private result')
        command=ExecuteCommandRequest(session_id='fixture-session',name='pa:status',idempotency_key='fixture-command')
        cmd=CommandResult(session_id='fixture-session',command_name='pa:status',status='applied',reason='Private result')
        decoys=[]
        for i in range(1000):
            req=request.model_copy(update={'idempotency_key':f'decoy-{i}'})
            res=result.model_copy(update={'request_id':f'decoy-result-{i}'})
            decoys.append((res.request_id,req.session_id,req.idempotency_key,module.request_fingerprint(req),req.model_dump_json(),res.model_dump_json(),0,datetime.now(timezone.utc).isoformat()))
        for store in stores.values():
            store.save_mode_request(request,result)
            store.save_command_result(command.model_dump(mode='json',exclude={'idempotency_key'}),cmd,idempotency_key=command.idempotency_key)
            with store._conn()as conn:
                conn.executemany('INSERT INTO collaboration_mode_requests VALUES(?,?,?,?,?,?,?,?)',decoys)
                assert conn.execute('SELECT COUNT(*) FROM collaboration_mode_requests').fetchone()[0]==1001
        rows=[];expected={}
        for observe in [False,True]:
            for case in ['mode_present','command_present','mode_missing']:
                for pair in range(30):
                    for variant in (['baseline','candidate']if pair%2==0 else ['candidate','baseline']):
                        store=stores[variant];observation=OperationObservation();token=current_observation.set(observation if observe else None)
                        wall,cpu=time.perf_counter_ns(),time.thread_time_ns()
                        try:
                            value=store.get_command_result(request.session_id,command.idempotency_key)if case=='command_present'else store.get_mode_request(request.session_id,request.idempotency_key if case=='mode_present'else 'missing')
                        finally:
                            wall_ms=(time.perf_counter_ns()-wall)/1e6;cpu_ms=(time.thread_time_ns()-cpu)/1e6;current_observation.reset(token)
                        canonical=json.dumps([x.model_dump(mode='json')if hasattr(x,'model_dump')else x for x in value]if value else None,sort_keys=True,separators=(',',':'))
                        digest=hashlib.sha256(canonical.encode()).hexdigest()
                        expected.setdefault(case,digest);assert expected[case]==digest
                        if observe and variant=='candidate':
                            assert observation.sqlite_calls>=4 and observation.lock_events_total==1
                            assert observation.lock_events[0]['name']=='collaboration.store'
                        rows.append(dict(observe=observe,case=case,pair=pair,variant=variant,wall_ms=wall_ms,thread_cpu_ms=cpu_ms,result_sha256=digest,phases=asdict(observation)))
        summary={}
        for observe in [False,True]:
            for case in ['mode_present','command_present','mode_missing']:
                summary[f'{case}:observer={observe}']={variant:dict(wall_ms=distribution([r['wall_ms']for r in rows if r['observe']==observe and r['case']==case and r['variant']==variant]),thread_cpu_ms=distribution([r['thread_cpu_ms']for r in rows if r['observe']==observe and r['case']==case and r['variant']==variant]))for variant in stores}
        value=dict(contract='pa.collaboration-store-observation-comparison/v1',observed_at=datetime.now(timezone.utc).isoformat(),baseline=baseline,baseline_source_sha256=hashlib.sha256(source.encode()).hexdigest(),candidate_source_sha256=hashlib.sha256(Path('src/pa/collaboration/store.py').read_bytes()).hexdigest(),retained_mode_receipts_per_store=1001,rows=rows,summary=summary,complete_receipt_result_parity=True,scope='180 alternating private pairs/360calls: presentmode/presentcommand/missingmode, observeroff/on,30pairs each. All results/counters/wall/currentthreadCPU retained, including adverse overhead. Both private roots precede imports, real SQLite/native existing RLock semantics. No provider/configuration/admission/notification/production effects, threshold/default/GC changes. Statement/lazyfetch and namedlock observations only; native successfulbusywait/physicalIO/connect/close/Ccontext implicitcommit+rollback and async receipt-keyguard waits remain unseparated. No installed/deployed/fullPAacceptance or historicalcause claim.')
        args.output.write_text(json.dumps(value,indent=2)+'\n')
        print(json.dumps(dict(output=str(args.output),rows=len(rows),summary=summary)))

if __name__=='__main__':main()

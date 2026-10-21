"""Measure numeric SQLite observation overhead using disposable heartbeat commits."""
import argparse,hashlib,json,math,os,tempfile,time
from datetime import UTC,datetime
from pathlib import Path
from pa.execution.dispatch import DispatchRecord,DispatchStore
from pa.execution.progress import DispatchProgressEventV1,DispatchProgressHeartbeatV1,ProgressPhase
from pa.repository.workspace import WorkspaceManager

parser=argparse.ArgumentParser();parser.add_argument("--output",type=Path,required=True);args=parser.parse_args();assert not args.output.exists()
source=Path("src/pa/execution/dispatch.py").read_text()
from dataclasses import asdict
from unittest.mock import patch
import sqlite3
from pa.core.performance import OperationObservation,current_observation
from pa.core.sqlite_metrics import ObservedConnection
connect=sqlite3.connect
def distribution(xs):
    xs=sorted(xs);return {'samples':len(xs),**{k:round(xs[max(0,math.ceil(p*len(xs))-1)],6) for k,p in [('p50',.5),('p95',.95),('p99',.99),('max',1)]}}
def state(store,key):return store.get(key).model_dump(mode='json',exclude={'updated_at'})
out={'contract':'pa.dispatch-sql-observer-overhead/v1','started_at':datetime.now(UTC).isoformat(),'source_sha256':hashlib.sha256(source.encode()).hexdigest(),'scenarios':{}}
with tempfile.TemporaryDirectory(prefix='dispatch-sql-observation-',dir='.dev') as directory:
    root=Path(directory).resolve();os.environ['PA_DATA_DIR']=str(root/'unused-data');os.environ['PA_WORKSPACE_ROOT']=str(root/'unused-workspaces')
    def deny(*args,**kwargs):raise AssertionError('No workspace construction')
    WorkspaceManager.__init__=deny
    for count in (0,128):
        record=DispatchRecord(mutation_id='fixture',authority_instance_id='authority',authority_url='https://fixture.invalid',target_instance_id='origin',session_id='session',card_id='card',materialization_plan={'steps':[{'proof':['x'*256]} for _ in range(count)]},progress_highest_accepted_sequence=count,progress_next_sequence=count+1,progress_accepted_ranges=[[1,count]] if count else [])
        record.progress_events=[DispatchProgressEventV1(card_id='card',dispatch_id=record.dispatch_id,acp_session_id='session',originating_instance_id='origin',authority_instance_id='authority',sequence=i+1,idempotency_key=f'event-{i}',phase=ProgressPhase.TURN_ENDED,summary='retained '+str(i)) for i in range(count)]
        stores={}
        for kind in ('baseline','source'):
            location=root/str(count)/kind;location.mkdir(parents=True);assert location.is_relative_to(root)
            (location/'dispatch_mutations.json').write_text(json.dumps({record.dispatch_id:record.model_dump(mode='json')}));
            def guarded(path,*a,**k):
                assert Path(path).resolve().is_relative_to(root)
                k['factory']=sqlite3.Connection if kind=='baseline' else ObservedConnection
                return connect(path,*a,**k)
            with patch.object(sqlite3,'connect',guarded):stores[kind]=DispatchStore(location)
        pairs=[]
        try:
            for i in range(10):
                heartbeat=DispatchProgressHeartbeatV1(card_id='card',dispatch_id=record.dispatch_id,acp_session_id='session',originating_instance_id='origin',authority_instance_id='authority',sequence=count+i+1,idempotency_key=f'heartbeat-{i}',phase=ProgressPhase.IMPLEMENTING,summary='fixture')
                pair={'order':['baseline','source'] if i%2==0 else ['source','baseline']}
                for kind in pair['order']:
                    observation=OperationObservation();token=current_observation.set(observation)
                    wall,cpu=time.perf_counter(),time.thread_time()
                    try:result=stores[kind].ingest_heartbeat(heartbeat)
                    finally:current_observation.reset(token)
                    pair[kind]={'wall_ms':(time.perf_counter()-wall)*1000,'thread_cpu_ms':(time.thread_time()-cpu)*1000,'observation':asdict(observation)};assert result.status=='accepted'
                assert state(stores['baseline'],record.dispatch_id)==state(stores['source'],record.dispatch_id)
                pairs.append(pair)
            verified={}
            for kind,store in stores.items():
                current=store.get(record.dispatch_id);assert current.progress_events==record.progress_events and current.materialization_plan==record.materialization_plan
                assert len(current.progress_seen_keys)==10 and current.progress_heartbeat.sequence==count+10
                verified[kind]={'retained_events':len(current.progress_events),'retained_plan_steps':len(current.materialization_plan['steps']),'seen_keys':len(current.progress_seen_keys),'core_bytes':len(store._core_payload(current).encode()),'write_rows':store._write_rows}
            out['scenarios'][str(count)]={'pairs':pairs,'verified':verified,'latency_ms':{kind:{metric:distribution([p[kind][metric] for p in pairs]) for metric in ('wall_ms','thread_cpu_ms')} for kind in stores}}
        finally:
            for store in stores.values():store.close()
out['finished_at']=datetime.now(UTC).isoformat();out['scope']='Ten alternating paired real disposable commits per0/128protected retained-event/plan case; identical source except constructor factory raw versus observed. Worker observation active in both. Complete visible state excluding updated_at, retained history/plans, core bytes and logical writes compared. Context instrumentation overhead included, no causal CPU/disk/busy partition, no physical bytes/fsync attribution. Shared host small sample, p95/p99=max, no controlled statistical overhead bound, speedup claim, live workload, activation or deployed performance acceptance. No SQL/parameters/results retained by observations.' 
p=args.output;assert not p.exists();p.write_text(json.dumps(out,indent=2)+'\n');print(json.dumps({'scenarios':{k:v['latency_ms'] for k,v in out['scenarios'].items()}}))

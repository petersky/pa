"""Compare archived and native heartbeat updates using disposable commits."""
import argparse,ast,hashlib,json,math,os,subprocess,tempfile,time
from datetime import UTC,datetime
from pathlib import Path
from types import MethodType
import pa.execution.dispatch as module
from pa.execution.dispatch import DispatchRecord,DispatchStore
from pa.execution.progress import DispatchProgressEventV1,DispatchProgressHeartbeatV1,ProgressPhase
from pa.repository.workspace import WorkspaceManager

parser=argparse.ArgumentParser();parser.add_argument("--baseline-revision",required=True);parser.add_argument("--output",type=Path,required=True);args=parser.parse_args();assert not args.output.exists()
source=Path("src/pa/execution/dispatch.py").read_text()
baseline=subprocess.check_output(["git","show",args.baseline_revision+":src/pa/execution/dispatch.py"],text=True)
old_tree,new_tree=ast.parse(baseline),ast.parse(source)
old_cls,new_cls=[next(n for n in t.body if isinstance(n,ast.ClassDef) and n.name=="DispatchStore") for t in (old_tree,new_tree)]
method=next(n for n in old_cls.body if isinstance(n,ast.FunctionDef) and n.name=="ingest_heartbeat")
for cls in (old_cls,new_cls):cls.body=[n for n in cls.body if getattr(n,"name",None)!="ingest_heartbeat"]
assert ast.dump(old_tree)==ast.dump(new_tree),"Shared dependencies changed; reassess baseline"
namespace=dict(vars(module));exec(compile(ast.Module(body=[method],type_ignores=[]),"archived-heartbeat","exec"),namespace)
def distribution(xs):
    xs=sorted(xs);return {'samples':len(xs),**{k:round(xs[max(0,math.ceil(p*len(xs))-1)],6) for k,p in [('p50',.5),('p95',.95),('p99',.99),('max',1)]}}
def state(store,key):return store.get(key).model_dump(mode='json',exclude={'updated_at'})
out={'contract':'pa.heartbeat-copy-comparison/v1','started_at':datetime.now(UTC).isoformat(),'source_sha256':hashlib.sha256(source.encode()).hexdigest(),'baseline_revision':args.baseline_revision,'baseline_module_sha256':hashlib.sha256(baseline.encode()).hexdigest(),'scenarios':{}}
with tempfile.TemporaryDirectory(prefix='heartbeat-copy-',dir='.dev') as directory:
    root=Path(directory).resolve();os.environ['PA_DATA_DIR']=str(root/'unused-data');os.environ['PA_WORKSPACE_ROOT']=str(root/'unused-workspaces')
    def deny(*args,**kwargs):raise AssertionError('No workspace construction')
    WorkspaceManager.__init__=deny
    for count in (0,128,1000):
        record=DispatchRecord(mutation_id='fixture',authority_instance_id='authority',authority_url='https://fixture.invalid',target_instance_id='origin',session_id='session',card_id='card',materialization_plan={'steps':[{'proof':['x'*256]} for _ in range(count)]},progress_highest_accepted_sequence=count,progress_next_sequence=count+1,progress_accepted_ranges=[[1,count]] if count else [])
        record.progress_events=[DispatchProgressEventV1(card_id='card',dispatch_id=record.dispatch_id,acp_session_id='session',originating_instance_id='origin',authority_instance_id='authority',sequence=i+1,idempotency_key=f'event-{i}',phase=ProgressPhase.TURN_ENDED,summary='retained '+str(i)) for i in range(count)]
        stores={}
        for kind in ('baseline','source'):
            location=root/str(count)/kind;location.mkdir(parents=True);assert location.is_relative_to(root)
            (location/'dispatch_mutations.json').write_text(json.dumps({record.dispatch_id:record.model_dump(mode='json')}));stores[kind]=DispatchStore(location)
        stores['baseline'].ingest_heartbeat=MethodType(namespace['ingest_heartbeat'],stores['baseline'])
        pairs=[]
        try:
            for i in range(10):
                heartbeat=DispatchProgressHeartbeatV1(card_id='card',dispatch_id=record.dispatch_id,acp_session_id='session',originating_instance_id='origin',authority_instance_id='authority',sequence=count+i+1,idempotency_key=f'heartbeat-{i}',phase=ProgressPhase.IMPLEMENTING,summary='fixture')
                pair={'order':['baseline','source'] if i%2==0 else ['source','baseline']}
                for kind in pair['order']:
                    wall,cpu=time.perf_counter(),time.thread_time();result=stores[kind].ingest_heartbeat(heartbeat)
                    pair[kind]={'wall_ms':(time.perf_counter()-wall)*1000,'thread_cpu_ms':(time.thread_time()-cpu)*1000};assert result.status=='accepted'
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
out['finished_at']=datetime.now(UTC).isoformat();out['scope']='Archived baseline versus native source. Ten alternating real disposable heartbeat commits per implementation per0/128/1000protected-history/plan-step case. Full returned state equal excluding volatile record.updated_at. No Kernel, service workload, activation, backup or GC. No physical disk/serialization phase attribution. Shared host, not deployed acceptance. Correctness contracts must be validated separately. No production acceptance inference.'
p=args.output;assert not p.exists();p.write_text(json.dumps(out,indent=2)+'\n');print(json.dumps({'scenarios':{k:v['latency_ms'] for k,v in out['scenarios'].items()}}))

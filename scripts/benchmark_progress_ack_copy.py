"""Compare archived and native acknowledgments using guarded disposable commits."""
import argparse,ast,hashlib,json,math,os,subprocess,tempfile,time
from dataclasses import asdict
from datetime import UTC,datetime,timedelta
from pathlib import Path
from types import MethodType
from unittest.mock import patch
import pa.execution.dispatch as module
from pa.core.performance import OperationObservation,current_observation
from pa.execution.dispatch import DispatchRecord,DispatchStore
from pa.execution.progress import DispatchProgressEventV1,DispatchProgressHeartbeatV1,ProgressPhase
from pa.repository.workspace import WorkspaceManager

parser=argparse.ArgumentParser();parser.add_argument('--baseline-revision',required=True);parser.add_argument('--output',type=Path,required=True);args=parser.parse_args();assert not args.output.exists()
source=Path('src/pa/execution/dispatch.py').read_text()
baseline=subprocess.check_output(['git','show',args.baseline_revision+':src/pa/execution/dispatch.py'],text=True)
old_tree,new_tree=ast.parse(baseline),ast.parse(source)
classes=[next(n for n in t.body if isinstance(n,ast.ClassDef) and n.name=='DispatchStore') for t in (old_tree,new_tree)]
names={'mark_progress_delivered','mark_progress_delivery_failed','_progress_delivery_copy'}
methods=[n for n in classes[0].body if isinstance(n,ast.FunctionDef) and n.name in names]
assert len(methods)==2
for cls in classes:cls.body=[n for n in cls.body if getattr(n,'name',None) not in names]
assert ast.dump(old_tree)==ast.dump(new_tree),'Shared application dependencies changed; reassess baseline'
class FixedNow:
    stamp=datetime.now(UTC)
    @classmethod
    def now(cls,tz=None):return cls.stamp
namespace=dict(vars(module));namespace['datetime']=FixedNow
exec(compile(ast.Module(body=methods,type_ignores=[]),'archived-progress-ack','exec'),namespace)
def distribution(xs):
    xs=sorted(xs);return {'samples':len(xs),**{k:round(xs[max(0,math.ceil(p*len(xs))-1)],6) for k,p in [('p50',.5),('p95',.95),('p99',.99),('max',1)]}}
def state(store):return store.get('dispatch').model_dump(mode='json')
out={'contract':'pa.progress-ack-copy-comparison/v1','started_at':datetime.now(UTC).isoformat(),'baseline_revision':args.baseline_revision,'baseline_source_sha256':hashlib.sha256(baseline.encode()).hexdigest(),'source_sha256':hashlib.sha256(source.encode()).hexdigest(),'all_other_application_ast_equal':True,'scenarios':{}}
with tempfile.TemporaryDirectory(prefix='progress-ack-copy-',dir='.dev') as directory:
    root=Path(directory).resolve();os.environ['PA_DATA_DIR']=str(root/'unused-data');os.environ['PA_WORKSPACE_ROOT']=str(root/'unused-workspaces')
    def deny(*a,**k):raise AssertionError('No workspace construction')
    WorkspaceManager.__init__=deny
    for count in (0,128,1000):
        record=DispatchRecord(dispatch_id='dispatch',mutation_id='fixture',authority_instance_id='authority',authority_url='https://fixture.invalid',target_instance_id='origin',session_id='session',card_id='card',materialization_plan={'steps':[{'proof':['x'*256]} for _ in range(count)]})
        fields=dict(card_id='card',dispatch_id='dispatch',acp_session_id='session',originating_instance_id='origin',authority_instance_id='authority',phase=ProgressPhase.TURN_ENDED)
        record.progress_events=[DispatchProgressEventV1(**fields,sequence=i+1,idempotency_key=f'event-{i}',summary='retained '+str(i)) for i in range(count)]
        record.progress_heartbeat=DispatchProgressHeartbeatV1(**fields,sequence=count+1,idempotency_key='heartbeat',summary='heartbeat')
        for action in ('delivered','failed','absent'):
            stores={}
            for kind in ('baseline','source'):
                location=root/str(count)/action/kind;location.mkdir(parents=True);assert location.is_relative_to(root)
                (location/'dispatch_mutations.json').write_text(json.dumps({'dispatch':record.model_dump(mode='json')}));stores[kind]=DispatchStore(location)
            for method in methods:setattr(stores['baseline'],method.name,MethodType(namespace[method.name],stores['baseline']))
            pairs=[];key='absent' if action=='absent' else 'event-0' if count else 'heartbeat'
            try:
                for i in range(10):
                    FixedNow.stamp=record.created_at+timedelta(seconds=i+1)
                    pair={'order':['baseline','source'] if i%2==0 else ['source','baseline']}
                    for kind in pair['order']:
                        store=stores[kind];original_snapshot=store._snapshot;copies=[]
                        def counted(value):
                            copies.append({'events':len(value.progress_events),'plan_steps':len(value.materialization_plan['steps'])})
                            return original_snapshot(value)
                        observation=OperationObservation();token=current_observation.set(observation)
                        try:
                            with patch.object(store,'_snapshot',counted),patch.object(module,'datetime',FixedNow):
                                wall,cpu=time.perf_counter(),time.thread_time()
                                if action=='failed':store.mark_progress_delivery_failed('dispatch',key,'fixture failure')
                                else:store.mark_progress_delivered('dispatch',key)
                                wall_ms=(time.perf_counter()-wall)*1000;cpu_ms=(time.thread_time()-cpu)*1000
                        finally:current_observation.reset(token)
                        pair[kind]={'wall_ms':wall_ms,'thread_cpu_ms':cpu_ms,'full_snapshots':copies,'observation':asdict(observation)}
                    assert state(stores['baseline'])==state(stores['source'])
                    pairs.append(pair)
                verified={}
                for kind,store in stores.items():
                    current=store.get('dispatch');assert len(current.progress_events)==count and current.materialization_plan==record.materialization_plan
                    if action=='absent':assert current==record
                    verified[kind]={'retained_events':len(current.progress_events),'retained_plan_steps':len(current.materialization_plan['steps']),'core_bytes':len(store._core_payload(current).encode()),'write_rows':store._write_rows,'sqlite_total_changes':store._conn.total_changes}
                assert verified['baseline']==verified['source']
                out['scenarios'][f'{count}:{action}']={'selected_payload':'absent' if action=='absent' else 'event' if count else 'heartbeat','pairs':pairs,'verified':verified,'latency_ms':{kind:{metric:distribution([p[kind][metric] for p in pairs]) for metric in ('wall_ms','thread_cpu_ms')} for kind in stores}}
            finally:
                for store in stores.values():store.close()
out['finished_at']=datetime.now(UTC).isoformat();out['scope']='Ten alternating paired calls per0/128/1000protected-history/plan-step anddelivered/failed/absent-key shape.0historytargetsheartbeat. Full models compared including same pinned delivery/update timestamps, corebytes/logicalwrites/SQLitechanges equal. Only ack helper/twomethods differ in applicationAST. Snapshot/SQL instrumentation active onboth. No physicaldisk/fsync/serialization attribution, controlled hardware/statistical bound, realprovider/network/service workload, install/restart, backup/GC or deployed performance acceptance. Allsamples/outliers retained. At10samples nearest-rankp95/p99=max.'
args.output.write_text(json.dumps(out,indent=2)+'\n');print(json.dumps({'scenarios':{k:v['latency_ms'] for k,v in out['scenarios'].items()}}))

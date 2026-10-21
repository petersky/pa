"""Bounded disposable legacy/native delivery comparison; no service calls."""
import argparse,hashlib,json,math,os,tempfile,time
from datetime import UTC,datetime
from pathlib import Path
from pa.execution.dispatch import DispatchRecord,DispatchStore
from pa.execution.progress import DispatchProgressEventV1,ProgressPhase
from pa.repository.workspace import WorkspaceManager

def values(rows):return [(r.dispatch_id,r.authority_instance_id,r.authority_url,p.model_dump(mode='json')) for r,p in rows]
def dist(xs):
    xs=sorted(xs);return {'samples':len(xs),**{k:round(xs[max(0,math.ceil(len(xs)*p)-1)],6) for k,p in [('p50',.5),('p95',.95),('p99',.99),('max',1)]}}
parser=argparse.ArgumentParser();parser.add_argument("--output",type=Path,required=True);args=parser.parse_args();assert not args.output.exists()
out={'contract':'pa.pending-progress-projection-comparison/v1','started_at':datetime.now(UTC).isoformat(),'scenarios':{}}
with tempfile.TemporaryDirectory(prefix='pending-progress-probe-',dir='.dev') as directory:
    root=Path(directory).resolve();os.environ['PA_DATA_DIR']=str(root/'unused-data');os.environ['PA_WORKSPACE_ROOT']=str(root/'unused-workspaces')
    def deny(*args,**kwargs):raise AssertionError('Workspace construction denied')
    WorkspaceManager.__init__=deny
    for count in (0,1,32):
        location=root/str(count);location.mkdir()
        record=DispatchRecord(mutation_id='fixture',authority_instance_id='authority',authority_url='https://fixture.invalid',target_instance_id='origin',materialization_plan={'steps':[{'evidence':['x'*256]} for _ in range(128)]})
        record.progress_events=[DispatchProgressEventV1(card_id='fixture',dispatch_id=record.dispatch_id,acp_session_id='fixture',originating_instance_id='origin',authority_instance_id='authority',sequence=i+1,idempotency_key=f'fixture-{i}',phase=ProgressPhase.TURN_ENDED,summary='fixture '+str(i),delivered_at=None if i<count else datetime.now(UTC)) for i in range(128)]
        (location/'dispatch_mutations.json').write_text(json.dumps({record.dispatch_id:record.model_dump(mode='json')}))
        store=DispatchStore(location);assert store.db_path.resolve().is_relative_to(root)
        try:
            changes_before=store._conn.total_changes;writes_before=store._write_rows
            before=values(store.pending_progress('origin'));after=values(store.pending_progress_deliveries('origin'));assert before==after and len(after)==count
            observed={'full_dispatch_snapshots':0,'retained_event_models_copied':0}
            original=store._snapshot
            def counted(record):
                observed['full_dispatch_snapshots']+=1;observed['retained_event_models_copied']+=len(record.progress_events)
                return original(record)
            store._snapshot=counted;store.pending_progress('origin');store._snapshot=original
            pairs=[]
            for i in range(10):
                pair={'order':['baseline','projection'] if i%2==0 else ['projection','baseline']}
                for kind in pair['order']:
                    wall,cpu=time.perf_counter(),time.thread_time()
                    result=store.pending_progress('origin') if kind=='baseline' else store.pending_progress_deliveries('origin')
                    pair[kind]={'wall_ms':(time.perf_counter()-wall)*1000,'thread_cpu_ms':(time.thread_time()-cpu)*1000}
                    assert values(result)==before
                pairs.append(pair)
            out['scenarios'][str(count)]={'retained_events':128,'plan_steps':128,'pending_payloads':count,'transport_projection_equal':True,'copy_counts_outside_timing':{'baseline':observed,'projection':{'full_dispatch_snapshots':0,'copied_payload_models':count}},'pairs':pairs,'latency_ms':{k:{m:dist([p[k][m] for p in pairs]) for m in ('wall_ms','thread_cpu_ms')} for k in ('baseline','projection')}}
            assert store._conn.total_changes==changes_before and store._write_rows==writes_before
        finally:store.close()
out['finished_at']=datetime.now(UTC).isoformat();out['scope']='Source comparison. Each case128retained events/plansteps, pending0/1/32;10alternating pairs. Real disposable migration then read-only calls; no Kernel/service/network/activation/backup or GC operation. Full old API payload identity/mutability contracts not replaced. Native delivery projection is implemented; separate concurrency, isolation, retry and adapter contracts are required. No mixed-load or deployed acceptance. Uncontrolled host, not live causal or acceptance evidence.'
out['source_sha256']=hashlib.sha256(Path('src/pa/execution/dispatch.py').read_bytes()).hexdigest()
path=args.output;assert not path.exists();path.write_text(json.dumps(out,indent=2)+'\n');print(json.dumps({'scenarios':{k:{f:v[f] for f in ('copy_counts_outside_timing','latency_ms')} for k,v in out['scenarios'].items()}}))

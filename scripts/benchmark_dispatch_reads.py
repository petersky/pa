"""Bounded isolated dispatch read API inventory. No worker/admission/control."""
import asyncio
from collections import Counter, defaultdict
from contextlib import ExitStack
from contextvars import ContextVar
from functools import wraps
import hashlib
import importlib.util
import json
import logging
from pathlib import Path
import shutil
import tempfile
import threading
import time
from types import SimpleNamespace
from unittest.mock import patch
from uuid import UUID

from fastapi import FastAPI
import httpx
from pa.core.async_runtime import AsyncRuntime
from pa.domain.models import FleetInstance
from pa.execution.dispatch import DispatchRecord, DispatchStore, summarize_dispatch
from pa.fleet.registry import FleetRegistry
from pa.modules import fleet

spec = importlib.util.spec_from_file_location('dispatch_read_support', Path(__file__).with_name('benchmark_systemic_latency.py'))
support = importlib.util.module_from_spec(spec); spec.loader.exec_module(support)
COLUMNS = ['samples', 'p50', 'p95', 'p99', 'max']


def distribution(values):
    value = support.distribution(values)
    return [value['samples'], *[value[x+'_ms'] for x in COLUMNS[1:]]]


def expected_rows(records, names, params):
    """Independent selection/name oracle; public model contract is the reference."""
    selected = sorted((r for r in records if not params.get('target_instance_id') or
        r.target_instance_id == params['target_instance_id']), key=lambda r:r.updated_at, reverse=True)
    result = []
    for r in selected[:max(1, min(int(params.get('limit', 100)), 500))]:
        full = r.public_dict()
        if params.get('view', 'status') == 'full': row = full
        else:
            row = summarize_dispatch(full)
            row.update(card_version=r.card_version,
                events=[{'state':r.events[-1].state, 'message':r.events[-1].message[:500]}] if r.events else [],
                materialization_plan={'summary':str((r.materialization_plan or {}).get('summary') or '')[:500]},
                placement_decision={'tie_breaking_reason':str((r.placement_decision or {}).get('tie_breaking_reason') or '')[:500]},
                stale_session_recovery={'eligible_candidate':False, 'recovered':False},
                followup_state={'scheduled':False}, sync_evidence={'degraded_peer_count':0}, view='status')
        for role in ('authority', 'target'):
            ident, snapshot = getattr(r,role+'_instance_id'), getattr(r,role+'_instance_name')
            current = names.get(ident) or snapshot or 'Unknown instance · '+(ident[:8] if len(ident)>=32 else ident[:12])
            row.update({role+'_instance_name':current, role+'_instance_name_snapshot':snapshot,
                role+'_instance_name_at_dispatch':snapshot if snapshot and snapshot!=current else None})
        result.append(row)
    return result


def seed(folder, population, members, payload_bytes):
    store = DispatchStore(folder); registry = FleetRegistry(folder, 'synthetic-fleet')
    names = {str(UUID(int=i+1)):f'Synthetic member {i}' for i in range(members)}
    for ident,name in names.items(): registry.upsert_instance(FleetInstance(instance_id=ident, name=name, url=f'http://member-{UUID(ident).int}.invalid'))
    ids = list(names); unknown = str(UUID(int=10000)); records = []
    for i in range(population):
        record = DispatchRecord(dispatch_id=str(UUID(int=i+20000)), mutation_id=f'synthetic-{i}',
            principal_id='synthetic-owner-'+str(i%2), card_id=f'synthetic-card-{i}',
            authority_instance_id=ids[i%members], authority_instance_name='Snapshot authority', authority_url='http://synthetic.invalid',
            target_instance_id=unknown if i%11==0 else ids[(i+1)%members], target_instance_name=None if i%22==0 else 'Snapshot target',
            state='waiting_capacity' if i%3==0 else 'queued', requested_priority=i%3,
            materialization_plan={'summary':'Synthetic retained plan '+str(i), 'input': 'x'*payload_bytes},
            placement_decision={'tie_breaking_reason':'Synthetic fixture; no admission'},
            request_payload={'private_prompt':'PRIVATE_SENTINEL','execution_selection':{'synthetic':True}},
            card_snapshot={'body':'PRIVATE_SENTINEL'}, events=[{'state':'queued','message':'Synthetic unlaunched record'}])
        records.append(store.put(record))
    # Independent expected positions for the synthetic waiting rows, no queue controls.
    waiting = sorted((r for r in records if r.state=='waiting_capacity'), key=lambda r:(-r.requested_priority,r.scheduling_class_sequence,r.created_at,r.dispatch_id))
    for n,r in enumerate(waiting,1): r.queue_position=n
    return store, registry, records, names


async def profile(folder, *, population, members, payload_bytes, iterations, progress):
    folder.mkdir(); start=time.perf_counter(); store, registry, records, names=seed(folder,population,members,payload_bytes)
    setup_ms=(time.perf_counter()-start)*1000; runtime=AsyncRuntime()
    ctx=SimpleNamespace(settings=SimpleNamespace(data_dir=folder), services={'dispatch_store':store,'fleet_registry':registry,'async_runtime':runtime})
    app=FastAPI();app.state.ctx=ctx;app.include_router(fleet.router,prefix='/api')
    selected=ContextVar('private_dispatch_read',default=None); counts=Counter(); phases=defaultdict(list); guard=threading.Lock()
    samples,sizes=defaultdict(list),defaultdict(list); statuses=Counter(); hashes={};oracle_wall=[];oracle_cpu=[]
    warning_counts=Counter(); warnings=[]; progress.update(phase='setup',samples=samples,statuses=statuses,counts=counts,warnings=warnings,warning_counts=warning_counts)
    @app.middleware('http')
    async def auth(request,next):
        if request.headers.get('test-user'):
            request.state.user=SimpleNamespace(role='viewer'); request.state.principal_id=request.headers['test-user']
        wall=time.perf_counter(); response=await next(request)
        runtime.record_request(getattr(request.scope.get('route'),'path','unknown'),response.status_code,(time.perf_counter()-wall)*1000,response_bytes=int(response.headers.get('content-length',0)))
        return response
    def observed(name,call):
        @wraps(call)
        def run(*args,**kwargs):
            assert store.db_path.parent.resolve()==folder.resolve() and registry.instances_path.parent.resolve()==folder.resolve()
            wall,cpu=time.perf_counter(),time.thread_time()
            try:return call(*args,**kwargs)
            finally:
                label=selected.get()
                if label:
                    with guard:
                        counts[label+':'+name]+=1
                        phases[name+':wall_ms'].append((time.perf_counter()-wall)*1000)
                        phases[name+':thread_cpu_ms'].append((time.thread_time()-cpu)*1000)
        return run
    def trace(sql):
        if selected.get():
            with guard:counts[selected.get()+':sql_'+sql.lstrip().split(None,1)[0].upper()]+=1
    class NumericWarnings(logging.Handler):
        def emit(self,record):
            if record.levelno>=logging.WARNING:
                warning_counts[record.name+':'+record.levelname]+=1
                value=getattr(record,'performance_phases',None)
                if isinstance(value,dict):warnings.append({k:v for k,v in value.items() if isinstance(v,(int,float)) or k=='operation'})
    handler=NumericWarnings();logger=logging.getLogger();logger.addHandler(handler)
    variants={
        'default':({},True,200), 'full':({'view':'full'},True,200),
        'status500':({'limit':500},True,200), 'full500':({'limit':500,'view':'full'},True,200),
        'target':({'target_instance_id':next(iter(names)),'limit':500},True,200),
        'target_full':({'target_instance_id':next(iter(names)),'view':'full'},True,200),
        'empty':({'target_instance_id':'absent'},True,200),
        'clamp0':({'limit':0},True,200), 'clamp501':({'limit':501},True,200),
        'bad_view':({'view':'invalid'},True,422), 'bad_limit':({'limit':'invalid'},True,422),
        'unauthenticated':({},False,401)}
    changes=store._conn.total_changes; setup_commits=store._commits
    before_durable=hashlib.sha256(json.dumps([r.model_dump(mode='json') for r in store.list(limit=population)],sort_keys=True).encode()).hexdigest()
    try:
        await runtime.start();store._conn.set_trace_callback(trace)
        with ExitStack() as stack:
            for name in ('public_statuses','list','_refresh_queue_positions_locked','_snapshot'):
                stack.enter_context(patch.object(store,name,observed(name,getattr(store,name))))
            stack.enter_context(patch.object(registry,'list_instances',observed('membership_list',registry.list_instances)))
            stack.enter_context(patch.object(DispatchRecord,'public_dict',observed('public_dict',DispatchRecord.public_dict)))
            stack.enter_context(patch.object(fleet,'canonicalize_dispatch_public',observed('canonicalize',fleet.canonicalize_dispatch_public)))
            progress['phase']='requests'
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://fixture') as client:
                for _ in range(iterations):
                    for name,(params,authorized,status) in variants.items():
                        async def foreground():
                            token=selected.set(name);wall=time.perf_counter()
                            try:return await client.get('/api/fleet/dispatch-jobs',params=params,headers={'test-user':'synthetic-viewer'} if authorized else {})
                            finally:samples[name].append((time.perf_counter()-wall)*1000);selected.reset(token)
                        async def background():
                            token=selected.set('background');wall=time.perf_counter()
                            try:return await runtime.run_blocking('http.dispatch_list',store.public_statuses,limit=1)
                            finally:samples['background'].append((time.perf_counter()-wall)*1000);selected.reset(token)
                        response,bg=await asyncio.gather(foreground(),background())
                        statuses[name+':'+str(response.status_code)]+=1;sizes[name].append(len(response.content))
                        assert response.status_code==status and len(response.content)<16*1024**2
                        wall,cpu=time.perf_counter(),time.thread_time()
                        assert len(bg)==1 and bg[0]['dispatch_id']==records[-1].dispatch_id
                        if status==200:
                            assert response.json()==expected_rows(records,names,params),'complete public response mismatch'
                            assert b'PRIVATE_SENTINEL' not in response.content
                        oracle_wall.append((time.perf_counter()-wall)*1000);oracle_cpu.append((time.thread_time()-cpu)*1000)
                        digest=hashlib.sha256(response.content).hexdigest();assert hashes.setdefault(name,digest)==digest
            snapshot=runtime.snapshot()
        store._conn.set_trace_callback(None)
        after_durable=hashlib.sha256(json.dumps([r.model_dump(mode='json') for r in store.list(limit=population)],sort_keys=True).encode()).hexdigest()
        assert before_durable==after_durable and store._conn.total_changes==changes and store._commits==setup_commits
    finally:
        progress['phase']='shutdown';await runtime.close();logger.removeHandler(handler);store.close()
    extra_phases=('python_lock_wait_ms','python_lock_hold_ms','sqlite_statement_ms','sqlite_commit_ms','sqlite_busy_failures','sqlite_rows')
    return {'population':population,'members':members,'payload_bytes':payload_bytes,'iterations':iterations,'setup_ms':round(setup_ms,3),
        'request_ms':{k:distribution(v) for k,v in samples.items()},'response_bytes':{k:distribution(v) for k,v in sizes.items()},
        'response_byte_totals':{k:sum(v) for k,v in sizes.items()},'response_sha256':hashes,'status_counts':dict(statuses),
        'counts':dict(counts),'phase_ms':{k:distribution(v) for k,v in phases.items()},'oracle_ms':distribution(oracle_wall),'oracle_thread_cpu_ms':distribution(oracle_cpu),
        'explicit_ge500ms':{k:sum(x>=500 for x in v) for k,v in samples.items()},'runtime':support.compact_runtime(snapshot),
        'runtime_sql_lock_windows':{name:{key:data[key] for key in extra_phases if key in data} for name,data in snapshot['operations'].items() if data.get('submitted')},
        'work':snapshot['work'],'warning_counts':dict(warning_counts),'warnings':warnings,
        'measured_durable_changes':store._commits-setup_commits,'durable_model_sha256':before_durable,'durable_full_model_parity':True}


def benchmark(*, population=520, iterations=6, profiles=((4,1024),(16,4096))):
    if not 24<=population<=520 or not 1<=iterations<=6 or not 1<=len(profiles)<=2 or any(not 1<=m<=16 or not 1<=b<=4096 for m,b in profiles):raise ValueError('bounded population/iterations/membership/input')
    result={'contract':'pa.dispatch-read-api-inventory/v1','distribution_columns':COLUMNS,'profiles':[]}
    with tempfile.TemporaryDirectory(prefix='pa-dispatch-read-api-') as temporary:
        root=Path(temporary).resolve()
        if shutil.disk_usage(root).free<4*1024**3:raise RuntimeError('At least4GiB free required')
        progress={}
        try:
            for i,(members,payload) in enumerate(profiles):result['profiles'].append(asyncio.run(profile(root/str(i),population=population,members=members,payload_bytes=payload,iterations=iterations,progress=progress)))
        except Exception as exc:
            result.update(failed=True,**support.minimum_failure(exc),failed_phase=progress.get('phase'),partial_status_counts=dict(progress.get('statuses',{})),partial_counts=dict(progress.get('counts',{})),partial_request_ms={k:distribution(v) for k,v in progress.get('samples',{}).items()},partial_warning_counts=dict(progress.get('warning_counts',{})),partial_warnings=progress.get('warnings',[]))
        result['fixture_storage']=support.fixture_bytes(root)
    result.update(temporary_fixture_removed=not root.exists(),script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        scope='Actual isolated FastAPI fleet router/DispatchStore/FleetRegistry/default AsyncRuntime. Normal private put/upsert synthesize unlaunched queued/waiting records and local directory; no worker/admission/claim/control/receipt/provider/delivery/peer. Viewer middleware is synthetic, not native auth/globalCSRF/Kernel. Samehttp.dispatch_list budget for paired one-row background read, not a consumer. Full model public_dict is reference with independent order/filter/limit/name/snapshot/summary oracle; all oracle CPU AFTER awaited concurrent work. No native/production/whole-card acceptance. Numeric method observers add overhead, inclusive phases overlap. SQLtrace counts tokens only on exact private connection. Durable fullmodel hashes/commit/totalchanges parity across measured reads. Different profile population data, not a repair comparison; no physicalIO/peakRAM/APFS claim.')
    return result


if __name__=='__main__':print(json.dumps(benchmark(),separators=(',',':')))

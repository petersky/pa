"""Private actual PR read router/store/auth fixture; no supervisor worker/effects."""
import asyncio
from collections import Counter, defaultdict
from contextlib import ExitStack, contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime, timedelta
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

import httpx
from fastapi import FastAPI
import fastapi.routing as fastapi_routing
from starlette.responses import JSONResponse
from pa.auth.middleware import AuthMiddleware
from pa.auth.sessions import SessionManager
from pa.auth.users import UserDirectory
from pa.config import Settings
from pa.core.async_runtime import AsyncRuntime
from pa.core.performance import OperationObservation, current_observation, capture_sqlite_calls
from pa.modules import pr_supervisor as module
from pa.pr_supervisor.github import GitHubClient, GitHubCredentials
from pa.pr_supervisor.models import PRWatch, PRWatchEvent, PRWatchStatus
from pa.pr_supervisor.service import PRSupervisor
from pa.pr_supervisor.store import PRSupervisorStore

spec=importlib.util.spec_from_file_location('pr_read_support',Path(__file__).with_name('benchmark_systemic_latency.py'))
support=importlib.util.module_from_spec(spec);spec.loader.exec_module(support)
COLUMNS=['samples','p50','p95','p99','max']


def distribution(values):
    r=support.distribution(values)
    return [r['samples'],*[r[k+'_ms'] for k in COLUMNS[1:]]]


def expected_watches(records, *, realm='default', card=None, include_retired=False, view=None, query='', limit=None, offset=0):
    rows=[r for r in records if r.realm_id==realm and (not card or r.card_id==card)]
    if view=='history':rows=[r for r in rows if r.retired_at or r.status in {'retired','merged','closed'}]
    elif view!='all' and not include_retired:
        rows=[r for r in rows if r.retired_at is None and r.status in {'active','blocked'}]
    if view=='errors':rows=[r for r in rows if r.last_error]
    if query:rows=[r for r in rows if any(query.lower() in str(v or '').lower() for v in (r.repository,r.pr_number,r.id,r.card_id,r.project_id))]
    rows=sorted(rows,key=lambda r:(r.updated_at,r.id),reverse=True);total=len(rows)
    if limit is not None:rows=rows[max(0,offset):max(0,offset)+max(1,min(limit,100))]
    return [r.model_dump(mode='json') for r in rows],total


def fingerprint(store):
    digest=hashlib.sha256()
    with store._conn() as conn:
        tables=[r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name")]
        for table in tables:
            assert table.replace('_','').isalnum()
            for row in conn.execute('SELECT * FROM "'+table+'" ORDER BY rowid'):
                digest.update(repr(tuple(row)).encode())
    return digest.hexdigest()


def seed(folder,population,payload_bytes,event_count):
    store=PRSupervisorStore(folder/'pr.db');now=datetime.now(UTC)-timedelta(hours=1);records=[]
    for i in range(population):
        retired=i%4==0
        record=PRWatch(id=f'synthetic-watch-{i:04}',realm_id='other' if i%5==0 else 'default',card_id=f'synthetic-card-{i%7}',
            repository='synthetic/repository',pr_number=i+1,pr_url=f'https://synthetic.invalid/pr/{i+1}',
            status=PRWatchStatus.RETIRED if retired else PRWatchStatus.BLOCKED if i%4==1 else PRWatchStatus.ACTIVE,
            state={'synthetic_input':'x'*payload_bytes},last_error='Synthetic diagnostic' if i%4==1 else None,
            updated_at=now+timedelta(seconds=i),created_at=now,retired_at=now if retired else None)
        records.append(store.upsert_watch(record))
    target=records[1];events=[]
    for i in range(event_count):
        event=PRWatchEvent(id=f'synthetic-event-{i:04}',watch_id=target.id,event_key=f'synthetic-event-{i}',event_type='synthetic_fixture_read',source='private_fixture',payload={'sequence':i,'synthetic_input':'e'*512},created_at=now+timedelta(seconds=i))
        assert store.append_event(event);events.append(event.model_dump(mode='json'))
    return store,records,sorted(events,key=lambda r:r['created_at'],reverse=True),target.id


async def profile(folder,*,population,payload_bytes,event_count,iterations,progress):
    folder.mkdir();progress['phase']='setup';start=time.perf_counter()
    store,records,events,target=seed(folder,population,payload_bytes,event_count)
    settings=Settings(_env_file=None,data_dir=folder,instance_id='synthetic-local',instance_url='http://fixture',session_secret='private-fixture-only',auth_required=True,sync_token='',agent_enabled=False,peers=[],pr_supervisor_authority_url='',fleet_owner_url='')
    users=UserDirectory(folder);user=users.create_user('synthetic-viewer','private-fixture-password',role='viewer');sessions=SessionManager(settings.session_secret)
    assert store.db_path.resolve()==(folder/'pr.db').resolve() and users.path.parent.resolve()==folder.resolve()
    runtime=AsyncRuntime()
    def forbidden_network(request):raise AssertionError('Private read fixture attempted network')
    network=httpx.AsyncClient(transport=httpx.MockTransport(forbidden_network))
    github=GitHubClient(GitHubCredentials(),client=network)
    # Constructor needs capabilities only. No domain/project/policy operations are called.
    domain=SimpleNamespace(card_completion_capabilities=None)
    with patch.object(GitHubCredentials,'load',side_effect=AssertionError('Credential lookup')):
        service=PRSupervisor(settings,domain,supervisor_store=store,github_client=github,http_client=network,async_runtime=runtime)
    services={'pr_supervisor_store':store,'pr_supervisor':service,'async_runtime':runtime}
    ctx=SimpleNamespace(settings=settings,services=services,require_service=services.__getitem__)
    app=FastAPI();app.state.ctx=ctx;app.include_router(module.router,prefix='/api')
    app.add_middleware(AuthMiddleware,settings=settings,users=users,sessions=sessions)
    @app.middleware('http')
    async def requests(request,next):
        wall=time.perf_counter();response=await next(request)
        runtime.record_request(getattr(request.scope.get('route'),'path','unmatched_auth'),response.status_code,(time.perf_counter()-wall)*1000,response_bytes=int(response.headers.get('content-length',0)))
        return response
    expected_metrics={'audit_events':event_count,'active_watches':sum(r.actionable for r in records),'historical_watches':sum(not r.actionable for r in records),'archived_watches':sum(r.retired_at is not None for r in records),'terminal_retirement_backlog':0}
    health=service.authority_health();assert all(health[k]==v for k,v in expected_metrics.items() if k!='audit_events')
    assert health['locally_owned_watches']==health['max_fence_token']==0
    setup_ms=(time.perf_counter()-start)*1000;before=fingerprint(store)
    selected=ContextVar('private_pr_read',default=None);counts=Counter();phases=defaultdict(list);sql={};lock=threading.Lock()
    samples,sizes=defaultdict(list),defaultdict(list);statuses=Counter();oracle_wall=[];oracle_cpu=[];hashes={};warnings=[];warning_counts=Counter()
    progress.update(samples=samples,statuses=statuses,counts=counts,warnings=warnings,warning_counts=warning_counts)
    def measured(name,call):
        @wraps(call)
        def run(*args,**kwargs):
            label=selected.get()
            if not label:return call(*args,**kwargs)
            wall,cpu=time.perf_counter(),time.thread_time()
            try:return call(*args,**kwargs)
            finally:
                with lock:
                    counts[label+':'+name]+=1
                    phases[name+':wall_ms'].append((time.perf_counter()-wall)*1000)
                    phases[name+':thread_cpu_ms'].append((time.thread_time()-cpu)*1000)
        return run
    original_serialize=fastapi_routing.serialize_response
    @wraps(original_serialize)
    async def serialize(*args,**kwargs):
        label=selected.get();wall,cpu=time.perf_counter(),time.thread_time()
        try:return await original_serialize(*args,**kwargs)
        finally:
            if label:
                with lock:
                    counts[label+':fastapi_serialize']+=1
                    phases['fastapi_serialize:wall_ms'].append((time.perf_counter()-wall)*1000)
                    phases['fastapi_serialize:thread_cpu_ms'].append((time.thread_time()-cpu)*1000)
    original_conn=store._conn
    @contextmanager
    def connection(*args,**kwargs):
        label=selected.get()
        if not label:
            with original_conn(*args,**kwargs) as conn:yield conn
            return
        bucket={};owned=current_observation.get() is None;observation=current_observation.get() or OperationObservation()
        token=current_observation.set(observation) if owned else None
        initial_rows=observation.sqlite_rows;wall=time.perf_counter()
        try:
            with capture_sqlite_calls(bucket),original_conn(*args,**kwargs) as conn:
                initial_changes=conn.total_changes
                try:yield conn
                finally:
                    with lock:counts[label+':durable_changes']+=conn.total_changes-initial_changes
        finally:
            with lock:
                counts[label+':connections']+=1;counts[label+':fetched_rows']+=observation.sqlite_rows-initial_rows
                phases['connection_scope:wall_ms'].append((time.perf_counter()-wall)*1000)
                total=sql.setdefault(label,{})
                for kind,item in bucket.items():
                    accum=total.setdefault(kind,{k:0 for k in ('calls','errors','wall_ms','thread_cpu_ms')})
                    for key in accum:accum[key]+=item[key]
            if token is not None:current_observation.reset(token)
    class NumericWarnings(logging.Handler):
        def emit(self,record):
            if record.levelno>=logging.WARNING:
                warning_counts[record.name+':'+record.levelname]+=1
                data=getattr(record,'performance_phases',None)
                if isinstance(data,dict):warnings.append({k:v for k,v in data.items() if isinstance(v,(int,float)) or k=='operation'})
    handler=NumericWarnings();logger=logging.getLogger();logger.addHandler(handler)
    variants={
        'list':('watches',{},True,200),'include_retired':('watches',{'include_retired':'true'},True,200),
        'realm':('watches',{'realm':'other'},True,200),'card':('watches',{'card_id':'synthetic-card-1'},True,200),
        'empty':('watches',{'card_id':'absent'},True,200),'detail':('watches/'+target,{},True,200),
        'missing':('watches/missing',{},True,404),'bad_boolean':('watches',{'include_retired':'invalid'},True,422),
        'unauthenticated':('watches',{},False,401),'metrics':('metrics',{},True,200),'health':('health',{},True,200)}
    pages={'attention':{'view':'attention','limit':25},'page2':{'view':'attention','limit':25,'offset':25},
        'all_max':{'view':'all','limit':101},'errors':{'view':'errors','limit':25},'query':{'view':'all','query_text':'synthetic-card-1','limit':25},'other_realm':{'realm_id':'other','view':'all','limit':25},
        'events_first':{'limit':100},'events_third':{'limit':100,'offset':200}}
    try:
        progress['phase']='startup';start=time.perf_counter();await runtime.start();startup_ms=(time.perf_counter()-start)*1000
        with ExitStack() as stack:
            stack.enter_context(patch.object(store,'_conn',connection))
            stack.enter_context(patch.object(fastapi_routing,'serialize_response',serialize))
            for name in ('_row_to_watch','list_watches','get_watch','list_events','list_dispatches','metrics','watch_inventory','list_watch_page','list_event_page'):
                stack.enter_context(patch.object(store,name,measured(name,getattr(store,name))))
            stack.enter_context(patch.object(PRWatch,'model_dump',measured('watch_dump',PRWatch.model_dump)))
            stack.enter_context(patch.object(JSONResponse,'render',measured('json_render',JSONResponse.render)))
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://fixture') as client:
                for _ in range(iterations):
                    for name in [*variants,*pages]:
                        progress['phase']='requests:'+name
                        async def foreground():
                            token=selected.set(name);wall=time.perf_counter()
                            try:
                                if name in variants:
                                    path,params,auth,status=variants[name]
                                    before_age=time.monotonic()
                                    response=await client.get('/api/pr-supervisor/'+path,params=params,headers={'Authorization':'Bearer '+user.cli_token} if auth else {})
                                    response.extensions['fixture_interval']=(before_age,time.monotonic())
                                    return response
                                params=dict(pages[name])
                                call=(lambda:store.list_event_page(target,**params)) if name.startswith('events_') else (lambda:store.list_watch_page(**{'realm_id':'default',**params}))
                                return await asyncio.to_thread(call)
                            finally:samples[name].append((time.perf_counter()-wall)*1000);selected.reset(token)
                        async def background():
                            token=selected.set('background');wall=time.perf_counter()
                            try:return await runtime.run_blocking('pr_supervisor.metric',store.metrics)
                            finally:samples['background'].append((time.perf_counter()-wall)*1000);selected.reset(token)
                        response,bg=await asyncio.gather(foreground(),background());assert bg==expected_metrics
                        wall,cpu=time.perf_counter(),time.thread_time()
                        if name in variants:
                            path,params,auth,status=variants[name];statuses[name+':'+str(response.status_code)]+=1;sizes[name].append(len(response.content))
                            assert response.status_code==status and len(response.content)<16*1024**2
                            if status==200:
                                if name=='detail':expected={'watch':next(r for r in records if r.id==target).model_dump(mode='json'),'events':events[:200],'notifications':[]}
                                elif name=='metrics':expected=expected_metrics
                                elif name=='health':
                                    age=response.json()['worker']['progress_age_seconds'];low,high=response.extensions['fixture_interval']
                                    assert isinstance(age,(int,float)) and low-.001<=age<=high+.001
                                    expected={**health,'worker':{**health['worker'],'progress_age_seconds':age}}
                                else:expected=expected_watches(records,realm=params.get('realm','default'),card=params.get('card_id'),include_retired=params.get('include_retired')=='true')[0]
                                assert response.json()==expected,'complete public payload mismatch'
                            digest=hashlib.sha256(response.content).hexdigest()
                            if name=='health':hashes.setdefault(name,[]).append(digest)
                            else:assert hashes.setdefault(name,digest)==digest
                        else:
                            rows,total=response;actual=[r.model_dump(mode='json') for r in rows];params=pages[name]
                            if name.startswith('events_'):expected,total_expected=events[max(0,params.get('offset',0)):max(0,params.get('offset',0))+min(100,max(1,params['limit']))],len(events)
                            else:expected,total_expected=expected_watches(records,realm=params.get('realm_id','default'),view=params['view'],query=params.get('query_text',''),limit=params['limit'],offset=params.get('offset',0))
                            assert actual==expected and total==total_expected
                        oracle_wall.append((time.perf_counter()-wall)*1000);oracle_cpu.append((time.thread_time()-cpu)*1000)
            snapshot=runtime.snapshot()
        assert fingerprint(store)==before and not any(v for k,v in counts.items() if k.endswith(':durable_changes'))
        assert service._task is None and not service._local_leases and not service._loop_supervisor._task
    finally:
        start=time.perf_counter();await runtime.close();await network.aclose();shutdown_ms=(time.perf_counter()-start)*1000;logger.removeHandler(handler)
    return {'population':population,'payload_bytes':payload_bytes,'events':event_count,'iterations':iterations,'setup_ms':round(setup_ms,3),'startup_ms':round(startup_ms,3),'shutdown_ms':round(shutdown_ms,3),
        'request_ms':{k:distribution(v) for k,v in samples.items()},'response_bytes':{k:distribution(v) for k,v in sizes.items()},'response_byte_totals':{k:sum(v) for k,v in sizes.items()},'status_counts':dict(statuses),'response_sha256':hashes,
        'counts':dict(counts),'phase_ms':{k:distribution(v) for k,v in phases.items()},'sql_call_columns':['calls','errors','wall_ms','thread_cpu_ms'],'sql_calls':{label:{kind:[round(item[k],3) for k in ['calls','errors','wall_ms','thread_cpu_ms']] for kind,item in group.items()} for label,group in sql.items()},
        'oracle_ms':distribution(oracle_wall),'oracle_thread_cpu_ms':distribution(oracle_cpu),'explicit_ge500ms':{k:sum(x>=500 for x in v) for k,v in samples.items()},'runtime':support.compact_runtime(snapshot),'warnings':warnings,'warning_counts':dict(warning_counts),'durable_fingerprint':before,'durable_parity':True,'worker_started':False}


def benchmark(*,population=160,iterations=6,payloads=(4096,32768),event_count=240):
    if not 24<=population<=160 or not 1<=iterations<=6 or not 1<=len(payloads)<=2 or any(not 1<=n<=32768 for n in payloads) or not 12<=event_count<=240:raise ValueError('bounded private inventory')
    result={'contract':'pa.pr-read-api-inventory/v1','distribution_columns':COLUMNS,'profiles':[]}
    with tempfile.TemporaryDirectory(prefix='pa-pr-read-api-') as tmp:
        root=Path(tmp).resolve()
        if shutil.disk_usage(root).free<4*1024**3:raise RuntimeError('At least4GiB free required')
        progress={}
        try:
            for i,payload in enumerate(payloads):result['profiles'].append(asyncio.run(profile(root/str(i),population=population,payload_bytes=payload,event_count=event_count,iterations=iterations,progress=progress)))
        except Exception as exc:result.update(failed=True,**support.minimum_failure(exc),failed_phase=progress.get('phase'),partial_status_counts=dict(progress.get('statuses',{})),partial_counts=dict(progress.get('counts',{})),partial_request_ms={k:distribution(v) for k,v in progress.get('samples',{}).items()},partial_warnings=progress.get('warnings',[]),partial_warning_counts=dict(progress.get('warning_counts',{})))
        result['fixture_storage']=support.fixture_bytes(root)
    result.update(temporary_fixture_removed=not root.exists(),script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),scope='Actual isolatedFastAPI PRrouter/store/defaultRuntime/AuthMiddleware/UserDirectory/SessionManager/PRSupervisor health, no worker/network/credentiallookup/lease/effect/dispatch/acceptance/control. Syntheticviewer CLIbearer; no nativeinstalledauth/browser/CSRFmutation/fullKernel/domainpolicy/provider/peercoverage. Domaincapabilityconstructor stub only, no domainmethods. Syntheticretiredprojections are NOT realGitHubproof. PrivateSQL path verifiedONCE; normalObservedConnection numericexecute/fetch/commit scopes, no SQL/results retained. Completepublicmodel/independentrealm/card/lifecycle/page/order/count/events/healthinventory oracle AFTERawaitedfg+bg. Directpage/eventqueries areUIcomponents, notnewHTML/HTTP endpoints. Runtimeoperationwindows include backgroundmetric calls, notallHTTPthreadpoolSQL; perAPIconnectionSQLcaptures separate. No productfix/native/deployedacceptance; no physicalIO/peakRAMclaim.')
    return result


if __name__=='__main__':print(json.dumps(benchmark(),separators=(',',':')))

"""Bounded private history component and authenticated Memory read inventory.

No Kernel, EventLog, worker, remote request, effect or acceptance receipt is used.
Run with uv; numeric aggregates stay in RAM and all new fixtures are disposable.
"""
import asyncio
import base64
from collections import Counter, defaultdict
from contextlib import ExitStack, contextmanager
from contextvars import ContextVar
from datetime import UTC, datetime, timedelta
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
import fastapi.routing as routing
from pa.auth.middleware import AuthMiddleware
from pa.auth.sessions import SessionManager
from pa.auth.users import UserDirectory
from pa.config import Settings
from pa.core.async_runtime import AsyncRuntime
from pa.core.performance import OperationObservation, current_observation, capture_sqlite_calls
from pa.domain.models import KnowledgeEntry, KnowledgeAuditEvent, KnowledgeProvenance
from pa.domain.projection import CardProjection
from pa.modules import items

spec=importlib.util.spec_from_file_location('private_pr_reference',Path(__file__).with_name('benchmark_pr_reads.py'))
pr=importlib.util.module_from_spec(spec);spec.loader.exec_module(pr)
distribution=pr.distribution


class Capture:
    """Only wraps the supplied, once path-verified private store instance."""
    def __init__(self, store):
        self.store=store;self.selected=ContextVar('private_memory_read',default=None)
        self.lock=threading.Lock();self.counts=Counter();self.phases=defaultdict(list);self.sql={}
        self.warnings=[];self.warning_counts=Counter()

    def measured(self, name, call):
        def run(*args,**kwargs):
            label=self.selected.get();wall,cpu=time.perf_counter(),time.thread_time()
            try:return call(*args,**kwargs)
            finally:
                if label:
                    with self.lock:
                        self.counts[label+':'+name]+=1
                        self.phases[name+':wall_ms'].append((time.perf_counter()-wall)*1000)
                        self.phases[name+':thread_cpu_ms'].append((time.thread_time()-cpu)*1000)
        return run

    @contextmanager
    def installed(self, methods, model):
        original=self.store._conn;capture=self
        @contextmanager
        def connection(*args,**kwargs):
            label=capture.selected.get()
            if not label:
                with original(*args,**kwargs) as conn:yield conn
                return
            bucket={};owned=current_observation.get() is None
            observation=current_observation.get() or OperationObservation()
            token=current_observation.set(observation) if owned else None
            before=observation.sqlite_rows;wall=time.perf_counter()
            try:
                with capture_sqlite_calls(bucket),original(*args,**kwargs) as conn:
                    initial=conn.total_changes
                    try:yield conn
                    finally:
                        with capture.lock:capture.counts[label+':changes']+=conn.total_changes-initial
            finally:
                with capture.lock:
                    capture.counts[label+':connections']+=1
                    capture.counts[label+':rows']+=observation.sqlite_rows-before
                    capture.phases['connection_scope:wall_ms'].append((time.perf_counter()-wall)*1000)
                    total=capture.sql.setdefault(label,{})
                    for kind,item in bucket.items():
                        values=total.setdefault(kind,[0,0,0,0])
                        for i,key in enumerate(['calls','errors','wall_ms','thread_cpu_ms']):values[i]+=item[key]
                if token is not None:current_observation.reset(token)
        original_serialize=routing.serialize_response
        async def serialize(*args,**kwargs):
            label=capture.selected.get();wall,cpu=time.perf_counter(),time.thread_time()
            try:return await original_serialize(*args,**kwargs)
            finally:
                if label:
                    with capture.lock:
                        capture.counts[label+':serialize']+=1
                        capture.phases['serialize:wall_ms'].append((time.perf_counter()-wall)*1000)
                        capture.phases['serialize:thread_cpu_ms'].append((time.thread_time()-cpu)*1000)
        class Warnings(logging.Handler):
            def emit(self,record):
                if record.levelno>=logging.WARNING:
                    capture.warning_counts[record.name+':'+record.levelname]+=1
                    data=getattr(record,'performance_phases',None)
                    if isinstance(data,dict):capture.warnings.append({k:v for k,v in data.items() if isinstance(v,(int,float)) or k=='operation'})
        handler=Warnings();logger=logging.getLogger();logger.addHandler(handler)
        try:
            with ExitStack() as stack:
                stack.enter_context(patch.object(self.store,'_conn',connection))
                stack.enter_context(patch.object(routing,'serialize_response',serialize))
                for method in methods:stack.enter_context(patch.object(self.store,method,self.measured(method,getattr(self.store,method))))
                stack.enter_context(patch.object(model,'model_dump',self.measured('model_dump',model.model_dump)))
                yield
        finally:logger.removeHandler(handler)

    def result(self):
        labels=sorted({key.rsplit(':',1)[0] for key in self.counts})
        names=sorted({key.rsplit(':',1)[1] for key in self.counts})
        return {'counter_columns':names,'counters':{label:[self.counts[label+':'+name] for name in names] for label in labels},
            'phase_ms':{k:distribution(v) for k,v in self.phases.items()},'sql_columns':['calls','errors','wall_ms','thread_cpu_ms'],
            'sql':{label:{kind:[round(v,3) for v in values] for kind,values in group.items()} for label,group in self.sql.items()},
            'warning_counts':dict(self.warning_counts),'warnings':self.warnings}


async def history_profile(folder,iterations):
    folder.mkdir();start=time.perf_counter();store,records,_,_=pr.seed(folder,520,1024,12)
    assert store.db_path.resolve()==(folder/'pr.db').resolve()
    setup=(time.perf_counter()-start)*1000;before=pr.fingerprint(store)
    capture=Capture(store);runtime=AsyncRuntime();samples=defaultdict(list);oracle=[];counts={}
    cases={'default':{},'max':{'limit':100},'page2':{'offset':25},'query':{'query_text':'synthetic-card-1'},
        'other_realm':{'realm_id':'other'},'empty':{'query_text':'absent'},'past_end':{'offset':104},'clamp':{'limit':101,'offset':-1}}
    expected_metrics=store.metrics()
    start=time.perf_counter();await runtime.start();startup=(time.perf_counter()-start)*1000
    try:
        with capture.installed(['list_watch_page','_row_to_watch','metrics'],pr.PRWatch):
            for _ in range(iterations):
                for name,params in cases.items():
                    async def foreground():
                        token=capture.selected.set(name);wall=time.perf_counter()
                        try:return await asyncio.to_thread(store.list_watch_page,**{'realm_id':'default','view':'history',**params})
                        finally:samples[name].append((time.perf_counter()-wall)*1000);capture.selected.reset(token)
                    async def background():
                        token=capture.selected.set('background');wall=time.perf_counter()
                        try:return await runtime.run_blocking('pr_supervisor.metric',store.metrics)
                        finally:samples['background'].append((time.perf_counter()-wall)*1000);capture.selected.reset(token)
                    (rows,total),bg=await asyncio.gather(foreground(),background())
                    cpu=time.thread_time()
                    expected,expected_total=pr.expected_watches(records,realm=params.get('realm_id','default'),view='history',query=params.get('query_text',''),limit=params.get('limit',25),offset=params.get('offset',0))
                    assert [r.model_dump(mode='json') for r in rows]==expected and total==expected_total
                    assert bg==expected_metrics;counts[name]=[len(rows),total]
                    oracle.append((time.thread_time()-cpu)*1000)
            snapshot=runtime.snapshot()
        assert before==pr.fingerprint(store) and not any(v for k,v in capture.counts.items() if k.endswith(':changes'))
    finally:
        start=time.perf_counter();await runtime.close();close=(time.perf_counter()-start)*1000
    return {'population':520,'synthetic_retired_default':104,'iterations':iterations,'setup_ms':round(setup,3),'runtime_start_ms':round(startup,3),'runtime_close_ms':round(close,3),
        'case_counts_rows_total':counts,'request_ms':{k:distribution(v) for k,v in samples.items()},'oracle_cpu_ms':distribution(oracle),
        'ge500ms':{k:sum(v>=500 for v in values) for k,values in samples.items()},'capture':capture.result(),'runtime':pr.support.compact_runtime(snapshot),'fingerprint':before,'durable_parity':True,
        'scope':'ActualprivatePRStore history-only UIquerycomponent, notHTML/API/newbudget/nativeGitHubretirementproof. Syntheticretiredprojections; fullmodel/order/realm/query/count/default25/max100/offset/clamp parity after awaitedfg+bg. No worker/lease/effects.'}


def memory_seed(folder,population,payload_bytes,owner):
    store=CardProjection(folder/'memory.db');now=datetime.now(UTC)-timedelta(days=2);records=[]
    for i in range(population):
        entry=KnowledgeEntry(id=f'synthetic-memory-{i:04}',item_id=f'synthetic-card-{i%7}',card_id=f'synthetic-card-{i%7}',
            summary=f'Synthetic note {i:04}: '+('context '*((payload_bytes+7)//8))[:payload_bytes],
            source='transcript' if i%17==0 else 'manual',source_url=f'https://synthetic.invalid/memory/{i}',
            kind=['memory','decision','constraint','runbook','learning'][i%5],tier=['working','episodic','semantic','procedural'][i%4],
            status='review' if i%9==0 else 'archived' if i%19==0 else 'active',scope='project' if i%3==0 else 'realm',
            owner=owner if i%2==0 else 'synthetic-other',confidence=.75,sensitivity=['public','internal','confidential','restricted'][i%4],
            provenance_trust='unverified',supersedes_id='synthetic-memory-0001' if i%11==0 else None,
            review_at=now+timedelta(days=4),expires_at=now if i%6==0 else now+timedelta(days=10) if i%6==1 else None,
            tags=['synthetic','auto-capture'] if i%23==0 else ['synthetic',f'topic-{i%3}'],
            provenance=KnowledgeProvenance(actor='synthetic-fixture',action='manual-note',transformation='private-fixture/v1',recorded_at=now),
            created_at=now,updated_at=now+timedelta(seconds=i//2))
        actual=store.add_knowledge(entry);assert actual.id==entry.id;records.append(entry)
    events=[]
    for i in range(120):
        event=KnowledgeAuditEvent(id=f'synthetic-audit-{i:04}',knowledge_id=records[-1].id,action='synthetic-note-seeded',actor='synthetic-fixture',payload={'sequence':i},created_at=now+timedelta(seconds=i//2))
        store.add_knowledge_audit(event);events.append(event)
    assert len(store.list_knowledge(limit=10000,status=None))==population
    return store,records,events


def expected_memory(records,params,principal):
    rows=[r for r in records if r.source not in {'session','acp_session','remote_dispatch','dispatch','transcript'}]
    for key in ['kind','status','scope','source','tier','sensitivity']:
        value=params.get(key,'active' if key=='status' else None)
        if value:rows=[r for r in rows if str(getattr(r,key))==value]
    if params.get('item_id'):rows=[r for r in rows if params['item_id'] in [r.item_id,r.card_id]]
    if params.get('provenance'):rows=[r for r in rows if r.provenance_trust==params['provenance']]
    if params.get('q'):
        needle=params['q'].lower();rows=[r for r in rows if needle in r.summary.lower() or needle in json.dumps(r.tags).lower() or needle in (r.owner or '').lower()]
    if params.get('expiry'):
        now=datetime.now(UTC);rows=[r for r in rows if (r.expires_at is not None and r.expires_at<=now)==(params['expiry']=='expired')]
    if params.get('supersession'):rows=[r for r in rows if (r.supersedes_id is not None)==(params['supersession']=='supersedes')]
    if params.get('cursor'):
        stamp,identity=base64.urlsafe_b64decode(params['cursor']+'='*(-len(params['cursor'])%4)).decode().split('|',1)
        cutoff=datetime.fromisoformat(stamp);rows=[r for r in rows if (r.updated_at,r.id)<(cutoff,identity)]
    rows.sort(key=lambda r:(r.updated_at,r.id),reverse=True);limit=int(params.get('limit',25));more=len(rows)>limit;rows=rows[:limit]
    public=[]
    for row in rows:
        value=row.model_dump(mode='json')
        if row.sensitivity in {'restricted','confidential'} and row.owner!=principal:
            value.update(summary='[redacted: restricted Memory]',source_url=None,tags=[],provenance=None)
        public.append(value)
    cursor=base64.urlsafe_b64encode((rows[-1].updated_at.isoformat()+'|'+rows[-1].id).encode()).decode().rstrip('=') if rows and more else None
    return {'records':public,'page_size':limit,'has_more':more,'next_cursor':cursor}


async def memory_profile(folder,population,payload_bytes,iterations,progress):
    folder.mkdir();start=time.perf_counter()
    settings=Settings(_env_file=None,data_dir=folder,instance_id='synthetic-local',instance_url='http://fixture',session_secret='private-fixture-only',auth_required=True,sync_token='',agent_enabled=False,peers=[])
    users=UserDirectory(folder);owner=users.create_user('synthetic-owner','private-fixture-password',role='viewer');other=users.create_user('synthetic-reader','private-fixture-password',role='viewer')
    principal='user:'+owner.id;other_principal='user:'+other.id
    store,records,events=memory_seed(folder,population,payload_bytes,principal)
    assert store.db_path.resolve()==(folder/'memory.db').resolve() and users.path.parent.resolve()==folder.resolve()
    # Imported global factory is scoped to this private actual projection only.
    runtime=AsyncRuntime();ctx=SimpleNamespace(settings=settings,services={'async_runtime':runtime})
    app=FastAPI();app.state.ctx=ctx;app.include_router(items.router,prefix='/api')
    app.add_middleware(AuthMiddleware,settings=settings,users=users,sessions=SessionManager(settings.session_secret))
    @app.middleware('http')
    async def requests(request,next):
        wall=time.perf_counter();response=await next(request)
        runtime.record_request(getattr(request.scope.get('route'),'path','unmatched_auth'),response.status_code,(time.perf_counter()-wall)*1000,response_bytes=int(response.headers.get('content-length',0)))
        return response
    setup=(time.perf_counter()-start)*1000;before=pr.fingerprint(store);capture=Capture(store)
    samples,sizes=defaultdict(list),defaultdict(list);status_counts=Counter();oracle_cpu=[];hashes={}
    cursor=expected_memory(records,{},principal)['next_cursor'];assert cursor
    cases={'default':({},200),'max100':({'limit':100},200),'cursor':({'cursor':cursor},200),'search':({'q':'note 01'},200),
        'tags':({'q':'topic-1'},200),'owner_search':({'q':principal},200),'kind':({'kind':'decision'},200),'review':({'status':'review'},200),
        'scope':({'scope':'project'},200),'source':({'source':'manual'},200),'curated_excludes':({'source':'transcript'},200),
        'tier':({'tier':'working'},200),'restricted':({'sensitivity':'restricted'},200),'provenance':({'provenance':'unverified'},200),
        'expired':({'expiry':'expired'},200),'current':({'expiry':'current'},200),'original':({'supersession':'original'},200),
        'supersedes':({'supersession':'supersedes'},200),'item':({'item_id':'synthetic-card-1'},200),'empty':({'q':'absent-marker'},200),
        'other_principal':({'limit':100},200),'bad_cursor':({'cursor':'invalid'},422),'bad_kind':({'kind':'invalid'},422),
        'bad_expiry':({'expiry':'invalid'},422),'limit0':({'limit':0},422),'limit101':({'limit':101},422),
        'unauthenticated':({},401),'audit':({},200),'entry_audit':({},200),'missing_audit':({},404)}
    audit_expected=[{'id':r.id,'source':r.source,'status':r.status.value,'reasons':['unintended-auto-capture'],'session_id':None,'recoverable':False,'repair':'review-only'} for r in sorted(records,key=lambda r:(r.updated_at,r.id),reverse=True) if 'auto-capture' in r.tags]
    events_expected=[r.model_dump(mode='json') for r in sorted(events,key=lambda r:(r.created_at,r.id),reverse=True)[:100]]
    progress.update(samples=samples,status_counts=status_counts,capture=capture)
    start=time.perf_counter();await runtime.start();startup=(time.perf_counter()-start)*1000
    try:
        with patch.object(items,'get_store',return_value=store),capture.installed(['list_knowledge','get_knowledge','list_knowledge_audit','_row_to_knowledge'],KnowledgeEntry):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://fixture') as client:
                for _ in range(iterations):
                    for name,(params,status) in cases.items():
                        progress['phase']='memory:'+name
                        async def foreground():
                            token=capture.selected.set(name);wall=time.perf_counter()
                            path='/api/knowledge/audit' if name=='audit' else '/api/knowledge/'+records[-1].id+'/audit' if name=='entry_audit' else '/api/knowledge/absent/audit' if name=='missing_audit' else '/api/knowledge'
                            user=other if name=='other_principal' else owner
                            try:return await client.get(path,params=params,headers={} if name=='unauthenticated' else {'Authorization':'Bearer '+user.cli_token})
                            finally:samples[name].append((time.perf_counter()-wall)*1000);capture.selected.reset(token)
                        async def background():
                            token=capture.selected.set('background');wall=time.perf_counter()
                            def read():
                                try:return store.get_knowledge(records[-1].id)
                                finally:store.close_thread_connection()
                            try:return await asyncio.to_thread(read)
                            finally:samples['background'].append((time.perf_counter()-wall)*1000);capture.selected.reset(token)
                        response,bg=await asyncio.gather(foreground(),background())
                        cpu=time.thread_time();status_counts[name+':'+str(response.status_code)]+=1;sizes[name].append(len(response.content))
                        assert len(response.content)<16*1024**2 and response.status_code==status,(name,response.status_code)
                        assert bg.model_dump(mode='json')==records[-1].model_dump(mode='json')
                        if status==200:
                            expected={'records':audit_expected,'count':len(audit_expected),'mutated':False} if name=='audit' else events_expected if name=='entry_audit' else expected_memory(records,params,other_principal if name=='other_principal' else principal)
                            assert response.json()==expected,('full_public_oracle',name)
                        if name=='missing_audit':assert response.json()=={'detail':'Memory not found'}
                        if name=='bad_cursor':assert response.json()=={'detail':'Invalid Memory cursor'}
                        digest=hashlib.sha256(response.content).hexdigest();assert hashes.setdefault(name,digest)==digest
                        oracle_cpu.append((time.thread_time()-cpu)*1000)
            snapshot=runtime.snapshot()
        assert pr.fingerprint(store)==before and not any(v for k,v in capture.counts.items() if k.endswith(':changes'))
    finally:
        start=time.perf_counter();await runtime.close();store.close_thread_connection();close=(time.perf_counter()-start)*1000
    return {'population':population,'payload_bytes':payload_bytes,'iterations':iterations,'setup_ms':round(setup,3),'runtime_start_ms':round(startup,3),'runtime_close_ms':round(close,3),
        'request_ms':{k:distribution(v) for k,v in samples.items()},'bytes_sum_max':{k:[sum(v),max(v)] for k,v in sizes.items()},'status_counts':dict(status_counts),'response_hashes':hashes,
        'oracle_cpu_ms':distribution(oracle_cpu),'ge500ms':{k:sum(v>=500 for v in values) for k,values in samples.items()},'capture':capture.result(),'runtime':pr.support.compact_runtime(snapshot),'fingerprint':before,'durable_parity':True}


def benchmark(*,iterations=6,population=240,payloads=(1024,8192),history=True):
    if not 1<=iterations<=6 or not 120<=population<=240 or not 1<=len(payloads)<=2 or any(not 1<=v<=8192 for v in payloads):raise ValueError('bounded read inventory')
    result={'contract':'pa.history-memory-read-inventory/v1','distribution_columns':pr.COLUMNS,'profiles':[]};progress={}
    with tempfile.TemporaryDirectory(prefix='pa-history-memory-') as tmp:
        root=Path(tmp).resolve()
        if shutil.disk_usage(root).free<4*1024**3:raise RuntimeError('At least4GiB free required')
        try:
            if history:result['history']=asyncio.run(history_profile(root/'history',iterations))
            for i,payload in enumerate(payloads):result['profiles'].append(asyncio.run(memory_profile(root/str(i),population,payload,iterations,progress)))
        except Exception as exc:
            result.update(failed=True,**pr.support.minimum_failure(exc),failed_phase=progress.get('phase'),partial_status_counts=dict(progress.get('status_counts',{})),partial_request_ms={k:distribution(v) for k,v in progress.get('samples',{}).items()},partial_capture=progress['capture'].result() if 'capture' in progress else {})
        result['fixture_storage']=pr.support.fixture_bytes(root)
    result.update(temporary_fixture_removed=not root.exists(),script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        scope='Actualprivateprojection and actualitemsFastAPIrouter/AuthMiddleware/UserDirectory/SessionManager/defaultRuntime; scopeditems.get_store adapter returnsONLYexactfreshprivateCardProjection. No Kernel/EventLog/sync/bootstrap/othermiddleware/CSRFmutation/nativeauth/provider/network/worker/lease/effect/receipt/acceptance/deployment proof. Knowledgeaudit candidates harmlesssyntheticmanualauto-capturetags, session_idNone; no recoverabletranscript/providerbranch. No product/default/thresholdchange. PrivateObservedConnection numericalSQL/counters inclusivephases; rootsguardedONCE, no globalSQLiteinterception/perrowPath.resolve; no successfulbusywait/lockhold/physicalIO/SQLbyte/peakRAMclaim. API500ms threshold compared; backgrounddirectcomponent timer NOTnewruntimebudget. Fullpublicindependentfilter/order/cursor/redaction/audit oracleAFTERallawaitedfg/bg/serverwork; RAMresponsebounded16MiB. Readfingerprints/changes verify no durablewrites; notconcurrentwriter/CAS/acceptancecoverage.')
    return result


def compact(result):
    """Lossless column encoding; absent phase remains absent, never a fake zero."""
    document=json.loads(json.dumps(result))
    profiles=([document['history']] if 'history' in document else [])+document['profiles']
    for profile in profiles:
        capture=profile['capture'];names=list(capture['phase_ms'])
        capture['phase_names']=names;capture['phase_ms']=[capture['phase_ms'][key] for key in names]
        labels=list(capture['sql']);kinds=sorted({kind for group in capture['sql'].values() for kind in group})
        capture['sql_kinds']=kinds
        original=capture['sql'];capture['sql']={label:[original[label].get(kind) for kind in kinds] for label in labels}
        assert {label:{kind:value for kind,value in zip(kinds,values) if value is not None} for label,values in capture['sql'].items()}==original
        if 'response_hashes' in profile:
            values=list(dict.fromkeys(profile['response_hashes'].values()))
            original=profile['response_hashes'];profile['hash_values']=values
            profile['response_hashes']={label:values.index(value) for label,value in original.items()}
            assert {label:values[index] for label,index in profile['response_hashes'].items()}==original
        runtime=profile['runtime'];routes=runtime.get('routes',{})
        # Runtime's dictionaries already identify sample windows vs process totals.
        if routes:
            columns=sorted({key for route in routes.values() for key in route})
            runtime['route_columns']=columns;runtime['routes']={name:[route.get(key) for key in columns] for name,route in routes.items()}
    return document


if __name__=='__main__':print(json.dumps(benchmark(),separators=(',',':')))

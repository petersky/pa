"""Private actual Memory partial/context read probe; no Kernel or worker."""
import asyncio
from collections import Counter, defaultdict
from datetime import UTC, datetime
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import tempfile
import time
from types import SimpleNamespace
from unittest.mock import patch

import httpx
from fastapi import FastAPI
from fastapi.templating import Jinja2Templates
from jinja2 import Template
from pa.auth.middleware import AuthMiddleware
from pa.auth.sessions import SessionManager
from pa.auth.users import UserDirectory
from pa.config import Settings
from pa.core.async_runtime import AsyncRuntime
from pa.domain.models import (CardCreate, ProjectCreate, KnowledgeEntry,
    KnowledgeKind, KnowledgeStatus, KnowledgeTier, KnowledgeSensitivity)
from pa.modules import items
from pa.server.template_cache import PackagedTemplateCache

spec=importlib.util.spec_from_file_location('private_memory_reference',Path(__file__).with_name('benchmark_memory_reads.py'))
memory=importlib.util.module_from_spec(spec);spec.loader.exec_module(memory)
distribution=memory.distribution


def expected_context(records,cards,projects,params,principal,settings):
    filters={key:params.get(key,'active' if key=='status' else '').strip() for key in ['q','kind','status','scope','source','tier','sensitivity','provenance','expiry','supersession']}
    query={key:value for key,value in filters.items() if value}
    query['status']=filters['status'] or None
    if params.get('cursor'):query['cursor']=params['cursor']
    page=memory.expected_memory(records,query,principal)
    realm=params.get('realm') or settings.primary_realm
    from urllib.parse import urlencode
    next_url='/knowledge?'+urlencode({**params,'cursor':page['next_cursor']}) if page['has_more'] else None
    selected_cards=sorted([c for c in cards if c.realm_id==realm],key=lambda c:c.updated_at,reverse=True)[:500]
    selected_projects=sorted([p for p in projects if p.realm_id==realm],key=lambda p:p.updated_at,reverse=True)
    return {'knowledge':[KnowledgeEntry.model_validate(row) for row in page['records']],
        'memory_page_size':25,'memory_has_more':page['has_more'],'memory_next_url':next_url,
        'cards':selected_cards,'items':selected_cards,'projects':selected_projects,
        'knowledge_kinds':list(KnowledgeKind),'knowledge_statuses':list(KnowledgeStatus),
        'knowledge_filters':filters,'knowledge_tiers':list(KnowledgeTier),'knowledge_sensitivities':list(KnowledgeSensitivity),
        'knowledge_provenance_trust':['verified','unverified','legacy'],'knowledge_sources':['promoted','manual','generated','imported'],
        'promote_session_id':params.get('session',''),'realms':settings.subscribed_realms,'active_realm':realm}


def public_context(context):
    return {key:[row.model_dump(mode='json') for row in value] if key in {'knowledge','cards','items','projects'} else value for key,value in context.items()}


def difference_paths(left,right,path=''):
    if isinstance(left,dict) and isinstance(right,dict):
        return [p for key in sorted(left.keys()|right.keys()) for p in difference_paths(left.get(key),right.get(key),path+'/'+key)]
    if isinstance(left,list) and isinstance(right,list):
        if len(left)!=len(right):return [path+'/length']
        return [p for i,(a,b) in enumerate(zip(left,right)) for p in difference_paths(a,b,path+'/'+str(i))]
    return [] if left==right else [path]


async def profile(folder,*,card_count,card_bytes,note_count,note_bytes,iterations,progress):
    folder.mkdir();start=time.perf_counter()
    settings=Settings(_env_file=None,data_dir=folder,instance_id='synthetic-local',instance_url='http://fixture',session_secret='private-fixture-only',auth_required=True,sync_token='',agent_enabled=False,peers=[],subscribed_realms=['default','other'])
    users=UserDirectory(folder);owner=users.create_user('synthetic-owner','private-fixture-password',role='viewer');other=users.create_user('synthetic-reader','private-fixture-password',role='viewer')
    assert users.ensure_default_user().id==owner.id
    principal='user:'+owner.id;other_principal='user:'+other.id
    store,records,_=memory.memory_seed(folder,note_count,note_bytes,principal)
    markup=store.add_knowledge(KnowledgeEntry(id='synthetic-markup',summary='Synthetic markup <strong>context & safety</strong> "quoted"',owner=principal,sensitivity='restricted',tags=['synthetic-markup'],source_url='https://synthetic.invalid/?a=1&b=2'))
    records.append(markup)
    projects=[store.create_project(ProjectCreate(title=f'Synthetic project {i} <&>',realm_id='other' if i%2 else 'default'),principal_id=principal,instance_id='synthetic-local') for i in range(6)]
    cards=[]
    for i in range(card_count):
        realm='other' if i%4==0 else 'default';project=next(p for p in projects if p.realm_id==realm)
        cards.append(store.create_card(CardCreate(title=f'Synthetic card {i:04} <&>',body='c'*card_bytes,summary=f'Synthetic summary {i:04}',realm_id=realm,project_id=project.id),principal_id=principal,instance_id='synthetic-local'))
    assert len(store.list_cards(limit=500))==card_count and len(store.list_knowledge(limit=10000,status=None))==note_count+1
    assert store.db_path.resolve()==(folder/'memory.db').resolve() and users.path.parent.resolve()==folder.resolve()
    runtime=AsyncRuntime();app=FastAPI();app.state.ctx=SimpleNamespace(settings=settings,services={'async_runtime':runtime})
    template_root=Path(items.__file__).parents[1]/'server/templates'
    templates=Jinja2Templates(directory=str(template_root));templates.env.bytecode_cache=PackagedTemplateCache(template_root.parent/'template-bytecode.json')
    app.state.templates=templates
    reference_templates=Jinja2Templates(directory=str(template_root))
    app.include_router(items.ui_router);app.add_middleware(AuthMiddleware,settings=settings,users=users,sessions=SessionManager(settings.session_secret))
    @app.middleware('http')
    async def requests(request,next):
        wall=time.perf_counter();response=await next(request)
        runtime.record_request(getattr(request.scope.get('route'),'path','unmatched_auth'),response.status_code,(time.perf_counter()-wall)*1000,response_bytes=int(response.headers.get('content-length',0)))
        return response
    setup=(time.perf_counter()-start)*1000;before=memory.pr.fingerprint(store);users_before=hashlib.sha256(users.path.read_bytes()).digest()
    capture=memory.Capture(store);contexts={};samples,sizes=defaultdict(list),defaultdict(list);statuses=Counter();hashes={};oracle_cpu=[];context_counts={}
    original_context=items._knowledge_context
    def context(request):
        value=capture.measured('knowledge_context',original_context)(request)
        contexts[capture.selected.get()]=(value,request)
        return value
    cursor=memory.expected_memory(records,{},principal)['next_cursor'];assert cursor
    cases={'default':({},200),'cursor':({'cursor':cursor},200),'empty':({'q':'absent-marker'},200),
        'other_realm':({'realm':'other'},200),'other_reader':({},200),'review':({'status':'review'},200),
        'restricted':({'sensitivity':'restricted'},200),'markup':({'q':'Synthetic markup'},200),
        'raw_unknown_kind':({'kind':'not-a-kind'},200),'bad_cursor':({'cursor':'invalid'},422),'no_bearer_default':({},200)}
    progress.update(samples=samples,statuses=statuses,capture=capture)
    start=time.perf_counter();await runtime.start();startup=(time.perf_counter()-start)*1000
    try:
        with patch.object(items,'get_store',return_value=store),patch.object(items,'get_settings',return_value=settings),patch.object(items,'_knowledge_context',context),patch.object(Template,'render',capture.measured('template_render',Template.render)),capture.installed(['list_knowledge','get_knowledge','_row_to_knowledge','list_cards','_row_to_card','list_projects','_row_to_project'],KnowledgeEntry):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url='http://fixture') as client:
                for _ in range(iterations):
                    for name,(params,status) in cases.items():
                        progress['phase']='partial:'+name
                        async def foreground():
                            token=capture.selected.set(name);wall=time.perf_counter()
                            try:return await client.get('/partials/knowledge',params=params,headers={} if name=='no_bearer_default' else {'Authorization':'Bearer '+(other if name=='other_reader' else owner).cli_token})
                            finally:samples[name].append((time.perf_counter()-wall)*1000);capture.selected.reset(token)
                        async def background():
                            token=capture.selected.set('background');wall=time.perf_counter()
                            def read():
                                try:return store.get_knowledge(markup.id)
                                finally:store.close_thread_connection()
                            try:return await asyncio.to_thread(read)
                            finally:samples['background'].append((time.perf_counter()-wall)*1000);capture.selected.reset(token)
                        response,bg=await asyncio.gather(foreground(),background())
                        cpu=time.thread_time();statuses[name+':'+str(response.status_code)]+=1;sizes[name].append(len(response.content))
                        assert response.status_code==status and len(response.content)<16*1024**2,(name,response.status_code)
                        assert bg.model_dump(mode='json')==markup.model_dump(mode='json')
                        if status==200:
                            actual,request=contexts.pop(name)
                            expected=expected_context(records,cards,projects,params,other_principal if name=='other_reader' else principal,settings)
                            # Starlette TemplateResponse inserts this exact request
                            # into the supplied dictionary before rendering.
                            assert actual['request'] is request
                            assert request.state.principal_id==(other_principal if name=='other_reader' else principal)
                            expected['request']=request
                            actual_public,expected_public=public_context(actual),public_context(expected)
                            if actual_public!=expected_public:
                                progress['oracle_diff_paths']=difference_paths(actual_public,expected_public)[:24]
                                raise AssertionError('complete_context')
                            if actual['cards']:assert actual['cards'][0] is not actual['items'][0]
                            body=reference_templates.TemplateResponse(request,'partials/knowledge.html',expected).body
                            assert response.content==body,('complete_HTML',name)
                            context_counts[name]=[len(actual['knowledge']),len(actual['cards']),len(actual['items']),len(actual['projects']),actual['memory_has_more']]
                            del actual,expected,body,request,actual_public,expected_public
                        elif name=='bad_cursor':assert response.json()=={'detail':'Invalid Memory cursor'} and name not in contexts
                        digest=hashlib.sha256(response.content).hexdigest();assert hashes.setdefault(name,digest)==digest
                        oracle_cpu.append((time.thread_time()-cpu)*1000)
            snapshot=runtime.snapshot()
        assert before==memory.pr.fingerprint(store) and users_before==hashlib.sha256(users.path.read_bytes()).digest() and not any(v for k,v in capture.counts.items() if k.endswith(':changes')) and not contexts
    finally:
        start=time.perf_counter();await runtime.close();store.close_thread_connection();close=(time.perf_counter()-start)*1000
    return {'cards':card_count,'card_bytes':card_bytes,'notes':note_count+1,'note_bytes':note_bytes,'projects':6,'iterations':iterations,
        'setup_ms':round(setup,3),'runtime_start_ms':round(startup,3),'runtime_close_ms':round(close,3),
        'context_columns':['knowledge','cards','items','projects','has_more'],'contexts':context_counts,
        'request_ms':{k:distribution(v) for k,v in samples.items()},'bytes_sum_max':{k:[sum(v),max(v)] for k,v in sizes.items()},'status_counts':dict(statuses),'response_hashes':hashes,
        'oracle_cpu_ms':distribution(oracle_cpu),'ge500ms':{k:sum(v>=500 for v in values) for k,values in samples.items()},
        'capture':capture.result(),'runtime':memory.pr.support.compact_runtime(snapshot),'fingerprint':before,'durable_parity':True,'auth_directory_unchanged':True}


def benchmark(*,iterations=6,card_count=160,card_bytes=16384,note_count=240,note_bytes=1024):
    if not 1<=iterations<=6 or not 8<=card_count<=160 or not 1<=card_bytes<=16384 or not 120<=note_count<=240 or not 1<=note_bytes<=4096:raise ValueError('bounded private Memory UI inventory')
    result={'contract':'pa.memory-ui-read-inventory/v1','distribution_columns':memory.pr.COLUMNS,'profiles':[]};progress={}
    with tempfile.TemporaryDirectory(prefix='pa-memory-ui-') as tmp:
        root=Path(tmp).resolve()
        if shutil.disk_usage(root).free<4*1024**3:raise RuntimeError('At least4GiB free required')
        try:result['profiles'].append(asyncio.run(profile(root/'profile',iterations=iterations,card_count=card_count,card_bytes=card_bytes,note_count=note_count,note_bytes=note_bytes,progress=progress)))
        except Exception as exc:result.update(failed=True,**memory.pr.support.minimum_failure(exc),failed_phase=progress.get('phase'),oracle_diff_paths=progress.get('oracle_diff_paths'),partial_status_counts=dict(progress.get('statuses',{})),partial_request_ms={k:distribution(v) for k,v in progress.get('samples',{}).items()},partial_capture=progress['capture'].result() if 'capture' in progress else {})
        result['fixture_storage']=memory.pr.support.fixture_bytes(root)
    result.update(temporary_fixture_removed=not root.exists(),script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        scope='ActualprivateitemsUIrouter/knowledge_partial/_knowledge_context/CardProjection/Jinja2Templates/readonlyPackagedTemplateCache/AuthMiddleware/UserDirectory/SessionManager/defaultRuntime. Bothglobalitems.get_store/get_settings boundONLYexactfreshprivateprojection/settings; nohostdefault. NoKernel/UnifiedStore/EventLog/sync/bootstrap/shell/fullpageHTML/contextprocessors/nativeinstalledauth/policy/provider/peer/worker/lease/effect/receipt/acceptance/deployment proof. OnlyGETpartial; noformPOST/CSRFmutation. Actualpartial suppliesnoexplicitcsrf_token (existingundefinedtemplatevalue), no inventedtoken. Realmqueryfilterscards/projects, NOTknowledge (existingstorecontract); notnativeMemorypolicyacceptance. Syntheticunprivilegednotes/cards/projects normalprivatecreation/nofakeacceptance. Completecontext/HTMLoracleusesindependentselectedmodels andfreshreferenceJinjaenvironment/sametemplateAFTERallawaitedfg/bg/serverwork; notindependenttemplateimplementationproof. Contextsdiscardedperrequest/boundedresponses16MiB; nopeakRAM/physicalIOclaim. NumericprivateObservedConnection captures/phaseoverlap,notnewruntimebudget; nativebusywait/lockhold/SQLbytes/observeroverheadnotisolated. Singleboundedcohort/sixsamplesnotnativecapacity; no productfixwithoutmaterialevidence.')
    return result


if __name__=='__main__':print(json.dumps(memory.compact(benchmark()),separators=(',',':')))

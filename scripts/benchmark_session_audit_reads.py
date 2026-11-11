"""Private Session audit context/template probe, explicitly not an HTTP fixture.

Requires report reserve before creating any disposable storage. No worker, provider,
dispatch, binding, lease, transition or acceptance receipt is created or exercised.
"""
import asyncio
import base64
from collections import Counter, defaultdict
from datetime import UTC, datetime, timedelta
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import tempfile
import time
from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import urlencode

from fastapi import HTTPException
from fastapi.templating import Jinja2Templates
from jinja2 import Template
from starlette.requests import Request
from pa.config import Settings
from pa.core.async_runtime import AsyncRuntime
from pa.domain.models import AgentSession
from pa.domain.projection import CardProjection
from pa.modules import items
from pa.server.template_cache import PackagedTemplateCache

spec=importlib.util.spec_from_file_location('session_audit_support',Path(__file__).with_name('benchmark_memory_reads.py'))
memory=importlib.util.module_from_spec(spec);spec.loader.exec_module(memory)
distribution=memory.distribution


def expected_page(records,params,realms):
    realm=params.get('realm','default');rows=[r for r in records if r.realm_id==realm]
    if params.get('cursor'):
        raw=base64.urlsafe_b64decode(params['cursor']+'='*(-len(params['cursor'])%4)).decode()
        stamp,identity=raw.split('|',1);before=datetime.fromisoformat(stamp)
        rows=[r for r in rows if (r.updated_at,r.id)<(before,identity)]
    rows=sorted(rows,key=lambda r:(r.updated_at,r.id),reverse=True);more=len(rows)>25;page=rows[:25]
    cursor=base64.urlsafe_b64encode((page[-1].updated_at.isoformat()+'|'+page[-1].id).encode()).decode().rstrip('=') if more and page else None
    return {'audit_sessions':page,'memory_page_size':25,'memory_has_more':more,
        'memory_next_url':'/memory-audit?'+urlencode({'realm':realm,'cursor':cursor}) if cursor else None,
        'realms':realms,'active_realm':realm}


def public_page(context):
    return {key:[r.model_dump(mode='json') for r in value] if key=='audit_sessions' else value for key,value in context.items()}


def seed(folder,population,payload_bytes):
    store=CardProjection(folder/'sessions.db');now=datetime.now(UTC)-timedelta(days=1);records=[]
    for i in range(population):
        record=AgentSession(id=f'synthetic-audit-{i:04}',agent_name='Synthetic unlaunched <&>',realm_id='other' if i%4==0 else 'default',
            title=f'Synthetic audit {i:04}',config_json={'synthetic_note':'c'*min(payload_bytes,2048)},
            metrics_json={'synthetic_sample':'m'*payload_bytes},created_at=now,updated_at=now+timedelta(seconds=i//2))
        assert store.save_session(record).id==record.id
        records.append(record)
    assert len(store.list_session_audit_page(realm_id='default',limit=population))==sum(r.realm_id=='default' for r in records)
    assert store.get_session(records[-1].id).model_dump(mode='json')==records[-1].model_dump(mode='json')
    return store,records


async def profile(folder,*,population,payload_bytes,iterations,progress):
    folder.mkdir();start=time.perf_counter();store,records=seed(folder,population,payload_bytes)
    settings=Settings(_env_file=None,data_dir=folder,instance_id='synthetic-local',agent_enabled=False,peers=[],subscribed_realms=['default','other'])
    assert store.db_path.resolve()==(folder/'sessions.db').resolve()
    template_root=Path(items.__file__).parents[1]/'server/templates'
    templates=Jinja2Templates(directory=str(template_root));templates.env.bytecode_cache=PackagedTemplateCache(template_root.parent/'template-bytecode.json')
    reference=Jinja2Templates(directory=str(template_root));app=SimpleNamespace(state=SimpleNamespace(ctx=SimpleNamespace(settings=settings)))
    runtime=AsyncRuntime();capture=memory.Capture(store)
    before=memory.pr.fingerprint(store);setup_ms=(time.perf_counter()-start)*1000
    cursor=expected_page(records,{},settings.subscribed_realms)['memory_next_url'].split('cursor=',1)[1]
    from urllib.parse import unquote
    cursor=unquote(cursor)
    final_default=sorted([r for r in records if r.realm_id=='default'],key=lambda r:(r.updated_at,r.id),reverse=True)[-1]
    end_cursor=base64.urlsafe_b64encode((final_default.updated_at.isoformat()+'|'+final_default.id).encode()).decode().rstrip('=')
    cases={'default':{},'cursor':{'cursor':cursor},'other_realm':{'realm':'other'},'empty':{'realm':'absent'},'past_end':{'cursor':end_cursor},'invalid_cursor':{'cursor':'invalid'}}
    samples,sizes=defaultdict(list),defaultdict(list);outcomes=Counter();hashes={};oracle_cpu=[];counts={}
    progress.update(samples=samples,outcomes=outcomes,capture=capture)
    start=time.perf_counter();await runtime.start();startup_ms=(time.perf_counter()-start)*1000
    try:
        with patch.object(items,'get_store',return_value=store),patch.object(items,'get_settings',return_value=settings),patch.object(items,'_memory_audit_context',capture.measured('audit_context',items._memory_audit_context)),patch.object(Template,'render',capture.measured('template_render',Template.render)),capture.installed(['list_session_audit_page','get_session','_row_to_session'],AgentSession):
            for _ in range(iterations):
                for name,params in cases.items():
                    progress['phase']='component:'+name
                    request=Request({'type':'http','method':'GET','scheme':'http','server':('fixture',80),'path':'/memory-audit','query_string':urlencode(params).encode(),'headers':[],'app':app})
                    async def foreground():
                        token=capture.selected.set(name);wall=time.perf_counter()
                        def read():
                            try:
                                context=items._memory_audit_context(request)
                                return context,templates.get_template('pages/memory-audit.html').render({'request':request,**context}).encode(),None
                            except HTTPException as exc:return None,None,exc.status_code
                        try:return await asyncio.to_thread(read)
                        finally:samples[name].append((time.perf_counter()-wall)*1000);capture.selected.reset(token)
                    async def background():
                        token=capture.selected.set('background');wall=time.perf_counter()
                        try:return await asyncio.to_thread(store.get_session,records[-1].id)
                        finally:samples['background'].append((time.perf_counter()-wall)*1000);capture.selected.reset(token)
                    (actual,body,error),bg=await asyncio.gather(foreground(),background())
                    cpu=time.thread_time();assert bg.model_dump(mode='json')==records[-1].model_dump(mode='json')
                    if name=='invalid_cursor':assert error==422;outcomes['expected_component_exception422']+=1
                    else:
                        assert error is None and len(body)<16*1024**2
                        expected=expected_page(records,params,settings.subscribed_realms)
                        assert public_page(actual)==public_page(expected),'complete_session_context'
                        assert body==reference.get_template('pages/memory-audit.html').render({'request':request,**expected}).encode(),'complete_template_HTML'
                        outcomes['component_success']+=1;sizes[name].append(len(body));counts[name]=[len(actual['audit_sessions']),actual['memory_has_more']]
                        digest=hashlib.sha256(body).hexdigest();assert hashes.setdefault(name,digest)==digest
                        del actual,expected,body
                    oracle_cpu.append((time.thread_time()-cpu)*1000)
            snapshot=runtime.snapshot()
        assert before==memory.pr.fingerprint(store) and not any(v for k,v in capture.counts.items() if k.endswith(':changes'))
    finally:
        start=time.perf_counter();await runtime.close();store.close_thread_connection();shutdown_ms=(time.perf_counter()-start)*1000
    return {'population':population,'payload_bytes':payload_bytes,'iterations':iterations,'setup_ms':round(setup_ms,3),'runtime_start_ms':round(startup_ms,3),'runtime_close_ms':round(shutdown_ms,3),
        'request_ms':{k:distribution(v) for k,v in samples.items()},'bytes_sum_max':{k:[sum(v),max(v)] for k,v in sizes.items()},'component_outcomes':dict(outcomes),'page_counts':counts,'response_hashes':hashes,
        'oracle_cpu_ms':distribution(oracle_cpu),'ge500ms':{k:sum(v>=500 for v in values) for k,values in samples.items()},'capture':capture.result(),'runtime':memory.pr.support.compact_runtime(snapshot),'fingerprint':before,'durable_parity':True}


def benchmark(*,population=160,payloads=(4096,16384),iterations=6,report_path=Path('docs/performance-current.json')):
    if not 64<=population<=160 or not 1<=len(payloads)<=2 or any(not 1<=p<=16384 for p in payloads) or not 1<=iterations<=6:raise ValueError('bounded private Session audit inventory')
    if 131072-report_path.stat().st_size<24576:raise RuntimeError('Reserve24KiB in the sole current report BEFORE fixtures')
    result={'contract':'pa.session-audit-component-inventory/v1','distribution_columns':memory.pr.COLUMNS,'profiles':[]};progress={}
    with tempfile.TemporaryDirectory(prefix='pa-session-audit-') as tmp:
        root=Path(tmp).resolve()
        if shutil.disk_usage(root).free<4*1024**3:raise RuntimeError('At least4GiB free required')
        try:
            for i,payload in enumerate(payloads):result['profiles'].append(asyncio.run(profile(root/str(i),population=population,payload_bytes=payload,iterations=iterations,progress=progress)))
        except Exception as exc:result.update(failed=True,**memory.pr.support.minimum_failure(exc),failed_phase=progress.get('phase'),partial_component_outcomes=dict(progress.get('outcomes',{})),partial_request_ms={k:distribution(v) for k,v in progress.get('samples',{}).items()},partial_capture=progress['capture'].result() if 'capture' in progress else {})
        result['fixture_storage']=memory.pr.support.fixture_bytes(root)
    result.update(temporary_fixture_removed=not root.exists(),script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),scope='ActualprivateCardProjection/list_session_audit_page/_memory_audit_context/JinjaPAGEtemplate/defaultRuntime directcomponents ONLY, NOTHTTP/newendpoint/newbudget/auth/fullshell/Kernel/nativeprovider/startup/lease/effect/receipt/acceptance/deployment proof. Onlynormalprivateunlaunchedidle standalone sessions/emptybinding/noactualdispatch/providerhistory. Bothglobalfactories exactprivatebindings; rootsguardedONCE, numericprivateObservedConnection/noSQL/results/global-liveinterception. Fullmodel/lookahead/realm/order/cursor/nextURL/context/HTMLoracle afterallawaitedfg/bgwork. Normalasyncio.to_thread pool/context plusrender latency; no requestmetrics manufactured; expected422 isdirectcomponentHTTPException,notobservedHTTP. Sameactualtemplate/independentselectedmodels, notindependenttemplateimplementationproof. No transcriptreads/worker/auth/control/mutationrequests. Single/independentcohortsnotbefore-afterrepair; inclusivephases/observeroverheadincluded, no nativebusywait/lockhold/SQLbytes/physicalIO/peakRAMclaim; responsesbounded16MiB/no raws retained.')
    return result


if __name__=='__main__':print(json.dumps(memory.compact(benchmark()),separators=(',',':')))

"""Controlled private authority reads; no actual mode/command/provider effects.

Both variants use identical native200ms session reads, authorization and response
bodies. Only loop progress is compared, not request speedup or historical cause.
"""
import argparse,ast,asyncio,hashlib,json,os,subprocess,tempfile,threading,time
from datetime import UTC,datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock,patch

BASELINE="40f1b539d4561e892a04d803ae8bc1ad05daa6ab"

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',type=Path,required=True);a=p.parse_args();assert not a.output.exists()
    with tempfile.TemporaryDirectory(prefix='pa-collaboration-read-benchmark-') as tmp:
        os.environ.update(PA_DATA_DIR=str(Path(tmp)/'data'),PA_WORKSPACE_ROOT=str(Path(tmp)/'workspaces'),PA_AGENT_ENABLED='false')
        from pa.core.async_runtime import AsyncRuntime
        from pa.domain.models import AgentSession
        from pa.domain.projection import CardProjection
        from pa.modules import collaboration as module
        source=subprocess.check_output(['git','show',BASELINE+':src/pa/modules/collaboration.py'],text=True)
        baseline=dict(vars(module))
        for name in ['request_collaboration_transition','execute_session_command']:
            node=next(n for n in ast.parse(source).body if isinstance(n,ast.AsyncFunctionDef) and n.name==name)
            exec(compile(ast.get_source_segment(source,node),'<exact-collaboration-read-baseline>','exec'),baseline)
        controls=dict(require_user=lambda _:SimpleNamespace(role='editor'),get_principal_id=lambda _:'user:fixture');baseline.update(controls)
        value=dict(baseline=BASELINE,source_sha256=hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest(),started_at=datetime.now(UTC).isoformat(),rows=[],private_roots_before_imports=True,deliberate_read_ms=200,scope=__doc__)
        a.output.write_text(json.dumps(value,indent=2)+'\n')
        async def run():
            runtime=AsyncRuntime(max_workers=1);store=CardProjection(Path(tmp)/'fixture.db')
            store.save_session(AgentSession(id='session',agent_name='fixture',principal_id='user:fixture'))
            reply=SimpleNamespace(model_dump=lambda **_:{'fixture':True})
            service=SimpleNamespace(request_transition=AsyncMock(return_value=reply),execute_command=AsyncMock(return_value=reply))
            manager=SimpleNamespace(get=lambda _:None,store=store)
            services={'async_runtime':runtime,'instance_agent':manager,'collaboration':service}
            ctx=SimpleNamespace(services=services,require_service=services.__getitem__,settings=SimpleNamespace(auth_required=True))
            request=SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(ctx=ctx)),state=SimpleNamespace())
            expected=None;read=store.get_session
            try:
                with patch.multiple(module,**controls):
                    for pair in range(3):
                        for variant in (['baseline','candidate'] if pair%2==0 else ['candidate','baseline']):
                            for family in ['transition','command']:
                                threads,ticks=[],[];owner=threading.get_ident()
                                def delayed(sid):
                                    threads.append(threading.get_ident());time.sleep(.2);return read(sid)
                                done=False
                                async def heartbeat():
                                    before=time.perf_counter()
                                    while not done:
                                        await asyncio.sleep(.005);now=time.perf_counter();ticks.append((now-before)*1000);before=now
                                beat=asyncio.create_task(heartbeat());await asyncio.sleep(0);start=time.perf_counter()
                                try:
                                    with patch.object(store,'get_session',side_effect=delayed):
                                        name='request_collaboration_transition' if family=='transition' else 'execute_session_command'
                                        body=module.ModeTransitionRequest(requested_mode='default',purpose='Private fixture',intended_next_action='Private fixture',session_id='session',idempotency_key='fixture') if family=='transition' else module.CommandExecutionBody(name='pa:status',idempotency_key='fixture')
                                        response=await (baseline[name] if variant=='baseline' else getattr(module,name))(request,'session',body)
                                    elapsed=(time.perf_counter()-start)*1000
                                finally:
                                    done=True;await beat
                                encoded=json.dumps(response,sort_keys=True).encode();expected=expected or encoded
                                assert encoded==expected and len(threads)==1 and (threads[0]==owner)==(variant=='baseline')
                                value['rows'].append(dict(pair=pair,variant=variant,family=family,request_ms=elapsed,max_heartbeat_ms=max(ticks),response_bytes=len(encoded),response_sha256=hashlib.sha256(encoded).hexdigest()))
                                a.output.write_text(json.dumps(value,indent=2)+'\n')
                value['runtime']=runtime.snapshot()
            finally:
                await runtime.close()
        asyncio.run(run());value.update(finished_at=datetime.now(UTC).isoformat(),complete_response_bytes_equal=True)
        a.output.write_text(json.dumps(value,indent=2)+'\n');print(json.dumps(dict(rows=len(value['rows']),complete_response_bytes_equal=True,max_heartbeat_ms={v:max(r['max_heartbeat_ms'] for r in value['rows'] if r['variant']==v) for v in ['baseline','candidate']})))

if __name__=='__main__':main()

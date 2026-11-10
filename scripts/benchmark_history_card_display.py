"""Bounded private large history-page display hydration comparison; aggregate stdout."""
import asyncio
from collections import Counter, defaultdict
from contextvars import ContextVar
from datetime import UTC, datetime
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import tempfile
import time
from unittest.mock import patch

import httpx

spec=importlib.util.spec_from_file_location("history_display_support",Path(__file__).with_name("benchmark_file_routes.py"))
shared=importlib.util.module_from_spec(spec);spec.loader.exec_module(shared)
from pa.domain.models import AgentSession, CardCreate
from pa.domain.projection import CardProjection
from pa.modules import agent_chat
import pa.execution.session_presentation as presentation


async def profile(root, *, name, sessions, cards, body_bytes, iterations, progress):
    root.mkdir()
    settings=shared.Settings(data_dir=root/"data",workspace_root=root/"workspaces",agent_enabled=False,
        telemetry_enabled=False,auth_required=True)
    user=shared.UserDirectory(settings.data_dir).create_user("fixture","private-fixture-password")
    original_workspace=shared.WorkspaceManager.__init__
    def guarded(manager,configured,store):
        assert Path(configured.data_dir).resolve().is_relative_to(root)
        assert Path(configured.workspace_root).expanduser().resolve().is_relative_to(root)
        return original_workspace(manager,configured,store)
    original_presentation=presentation.build_session_presentation
    def fixed_presentation(*args,**kwargs):
        return original_presentation(*args,**(kwargs|{"now":datetime(2026,11,10,tzinfo=UTC)}))
    with (patch.object(shared.WorkspaceManager,"__init__",guarded),
          patch.object(shared.agent_module,"_instance_agent",None),
          patch.object(shared.GitHubCredentials,"load",return_value=shared.GitHubCredentials()),
          patch.object(presentation,"build_session_presentation",fixed_presentation),
          shared.support.fixture_logging() as (warnings,console)):
        start=time.perf_counter();app=shared.Kernel.boot(settings=settings).build_app()
        async with app.router.lifespan_context(app):
            startup=(time.perf_counter()-start)*1000;ctx=app.state.ctx;store=ctx.store
            def seed():
                values=[store.create_card(CardCreate(title=f"Card{i}",body="x"*body_bytes)) for i in range(cards)]
                for index in range(sessions):
                    store.save_session(AgentSession(id=f"s-{index:04d}",agent_name="fixture",status="closed",
                        principal_id=f"user:{user.id}",title=f"Session{index}",card_id=values[index%cards].id,
                        metrics_json={"retained":"x"*128}))
            await asyncio.to_thread(seed)
            mode=ContextVar("history_display_mode",default=None)
            counts=Counter();samples=defaultdict(list);statuses=Counter()
            progress.update(profile=name,samples=samples,statuses=statuses)
            original_card=CardProjection._row_to_card;original_session=CardProjection._row_to_session
            def card(row):
                selected=mode.get()
                if selected:
                    counts[selected+":card_models"]+=1
                    counts[selected+":body_characters_materialized"]+=len(row["body"])
                return original_card(row)
            def session(row):
                selected=mode.get()
                if selected:counts[selected+":session_models"]+=1
                return original_session(row)
            runtime=ctx.require_service("async_runtime")
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url="http://fixture",
                    headers={"Authorization":"Bearer "+user.cli_token}) as client:
                params={"limit":sessions}
                warm=await client.get("/api/agent/history",params=params)
                assert warm.status_code==200 and len(warm.json())==sessions
                oracle=warm.content
                before=runtime.snapshot();implementation=agent_chat._history_card_summaries
                with (patch.object(CardProjection,"_row_to_card",staticmethod(card)),
                      patch.object(CardProjection,"_row_to_session",staticmethod(session))):
                    for pair in range(iterations):
                        for selected in (("before","after") if pair%2==0 else ("after","before")):
                            token=mode.set(selected)
                            try:
                                with patch.object(agent_chat,"_history_card_summaries",
                                        (lambda *args:None) if selected=="before" else implementation):
                                    start=time.perf_counter();response=await client.get("/api/agent/history",params=params)
                                    samples[selected].append((time.perf_counter()-start)*1000)
                                    statuses[selected+":"+str(response.status_code)]+=1
                                    assert response.status_code==200 and response.content==oracle
                                    assert len(response.content)<=4*1024*1024
                            finally:mode.reset(token)
                after=runtime.snapshot()
            deltas={op:{key:value.get(key,0)-before.get("operations",{}).get(op,{}).get(key,0)
                for key in ("submitted","completed","failed","timed_out","cancelled","rejected","slow_calls")}
                for op,value in after.get("operations",{}).items()}
            deltas={op:values for op,values in deltas.items() if any(values.values())}
            compact=shared.support.compact_runtime(after)
            compact["operations"]={op:value for op,value in compact["operations"].items() if op in deltas}
            result={"profile":name,"sessions":sessions,"cards":cards,"body_bytes_per_card":body_bytes,
                "alternating_pairs":iterations,"startup_ms":round(startup,3),
                "request_ms":{key:shared.support.distribution(v) for key,v in samples.items()},
                "response_bytes":len(oracle),"response_sha256":hashlib.sha256(oracle).hexdigest(),
                "status_counts":dict(statuses),"request_decoder_counts":dict(counts),
                "explicit_request_ge500ms":{key:sum(t>=500 for t in values) for key,values in samples.items()},
                "operation_deltas":deltas,"runtime":compact}
        result.update(warning_counts=dict(warnings.counts),console_characters=console.characters)
    return result


def benchmark(*,sessions=500,iterations=6):
    if not 3<=sessions<=500 or not 1<=iterations<=6:raise ValueError("sessions3..500 iterations1..6")
    progress={};result={"contract":"pa.history-card-display-inventory/v1","profiles":[]}
    with tempfile.TemporaryDirectory(prefix="pa-history-display-http-") as temporary:
        root=Path(temporary).resolve()
        if shutil.disk_usage(root).free<4*1024**3:raise RuntimeError("At least4GiB free required")
        try:
            for name,cards,body in (("shared_large",2,512*1024),("shared_small",2,1024),("unique_small",sessions,1024)):
                result["profiles"].append(asyncio.run(profile(root/name,name=name,sessions=sessions,cards=cards,
                    body_bytes=body,iterations=iterations,progress=progress)))
        except Exception as exc:
            result.update(failed=True,**shared.support.minimum_failure(exc),failed_profile=progress.get("profile"),
                partial_status_counts=dict(progress.get("statuses",{})),
                partial_request_ms={k:shared.support.distribution(v) for k,v in progress.get("samples",{}).items()})
        result["fixture_storage"]=shared.support.fixture_bytes(root)
    result.update(temporary_fixture_removed=not root.exists(),script_sha256=hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        scope="Three separate private full Kernels/authenticated actual history pages; original per-row payload vs invocation-local summaries with full distinct Card decoding. Complete bytes equal with fixture-only presentation clock fixed. ContextVar counters include request work only; ASCII body character counts are not physical IO/totalSQL bytes. Runtime snapshots include warmup/boot; deltas and request timers distinct. Credential wrapper stub; no provider/peer/native/deployed/whole-PA acceptance. Postworkload fixture bytes are not peak or exclusive APFS allocation.")
    assert len(json.dumps(result).encode())<=128*1024
    return result


if __name__=="__main__":print(json.dumps(benchmark(),separators=(",",":")))

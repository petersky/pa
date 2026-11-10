"""Private authenticated history/card-query and remaining file branch inventory.

Aggregate stdout only; all new fixture state is removed on success or failure.
"""
import asyncio
from collections import Counter, defaultdict
from contextlib import contextmanager
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

spec = importlib.util.spec_from_file_location("file_route_support", Path(__file__).with_name("benchmark_file_routes.py"))
shared = importlib.util.module_from_spec(spec)
spec.loader.exec_module(shared)
from pa.domain.models import AgentSession, CardCreate
from pa.domain.projection import CardProjection
from pa.modules.files import MAX_RAW_BYTES
import pa.execution.session_presentation as presentation


def changed_paths(left, right, prefix=""):
    if type(left) is not type(right):
        return [prefix]
    if isinstance(left, dict):
        return [path for key in left.keys() | right.keys()
                for path in ([prefix+"/"+key] if key not in left or key not in right else
                             changed_paths(left[key],right[key],prefix+"/"+key))][:40]
    if isinstance(left, list):
        return ([prefix+"/length"] if len(left)!=len(right) else
                [path for i,(a,b) in enumerate(zip(left,right)) for path in changed_paths(a,b,prefix+"/"+str(i))][:40])
    return [prefix] if left!=right else []


class ParityFailure(AssertionError):
    def __init__(self, paths):
        self.paths=paths


async def run(root, *, sessions, iterations, entries, progress):
    settings = shared.Settings(data_dir=root/"data", workspace_root=root/"workspaces",
        agent_enabled=False, telemetry_enabled=False, auth_required=True)
    user = shared.UserDirectory(settings.data_dir).create_user("fixture", "private-fixture-password")
    folder = root/"files"; folder.mkdir()
    for index in range(entries):
        (folder/f"entry-{index:06d}").touch()
    (folder/"sample.txt").write_bytes(b"private fixture\n")
    large = folder/"large.bin"
    with large.open("wb") as handle:
        handle.truncate(MAX_RAW_BYTES+1)
    original_workspace = shared.WorkspaceManager.__init__
    def guarded(manager, configured, store):
        assert Path(configured.data_dir).resolve().is_relative_to(root)
        assert Path(configured.workspace_root).expanduser().resolve().is_relative_to(root)
        return original_workspace(manager, configured, store)
    original_presentation = presentation.build_session_presentation
    fixed_time = datetime(2026,11,10,tzinfo=UTC)
    def fixed_presentation(*args,**kwargs):
        return original_presentation(*args,**(kwargs | {"now":fixed_time}))
    with (patch.object(shared.WorkspaceManager,"__init__",guarded),
          patch.object(shared.agent_module,"_instance_agent",None),
          patch.object(shared.GitHubCredentials,"load",return_value=shared.GitHubCredentials()),
          patch.object(presentation,"build_session_presentation",fixed_presentation),
          shared.support.fixture_logging() as (warnings, console)):
        start = time.perf_counter()
        app = shared.Kernel.boot(settings=settings).build_app()
        async with app.router.lifespan_context(app):
            startup = (time.perf_counter()-start)*1000
            ctx = app.state.ctx; store = ctx.store
            def seed():
                target = store.create_card(CardCreate(title="Target"))
                other = store.create_card(CardCreate(title="Other"))
                for index in range(sessions):
                    store.save_session(AgentSession(id=f"s-{index:04d}", agent_name="fixture",status="closed",
                        purpose="chat",principal_id=f"user:{user.id}",cwd=str(folder),title=f"Session{index}",
                        card_id=target.id if index%20==0 else other.id,
                        archived_at=datetime.now(UTC) if index%7==0 else None,
                        metrics_json={"retained":"x"*8192}))
                store.link_session_card("s-0001",target.id,make_primary=False)
                return target.id
            target = await asyncio.to_thread(seed)
            timings = defaultdict(list); sizes = {}; statuses = Counter(); queries = Counter(); models = Counter()
            progress.update(timings=timings,statuses=statuses)
            mode = [""]
            original_conn = CardProjection._conn
            original_decode = CardProjection._row_to_session
            @contextmanager
            def observed_conn(projection,*args,**kwargs):
                with original_conn(projection,*args,**kwargs) as conn:
                    def observed(statement):
                        sql = " ".join(statement.split())
                        if sql.startswith("SELECT card_id FROM agent_session_cards"):
                            queries[mode[0]+":per_session"] += 1
                        elif sql.startswith("SELECT session_id FROM agent_session_cards WHERE card_id="):
                            queries[mode[0]+":batch"] += 1
                    conn.set_trace_callback(observed)
                    try:
                        yield conn
                    finally:
                        conn.set_trace_callback(None)
            def decode(row):
                models[mode[0]] += 1
                return original_decode(row)
            runtime = ctx.require_service("async_runtime")
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url="http://fixture",
                    headers={"Authorization":"Bearer "+user.cli_token}) as client:
                params = {"card_id":target,"limit":3,"selected_session_id":"s-0001"}
                warm = await client.get("/api/agent/history",params=params)
                assert warm.status_code==200
                oracle = warm.json()
                before = runtime.snapshot()
                implementation = CardProjection.session_ids_linked_to_card
                with (patch.object(CardProjection,"_conn",observed_conn),
                      patch.object(CardProjection,"_row_to_session",staticmethod(decode))):
                    for pair in range(iterations):
                        for name in (("before","after") if pair%2==0 else ("after","before")):
                            mode[0] = name
                            with patch.object(CardProjection,"session_ids_linked_to_card",None if name=="before" else implementation):
                                start = time.perf_counter()
                                response = await client.get("/api/agent/history",params=params)
                                timings[name].append((time.perf_counter()-start)*1000)
                                assert response.status_code==200
                                paths=changed_paths(oracle,response.json())
                                if paths:
                                    raise ParityFailure(paths)
                                sizes[name] = len(response.content)
                                statuses[name+":200"] += 1
                mode[0] = "file_branches"
                variants = {
                    "directory":("/browse",{"path":str(folder)},200),
                    "raw_download":("/api/files/raw",{"path":str(folder/"sample.txt"),"download":"true"},200),
                    "raw_range":("/api/files/raw",{"path":str(folder/"sample.txt")},206),
                    "raw_directory":("/api/files/raw",{"path":str(folder)},400),
                    "raw_missing":("/api/files/raw",{"path":str(folder/"missing")},404),
                    "raw_too_large":("/api/files/raw",{"path":str(large)},413),
                    "relative":("/api/files/raw",{"path":"relative.txt"},400),
                }
                for _ in range(3):
                    for name,(path,query,expected) in variants.items():
                        start = time.perf_counter()
                        response = await client.get(path,params=query,headers={"Range":"bytes=0-6"} if name=="raw_range" else {})
                        timings[name].append((time.perf_counter()-start)*1000)
                        statuses[name+":"+str(response.status_code)] += 1
                        assert response.status_code==expected and len(response.content)<1024*1024
                        sizes[name] = len(response.content)
                        if name=="directory":
                            # All files: exact first1000 of the full ordering oracle.
                            names = sorted(p.name for p in folder.iterdir())[:1000]
                            import re
                            listing=response.text.split('class="file-directory-list">',1)[1].split("</div>",1)[0]
                            assert re.findall(r"<strong>(.*?)</strong>",listing)==names
                        elif name=="raw_download":
                            assert response.content==b"private fixture\n" and "attachment" in response.headers["content-disposition"]
                        elif name=="raw_range":
                            assert response.content==b"private" and response.headers["content-range"]=="bytes 0-6/16"
                        if name.startswith("raw_") and expected<300:
                            assert "sandbox" in response.headers["content-security-policy"]
                            assert response.headers["x-content-type-options"]=="nosniff"
                after = runtime.snapshot()
            deltas = {name:{key:value.get(key,0)-before.get("operations",{}).get(name,{}).get(key,0)
                for key in ("submitted","completed","failed","timed_out","cancelled","rejected","slow_calls")}
                for name,value in after.get("operations",{}).items()}
            deltas = {name:values for name,values in deltas.items() if any(values.values())}
            compact = shared.support.compact_runtime(after)
            compact["operations"] = {name:value for name,value in compact["operations"].items() if name in deltas}
            result = {"contract":"pa.history-card-filter-inventory/v1","sessions":sessions,"entries":entries,
                "alternating_pairs":iterations,"startup_ms":round(startup,3),"request_ms":{k:shared.support.distribution(v) for k,v in timings.items()},
                "response_bytes":sizes,"status_counts":dict(statuses),"query_counts":dict(queries),"decoded_sessions":dict(models),
                "selected_ids":[row["id"] for row in oracle],"operation_deltas":deltas,"runtime":compact,
                "scope":"Actual authenticated full-Kernel ASGI requests, full session decode retained. Original no-batch branch vs native batch. Three returned sessions plus selected secondary association; unchanged complete JSON parity with fixture-only presentation now fixed for observed_at. SQL trace callback/counting adds cost and retains no query text. Runtime window includes guards/boot, explicit request timers separate. Credentials wrapper stub. No provider/peer/native/deployed/physicalIO or whole-PA acceptance."}
        result.update(warning_counts=dict(warnings.counts),console_characters=console.characters,fixture_storage=shared.support.fixture_bytes(root))
    return result


def benchmark(*,sessions=1000,iterations=12,entries=20000):
    if not 3<=sessions<=1000 or not 1<=iterations<=12 or not 1000<=entries<=20000:
        raise ValueError("sessions3..1000 iterations1..12 entries1000..20000")
    with tempfile.TemporaryDirectory(prefix="pa-history-card-http-") as temporary:
        root=Path(temporary).resolve()
        if shutil.disk_usage(root).free<4*1024**3:
            raise RuntimeError("At least4GiB free required")
        progress={}
        try:
            result=asyncio.run(run(root,sessions=sessions,iterations=iterations,entries=entries,progress=progress))
        except Exception as exc:
            result={"failed":True,**shared.support.minimum_failure(exc),"scope":"Incomplete private fixture, not acceptance"}
            result["completed_status_counts"]=dict(progress.get("statuses",{}))
            result["partial_request_ms"]={key:shared.support.distribution(v) for key,v in progress.get("timings",{}).items()}
            if isinstance(exc,ParityFailure):
                result["changed_field_paths"]=exc.paths
    result["temporary_fixture_removed"]=not root.exists()
    result["script_sha256"]=hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    assert len(json.dumps(result).encode())<=128*1024
    return result


if __name__=="__main__":
    print(json.dumps(benchmark(),separators=(",",":")))

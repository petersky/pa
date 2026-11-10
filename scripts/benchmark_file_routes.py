"""Bounded authenticated file HTTP inventory in a disposable full Kernel.

Aggregate stdout only. No provider, peer, credential lookup or live PA state.
"""
import asyncio
from collections import Counter, defaultdict
from datetime import UTC, datetime
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
from unittest.mock import patch

import httpx

from pa.auth.users import UserDirectory
from pa.config import Settings
from pa.core.kernel import Kernel
from pa.domain.models import AgentSession
from pa.modules import files as files_module
from pa.pr_supervisor.github import GitHubCredentials
from pa.repository.workspace import WorkspaceManager
import pa.instance.agent_session as agent_module

spec = importlib.util.spec_from_file_location("file_route_benchmark_support",
    Path(__file__).with_name("benchmark_systemic_latency.py"))
support = importlib.util.module_from_spec(spec)
spec.loader.exec_module(support)


def original_root_and_default_base(path):
    repo = files_module._git_root(path)
    if not repo or not path.is_relative_to(repo):
        return None, None
    return repo, files_module._default_diff_base(repo)


async def run(root, *, sessions, iterations, progress):
    settings = Settings(data_dir=root/"data",workspace_root=root/"workspaces",
        agent_enabled=False,telemetry_enabled=False,auth_required=True)
    users = UserDirectory(settings.data_dir)
    alice = users.create_user("alice","private-fixture-password")
    bob = users.create_user("bob","private-fixture-password")
    owner, other, legacy = (root/name for name in ("owner","other","legacy"))
    for folder in (owner,other,legacy):
        folder.mkdir()
        (folder/"sample.py").write_text("print('first')\n")
    (owner/"README.md").write_text("# File fixture\n")
    (owner/"escape").symlink_to(other/"sample.py")
    for args in (("init","-q"),("add","sample.py","README.md"),
                 ("-c","user.name=Fixture","-c","user.email=fixture@example.invalid","commit","-qm","fixture")):
        subprocess.run(["git","-C",str(owner),*args],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    (owner/"sample.py").write_text("print('second')\n")
    original = WorkspaceManager.__init__
    def guarded(manager, configured, store):
        assert Path(configured.data_dir).resolve().is_relative_to(root)
        assert Path(configured.workspace_root).expanduser().resolve().is_relative_to(root)
        return original(manager,configured,store)
    with (patch.object(WorkspaceManager,"__init__",guarded),
          patch.object(agent_module,"_instance_agent",None),
          patch.object(GitHubCredentials,"load",return_value=GitHubCredentials()),
          support.fixture_logging() as (warnings,console)):
        started = time.perf_counter()
        app = Kernel.boot(settings=settings).build_app()
        async with app.router.lifespan_context(app):
            startup_ms = (time.perf_counter()-started)*1000
            ctx = app.state.ctx
            store = ctx.store
            runtime = ctx.require_service("async_runtime")
            assert ctx.require_service("instance_agent").store is store
            def seed():
                fixed = [("owner",owner,f"user:{alice.id}"),("other",other,f"user:{bob.id}"),("legacy",legacy,None)]
                for index in range(sessions):
                    key,folder,principal = fixed[index] if index < 3 else (
                        f"s-{index}",root/"roots"/str(index),f"user:{alice.id}" if index%2 else f"user:{bob.id}")
                    store.save_session(AgentSession(id=key,agent_name="fixture",status="closed",purpose="chat",
                        cwd=str(folder),principal_id=principal,archived_at=datetime.now(UTC) if index%7==0 else None,
                        metrics_json={"retained":"x"*8192}))
            await asyncio.to_thread(seed)
            variants = {
                "directory":("/browse",{"path":str(owner)},200),
                "source":("/browse",{"path":str(owner/"sample.py")},200),
                "diff":("/browse",{"path":str(owner/"sample.py"),"base":"HEAD"},200),
                "markdown":("/browse",{"path":str(owner/"README.md")},200),
                "raw":("/api/files/raw",{"path":str(owner/"sample.py")},200),
                "other_principal":("/api/files/raw",{"path":str(other/"sample.py")},403),
                "legacy_principal":("/api/files/raw",{"path":str(legacy/"sample.py")},403),
                "symlink_escape":("/browse",{"path":str(owner/"escape")},403),
            }
            samples,sizes = defaultdict(list),defaultdict(list)
            statuses = Counter()
            progress.update(samples=samples,statuses=statuses)
            background_ms = []
            git_calls = Counter()
            selected_mode = [""]
            original_git = subprocess.run
            def counted_git(args, *a, **kw):
                if args and args[0] == "git":
                    kind = args[3] if len(args)>3 and args[1] == "-C" else "other"
                    git_calls[selected_mode[0]+":"+(kind if kind in {"rev-parse","diff"} else "other")] += 1
                return original_git(args,*a,**kw)
            optimized = files_module._git_root_and_default_base
            modes = {"before":original_root_and_default_base,"after":optimized}
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app),base_url="http://fixture",
                    headers={"Authorization":"Bearer "+alice.cli_token}) as client:
                # Separate cold route/template compilation from the declared window.
                for path,params,expected in variants.values():
                    response = await client.get(path,params=params)
                    assert response.status_code == expected
                before = runtime.snapshot()
                async def background():
                    start = time.perf_counter()
                    result = await runtime.run_blocking("sqlite.agent_session_read",store.get_session,"owner")
                    assert result.id == "owner"
                    background_ms.append((time.perf_counter()-start)*1000)
                async def request_one(key,path,params):
                    start = time.perf_counter()
                    response = await client.get(path,params=params)
                    samples[key].append((time.perf_counter()-start)*1000)
                    return response
                with patch.object(files_module.subprocess,"run",counted_git):
                    for pair in range(iterations):
                        for mode in (("before","after") if pair%2==0 else ("after","before")):
                            selected_mode[0] = mode
                            with patch.object(files_module,"_git_root_and_default_base",modes[mode]):
                                for name,(path,params,expected) in variants.items():
                                    key=mode+":"+name
                                    response,_ = await asyncio.gather(request_one(key,path,params),background())
                                    sizes[key].append(len(response.content))
                                    statuses[f"{key}:{response.status_code}"] += 1
                                    assert response.status_code == expected
                                    assert len(response.content) <= 512*1024
                                    if name == "raw":
                                        assert response.content == b"print('second')\n"
                                        assert "sandbox" in response.headers["content-security-policy"]
                                    elif name in {"source","diff"}:
                                        assert "print" in response.text and "second" in response.text
                after = runtime.snapshot()
            deltas = {name:{field:values.get(field,0)-before.get("operations",{}).get(name,{}).get(field,0)
                           for field in ("submitted","completed","failed","timed_out","cancelled","rejected","slow_calls")}
                      for name,values in after.get("operations",{}).items()}
            deltas = {name:values for name,values in deltas.items() if any(values.values())}
            compact = support.compact_runtime(after)
            compact["operations"] = {name:values for name,values in compact["operations"].items() if name in deltas}
            expected_failed = {"files.browse_render":2*iterations,"files.raw_resolve":4*iterations}
            unexpected = {name:values["failed"]-expected_failed.get(name,0) for name,values in deltas.items()
                          if values["failed"] != expected_failed.get(name,0)}
            result = {"contract":"pa.file-http-inventory/v1","sessions":sessions,"iterations":iterations,
                "startup_to_lifespan_ready_ms":round(startup_ms,3),
                "explicit_request_ms":{name:support.distribution(values) for name,values in samples.items()},
                "response_bytes":{name:{"count":len(v),"total":sum(v),"min":min(v),"max":max(v)} for name,v in sizes.items()},
                "status_counts":dict(statuses),"background_read_ms":support.distribution(background_ms),
                "operation_counter_deltas":deltas,"runtime":compact,"unexpected_failed_operation_deltas":unexpected,
                "measured_git_process_calls":dict(git_calls),
                "threshold_violations_500ms":{name:sum(v>=500 for v in values) for name,values in samples.items() if any(v>=500 for v in values)},
                "source_sha256":hashlib.sha256(Path(__file__).parents[1].joinpath("src/pa/modules/files.py").read_bytes()).hexdigest(),
                "scope":"Authenticated registered ASGI routes/full Kernel, synthetic closed sessions/local Git, alternating baseline/combined-root helper and paired background reads. Three expected403 variants separate from unexpected failures. Explicit request timers end at HTTP completion; background timers separate. Minimal fixed Git command counters add instrumentation cost. Runtime windows include warmup/boot/seed/guards; deltas cover declared window. Startup excludes imports/user+file setup. No native provider/peer/deployed/physicalIO or whole-PA acceptance."}
        result["warning_counts"] = dict(warnings.counts)
        result["console_characters_without_capture"] = console.characters
        result["fixture_storage"] = support.fixture_bytes(root)
    return result


def benchmark(*,sessions=1000,iterations=20):
    if not 3 <= sessions <= 1000 or not 1 <= iterations <= 20:
        raise ValueError("sessions3..1000; iterations1..20")
    with tempfile.TemporaryDirectory(prefix="pa-file-http-") as temporary:
        root = Path(temporary).resolve()
        if shutil.disk_usage(root).free < 4*1024**3:
            raise RuntimeError("At least4GiB free required")
        progress = {}
        try:
            result = asyncio.run(run(root,sessions=sessions,iterations=iterations,progress=progress))
        except Exception as exc:
            result = {"failed":True,**support.minimum_failure(exc),
                "completed_status_counts":dict(progress.get("statuses",{})),
                "partial_request_ms":{name:support.distribution(v) for name,v in progress.get("samples",{}).items()},
                "scope":"Incomplete private scenario; not acceptance"}
    result["temporary_fixture_removed"] = not root.exists()
    assert len(json.dumps(result).encode()) <= 128*1024
    return result


if __name__ == "__main__":
    print(json.dumps(benchmark(),separators=(",",":")))

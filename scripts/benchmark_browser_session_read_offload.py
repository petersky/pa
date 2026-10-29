"""Private browser authority-read loop comparison; no browser effects.

Use the same deliberate 200ms native session read in each variant. Requests
retain that delay; only event-loop progress is compared. Normal GC, authorization
and response bodies remain unchanged. Not real browser/network or PA acceptance.
"""
import argparse
import ast
import asyncio
import hashlib
import json
import os
import subprocess
import tempfile
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

BASELINE = "76ce544ebe917cd74853142b917ff2245552562c"
OWNER = "12345678-1234-4234-8234-123456789abc"
TARGET = "12345678-1234-4234-8234-123456789abd"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    assert not args.output.exists()
    with tempfile.TemporaryDirectory(prefix="pa-browser-read-benchmark-") as tmp:
        os.environ.update(PA_DATA_DIR=str(Path(tmp)/"data"),
                          PA_WORKSPACE_ROOT=str(Path(tmp)/"workspaces"), PA_AGENT_ENABLED="false")
        from pa.core.async_runtime import AsyncRuntime
        from pa.domain.models import AgentSession
        from pa.domain.projection import CardProjection
        from pa.modules import browser

        source = subprocess.check_output(["git", "show", BASELINE+":src/pa/modules/browser.py"], text=True)
        baseline = dict(vars(browser))
        for name in ["_scope", "browser_automation"]:
            node = next(n for n in ast.parse(source).body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == name)
            exec(compile(ast.get_source_segment(source, node), "<exact-browser-read-baseline>", "exec"), baseline)
        baseline["get_principal_id"] = lambda _: "user:fixture"
        value = dict(baseline=BASELINE, source_sha256=hashlib.sha256(Path(browser.__file__).read_bytes()).hexdigest(),
                     started_at=datetime.now(UTC).isoformat(), deliberate_read_ms=200, rows=[],
                     private_roots_before_imports=True, scope=__doc__)
        args.output.write_text(json.dumps(value, indent=2)+"\n")

        async def run():
            executor = AsyncRuntime(max_workers=1)
            store = CardProjection(Path(tmp)/"fixture.db")
            for sid in [OWNER, TARGET]:
                store.save_session(AgentSession(id=sid, agent_name="fixture", principal_id="user:fixture"))
            manager = SimpleNamespace(state=AsyncMock(return_value={"state": "fixture"}),
                                      share=AsyncMock(return_value={"share": "fixture"}))
            services = {"async_runtime": executor, "browser_sessions": manager}
            ctx = SimpleNamespace(store=store, settings=SimpleNamespace(instance_id="local"),
                                  services=services, require_service=services.__getitem__)
            request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(ctx=ctx)))
            expected = {}
            try:
                with patch.object(browser, "get_principal_id", return_value="user:fixture"):
                    for pair in range(3):
                        for variant in (["baseline", "candidate"] if pair % 2 == 0 else ["candidate", "baseline"]):
                            for family, delayed_id in [("caller_scope", OWNER), ("share_target", TARGET)]:
                                read = store.get_session
                                threads, ticks = [], []
                                owner = threading.get_ident()
                                def delayed(sid):
                                    if sid == delayed_id:
                                        threads.append(threading.get_ident())
                                        time.sleep(.2)
                                    return read(sid)
                                done = False
                                async def heartbeat():
                                    previous = time.perf_counter()
                                    while not done:
                                        await asyncio.sleep(.005)
                                        now = time.perf_counter()
                                        ticks.append((now-previous)*1000)
                                        previous = now
                                pulse = asyncio.create_task(heartbeat())
                                await asyncio.sleep(.01)
                                started = time.perf_counter()
                                try:
                                    with patch.object(store, "get_session", side_effect=delayed):
                                        operation = "state" if family == "caller_scope" else "share"
                                        body = browser.AutomationBody(agent_session_id=OWNER,
                                            authorized_session_id=TARGET if operation == "share" else None)
                                        call = baseline["browser_automation"] if variant == "baseline" else browser.browser_automation
                                        result = await call(request, operation, body)
                                    elapsed = (time.perf_counter()-started)*1000
                                finally:
                                    done = True
                                    await pulse
                                encoded = json.dumps(result, sort_keys=True, separators=(",", ":")).encode()
                                expected.setdefault(family, encoded)
                                assert encoded == expected[family] and len(threads) == 1
                                assert (threads[0] == owner) == (variant == "baseline")
                                value["rows"].append(dict(pair=pair, variant=variant, family=family,
                                    request_ms=elapsed, max_heartbeat_ms=max(ticks), delayed_read_calls=len(threads),
                                    response_bytes=len(encoded), response_sha256=hashlib.sha256(encoded).hexdigest()))
                                args.output.write_text(json.dumps(value, indent=2)+"\n")
                value["runtime"] = executor.snapshot()
            finally:
                await executor.close()
        asyncio.run(run())
        value.update(finished_at=datetime.now(UTC).isoformat(), complete_response_bytes_equal=True)
        args.output.write_text(json.dumps(value, indent=2)+"\n")
        print(json.dumps(dict(rows=len(value["rows"]), complete_response_bytes_equal=True,
            max_heartbeat_ms={v:max(r["max_heartbeat_ms"] for r in value["rows"] if r["variant"]==v) for v in ["baseline", "candidate"]})))


if __name__ == "__main__":
    main()

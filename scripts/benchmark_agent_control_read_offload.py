"""Compare loop progress during a deliberate private 200 ms storage read.

Only exact archived endpoint functions differ. No provider, restart, recovery
effect, production data, fault injection or deadline/GC policy change occurs.
"""
import argparse
import ast
import asyncio
import hashlib
import json
import math
import os
import subprocess
import tempfile
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

BASELINE = "00d2bd8419d558934519a86bde3767e9c177b04d"


def distribution(values):
    values = sorted(values)
    return {"samples": len(values), **{name: values[math.ceil(p * len(values)) - 1]
            for name, p in [("p50", .5), ("p95", .95), ("p99", .99), ("max", 1)]}}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    assert not args.output.exists()
    with tempfile.TemporaryDirectory(prefix="pa-control-read-benchmark-") as tmp:
        os.environ.update(PA_DATA_DIR=str(Path(tmp) / "data"), PA_WORKSPACE_ROOT=str(Path(tmp) / "workspaces"), PA_AGENT_ENABLED="false")
        from fastapi import HTTPException
        from pa.core.async_runtime import AsyncRuntime
        from pa.instance.agent_session import AgentSessionManager
        from pa.modules import agent_chat as module

        source = subprocess.check_output(["git", "show", BASELINE + ":src/pa/modules/agent_chat.py"], text=True)
        namespace = dict(vars(module))
        functions = ["_runtime_or_404", "request_restart_handoff", "edit_restart_handoff", "retry_restart_handoff", "recover_session", "get_session_observability", "queue_pause"]
        tree = ast.parse(source)
        for name in functions:
            node = next(n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == name)
            exec(compile(ast.get_source_segment(source, node), "<exact-control-read-baseline>", "exec"), namespace)
        rows = []
        partial = dict(baseline=BASELINE, started_at=datetime.now(UTC).isoformat(), rows=rows, scope="Incremental private benchmark outcomes; request data omitted.")
        args.output.write_text(json.dumps(partial, indent=2) + "\n")

        async def run():
            executor = AsyncRuntime(max_workers=1)
            # The native manager offload method and runtime are used, but no
            # manager startup/provider construction or durable effects occur.
            manager = object.__new__(AgentSessionManager)
            manager.async_runtime, manager._runtimes = executor, {}
            manager._runtime_lifecycle_lock = threading.RLock()
            manager.store = SimpleNamespace()
            ctx = SimpleNamespace(settings=SimpleNamespace(auth_required=False, instance_id="local"), services={}, require_service=lambda _: manager)
            request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(ctx=ctx)), state=SimpleNamespace())
            namespace["_require_session_traffic_ready"] = lambda _: manager
            expected = {}
            try:
                with patch.object(module, "_require_session_traffic_ready", return_value=manager):
                    for pair in range(3):
                        for variant in (["baseline", "candidate"] if pair % 2 == 0 else ["candidate", "baseline"]):
                            for name in functions[1:]:
                                readers, ticks = [], []
                                loop_thread = threading.get_ident()

                                def read(_):
                                    readers.append(threading.get_ident())
                                    time.sleep(.2)
                                    return None

                                manager.store.get_session = read
                                finished = False

                                async def heartbeat():
                                    previous = time.perf_counter()
                                    while not finished:
                                        await asyncio.sleep(.005)
                                        now = time.perf_counter()
                                        ticks.append((now - previous) * 1000)
                                        previous = now

                                beat = asyncio.create_task(heartbeat())
                                await asyncio.sleep(0)
                                fn = namespace[name] if variant == "baseline" else getattr(module, name)
                                call_args = [request, "s"]
                                if name == "request_restart_handoff":
                                    call_args.append(module.RestartHandoffBody(continuation_prompt="fixture", idempotency_key="fixture"))
                                elif name == "edit_restart_handoff":
                                    call_args += ["handoff", module.RestartHandoffEditBody(continuation_prompt="fixture")]
                                elif name == "retry_restart_handoff":
                                    call_args.append("handoff")
                                started = time.perf_counter()
                                try:
                                    await fn(*call_args)
                                    raise AssertionError("Fixture must reject without effects")
                                except HTTPException as exc:
                                    response = {"status": exc.status_code, "detail": exc.detail}
                                finally:
                                    elapsed = (time.perf_counter() - started) * 1000
                                    await asyncio.sleep(.01)
                                    finished = True
                                    await beat
                                body = json.dumps(response, sort_keys=True).encode()
                                expected.setdefault(name, body)
                                assert body == expected[name] and response["status"] == 404
                                assert len(readers) == 1 and (readers[0] == loop_thread) == (variant == "baseline")
                                rows.append(dict(pair=pair, variant=variant, operation=name, request_wall_ms=elapsed,
                                    heartbeat_gap_ms=distribution(ticks), reader_runs_on_loop=readers[0] == loop_thread,
                                    response_sha256=hashlib.sha256(body).hexdigest()))
                                args.output.write_text(json.dumps(partial, indent=2) + "\n")
                return executor.snapshot()
            except BaseException as exc:
                partial.update(error_type=type(exc).__name__, failed_at=datetime.now(UTC).isoformat())
                args.output.write_text(json.dumps(partial, indent=2) + "\n")
                raise
            finally:
                await executor.close()

        runtime = asyncio.run(run())
        summary = {name: {variant: {metric: distribution([row[metric] if metric == "request_wall_ms" else row["heartbeat_gap_ms"]["max"]
                    for row in rows if row["operation"] == name and row["variant"] == variant])
                    for metric in ["request_wall_ms", "heartbeat_max_gap_ms"]} for variant in ["baseline", "candidate"]} for name in functions[1:]}
        value = dict(contract="pa.control-read-loop-progress/v1", observed_at=datetime.now(UTC).isoformat(), baseline=BASELINE,
            source_sha256=hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest(), deliberate_read_delay_ms=200,
            pairs=3, rows=rows, summary=summary, runtime=runtime, complete_response_bytes_equal=True,
            scope="Private deterministic 200ms read with native manager offload/AsyncRuntime, six endpoint families and three alternating pairs. Requests still perform the same delayed read; loop responsiveness is the comparison, not request speedup. No real restart/recovery/provider/effect or production state. Startup gate is held ready in both variants; authorization and startup semantics have separate contracts. Normal GC/host load unchanged. Not historical-cause, live traffic, whole-Kernel or whole-PA acceptance.")
    args.output.write_text(json.dumps(value, indent=2) + "\n")
    print(json.dumps({"output": str(args.output), "summary": summary}))


if __name__ == "__main__":
    main()

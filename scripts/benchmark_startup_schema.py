"""Cold-process full PA schema parity and optional disposable lifespan timing.

Run baseline and source in separate fresh processes, alternating their order.
No provider, peer, live data, server listener or configuration writes are used.
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import tempfile
import threading
import time
from pathlib import Path
from unittest.mock import patch

import fastapi
from fastapi._compat import v2
from fastapi.openapi.utils import get_openapi as framework_get_openapi

import pa.openapi
from pa.config import Settings
from pa.core.kernel import Kernel
from pa.server import readiness
from pa.server.schema_generation import get_openapi


async def run(root: Path, *, baseline: bool, startup: bool) -> dict:
    settings = Settings(data_dir=root / "data", workspace_root=root / "workspaces",
                        agent_enabled=False, telemetry_enabled=False, auth_required=False, peers=[])
    build_start = time.perf_counter()
    app = Kernel.boot(settings=settings).build_app()
    build_ms = (time.perf_counter() - build_start) * 1000
    counts = {"type_adapters": 0}
    post_init = v2.ModelField.__post_init__
    openapi = app.openapi
    loop_thread = threading.get_ident()
    schema_timing = {}

    def counted(field):
        counts["type_adapters"] += 1
        return post_init(field)

    def measured():
        if app.openapi_schema:
            return openapi()
        start, cpu = time.perf_counter(), time.thread_time()
        try:
            return openapi()
        finally:
            schema_timing.update(wall_ms=(time.perf_counter() - start) * 1000,
                                 thread_cpu_ms=(time.thread_time() - cpu) * 1000,
                                 on_loop_thread=threading.get_ident() == loop_thread)

    with patch.object(pa.openapi, "get_openapi", framework_get_openapi if baseline else get_openapi), \
         patch.object(v2.ModelField, "__post_init__", counted), \
         patch.object(app, "openapi", measured):
        if startup:
            started = time.perf_counter()
            async with app.router.lifespan_context(app):
                startup_ms = (time.perf_counter() - started) * 1000
                await asyncio.sleep(.1)
                runtime = app.state.ctx.require_service("async_runtime").snapshot()
                ready = readiness.evaluate_owner_ready(app, app.state.ctx)
                slow = runtime["slow_operations"]
        else:
            readiness.warm_ready_contract(app)
            startup_ms, ready, slow = None, None, None

    canonical = json.dumps(app.openapi_schema, sort_keys=True, separators=(",", ":")).encode()
    return {
        "scope": "Fresh disposable full Kernel app; complete schema warmed before ready. "
                 "Agent and telemetry disabled; no provider/peer/network/listener/live state. "
                 "Adapter counter wraps unchanged constructor. Startup run includes real module "
                 "lifespan/background work; schema-only run does not. Not deployed acceptance.",
        "mode": "baseline" if baseline else "source", "fastapi": fastapi.__version__,
        "build_ms": build_ms, "schema": schema_timing, "counts": counts,
        "paths": len(app.openapi_schema["paths"]), "schema_bytes": len(canonical),
        "schema_sha256": hashlib.sha256(canonical).hexdigest(),
        "required_paths_present": readiness.REQUIRED_READY_PATHS <= app.state.ready_paths,
        "startup_ms": startup_ms, "owner_ready_blocked": ready,
        "startup_slow_operations": slow,
        "startup_openapi": runtime["operations"].get("startup.openapi") if startup else None,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--baseline", action="store_true")
    parser.add_argument("--startup", action="store_true")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="pa-schema-benchmark-") as tmp:
        result = asyncio.run(run(Path(tmp), baseline=args.baseline, startup=args.startup))
    args.output.write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps(result))

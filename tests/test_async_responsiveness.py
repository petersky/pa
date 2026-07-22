from __future__ import annotations

import asyncio
import threading
import time
import unittest
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import httpx
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, StreamingResponse

from pa.core.async_runtime import (
    AsyncRuntime,
    BlockingOperationTimeout,
    BlockingQueueFull,
)
from pa.config import Settings
from pa.core.kernel import Kernel
from pa.modules.sync import router as sync_router


class _Context:
    def __init__(self, runtime: AsyncRuntime, started: threading.Event) -> None:
        self.settings = SimpleNamespace(primary_realm="default", sync_token="")
        self.store = SimpleNamespace(get_projection_head=lambda _realm: "head")
        self.services = {
            "async_runtime": runtime,
            "membership": SimpleNamespace(has_role=lambda *_args: True),
            "sync_engine": SimpleNamespace(
                status=lambda _realm: self._slow_status(started),
            ),
            "event_log": SimpleNamespace(get_head=lambda _realm: "head"),
            "sync_metrics": SimpleNamespace(snapshot=lambda: {}),
        }

    @staticmethod
    def _slow_status(started: threading.Event) -> dict:
        started.set()
        time.sleep(0.2)
        return {
            "realm_id": "default",
            "head": "head",
            "object_count": 1,
            "peer_count": 0,
            "zone": "test",
            "convergence": {},
        }

    def require_service(self, name: str):
        return self.services[name]


class RequestPathResponsivenessTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.runtime = AsyncRuntime(
            max_workers=4,
            max_queue=8,
            default_timeout=1,
            lag_interval_seconds=0.005,
            slow_call_seconds=1,
        )
        await self.runtime.start()
        self.slow_started = threading.Event()
        app = FastAPI()
        app.state.ctx = _Context(self.runtime, self.slow_started)
        app.include_router(sync_router, prefix="/api")

        @app.get("/health")
        async def health():
            return {"status": "ok"}

        @app.get("/ui", response_class=HTMLResponse)
        async def ui():
            return "<main>ready</main>"

        @app.get("/mcp-probe")
        async def mcp_probe():
            return {"tools": "ready"}

        @app.get("/agent-probe")
        async def agent_probe():
            return {"sessions": "ready"}

        @app.get("/events")
        async def events():
            async def stream():
                yield "event: ready\ndata: {}\n\n"

            return StreamingResponse(stream(), media_type="text/event-stream")

        self.client = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        )

    async def asyncTearDown(self) -> None:
        await self.client.aclose()
        await self.runtime.close()

    async def test_slow_sync_disk_read_does_not_stall_unrelated_surfaces(self) -> None:
        slow = asyncio.create_task(self.client.get("/api/sync/status"))
        await asyncio.to_thread(self.slow_started.wait, 1)

        started = time.perf_counter()
        responses = await asyncio.gather(
            self.client.get("/health"),
            self.client.get("/ui"),
            self.client.get("/mcp-probe"),
            self.client.get("/agent-probe"),
            self.client.get("/events"),
        )
        unrelated_ms = (time.perf_counter() - started) * 1000

        self.assertTrue(all(response.status_code == 200 for response in responses))
        self.assertLess(unrelated_ms, 100)
        self.assertEqual((await slow).status_code, 200)
        metrics = self.runtime.snapshot()["operations"]["sync.status"]
        self.assertGreaterEqual(metrics["max_runtime_ms"], 190)

    async def test_slow_blocking_classes_share_bounded_workers_without_loop_lag(self) -> None:
        release = threading.Event()
        started = [threading.Event() for _ in range(4)]
        names = ["git", "sqlite", "provider", "subprocess"]

        def blocked(marker: threading.Event) -> None:
            marker.set()
            release.wait(1)

        tasks = [
            asyncio.create_task(
                self.runtime.run_blocking(name, blocked, marker)
            )
            for name, marker in zip(names, started, strict=True)
        ]
        await asyncio.gather(
            *(asyncio.to_thread(marker.wait, 1) for marker in started)
        )
        before = time.perf_counter()
        response = await self.client.get("/health")
        elapsed_ms = (time.perf_counter() - before) * 1000
        release.set()
        await asyncio.gather(*tasks)
        self.assertEqual(response.status_code, 200)
        self.assertLess(elapsed_ms, 50)


class StartupResponsivenessTests(unittest.IsolatedAsyncioTestCase):
    async def test_provider_startup_is_backgrounded_after_manager_admission(self) -> None:
        release = asyncio.Event()

        async def slow_start(**_kwargs) -> None:
            await release.wait()

        fake_agent = SimpleNamespace(
            browser=SimpleNamespace(async_runtime=None),
            _accepting=True,
            connected=False,
            start=AsyncMock(side_effect=slow_start),
            stop=AsyncMock(),
            list_runtimes=lambda: [],
        )
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(
                data_dir=Path(tmp) / "data",
                workspace_root=Path(tmp) / "workspaces",
                agent_enabled=True,
            )
            kernel = Kernel.boot(settings=settings, load_modules=False)
            app = FastAPI()
            with patch(
                "pa.instance.agent_session.get_instance_agent",
                return_value=fake_agent,
            ):
                started = time.perf_counter()
                await kernel.startup(app)
                startup_ms = (time.perf_counter() - started) * 1000
                self.assertLess(startup_ms, 100)
                self.assertEqual(
                    kernel.ctx.require_service("agent_lifecycle")["phase"],
                    "starting",
                )
                self.assertFalse(kernel.ctx.require_service("agent_start_task").done())
                release.set()
                await kernel.ctx.require_service("agent_start_task")
                await kernel.shutdown(app)


class RuntimeFailureResponseTests(unittest.IsolatedAsyncioTestCase):
    async def test_bounded_runtime_failures_are_explicit_http_responses(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(
                data_dir=Path(tmp) / "data",
                workspace_root=Path(tmp) / "workspaces",
            )
            app = Kernel.boot(settings=settings, load_modules=False).build_app()

            @app.get("/overloaded")
            async def overloaded():
                raise BlockingQueueFull("worker capacity exhausted")

            @app.get("/timed-out")
            async def timed_out():
                raise BlockingOperationTimeout("disk deadline exceeded")

            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as client:
                overloaded_response, timeout_response = await asyncio.gather(
                    client.get("/overloaded"), client.get("/timed-out")
                )

        self.assertEqual(overloaded_response.status_code, 503)
        self.assertEqual(overloaded_response.headers["retry-after"], "1")
        self.assertEqual(timeout_response.status_code, 504)


if __name__ == "__main__":
    unittest.main()

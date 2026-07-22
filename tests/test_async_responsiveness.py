from __future__ import annotations

import asyncio
import threading
import time
import unittest
import tempfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

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
from pa.domain.models import AgentSession
from pa.execution.dispatch import DispatchWorker
from pa.fleet.overview import probe_dimension
from pa.instance.agent_session import AgentSessionManager, AgentSessionRuntime
from pa.modules.agent_providers import list_local_providers
from pa.modules.files import browse_files
from pa.modules.instance import health
from pa.modules.sync import router as sync_router
from pa.pr_supervisor.github import GitHubCredentials
from pa.pr_supervisor.models import GitHubCapability, PRWatch
from pa.pr_supervisor.service import PRSupervisor
from pa.pr_supervisor.store import PRSupervisorStore
from pa.domain.models import FleetInstance


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


class WorkerResponsivenessTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.runtime = AsyncRuntime(
            max_workers=4,
            max_queue=8,
            default_timeout=1,
            lag_interval_seconds=0.005,
            slow_call_seconds=1,
        )
        await self.runtime.start()

    async def asyncTearDown(self) -> None:
        await self.runtime.close()

    async def test_transcript_sqlite_stall_does_not_delay_agent_or_sse_work(
        self,
    ) -> None:
        release = threading.Event()
        entered = threading.Event()
        store = MagicMock()

        def blocked_append(_events) -> None:
            entered.set()
            release.wait(1)

        store.append_transcript_events.side_effect = blocked_append
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(
                data_dir=Path(tmp) / "data",
                workspace_root=Path(tmp) / "workspaces",
            )
            manager = AgentSessionManager(settings, store)
            manager.async_runtime = self.runtime
            session = AgentSession(id="session-blocked", agent_name="codex")
            runtime = AgentSessionRuntime(
                manager, session, initial_transcript_seq=0
            )
            runtime._append_transcript("output", {"text": "one"})
            runtime._flush_transcript()
            await asyncio.to_thread(entered.wait, 1)

            started = time.perf_counter()
            live_event = runtime._append_transcript("output", {"text": "two"})
            await asyncio.sleep(0)
            unrelated_ms = (time.perf_counter() - started) * 1000

            self.assertEqual(live_event["seq"], 2)
            self.assertLess(unrelated_ms, 25)
            release.set()
            await runtime._drain_transcripts()
            self.assertEqual(store.append_transcript_events.call_count, 2)

    async def test_dispatch_reconciliation_stall_keeps_loop_responsive(self) -> None:
        release = threading.Event()
        entered = threading.Event()
        store = MagicMock()

        def blocked_reconcile() -> None:
            entered.set()
            release.wait(1)

        store.reconcile_interrupted.side_effect = blocked_reconcile
        store.runnable.return_value = []
        worker = DispatchWorker(
            store, AsyncMock(), async_runtime=self.runtime
        )
        worker.start()
        await asyncio.to_thread(entered.wait, 1)

        started = time.perf_counter()
        await asyncio.sleep(0)
        unrelated_ms = (time.perf_counter() - started) * 1000

        self.assertLess(unrelated_ms, 25)
        release.set()
        await worker.close()

    async def test_pr_supervisor_store_stall_keeps_health_work_responsive(self) -> None:
        release = threading.Event()
        entered = threading.Event()
        store = MagicMock()
        watch = PRWatch(
            repository="owner/repo",
            pr_number=1,
            pr_url="https://github.com/owner/repo/pull/1",
        )

        def blocked_upsert(value):
            entered.set()
            release.wait(1)
            return value

        store.upsert_watch.side_effect = blocked_upsert
        store.append_event.return_value = True
        github = SimpleNamespace(
            credentials=GitHubCredentials(token="test"),
            _provided_client=None,
            async_runtime=None,
        )
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="instance-a")
            service = PRSupervisor(
                settings,
                MagicMock(),
                supervisor_store=store,
                github_client=github,
                dispatcher=MagicMock(),
                async_runtime=self.runtime,
            )
            task = asyncio.create_task(
                service.register_watch(watch, replicate=False)
            )
            await asyncio.to_thread(entered.wait, 1)

            started = time.perf_counter()
            await asyncio.sleep(0)
            unrelated_ms = (time.perf_counter() - started) * 1000

            self.assertLess(unrelated_ms, 25)
            release.set()
            self.assertIs(await task, watch)
            await service.stop()

    async def test_pr_supervisor_start_does_not_wait_for_provider_network(self) -> None:
        entered = asyncio.Event()

        class SlowGitHub:
            credentials = GitHubCredentials(token="test")
            _provided_client = None
            async_runtime = None

            async def probe(self, _instance_id):
                entered.set()
                await asyncio.Event().wait()
                return GitHubCapability(instance_id="instance-a")

        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="instance-a")
            store = PRSupervisorStore(Path(tmp) / "supervisor.db")
            domain = MagicMock()
            domain.list_cards.return_value = []
            service = PRSupervisor(
                settings,
                domain,
                supervisor_store=store,
                github_client=SlowGitHub(),
                dispatcher=MagicMock(),
                async_runtime=self.runtime,
            )
            started = time.perf_counter()
            await service.start()
            startup_ms = (time.perf_counter() - started) * 1000
            await entered.wait()

            self.assertLess(startup_ms, 25)
            await service.stop()

    async def test_fleet_local_dimension_sqlite_stall_keeps_ui_responsive(self) -> None:
        release = threading.Event()
        entered = threading.Event()
        store = MagicMock()

        def blocked_sessions():
            entered.set()
            release.wait(1)
            return []

        store.list_sessions.side_effect = blocked_sessions
        store.get_projection_head.return_value = "head"
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="local")
            ctx = SimpleNamespace(
                settings=settings,
                store=store,
                services={"async_runtime": self.runtime},
                require_service=lambda name: (
                    self.runtime if name == "async_runtime" else None
                ),
            )
            inst = FleetInstance(
                instance_id="local", name="local", url="http://local"
            )
            task = asyncio.create_task(
                probe_dimension(ctx, inst, "activity", force=True)
            )
            await asyncio.to_thread(entered.wait, 1)

            started = time.perf_counter()
            await asyncio.sleep(0)
            unrelated_ms = (time.perf_counter() - started) * 1000

            self.assertLess(unrelated_ms, 25)
            release.set()
            result = await task
            self.assertEqual(result["state"], "fresh")

    async def test_provider_discovery_stall_keeps_health_responsive(self) -> None:
        release = threading.Event()
        entered = threading.Event()

        def blocked_discovery(_data_dir):
            entered.set()
            release.wait(1)
            return []

        with tempfile.TemporaryDirectory() as tmp:
            ctx = SimpleNamespace(
                settings=SimpleNamespace(data_dir=Path(tmp)),
                require_service=lambda name: self.runtime,
            )
            request = MagicMock()
            request.app.state.ctx = ctx
            with patch(
                "pa.modules.agent_providers.list_provider_summaries",
                side_effect=blocked_discovery,
            ):
                task = asyncio.create_task(list_local_providers(request))
                await asyncio.to_thread(entered.wait, 1)

                started = time.perf_counter()
                response = await health()
                unrelated_ms = (time.perf_counter() - started) * 1000

                self.assertEqual(response, {"status": "ok"})
                self.assertLess(unrelated_ms, 25)
                release.set()
                self.assertEqual(await task, [])

    async def test_file_render_stall_keeps_health_responsive(self) -> None:
        release = threading.Event()
        entered = threading.Event()

        def blocked_render(*_args):
            entered.set()
            release.wait(1)
            return HTMLResponse("ready")

        request = MagicMock()
        request.app.state.ctx.require_service.return_value = self.runtime
        with patch("pa.modules.files._render_browser", side_effect=blocked_render):
            task = asyncio.create_task(browse_files(request, "/slow"))
            await asyncio.to_thread(entered.wait, 1)

            started = time.perf_counter()
            response = await health()
            unrelated_ms = (time.perf_counter() - started) * 1000

            self.assertEqual(response, {"status": "ok"})
            self.assertLess(unrelated_ms, 25)
            release.set()
            self.assertEqual((await task).status_code, 200)


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from fastapi import FastAPI
from fastapi.testclient import TestClient

from pa.cli.startup import wait_for_health, wait_for_ready
from pa.config import Settings, reset_settings
from pa.core.kernel import Kernel, reset_kernel
from pa.domain.store import reset_store
from pa.instance.agent_session import reset_instance_agent
from pa.server.readiness import (
    REQUIRED_READY_PATHS,
    REQUIRED_READY_SERVICES,
    evaluate_ready,
)


def _fake_agent(*, start=None) -> SimpleNamespace:
    return SimpleNamespace(
        browser=SimpleNamespace(async_runtime=None),
        _accepting=True,
        _quiescing=False,
        connected=False,
        start=start or AsyncMock(),
        stop=AsyncMock(),
        list_runtimes=lambda: [],
        quiesce=AsyncMock(),
    )


class EvaluateReadyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.settings = Settings(data_dir=Path(self.tmp.name), agent_enabled=False)
        self.app = FastAPI()
        self.app.state.ready_openapi_warmed = True
        self.app.state.ready_paths = REQUIRED_READY_PATHS
        self.app.state.required_ready_paths = REQUIRED_READY_PATHS
        self.ctx = SimpleNamespace(
            services={
                name: object()
                for name in REQUIRED_READY_SERVICES
            }
        )
        self.ctx.services["agent_lifecycle"] = {"phase": "ready"}
        self.ctx.services["sync_startup_repaired"] = True

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_missing_services_are_starting(self) -> None:
        self.ctx.services.pop("event_log")
        blocked = evaluate_ready(self.app, self.ctx, self.settings)
        self.assertEqual(blocked["status"], "starting")
        self.assertEqual(blocked["missing_services"], ["event_log"])

    def test_unwarmed_openapi_is_starting(self) -> None:
        self.app.state.ready_openapi_warmed = False
        blocked = evaluate_ready(self.app, self.ctx, self.settings)
        self.assertEqual(blocked["missing_routes"], ["openapi"])

    def test_lifecycle_starting_is_not_ready(self) -> None:
        self.ctx.services["agent_lifecycle"] = {"phase": "starting"}
        blocked = evaluate_ready(self.app, self.ctx, self.settings)
        self.assertEqual(blocked["lifecycle"], "starting")

    def test_sync_repair_pending_is_not_ready(self) -> None:
        self.ctx.services.pop("sync_startup_repaired")
        blocked = evaluate_ready(self.app, self.ctx, self.settings)
        self.assertEqual(blocked["sync"], "repair_pending")

    def test_disconnected_owner_channel_is_not_ready(self) -> None:
        with patch(
            "pa.server.readiness.owner_channel_health",
            return_value={"state": "disconnected"},
        ):
            blocked = evaluate_ready(self.app, self.ctx, self.settings)
        self.assertEqual(blocked["owner_channel"], "disconnected")

    def test_unverified_owner_channel_is_ready(self) -> None:
        blocked = evaluate_ready(self.app, self.ctx, self.settings)
        self.assertIsNone(blocked)


class WaitForReadyCLITests(unittest.TestCase):
    def test_wait_for_ready_polls_ready_not_health(self) -> None:
        urls: list[str] = []

        class _Resp:
            status_code = 200

        class _Client:
            def __init__(self, *args, **kwargs) -> None:
                pass

            def __enter__(self):
                return self

            def __exit__(self, *args) -> bool:
                return False

            def get(self, url, headers=None):
                urls.append(url)
                return _Resp()

        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp))
            with patch("pa.cli.startup.httpx.Client", _Client):
                self.assertTrue(
                    wait_for_ready(settings, "http://127.0.0.1:8080", timeout_s=1)
                )
        self.assertTrue(any(url.endswith("/api/ready") for url in urls))
        self.assertFalse(any("/api/health" in url for url in urls))

    def test_wait_for_health_still_uses_liveness(self) -> None:
        urls: list[str] = []

        class _Resp:
            status_code = 200

        class _Client:
            def __init__(self, *args, **kwargs) -> None:
                pass

            def __enter__(self):
                return self

            def __exit__(self, *args) -> bool:
                return False

            def get(self, url, headers=None):
                urls.append(url)
                return _Resp()

        with patch("pa.cli.startup.httpx.Client", _Client):
            self.assertTrue(wait_for_health("http://127.0.0.1:8080", timeout_s=1))
        self.assertTrue(any(url.endswith("/api/health") for url in urls))


class ReadyContractKernelTests(unittest.IsolatedAsyncioTestCase):
    async def asyncTearDown(self) -> None:
        reset_instance_agent()
        reset_store()
        reset_settings()
        reset_kernel()

    async def test_startup_warms_openapi_without_invoking_it_on_ready(self) -> None:
        fake_agent = _fake_agent()
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(
                data_dir=Path(tmp) / "data",
                workspace_root=Path(tmp) / "workspaces",
                agent_enabled=False,
            )
            kernel = Kernel.boot(settings=settings, load_modules=False)
            app = FastAPI()

            @app.get("/api/cards")
            async def cards() -> dict:
                return {}

            with patch(
                "pa.instance.agent_session.get_instance_agent",
                return_value=fake_agent,
            ):
                await kernel.startup(app)
            try:
                self.assertTrue(app.state.ready_openapi_warmed)
                self.assertIn("/api/cards", app.state.ready_paths)
                calls = {"n": 0}
                original = app.openapi

                def wrapped():
                    calls["n"] += 1
                    return original()

                app.openapi = wrapped
                evaluate_ready(app, kernel.ctx, settings)
                self.assertEqual(calls["n"], 0)
            finally:
                await kernel.shutdown(app)

    async def test_ready_blocked_while_agent_lifecycle_is_starting(self) -> None:
        release = asyncio.Event()

        async def slow_start(**_kwargs) -> None:
            await release.wait()

        fake_agent = _fake_agent(start=AsyncMock(side_effect=slow_start))
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
                await kernel.startup(app)
            try:
                self.assertEqual(
                    kernel.ctx.require_service("agent_lifecycle")["phase"],
                    "starting",
                )
                kernel.ctx.register_service("event_log", object())
                kernel.ctx.register_service("fleet_registry", object())
                kernel.ctx.register_service("sync_startup_repaired", True)
                app.state.ready_openapi_warmed = True
                app.state.ready_paths = REQUIRED_READY_PATHS
                app.state.required_ready_paths = REQUIRED_READY_PATHS
                blocked = evaluate_ready(app, kernel.ctx, settings)
                self.assertEqual(blocked["lifecycle"], "starting")
                release.set()
                await kernel.ctx.require_service("agent_start_task")
                self.assertIsNone(evaluate_ready(app, kernel.ctx, settings))
            finally:
                if not release.is_set():
                    release.set()
                await kernel.shutdown(app)


class ReadyContractHTTPTests(unittest.TestCase):
    def tearDown(self) -> None:
        reset_instance_agent()
        reset_store()
        reset_settings()
        reset_kernel()

    def test_first_ready_after_boot_does_not_generate_schema(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(
                data_dir=Path(tmp),
                agent_enabled=False,
                telemetry_enabled=False,
            )
            app = Kernel.boot(settings=settings).build_app()
            with TestClient(app) as client:
                self.assertTrue(client.app.state.ready_openapi_warmed)
                self.assertTrue(
                    REQUIRED_READY_PATHS <= set(client.app.state.ready_paths)
                )

                def boom(*_args, **_kwargs):
                    raise AssertionError("schema generation on ready path")

                client.app.openapi = boom
                response = client.get("/api/ready")
                self.assertEqual(response.status_code, 200, response.text)
                payload = response.json()
                self.assertEqual(payload["status"], "ready")
                self.assertIn(payload["lifecycle"], {"ready", "idle", "error"})

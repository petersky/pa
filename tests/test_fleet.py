"""Tests for fleet registry, join wiring, and remote install helpers."""

from __future__ import annotations

import json
import asyncio
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
from fastapi import HTTPException

from pa.config import Settings
from pa.domain.instance_config import load_instance_config, update_instance_config
from pa.domain.models import Card, CardLane, FleetInstance, Project, ProjectRepo
from pa.fleet.join import (
    apply_join_response,
    apply_reachability_settings,
    ensure_sync_token,
    owner_public_url,
    readiness_issues,
    readiness_warnings,
    register_joiner_on_owner,
    remove_peer_url,
    unwire_instance_peers,
)
from pa.fleet.registry import FleetRegistry
from pa.fleet.remote_install import (
    RemoteInstallRequest,
    build_remote_command,
    build_remote_env,
)
from pa.network.peer_table import PeerTable
from pa.modules.fleet import _proxy_agent_providers, fleet_agent_provider_login_start


class FleetRegistryReloadTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.data_dir = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_cli_token_visible_to_separate_registry(self) -> None:
        writer = FleetRegistry(self.data_dir, "fleet-a")
        token = writer.create_join_token().token

        # Simulate running server that loaded before the CLI wrote the token.
        reader = FleetRegistry(self.data_dir, "fleet-a")
        # Clear in-memory cache to force disk reload path on consume.
        reader._tokens.clear()
        consumed = reader.consume_join_token(token)
        self.assertIsNotNone(consumed)
        self.assertEqual(consumed.token, token)
        self.assertIsNone(reader.consume_join_token(token))

    def test_create_merges_disk_tokens(self) -> None:
        a = FleetRegistry(self.data_dir, "fleet-a")
        t1 = a.create_join_token().token
        b = FleetRegistry(self.data_dir, "fleet-a")
        b._tokens.clear()
        t2 = b.create_join_token().token
        # Both tokens should be on disk / consumable after reload
        c = FleetRegistry(self.data_dir, "fleet-a")
        c._tokens.clear()
        self.assertIsNotNone(c.consume_join_token(t1))
        c2 = FleetRegistry(self.data_dir, "fleet-a")
        c2._tokens.clear()
        self.assertIsNotNone(c2.consume_join_token(t2))

    def test_codex_login_start_proxies_consent_only_to_target(self) -> None:
        request = MagicMock()
        with patch(
            "pa.modules.fleet._proxy_agent_providers",
            new_callable=AsyncMock,
            return_value={"job_id": "remote-job", "state": "pending"},
        ) as proxy:
            result = __import__("asyncio").run(
                fleet_agent_provider_login_start(
                    request,
                    "peer-1",
                    "codex",
                    {"consent": True, "timeout_seconds": 600},
                )
            )
        self.assertEqual(result["job_id"], "remote-job")
        proxy.assert_awaited_once_with(
            request,
            "peer-1",
            "POST",
            "/codex/login-jobs",
            body={"consent": True, "timeout_seconds": 600},
        )

    def test_provider_proxy_preserves_structured_active_login_detail(self) -> None:
        settings = Settings(data_dir=self.data_dir, sync_token="shared")
        fleet = FleetRegistry(self.data_dir, settings.fleet_id)
        fleet.upsert_instance(
            FleetInstance(instance_id="peer-1", name="peer", url="http://peer:8080")
        )
        ctx = MagicMock()
        ctx.settings = settings
        ctx.services = {}
        ctx.require_service.return_value = fleet
        request = MagicMock()
        request.app.state.ctx = ctx
        response = MagicMock()
        response.status_code = 409
        response.json.return_value = {
            "detail": {"message": "A Codex login is already active", "job_id": "job-1"}
        }
        client = AsyncMock()
        client.request.return_value = response
        with (
            patch("pa.modules.fleet.require_user"),
            patch("pa.modules.fleet.httpx.AsyncClient", return_value=client),
            self.assertRaises(HTTPException) as raised,
        ):
            asyncio.run(
                _proxy_agent_providers(
                    request,
                    "peer-1",
                    "POST",
                    "/codex/login-jobs",
                    body={"consent": True},
                )
            )
        self.assertEqual(raised.exception.status_code, 409)
        self.assertEqual(raised.exception.detail["job_id"], "job-1")

    def test_device_login_ui_supports_local_proxy_resume_and_success_refresh(
        self,
    ) -> None:
        source = Path("src/pa/server/static/js/fleet.js").read_text()
        self.assertIn('return "/api/agent/providers/codex/login-jobs"', source)
        self.assertIn('data-codex-login-resume="', source)
        self.assertIn("Use any browser to finish signing in", source)
        self.assertIn(
            'if (job.state === "succeeded") setTimeout(loadLiveStatus, 1000)',
            source,
        )


class FleetJoinWiringTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.data_dir = Path(self._tmp.name)
        self.settings = Settings(
            data_dir=self.data_dir,
            instance_name="owner",
            instance_url="http://macbook:8080",
            host="0.0.0.0",
            subscribed_realms=["personal"],
            sync_token="",
            peers=[],
        )
        update_instance_config(
            self.data_dir,
            instance_id=self.settings.instance_id,
            instance_name="owner",
            fleet_id=self.settings.fleet_id,
            instance_url="http://macbook:8080",
            subscribed_realms=["personal"],
        )

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_owner_public_url_prefers_instance_url(self) -> None:
        self.assertEqual(owner_public_url(self.settings), "http://macbook:8080")

    def test_owner_public_url_avoids_zero_bind(self) -> None:
        settings = Settings(
            data_dir=self.data_dir, host="0.0.0.0", port=8080, instance_url=""
        )
        url = owner_public_url(settings)
        self.assertNotIn("0.0.0.0", url)
        self.assertIn("127.0.0.1", url)

    def test_readiness_warns_on_loopback_url(self) -> None:
        settings = Settings(
            data_dir=self.data_dir,
            instance_url="http://127.0.0.1:8080",
            host="127.0.0.1",
        )
        issues = readiness_issues(settings)
        ids = {i["id"] for i in issues}
        self.assertIn("loopback_instance_url", ids)
        self.assertIn("loopback_bind", ids)
        for issue in issues:
            self.assertTrue(issue["fix"])
            self.assertIn(
                issue["action"],
                {"set_instance_url", "set_bind_all", "ensure_sync_token"},
            )
        warnings = readiness_warnings(settings)
        self.assertTrue(
            any("loopback" in w.lower() or "127.0.0.1" in w for w in warnings)
        )

    def test_apply_reachability_settings_persists(self) -> None:
        settings = Settings(
            data_dir=self.data_dir,
            instance_url="",
            host="127.0.0.1",
        )
        result = apply_reachability_settings(
            settings,
            instance_url="http://macbook:8080",
            host="0.0.0.0",
        )
        self.assertEqual(settings.instance_url, "http://macbook:8080")
        self.assertEqual(settings.host, "0.0.0.0")
        self.assertTrue(result["restart_required"])
        cfg = load_instance_config(self.data_dir)
        self.assertEqual(cfg.instance_url, "http://macbook:8080")
        self.assertEqual(cfg.host, "0.0.0.0")
        self.assertFalse(
            any(i["id"] == "missing_instance_url" for i in readiness_issues(settings))
        )
        self.assertFalse(
            any(i["id"] == "loopback_bind" for i in readiness_issues(settings))
        )

    def test_apply_reachability_rejects_loopback_url(self) -> None:
        settings = Settings(data_dir=self.data_dir, instance_url="", host="0.0.0.0")
        with self.assertRaises(ValueError):
            apply_reachability_settings(settings, instance_url="http://127.0.0.1:8080")

    def test_ensure_sync_token_persists(self) -> None:
        token = ensure_sync_token(self.settings)
        self.assertTrue(token)
        self.assertEqual(self.settings.sync_token, token)
        cfg = load_instance_config(self.data_dir)
        self.assertEqual(cfg.sync_token, token)
        # Second call returns same
        self.assertEqual(ensure_sync_token(self.settings), token)

    def test_register_joiner_wires_peers_and_token(self) -> None:
        fleet = FleetRegistry(self.data_dir, self.settings.fleet_id)
        peer_table = PeerTable(self.data_dir)
        inst, sync_token = register_joiner_on_owner(
            fleet,
            peer_table,
            self.settings,
            joiner_id="joiner-1",
            name="mini",
            url="http://mini:8080",
            realms=["personal"],
        )
        self.assertEqual(inst.name, "mini")
        self.assertTrue(sync_token)
        self.assertIn("http://mini:8080", self.settings.peers)
        routes = peer_table.routes_for_realm("personal")
        self.assertTrue(any(r.target_url == "http://mini:8080" for r in routes))
        cfg = load_instance_config(self.data_dir)
        self.assertIn("http://mini:8080", cfg.peers)
        self.assertEqual(cfg.sync_token, sync_token)

    def test_remove_cleans_peers_and_routes(self) -> None:
        fleet = FleetRegistry(self.data_dir, self.settings.fleet_id)
        peer_table = PeerTable(self.data_dir)
        inst, _ = register_joiner_on_owner(
            fleet,
            peer_table,
            self.settings,
            joiner_id="joiner-1",
            name="mini",
            url="http://mini:8080",
            realms=["personal"],
        )
        unwire_instance_peers(peer_table, instance_id=inst.instance_id, url=inst.url)
        remove_peer_url(self.settings, inst.url)
        fleet.remove_instance(inst.instance_id)
        self.assertNotIn("http://mini:8080", self.settings.peers)
        self.assertFalse(peer_table.routes_for_realm("personal"))
        self.assertIsNone(fleet.get_instance("joiner-1"))

    def test_apply_join_response_persists_sync_token(self) -> None:
        apply_join_response(
            self.data_dir,
            fleet_id="fleet-remote",
            owner_url="http://macbook:8080",
            subscribed_realms=["personal"],
            sync_token="abc123",
            peers=["http://macbook:8080"],
        )
        cfg = load_instance_config(self.data_dir)
        self.assertEqual(cfg.fleet_id, "fleet-remote")
        self.assertEqual(cfg.sync_token, "abc123")
        self.assertEqual(cfg.fleet_owner_url, "http://macbook:8080")
        self.assertIn("http://macbook:8080", cfg.peers)


class RemoteInstallHelperTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.data_dir = Path(self._tmp.name)
        self.settings = Settings(
            data_dir=self.data_dir,
            instance_url="http://macbook:8080",
            subscribed_realms=["personal"],
            sync_token="shared-secret",
            release_track="release",
        )

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_build_remote_env_includes_fleet_and_sync(self) -> None:
        req = RemoteInstallRequest(
            host="mini",
            user="peter",
            instance_name="mini",
            instance_url="http://mini:8080",
        )
        env = build_remote_env(self.settings, req, fleet_token="tok123")
        self.assertEqual(env["PA_FLEET_TOKEN"], "tok123")
        self.assertEqual(env["PA_SYNC_TOKEN"], "shared-secret")
        self.assertEqual(env["PA_FLEET_OWNER_URL"], "http://macbook:8080")
        self.assertEqual(env["PA_HOST"], "0.0.0.0")
        self.assertEqual(env["PA_INSTANCE_URL"], "http://mini:8080")

    def test_join_only_command(self) -> None:
        req = RemoteInstallRequest(
            host="mini",
            user="peter",
            instance_name="mini",
            instance_url="http://mini:8080",
            join_only=True,
        )
        cmd = build_remote_command(self.settings, req, fleet_token="tok123")
        self.assertIn("pa fleet join", cmd)
        self.assertIn("tok123", cmd)
        self.assertNotIn("password", cmd.lower())

    def test_job_persist_omits_secrets(self) -> None:
        from pa.fleet.remote_install import InstallJobStore

        store = InstallJobStore(self.data_dir)
        req = RemoteInstallRequest(
            host="mini",
            user="peter",
            instance_name="mini",
            instance_url="http://mini:8080",
            password="super-secret",
            passphrase="also-secret",
        )
        job = store.create(req)
        path = self.data_dir / "fleet_jobs" / f"{job.job_id}.json"
        text = path.read_text()
        self.assertNotIn("super-secret", text)
        self.assertNotIn("also-secret", text)
        self.assertNotIn("password", text)


class RemoteInstallJobMockTests(unittest.IsolatedAsyncioTestCase):
    async def test_run_install_job_success(self) -> None:
        from pa.fleet.remote_install import (
            InstallJobStatus,
            InstallJobStore,
            run_install_job,
        )

        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            settings = Settings(
                data_dir=data_dir,
                instance_url="http://macbook:8080",
                subscribed_realms=["personal"],
                sync_token="secret",
            )
            fleet = FleetRegistry(data_dir, settings.fleet_id)
            store = InstallJobStore(data_dir)
            req = RemoteInstallRequest(
                host="mini",
                user="peter",
                instance_name="mini",
                instance_url="http://mini:8080",
                password="once",
            )
            job = store.create(req)

            mock_conn = MagicMock()
            mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
            mock_conn.__aexit__ = AsyncMock(return_value=None)

            with (
                patch(
                    "pa.fleet.remote_install._connect_ssh",
                    AsyncMock(return_value=mock_conn),
                ),
                patch(
                    "pa.fleet.remote_install._run_remote_install",
                    AsyncMock(return_value=0),
                ),
                patch(
                    "pa.fleet.remote_install.verify_remote_health",
                    AsyncMock(return_value=True),
                ),
            ):
                result = await run_install_job(settings, fleet, store, job, req)

            self.assertEqual(result.status, InstallJobStatus.SUCCEEDED)
            # Password must not appear in logs or disk snapshot
            blob = (
                "\n".join(result.log_lines)
                + (data_dir / "fleet_jobs" / f"{job.job_id}.json").read_text()
            )
            self.assertNotIn("once", blob)


class FleetPageLazyLoadTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.data_dir = Path(self._tmp.name)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_fleet_context_does_not_probe_peers(self) -> None:
        from pa.domain.models import FleetInstance
        from pa.fleet.membership import MembershipStore
        from pa.modules.fleet import _fleet_context

        settings = Settings(
            data_dir=self.data_dir,
            instance_id="local-1",
            instance_name="owner",
            instance_url="http://macbook:8080",
            host="0.0.0.0",
            subscribed_realms=["personal"],
            sync_token="secret",
            peers=["http://mini:8080"],
        )
        fleet = FleetRegistry(settings.data_dir, settings.fleet_id)
        fleet.register_self(
            settings.instance_id,
            settings.instance_name,
            settings.instance_url,
            zone=settings.zone,
        )
        fleet.upsert_instance(
            FleetInstance(
                instance_id="remote-1",
                name="mini",
                url="http://mini:8080",
                zone=settings.zone,
            )
        )
        membership = MembershipStore(settings.data_dir)
        peer_table = PeerTable(settings.data_dir)

        ctx = MagicMock()
        ctx.settings = settings
        ctx.require_service = MagicMock(
            side_effect=lambda name: {
                "fleet_registry": fleet,
                "membership": membership,
                "peer_table": peer_table,
            }[name]
        )
        request = MagicMock()
        request.app.state.ctx = ctx

        with (
            patch("pa.modules.fleet.httpx.Client") as sync_client,
            patch("pa.modules.fleet.httpx.AsyncClient") as async_client,
        ):
            data = _fleet_context(request)

        sync_client.assert_not_called()
        async_client.assert_not_called()
        self.assertEqual(len(data["fleet_instances"]), 2)
        self.assertNotIn("provider_status", data)
        self.assertTrue(data["has_sync_token"])

    def test_fleet_page_and_dimension_endpoint_render_normalized_contract(
        self,
    ) -> None:
        from fastapi.testclient import TestClient

        from pa.core.kernel import Kernel
        from pa.domain.store import reset_store
        from pa.instance.agent_session import reset_instance_agent

        reset_store()
        reset_instance_agent()
        settings = Settings(
            data_dir=self.data_dir,
            instance_id="local-1",
            instance_name="owner",
            instance_url="http://owner:8080",
            agent_enabled=False,
            peers=[],
        )
        try:
            app = Kernel.boot(settings=settings).build_app()
            with TestClient(app) as client:
                page = client.get("/fleet")
                self.assertEqual(page.status_code, 200, page.text)
                self.assertIn("pa-fleet-overview-data", page.text)
                self.assertIn("pa-fleet-topology", page.text)
                self.assertNotIn("Checking…", page.text)

                dimension = client.get(
                    "/api/fleet/overview/dimension",
                    params={
                        "instance_id": "local-1",
                        "dimension": "reachability",
                        "generation": 17,
                    },
                )
                self.assertEqual(dimension.status_code, 200, dimension.text)
                self.assertEqual(dimension.json()["state"], "fresh")
                self.assertEqual(dimension.json()["generation"], 17)
                self.assertIn("fleet-reachability", dimension.headers["server-timing"])
                self.assertEqual(dimension.headers["x-fleet-generation"], "17")
        finally:
            reset_instance_agent()
            reset_store()


class FleetHealthParallelTests(unittest.IsolatedAsyncioTestCase):
    async def test_health_probes_in_parallel_and_includes_providers(self) -> None:
        from pa.domain.models import FleetInstance
        from pa.modules.fleet import fleet_health

        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            settings = Settings(
                data_dir=data_dir,
                instance_id="local-1",
                instance_name="owner",
                instance_url="http://macbook:8080",
                sync_token="secret",
            )
            fleet = FleetRegistry(data_dir, settings.fleet_id)
            fleet.upsert_instance(
                FleetInstance(
                    instance_id="a",
                    name="a",
                    url="http://a:8080",
                )
            )
            fleet.upsert_instance(
                FleetInstance(
                    instance_id="b",
                    name="b",
                    url="http://b:8080",
                )
            )

            ctx = MagicMock()
            ctx.settings = settings
            ctx.require_service = MagicMock(return_value=fleet)
            request = MagicMock()
            request.app.state.ctx = ctx

            class FakeResp:
                def __init__(self, status_code: int, payload=None):
                    self.status_code = status_code
                    self._payload = payload if payload is not None else {}

                def json(self):
                    return self._payload

            async def fake_get(url, headers=None, timeout=None):
                if url.endswith("/api/health"):
                    return FakeResp(200)
                if url.endswith("/api/agent/providers"):
                    host = "a" if "://a:" in url else "b"
                    return FakeResp(
                        200,
                        [{"id": host, "display_name": host.upper(), "available": True}],
                    )
                if url.endswith("/api/status"):
                    return FakeResp(200, {"version": "0.2.5", "release_track": "beta"})
                if url.endswith("/api/fleet/peer-update-check"):
                    return FakeResp(
                        200,
                        {
                            "available_version": "0.2.6",
                            "upgrade_available": True,
                            "channel": "beta",
                        },
                    )
                return FakeResp(404)

            mock_client = MagicMock()
            mock_client.get = AsyncMock(side_effect=fake_get)
            mock_client.__aenter__ = AsyncMock(return_value=mock_client)
            mock_client.__aexit__ = AsyncMock(return_value=None)

            with (
                patch("pa.modules.fleet.require_user", return_value=object()),
                patch("pa.modules.fleet.httpx.AsyncClient", return_value=mock_client),
            ):
                results = await fleet_health(request)

            by_id = {row["instance_id"]: row for row in results}
            self.assertTrue(by_id["a"]["healthy"])
            self.assertTrue(by_id["b"]["healthy"])
            self.assertEqual(by_id["a"]["providers"][0]["id"], "a")
            self.assertEqual(by_id["b"]["providers"][0]["id"], "b")
            self.assertEqual(by_id["a"]["current_version"], "0.2.5")
            self.assertEqual(by_id["b"]["available_version"], "0.2.6")
            provider_calls = [
                call
                for call in mock_client.get.await_args_list
                if call.args[0].endswith("/api/agent/providers")
            ]
            self.assertTrue(provider_calls)
            self.assertTrue(
                all(call.kwargs["timeout"] == 5.0 for call in provider_calls)
            )
            self.assertEqual(by_id["a"]["update_channel"], "beta")
            # health + providers + status + update check for each instance
            self.assertEqual(mock_client.get.await_count, 8)

    async def test_slow_peer_and_detail_timeouts_are_terminal_and_isolated(
        self,
    ) -> None:
        import asyncio
        from pa.modules.fleet import fleet_health

        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="local")
            fleet = FleetRegistry(settings.data_dir, settings.fleet_id)
            for instance_id in ("fast", "hung"):
                fleet.upsert_instance(
                    FleetInstance(
                        instance_id=instance_id,
                        name=instance_id,
                        url=f"http://{instance_id}:8080",
                    )
                )
            ctx = MagicMock(settings=settings)
            ctx.require_service.return_value = fleet
            request = MagicMock()
            request.app.state.ctx = ctx

            class Resp:
                status_code = 200

                def __init__(self, payload=None):
                    self.payload = payload or {}

                def json(self):
                    return self.payload

            async def get(url, **_kwargs):
                if "hung" in url:
                    await asyncio.Future()
                if url.endswith("/providers"):
                    return Resp([])
                if url.endswith("/api/status"):
                    return Resp({"version": "1.0.0"})
                if url.endswith("peer-update-check"):
                    await asyncio.Future()
                return Resp()

            client = MagicMock()
            client.get = AsyncMock(side_effect=get)
            client.__aenter__ = AsyncMock(return_value=client)
            client.__aexit__ = AsyncMock(return_value=None)
            with (
                patch("pa.modules.fleet.require_user"),
                patch("pa.modules.fleet.httpx.AsyncClient", return_value=client),
                patch("pa.modules.fleet.FLEET_HEALTH_TIMEOUT", 0.01),
                patch("pa.modules.fleet.FLEET_DETAIL_TIMEOUT", 0.01),
                patch("pa.modules.fleet.FLEET_AGGREGATE_TIMEOUT", 0.03),
            ):
                rows = await fleet_health(request)

            by_id = {row["instance_id"]: row for row in rows}
            self.assertEqual(by_id["hung"]["state"], "timeout")
            self.assertEqual(by_id["fast"]["state"], "up")
            self.assertEqual(by_id["fast"]["update_state"], "timeout")
            self.assertEqual(by_id["fast"]["providers_state"], "up")
            self.assertTrue(all(row["state"] != "checking" for row in rows))

    async def test_local_health_does_not_use_broken_advertised_url(self) -> None:
        from pa.modules.fleet import fleet_health

        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(
                data_dir=Path(tmp),
                instance_id="local",
                instance_url="http://broken.invalid:8080",
            )
            fleet = FleetRegistry(settings.data_dir, settings.fleet_id)
            fleet.upsert_instance(
                FleetInstance(
                    instance_id="local",
                    name="local",
                    url=settings.instance_url,
                )
            )
            ctx = MagicMock(settings=settings)
            ctx.require_service.return_value = fleet
            request = MagicMock()
            request.app.state.ctx = ctx
            client = MagicMock()
            client.get = AsyncMock(side_effect=AssertionError("local URL was probed"))
            client.__aenter__ = AsyncMock(return_value=client)
            client.__aexit__ = AsyncMock(return_value=None)
            with (
                patch("pa.modules.fleet.require_user"),
                patch("pa.modules.fleet.httpx.AsyncClient", return_value=client),
                patch(
                    "pa.acp.providers.resolve.list_provider_summaries", return_value=[]
                ),
                patch(
                    "pa.update.runner.check_update", side_effect=RuntimeError("offline")
                ),
            ):
                rows = await fleet_health(request)
            self.assertEqual(rows[0]["state"], "up")
            self.assertEqual(rows[0]["update_state"], "error")
            client.get.assert_not_awaited()


class FleetOverviewTests(unittest.IsolatedAsyncioTestCase):
    async def test_dimension_probes_are_single_flight_and_keep_last_good_value(
        self,
    ) -> None:
        from pa.fleet.overview import (
            cache_for,
            field,
            probe_dimension,
        )

        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="local")
            ctx = MagicMock(settings=settings)
            inst = FleetInstance(
                instance_id="remote",
                name="remote",
                url="http://remote:8080",
            )
            started = asyncio.Event()
            release = asyncio.Event()
            calls = 0

            async def slow_probe(*_args):
                nonlocal calls
                calls += 1
                started.set()
                await release.wait()
                return field(
                    "fresh",
                    {"health": "up"},
                    observed_at="2026-07-22T12:00:00+00:00",
                    duration_ms=12,
                )

            with patch("pa.fleet.overview._probe", side_effect=slow_probe):
                first = asyncio.create_task(
                    probe_dimension(ctx, inst, "reachability", force=True)
                )
                second = asyncio.create_task(
                    probe_dimension(ctx, inst, "reachability", force=True)
                )
                await started.wait()
                release.set()
                results = await asyncio.gather(first, second)

            self.assertEqual(calls, 1)
            self.assertEqual(results[0]["value"], {"health": "up"})
            self.assertEqual(results[1]["value"], {"health": "up"})

            with patch(
                "pa.fleet.overview._probe",
                new=AsyncMock(
                    return_value=field(
                        "timeout", None, duration_ms=2500, error="deadline"
                    )
                ),
            ):
                timed_out = await probe_dimension(ctx, inst, "reachability", force=True)

            self.assertEqual(timed_out["state"], "timeout")
            self.assertEqual(timed_out["value"], {"health": "up"})
            persisted = cache_for(settings.data_dir).get("remote", "reachability")
            self.assertEqual(persisted["value"], {"health": "up"})

    def test_topology_uses_same_nodes_for_routes_updates_and_supervisor(self) -> None:
        from pa.fleet.overview import build_overview
        from pa.fleet.update import UpdatePhase
        from pa.network.peer_table import PeerRoute

        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(
                data_dir=Path(tmp),
                instance_id="local",
                instance_name="owner",
                instance_url="http://owner:8080",
            )
            ctx = MagicMock(settings=settings)
            ctx.store.list_sessions.return_value = []
            ctx.store.list_repositories.return_value = []
            ctx.store.get_projection_head.return_value = "head"
            ctx.services = {}

            update = MagicMock()
            update.instance_id = "remote"
            update.phase = UpdatePhase.RESTARTING
            update.public_dict.return_value = {
                "job_id": "update-1",
                "phase": "restarting",
            }
            update_store = MagicMock()
            update_store.list.return_value = [update]

            watch = MagicMock()
            watch.id = "watch-1"
            watch.owner_instance_id = "remote"
            watch.originating_instance_id = "local"
            watch.repository = "petersky/pa"
            watch.pr_number = 99
            watch.last_error = None
            watch.model_dump.return_value = {
                "id": "watch-1",
                "owner_instance_id": "remote",
            }
            supervisor_store = MagicMock()
            supervisor_store.list_watches.return_value = [watch]
            ctx.services.update(
                fleet_update_job_store=update_store,
                pr_supervisor_store=supervisor_store,
            )
            instances = [
                FleetInstance(
                    instance_id="local",
                    name="owner",
                    url="http://owner:8080",
                ),
                FleetInstance(
                    instance_id="remote",
                    name="worker",
                    url="http://worker:8080",
                ),
            ]
            routes = [
                PeerRoute(
                    realm_id="default",
                    target_url="http://worker:8080",
                    target_instance_id="remote",
                )
            ]

            overview = build_overview(ctx, instances, routes)

            self.assertEqual(
                {node["id"] for node in overview["nodes"]}, {"local", "remote"}
            )
            self.assertEqual(
                {edge["kind"] for edge in overview["edges"]},
                {"sync", "supervisor"},
            )
            remote = next(node for node in overview["nodes"] if node["id"] == "remote")
            self.assertEqual(
                remote["dimensions"]["activity"]["value"]["state"], "starting"
            )
            supervisor = next(
                edge for edge in overview["edges"] if edge["kind"] == "supervisor"
            )
            self.assertEqual(
                (supervisor["source"], supervisor["target"]), ("remote", "local")
            )

    def test_local_activity_reports_multiple_sessions_cards_and_queue(self) -> None:
        from pa.domain.models import AgentSession
        from pa.fleet.overview import local_dimension

        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="local")
            ctx = MagicMock(settings=settings)
            manager = MagicMock()
            manager.progress.return_value = SimpleNamespace(
                model_dump=lambda mode: {
                    "phase": "prompting",
                    "active_sessions": 2,
                    "queued_prompts": 1,
                    "quiescing": False,
                    "prompting": True,
                    "message": "2 ACP sessions working, 1 queued",
                }
            )
            manager.list_runtimes.return_value = []
            ctx.services = {"instance_agent": manager}
            ctx.store.list_sessions.return_value = [
                AgentSession(
                    id="session-1",
                    agent_name="codex",
                    card_id="card-1",
                    status="working",
                    title="First card",
                ),
                AgentSession(
                    id="session-2",
                    agent_name="codex",
                    card_id="card-2",
                    status="idle",
                    title="Second card",
                ),
            ]

            activity = local_dimension(ctx, "activity")

            self.assertEqual(activity["state"], "working")
            self.assertEqual(activity["active_sessions"], 2)
            self.assertEqual(activity["queued_prompts"], 1)
            self.assertEqual(
                {session["card_id"] for session in activity["sessions"]},
                {"card-1", "card-2"},
            )


class FleetUpdateUiTests(unittest.TestCase):
    def test_update_form_uses_peer_track_and_rechecks_selected_channel(self) -> None:
        root = Path(__file__).parents[1]
        script = (root / "src/pa/server/static/js/fleet.js").read_text()
        template = (root / "src/pa/server/templates/pages/fleet.html").read_text()
        self.assertIn("updateValue.channel", script)
        self.assertIn("tr.dataset.updateChannel", script)
        self.assertIn("/update-check?channel=", script)
        self.assertIn("refreshFleetUpdateCheck().then", script)
        self.assertIn('name="install_timeout"', template)

    def test_live_health_is_single_flight_abortable_and_terminal(self) -> None:
        root = Path(__file__).parents[1]
        script = (root / "src/pa/server/static/js/fleet.js").read_text()
        template = (root / "src/pa/server/templates/pages/fleet.html").read_text()
        self.assertIn(
            "if (liveStatusRequest && !force) return liveStatusRequest", script
        )
        self.assertIn('document.body.addEventListener("htmx:beforeSwap"', script)
        self.assertIn("liveStatusController.abort()", script)
        self.assertIn("var concurrency = Math.min(4, work.length)", script)
        self.assertIn("browser deadline exceeded", script)
        self.assertIn("/api/fleet/overview/dimension", script)
        self.assertIn("function edgeVisualStatus(edge)", script)
        self.assertIn("providerLabel", script)
        self.assertIn("if (seq !== liveStatusSeq) return", script)
        self.assertIn("patch.generation !== seq", script)
        self.assertIn("Object.assign({}, previous", script)
        self.assertIn("Health check failed", script)
        self.assertIn("performance.measure", script)
        self.assertIn('id="pa-fleet-refresh"', template)

    def test_topology_is_accessible_responsive_and_has_no_js_equivalent(self) -> None:
        root = Path(__file__).parents[1]
        script = (root / "src/pa/server/static/js/fleet.js").read_text()
        template = (root / "src/pa/server/templates/pages/fleet.html").read_text()
        style = (root / "src/pa/server/static/style.css").read_text()
        self.assertIn('aria-label="Fleet instance and activity topology"', template)
        self.assertIn("<noscript>", template)
        self.assertIn('id="pa-fleet-edge-list"', template)
        self.assertIn('id="pa-fleet-instances"', template)
        self.assertIn('tabindex="0" role="button"', script)
        self.assertIn('event.key !== "Enter" && event.key !== " "', script)
        self.assertIn("@media (max-width: 1050px)", style)
        self.assertIn("@media (max-width: 900px)", style)
        self.assertIn("@media (prefers-reduced-motion: reduce)", style)
        self.assertIn(".fleet-edge-stale line", style)


class RemoteOperationsTests(unittest.IsolatedAsyncioTestCase):
    async def test_card_dispatch_returns_durable_admission_without_peer_wait(
        self,
    ) -> None:
        from pa.modules.fleet import RemoteAgentStartBody, start_remote_agent_work

        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(
                data_dir=Path(tmp),
                instance_id="controller-1",
                instance_name="controller",
                instance_url="http://controller:8080",
                primary_realm="default",
                sync_token="secret",
            )
            fleet = FleetRegistry(settings.data_dir, settings.fleet_id)
            fleet.upsert_instance(
                FleetInstance(
                    instance_id="mini-1",
                    name="macmini",
                    url="http://mini:8080",
                )
            )
            card = Card(
                id="card-1",
                title="Implement remote control",
                body="Build and validate the fleet operations console.",
                project_id="project-1",
            )
            project = Project(
                id="project-1",
                title="PA Core",
                repos=[
                    ProjectRepo(
                        url="https://github.com/petersky/pa.git",
                        path="/Users/petersky/repos/petersky/pa",
                    )
                ],
                agent_prompt="Use one worktree per card.",
                tool_config={"development_instance": "macmini"},
            )
            updated = card.model_copy(
                update={
                    "lane": CardLane.ACTIVE,
                    "preferred_instance": "mini-1",
                }
            )
            store = MagicMock()
            store.get_card.return_value = card
            store.get_project.return_value = project
            store.project_working_directory.return_value = "/srv/pa/remote"
            store.update_card.return_value = updated

            ctx = MagicMock()
            ctx.settings = settings
            ctx.store = store
            ctx.services = {"fleet_registry": fleet}
            ctx.require_service.side_effect = lambda name: ctx.services[name]
            ctx.register_service.side_effect = lambda name, value: (
                ctx.services.__setitem__(name, value)
            )
            request = MagicMock()
            request.app.state.ctx = ctx
            request.headers = {"idempotency-key": "browser-attempt-1"}

            peer = AsyncMock(
                side_effect=[
                    {"session": {"id": "remote-session", "title": card.title}},
                    {"started": True, "queued": False},
                ]
            )
            with (
                patch("pa.modules.fleet.require_user", return_value=object()),
                patch("pa.modules.fleet.get_principal_id", return_value="user:local"),
                patch("pa.modules.fleet._peer_agent_json", peer),
                patch(
                    "pa.modules.fleet._peer_dispatch_json",
                    AsyncMock(return_value={"resolvable": True}),
                ) as materialize,
            ):
                result = await start_remote_agent_work(
                    request,
                    "mini-1",
                    RemoteAgentStartBody(card_id=card.id, provider="codex"),
                )
                duplicate = await start_remote_agent_work(
                    request,
                    "mini-1",
                    RemoteAgentStartBody(card_id=card.id, provider="codex"),
                )

            self.assertTrue(result["accepted"])
            self.assertFalse(result["duplicate"])
            self.assertTrue(duplicate["duplicate"])
            self.assertEqual(duplicate["dispatch_id"], result["dispatch_id"])
            self.assertEqual(result["dispatch"]["state"], "queued")
            self.assertEqual(result["dispatch"]["card_id"], "card-1")
            self.assertEqual(peer.await_count, 0)
            self.assertEqual(materialize.await_count, 0)
            store.project_working_directory.assert_not_called()
            store.update_card.assert_not_called()
            store.add_knowledge.assert_not_called()

    async def test_remote_agent_start_omits_cwd_when_checkout_ambiguous(
        self,
    ) -> None:
        from pa.modules.fleet import RemoteAgentStartBody, start_remote_agent_work

        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(
                data_dir=Path(tmp),
                instance_id="controller-1",
                instance_name="controller",
                instance_url="http://controller:8080",
                primary_realm="default",
                sync_token="secret",
            )
            fleet = FleetRegistry(settings.data_dir, settings.fleet_id)
            fleet.upsert_instance(
                FleetInstance(
                    instance_id="mini-1",
                    name="macmini",
                    url="http://mini:8080",
                )
            )
            project = Project(
                id="project-1",
                title="PA Core",
                repos=[
                    ProjectRepo(
                        url="https://github.com/petersky/pa.git",
                        path="/Users/petersky/repos/petersky/pa",
                    )
                ],
                tool_config={"development_instance": "macmini"},
            )
            store = MagicMock()
            store.get_project.return_value = project
            store.project_working_directory.return_value = None
            ctx = MagicMock(settings=settings, store=store)
            ctx.services = {"fleet_registry": fleet}
            ctx.require_service.side_effect = lambda name: ctx.services[name]
            ctx.register_service.side_effect = lambda name, value: (
                ctx.services.__setitem__(name, value)
            )
            request = MagicMock()
            request.app.state.ctx = ctx
            request.headers = {"idempotency-key": "ambiguous-cwd"}
            peer = AsyncMock(
                return_value={"session": {"id": "remote-session", "title": "Remote"}}
            )

            with (
                patch("pa.modules.fleet.require_user", return_value=object()),
                patch("pa.modules.fleet.get_principal_id", return_value="user:local"),
                patch("pa.modules.fleet._peer_agent_json", peer),
            ):
                await start_remote_agent_work(
                    request,
                    "mini-1",
                    RemoteAgentStartBody(
                        project_id=project.id,
                        title="Remote smoke",
                    ),
                )

            self.assertEqual(peer.await_count, 0)
            record = next(iter(ctx.services["dispatch_store"].list()))
            self.assertEqual(record.project_id, project.id)
            store.project_working_directory.assert_not_called()

    async def test_remote_agent_start_uses_repo_paths_by_instance_fallback(
        self,
    ) -> None:
        from pa.modules.fleet import RemoteAgentStartBody, start_remote_agent_work

        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(
                data_dir=Path(tmp),
                instance_id="controller-1",
                instance_name="controller",
                instance_url="http://controller:8080",
                primary_realm="default",
                sync_token="secret",
            )
            fleet = FleetRegistry(settings.data_dir, settings.fleet_id)
            fleet.upsert_instance(
                FleetInstance(
                    instance_id="mini-1",
                    name="macmini",
                    url="http://mini:8080",
                )
            )
            project = Project(
                id="project-1",
                title="PA Core",
                repos=[
                    ProjectRepo(
                        url="https://github.com/petersky/pa.git",
                        path="/Users/petersky/repos/petersky/pa",
                    )
                ],
                tool_config={
                    "development_instance": "macmini",
                    "repo_paths_by_instance": {"mini-1": "/srv/pa/remote"},
                },
            )
            store = MagicMock()
            store.get_project.return_value = project
            store.project_working_directory.return_value = None
            ctx = MagicMock(settings=settings, store=store)
            ctx.services = {"fleet_registry": fleet}
            ctx.require_service.side_effect = lambda name: ctx.services[name]
            ctx.register_service.side_effect = lambda name, value: (
                ctx.services.__setitem__(name, value)
            )
            request = MagicMock()
            request.app.state.ctx = ctx
            request.headers = {"idempotency-key": "mapped-cwd"}
            peer = AsyncMock(
                return_value={"session": {"id": "remote-session", "title": "Remote"}}
            )

            with (
                patch("pa.modules.fleet.require_user", return_value=object()),
                patch("pa.modules.fleet.get_principal_id", return_value="user:local"),
                patch("pa.modules.fleet._peer_agent_json", peer),
            ):
                await start_remote_agent_work(
                    request,
                    "mini-1",
                    RemoteAgentStartBody(
                        project_id=project.id,
                        title="Remote smoke",
                    ),
                )

            self.assertEqual(peer.await_count, 0)
            record = next(iter(ctx.services["dispatch_store"].list()))
            self.assertEqual(record.project_id, project.id)
            store.project_working_directory.assert_not_called()

    async def test_dispatch_failure_preserves_allocated_session_for_retry(self) -> None:
        from fastapi import HTTPException

        from pa.modules.fleet import RemoteAgentStartBody, start_remote_agent_work

        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(
                data_dir=Path(tmp),
                instance_id="controller-1",
                instance_name="controller",
                instance_url="http://controller:8080",
                sync_token="secret",
            )
            fleet = FleetRegistry(settings.data_dir, settings.fleet_id)
            fleet.upsert_instance(
                FleetInstance(
                    instance_id="mini-1",
                    name="macmini",
                    url="http://mini:8080",
                )
            )
            store = MagicMock()
            ctx = MagicMock(settings=settings, store=store)
            ctx.services = {"fleet_registry": fleet}
            ctx.require_service.side_effect = lambda name: ctx.services[name]
            ctx.register_service.side_effect = lambda name, value: (
                ctx.services.__setitem__(name, value)
            )
            request = MagicMock()
            request.app.state.ctx = ctx
            request.headers = {"idempotency-key": "prompt-failure"}
            peer = AsyncMock(
                side_effect=[
                    {"session": {"id": "remote-session", "title": "Remote smoke"}},
                    HTTPException(status_code=503, detail="provider unavailable"),
                ]
            )

            with (
                patch("pa.modules.fleet.require_user", return_value=object()),
                patch("pa.modules.fleet.get_principal_id", return_value="user:local"),
                patch("pa.modules.fleet._peer_agent_json", peer),
            ):
                result = await start_remote_agent_work(
                    request,
                    "mini-1",
                    RemoteAgentStartBody(title="Remote smoke", message="Start work"),
                )

            record = ctx.services["dispatch_store"].get(result["dispatch_id"])
            app = MagicMock()
            app.state.ctx = ctx
            from pa.execution.dispatch import DispatchWorker
            from pa.modules.fleet import _process_remote_dispatch

            worker = DispatchWorker(
                ctx.services["dispatch_store"],
                lambda item: _process_remote_dispatch(app, item),
            )
            with (
                patch(
                    "pa.modules.fleet._peer_dispatch_json",
                    AsyncMock(return_value={"resolvable": True}),
                ),
                patch("pa.modules.fleet._peer_agent_json", peer),
            ):
                await worker._execute(record)

            self.assertEqual(record.session_id, "remote-session")
            self.assertEqual(record.state, "failed")
            self.assertIn("provider unavailable", record.last_error)
            store.add_knowledge.assert_not_called()

    async def test_card_dispatch_does_not_reuse_another_hosts_repo_path(self) -> None:
        from pa.modules.fleet import _project_working_directory

        project = Project(
            id="project-1",
            title="PA Core",
            repos=[
                ProjectRepo(
                    url="https://github.com/petersky/pa.git",
                    path="/Users/petersky/repos/petersky/pa",
                )
            ],
            tool_config={"development_instance": "macmini"},
        )

        self.assertIsNone(
            _project_working_directory(
                project,
                instance_id="linux-1",
                instance_name="monica",
            )
        )

    async def test_card_dispatch_prefers_instance_repo_path_mapping(self) -> None:
        from pa.modules.fleet import _project_working_directory

        project = Project(
            id="project-1",
            title="PA Core",
            repos=[
                ProjectRepo(
                    url="https://github.com/petersky/pa.git",
                    path="/Users/petersky/repos/petersky/pa",
                )
            ],
            tool_config={
                "development_instance": "macmini",
                "repo_paths_by_instance": {"monica": "/srv/pa"},
            },
        )

        self.assertEqual(
            _project_working_directory(
                project,
                instance_id="linux-1",
                instance_name="monica",
            ),
            "/srv/pa",
        )

    async def test_agent_proxy_rejects_path_traversal(self) -> None:
        from pa.modules.fleet import _agent_path

        with self.assertRaises(Exception):
            _agent_path("sessions/../config")

    async def test_agent_proxy_relays_query_json_and_fleet_auth(self) -> None:
        from pa.modules.fleet import fleet_agent_proxy

        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), sync_token="fleet-secret")
            fleet = FleetRegistry(settings.data_dir, settings.fleet_id)
            fleet.upsert_instance(
                FleetInstance(
                    instance_id="mini-1",
                    name="macmini",
                    url="http://mini:8080",
                )
            )
            ctx = MagicMock(settings=settings)
            ctx.require_service.return_value = fleet
            request = MagicMock()
            request.app.state.ctx = ctx
            request.method = "GET"
            request.query_params.multi_items.return_value = [("card_id", "card-1")]
            request.headers.get.side_effect = lambda name: {
                "accept": "application/json"
            }.get(name)
            request.body = AsyncMock(return_value=b"")
            seen = {}

            async def upstream_handler(
                upstream_request: httpx.Request,
            ) -> httpx.Response:
                seen["url"] = str(upstream_request.url)
                seen["authorization"] = upstream_request.headers.get("authorization")
                return httpx.Response(
                    200,
                    json=[{"id": "remote-session", "title": "Remote work"}],
                )

            upstream_client = httpx.AsyncClient(
                transport=httpx.MockTransport(upstream_handler)
            )
            with (
                patch("pa.modules.fleet.require_user", return_value=object()),
                patch(
                    "pa.modules.fleet.httpx.AsyncClient",
                    return_value=upstream_client,
                ) as client_factory,
            ):
                response = await fleet_agent_proxy(
                    request,
                    "mini-1",
                    "sessions",
                )

            self.assertEqual(response.status_code, 200)
            self.assertEqual(
                json.loads(response.body),
                [{"id": "remote-session", "title": "Remote work"}],
            )
            self.assertEqual(
                seen["url"],
                "http://mini:8080/api/agent/sessions?card_id=card-1",
            )
            self.assertEqual(seen["authorization"], "Bearer fleet-secret")
            self.assertEqual(client_factory.call_args.kwargs["timeout"].read, 120.0)

    async def test_agent_proxy_disables_read_timeout_only_for_session_events(
        self,
    ) -> None:
        from pa.modules.fleet import fleet_agent_proxy

        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), sync_token="fleet-secret")
            fleet = FleetRegistry(settings.data_dir, settings.fleet_id)
            fleet.upsert_instance(
                FleetInstance(
                    instance_id="mini-1",
                    name="macmini",
                    url="http://mini:8080",
                )
            )
            ctx = MagicMock(settings=settings)
            ctx.require_service.return_value = fleet
            request = MagicMock()
            request.app.state.ctx = ctx
            request.method = "GET"
            request.query_params.multi_items.return_value = []
            request.headers.get.side_effect = lambda name: {
                "accept": "text/event-stream"
            }.get(name)
            request.body = AsyncMock(return_value=b"")

            class EventStream(httpx.AsyncByteStream):
                async def __aiter__(self):
                    yield b"event: ready\ndata: {}\n\n"

            async def upstream_handler(_request: httpx.Request) -> httpx.Response:
                return httpx.Response(
                    200,
                    stream=EventStream(),
                    headers={"content-type": "text/event-stream"},
                )

            upstream_client = httpx.AsyncClient(
                transport=httpx.MockTransport(upstream_handler)
            )
            with (
                patch("pa.modules.fleet.require_user", return_value=object()),
                patch(
                    "pa.modules.fleet.httpx.AsyncClient",
                    return_value=upstream_client,
                ) as client_factory,
            ):
                response = await fleet_agent_proxy(
                    request,
                    "mini-1",
                    "sessions/remote-session/events",
                )
                body = b"".join([chunk async for chunk in response.body_iterator])

            self.assertIn(b"event: ready", body)
            self.assertIsNone(client_factory.call_args.kwargs["timeout"].read)

    async def test_agent_proxy_treats_peer_restart_as_event_stream_eof(self) -> None:
        from pa.modules.fleet import fleet_agent_proxy

        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), sync_token="fleet-secret")
            fleet = FleetRegistry(settings.data_dir, settings.fleet_id)
            fleet.upsert_instance(
                FleetInstance(
                    instance_id="mini-1",
                    name="macmini",
                    url="http://mini:8080",
                )
            )
            ctx = MagicMock(settings=settings)
            ctx.require_service.return_value = fleet
            request = MagicMock()
            request.app.state.ctx = ctx
            request.method = "GET"
            request.query_params.multi_items.return_value = []
            request.headers.get.side_effect = lambda name: {
                "accept": "text/event-stream"
            }.get(name)
            request.body = AsyncMock(return_value=b"")

            class InterruptedEventStream(httpx.AsyncByteStream):
                async def __aiter__(self):
                    yield b"event: ready\ndata: {}\n\n"
                    raise httpx.RemoteProtocolError("incomplete chunked read")

            async def upstream_handler(_request: httpx.Request) -> httpx.Response:
                return httpx.Response(
                    200,
                    stream=InterruptedEventStream(),
                    headers={"content-type": "text/event-stream"},
                )

            upstream_client = httpx.AsyncClient(
                transport=httpx.MockTransport(upstream_handler)
            )
            with (
                patch("pa.modules.fleet.require_user", return_value=object()),
                patch(
                    "pa.modules.fleet.httpx.AsyncClient",
                    return_value=upstream_client,
                ),
            ):
                response = await fleet_agent_proxy(
                    request,
                    "mini-1",
                    "sessions/remote-session/events",
                )
                body = b"".join([chunk async for chunk in response.body_iterator])

            self.assertIn(b"event: ready", body)


if __name__ == "__main__":
    unittest.main()

"""Tests for fleet registry, join wiring, and remote install helpers."""

from __future__ import annotations

import json
import asyncio
import os
import tempfile
import threading
import time
import unittest
import warnings
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
from fastapi import FastAPI, HTTPException

from pa.config import Settings
from pa.domain.instance_config import load_instance_config, update_instance_config
from pa.domain.models import (
    Card, CardLane, FleetInstance, Project, ProjectRepo, ProjectRepository, Repository
)
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
from pa.modules.fleet import (
    RemoteAgentStartBody,
    _apply_dispatch_mode_default,
    _proxy_agent_providers,
    fleet_agent_provider_login_start,
)


class FleetRegistryReloadTests(unittest.TestCase):
    def test_codex_and_cortex_dispatches_default_to_full_access(self) -> None:
        for provider in ("codex", "cortex"):
            with self.subTest(provider=provider):
                body = RemoteAgentStartBody(provider=provider)
                _apply_dispatch_mode_default(body)
                self.assertEqual(body.mode_id, "agent-full-access")

        explicit = RemoteAgentStartBody(provider="codex", mode_id="agent")
        _apply_dispatch_mode_default(explicit)
        self.assertEqual(explicit.mode_id, "agent")

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

    def test_instance_reload_skips_unchanged_mtime(self) -> None:
        registry = FleetRegistry(self.data_dir, "fleet-a")
        registry.upsert_instance(
            FleetInstance(instance_id="peer-1", name="peer", url="http://peer:8080")
        )
        with patch("pa.fleet.registry.json.loads", wraps=json.loads) as loads:
            first = registry.list_instances()
            second = registry.list_instances()
            loads.assert_not_called()
        self.assertEqual([item.instance_id for item in first], ["peer-1"])
        self.assertEqual([item.instance_id for item in second], ["peer-1"])
        info = registry.instances_path.stat()
        os.utime(
            registry.instances_path,
            ns=(info.st_atime_ns, info.st_mtime_ns + 1_000_000),
        )
        with patch("pa.fleet.registry.json.loads", wraps=json.loads) as loads:
            registry.list_instances()
            loads.assert_called()

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
        self.assertIn("/providers/catalog", source)
        self.assertIn('return "/api/agent/providers/codex/login-jobs"', source)
        self.assertIn('data-codex-login-resume="', source)
        self.assertIn("Use any browser to finish signing in", source)
        self.assertIn(
            'setTimeout(function () { loadLiveStatus(true, instanceId || undefined); }, 1000)',
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
        self.assertIn("fleet join", cmd)
        self.assertIn("tok123", cmd)
        self.assertNotIn("password", cmd.lower())

    def test_join_only_command_uses_preflight_executable(self) -> None:
        req = RemoteInstallRequest(
            host="mini",
            user="peter",
            instance_name="mini",
            instance_url="http://mini:8080",
            join_only=True,
            pa_executable="/home/peter/.local/bin/pa",
        )
        cmd = build_remote_command(self.settings, req, fleet_token="tok123")
        self.assertIn("PA_BIN=/home/peter/.local/bin/pa", cmd)
        self.assertIn('"$PA_BIN" fleet join', cmd)
        self.assertNotIn("command -v pa", cmd)

    def test_legacy_join_only_checks_the_uv_tool_bin_directory(self) -> None:
        req = RemoteInstallRequest(
            host="mini",
            user="peter",
            instance_name="mini",
            instance_url="http://mini:8080",
            join_only=True,
        )
        cmd = build_remote_command(self.settings, req, fleet_token="tok123")
        self.assertIn('$HOME/.local/bin/pa', cmd)
        self.assertIn('[ ! -x "$PA_BIN" ]', cmd)

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
        from pa.fleet.overview import build_overview as actual_build_overview
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

        def overview_with_prompt_backlog(ctx, instances, routes):
            overview = actual_build_overview(ctx, instances, routes)
            activity = overview["nodes"][0]["dimensions"]["activity"]
            value = dict(activity.get("value") or {})
            value.update(
                {
                    "state": "working",
                    "queued_prompts": 9,
                    "capacity": {
                        "consumed": 1,
                        "limit": 4,
                        "source": "configured",
                    },
                }
            )
            activity.update({"state": "fresh", "value": value})
            return overview

        try:
            app = Kernel.boot(settings=settings).build_app()
            with TestClient(app) as client:
                with patch(
                    "pa.modules.fleet.build_overview",
                    side_effect=overview_with_prompt_backlog,
                ):
                    page = client.get("/fleet")
                self.assertEqual(page.status_code, 200, page.text)
                self.assertIn("pa-fleet-overview-data", page.text)
                self.assertIn("pa-fleet-topology", page.text)
                self.assertNotIn("Checking…", page.text)

                self.assertIn(
                    'aria-label="1/4 slots used · 9 prompts queued"', page.text
                )
                self.assertIn(">1/4 slots used · 9 prompts queued</strong>", page.text)
                self.assertNotIn(">10/4", page.text)
                self.assertNotIn(">1/4 used</strong>", page.text)

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
                self.assertIn("OpenSSH target", page.text)
                self.assertIn("Create plan &amp; start", page.text)
                self.assertIn("pa-bootstrap-required-input", page.text)
        finally:
            reset_instance_agent()
            reset_store()

    def test_bootstrap_api_creates_idempotent_secret_free_plan(self) -> None:
        from fastapi.testclient import TestClient

        from pa.core.kernel import Kernel
        from pa.domain.store import reset_store
        from pa.fleet.bootstrap import TargetDiscovery
        from pa.auth.users import UserDirectory
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
        discovery = TargetDiscovery(
            target="peter@mini",
            host="mini",
            user="peter",
            port=22,
            host_key_fingerprint="SHA256:trusted",
            host_key_algorithm="ssh-ed25519",
            host_key_state="unknown",
        )
        body = {
            "idempotency_key": "setup-mini-api-1",
            "request": {
                "target": "peter@mini",
                "instance_name": "mini",
                "instance_url": "http://mini:8080",
                "host_key_policy": "pinned",
                "host_key_fingerprint": "SHA256:trusted",
                "password": "never-persist-this-password",
            },
        }
        headers = {
            "Authorization": (
                f"Bearer "
                f"{UserDirectory(self.data_dir).ensure_default_user().cli_token}"
            )
        }
        try:
            app = Kernel.boot(settings=settings).build_app()
            with (
                patch(
                    "pa.modules.fleet.discover_target",
                    AsyncMock(return_value=discovery),
                ),
                TestClient(app) as client,
            ):
                created = client.post(
                    "/api/fleet/bootstrap-jobs", json=body, headers=headers
                )
                self.assertEqual(created.status_code, 201, created.text)
                payload = created.json()
                self.assertFalse(payload["duplicate"])
                self.assertEqual(payload["state"], "planned")
                self.assertEqual(len(payload["phases"]), 13)
                self.assertNotIn(
                    "never-persist-this-password", json.dumps(payload)
                )

                duplicate = client.post(
                    "/api/fleet/bootstrap-jobs", json=body, headers=headers
                )
                self.assertEqual(duplicate.status_code, 201, duplicate.text)
                self.assertTrue(duplicate.json()["duplicate"])
                self.assertEqual(
                    duplicate.json()["job_id"], payload["job_id"]
                )

                listed = client.get("/api/fleet/bootstrap-jobs/incomplete")
                self.assertEqual(listed.status_code, 200, listed.text)
                self.assertEqual(
                    [item["job_id"] for item in listed.json()],
                    [payload["job_id"]],
                )
        finally:
            reset_instance_agent()
            reset_store()

    def test_fleet_page_has_one_semantic_control_for_duplicate_pr_watches(
        self,
    ) -> None:
        from fastapi.testclient import TestClient

        from pa.core.kernel import Kernel
        from pa.domain.store import reset_store
        from pa.instance.agent_session import reset_instance_agent
        from pa.pr_supervisor.models import PRWatch

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
                supervisor = app.state.ctx.require_service("pr_supervisor_store")
                watches = [
                    PRWatch(
                        id=watch_id,
                        repository=repository,
                        pr_number=65,
                        pr_url=f"https://github.com/{repository}/pull/65",
                        owner_instance_id="local-1",
                        originating_instance_id="local-1",
                    )
                    for watch_id, repository in (
                        ("watch-upper", "petersky/PA"),
                        ("watch-lower-a", "petersky/pa"),
                        ("watch-lower-b", "petersky/pa"),
                    )
                ]
                with patch.object(
                    supervisor, "list_watches", return_value=watches
                ):
                    page = client.get("/fleet")
                self.assertEqual(page.status_code, 200, page.text)
                route_list = page.text.split(
                    '<ul id="pa-fleet-edge-list">', maxsplit=1
                )[1].split("</ul>", maxsplit=1)[0]
                self.assertEqual(route_list.count("data-fleet-edge="), 1)
                self.assertIn("PR petersky/pa#65 · 3 watches", route_list)
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
                    "pa.acp.providers.resolve.list_provider_summaries_bounded",
                    new=AsyncMock(return_value=[]),
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
                    probe_dimension(ctx, inst, "reachability")
                )
                second = asyncio.create_task(
                    probe_dimension(ctx, inst, "reachability")
                )
                await started.wait()
                await asyncio.sleep(0)
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

            self.assertEqual(timed_out["state"], "stale")
            self.assertEqual(timed_out["last_attempt_state"], "timeout")
            self.assertEqual(timed_out["value"], {"health": "up"})
            self.assertEqual(
                timed_out["last_successful_at"], "2026-07-22T12:00:00+00:00"
            )
            persisted = cache_for(settings.data_dir).get("remote", "reachability")
            self.assertEqual(persisted["value"], {"health": "up"})

    async def test_manual_refresh_joins_older_background_probe(self) -> None:
        from pa.fleet.overview import field, probe_dimension

        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="local")
            ctx = MagicMock(settings=settings)
            inst = FleetInstance(
                instance_id="remote",
                name="remote",
                url="http://remote:8080",
            )
            older_started = asyncio.Event()
            release_older = asyncio.Event()
            calls = 0

            async def racing_probe(*_args):
                nonlocal calls
                calls += 1
                if calls == 1:
                    older_started.set()
                    await release_older.wait()
                    return field(
                        "timeout", None, duration_ms=2500, error="older deadline"
                    )
                return field(
                    "fresh",
                    {"health": "up"},
                    observed_at="2026-07-25T12:00:01+00:00",
                    duration_ms=10,
                )

            with patch("pa.fleet.overview._probe", side_effect=racing_probe):
                older = asyncio.create_task(
                    probe_dimension(ctx, inst, "reachability", force=True)
                )
                await older_started.wait()
                newer_task = asyncio.create_task(
                    probe_dimension(ctx, inst, "reachability", force=True)
                )
                await asyncio.sleep(0)
                release_older.set()
                older_result, newer = await asyncio.gather(older, newer_task)

            self.assertEqual(calls, 1)
            self.assertEqual(newer["last_attempt_state"], "timeout")
            self.assertEqual(older_result, newer)

    async def test_older_timeout_cannot_replace_newer_success(self) -> None:
        from pa.fleet.overview import FleetOverviewCache, field

        with tempfile.TemporaryDirectory() as tmp:
            cache = FleetOverviewCache(Path(tmp))
            successful = field(
                "fresh",
                {"health": "up"},
                observed_at="2026-07-25T12:00:01+00:00",
                duration_ms=8,
            )
            timed_out = field(
                "timeout", None, duration_ms=2500, error="deadline"
            )

            self.assertTrue(
                cache.put("local", "reachability", successful, attempt_id=20)
            )
            self.assertFalse(
                cache.put("local", "reachability", timed_out, attempt_id=10)
            )
            current = cache.get("local", "reachability")
            self.assertEqual(current["state"], "fresh")
            self.assertEqual(current["value"], {"health": "up"})
            self.assertEqual(current["attempt_id"], 20)

    def test_provider_probe_failure_retains_affirmative_auth_only(self) -> None:
        from pa.fleet.overview import _merge_provider_snapshots

        previous = [{
            "id": "codex",
            "auth_state": "authenticated",
            "auth_configured": True,
            "auth_method": "chatgpt_oauth",
            "auth_status": "Signed in with ChatGPT on the target.",
            "auth_evidence": ["codex_cli_status"],
            "last_attempted_at": "2026-07-25T12:00:00+00:00",
        }]
        inconclusive = [{
            "id": "codex",
            "auth_state": "timed_out",
            "auth_method": "unknown",
            "auth_error": "codex login status timed out",
            "last_attempted_at": "2026-07-25T12:01:00+00:00",
        }]
        merged = _merge_provider_snapshots(previous, inconclusive)[0]
        self.assertEqual(merged["auth_state"], "authenticated")
        self.assertEqual(merged["auth_method"], "chatgpt_oauth")
        self.assertEqual(merged["last_attempt"]["state"], "timed_out")
        self.assertTrue(merged["stale"])

        signed_out = _merge_provider_snapshots(
            previous,
            [{"id": "codex", "auth_state": "signed_out", "auth_method": "none"}],
        )[0]
        self.assertEqual(signed_out["auth_state"], "signed_out")

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

    def test_topology_groups_case_variant_prs_and_keeps_stable_watch_ids(self) -> None:
        from pa.fleet.overview import build_overview
        from pa.pr_supervisor.models import PRWatch, PRWatchStatus

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
            supervisor_store = MagicMock()
            ctx.services = {"pr_supervisor_store": supervisor_store}
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
            watches = [
                PRWatch(
                    id="watch-upper",
                    repository="petersky/PA",
                    pr_number=65,
                    pr_url="https://github.com/petersky/PA/pull/65",
                    owner_instance_id="remote",
                    originating_instance_id="local",
                ),
                PRWatch(
                    id="watch-lower-a",
                    repository="petersky/pa",
                    pr_number=65,
                    pr_url="https://github.com/petersky/pa/pull/65",
                    owner_instance_id="remote",
                    originating_instance_id="local",
                ),
                PRWatch(
                    id="watch-lower-b",
                    repository="petersky/pa",
                    pr_number=65,
                    pr_url="https://github.com/petersky/pa/pull/65",
                    owner_instance_id="remote",
                    originating_instance_id="local",
                    status=PRWatchStatus.BLOCKED,
                    last_error="required check failed",
                ),
            ]
            supervisor_store.list_watches.return_value = watches

            first = build_overview(ctx, instances, [])
            supervisor_store.list_watches.return_value = list(reversed(watches))
            refreshed = build_overview(ctx, instances, [])

            edge = next(
                item for item in first["edges"] if item["kind"] == "supervisor"
            )
            refreshed_edge = next(
                item for item in refreshed["edges"] if item["kind"] == "supervisor"
            )
            self.assertEqual(edge["id"], refreshed_edge["id"])
            self.assertEqual(edge["count"], 3)
            self.assertEqual(edge["distinct_count"], 1)
            self.assertEqual(edge["status"], "degraded")
            self.assertEqual(edge["status_counts"], {"degraded": 1, "healthy": 2})
            self.assertEqual(edge["label"], "PR petersky/pa#65 · 3 watches")
            self.assertEqual(
                edge["details"]["pull_requests"],
                [
                    {
                        "id": "petersky/pa#65",
                        "repository": "petersky/pa",
                        "pr_number": 65,
                        "count": 3,
                        "status": "degraded",
                        "watch_ids": [
                            "watch-watch-lower-a",
                            "watch-watch-lower-b",
                            "watch-watch-upper",
                        ],
                    }
                ],
            )
            self.assertEqual(
                {item["id"] for item in edge["details"]["items"]},
                {
                    "watch-watch-upper",
                    "watch-watch-lower-a",
                    "watch-watch-lower-b",
                },
            )
            self.assertEqual(edge["details"]["items"], refreshed_edge["details"]["items"])

    def test_topology_ignores_85_legacy_merged_watches_with_stale_errors(
        self,
    ) -> None:
        from pa.fleet.overview import build_overview
        from pa.pr_supervisor.models import PRWatch, PRWatchStatus
        from pa.pr_supervisor.store import PRSupervisorStore

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
            supervisor_store = PRSupervisorStore(Path(tmp) / "supervisor.db")
            ctx.services = {"pr_supervisor_store": supervisor_store}
            supervisor_store.upsert_watch(
                PRWatch(
                    id="active-healthy",
                    repository="petersky/pa",
                    pr_number=200,
                    pr_url="https://github.com/petersky/pa/pull/200",
                    owner_instance_id="local",
                    originating_instance_id="local",
                ),
                preserve_lease=False,
            )
            for index in range(85):
                supervisor_store.upsert_watch(
                    PRWatch(
                        id=f"legacy-merged-{index}",
                        repository="petersky/pa",
                        pr_number=index + 1,
                        pr_url=(
                            "https://github.com/petersky/pa/pull/"
                            f"{index + 1}"
                        ),
                        status=PRWatchStatus.MERGED,
                        state={"supervisor_state": "retired_after_merge"},
                        last_error=(
                            f"historical error {index}" if index < 10 else None
                        ),
                        retired_at=None,
                    ),
                    preserve_lease=False,
                )

            overview = build_overview(
                ctx,
                [
                    FleetInstance(
                        instance_id="local",
                        name="owner",
                        url="http://owner:8080",
                    )
                ],
                [],
            )

            self.assertEqual(
                len(supervisor_store.list_watches(include_retired=True)), 86
            )
            self.assertEqual(
                [watch.id for watch in supervisor_store.list_watches()],
                ["active-healthy"],
            )
            supervisor_edges = [
                edge for edge in overview["edges"] if edge["kind"] == "supervisor"
            ]
            self.assertEqual(len(supervisor_edges), 1)
            self.assertEqual(supervisor_edges[0]["count"], 1)
            self.assertEqual(supervisor_edges[0]["distinct_count"], 1)
            self.assertEqual(supervisor_edges[0]["status"], "healthy")
            self.assertEqual(
                supervisor_edges[0]["details"]["items"][0]["id"],
                "watch-active-healthy",
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
            bounded_sessions = [
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
            ctx.store.list_sessions_for_workshop.return_value = (
                bounded_sessions,
                len(bounded_sessions),
            )

            activity = local_dimension(ctx, "activity")

            self.assertEqual(activity["state"], "working")
            self.assertEqual(activity["active_sessions"], 2)
            self.assertEqual(activity["session_total"], 2)
            self.assertEqual(activity["session_omitted"], 0)
            ctx.store.list_sessions.assert_not_called()
            self.assertEqual(activity["queued_prompts"], 1)
            self.assertEqual(
                {session["card_id"] for session in activity["sessions"]},
                {"card-1", "card-2"},
            )

    def test_local_activity_counts_one_backlogged_session_as_one_consumer(self) -> None:
        from pa.domain.models import AgentSession
        from pa.fleet.overview import local_dimension

        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="local")
            ctx = MagicMock(settings=settings)
            session = AgentSession(
                id="session-backlog",
                agent_name="codex",
                card_id="card-backlog",
                status="working",
                title="Backlogged session",
            )
            runtime = SimpleNamespace(
                session=session,
                session_id=session.id,
                connected=True,
                prompting=True,
                _closed=False,
                _queue=[object() for _ in range(9)],
            )
            manager = MagicMock()
            manager.progress.return_value = SimpleNamespace(
                model_dump=lambda mode: {
                    "phase": "prompting",
                    "active_sessions": 1,
                    "connected_runtimes": 1,
                    "idle_sessions": 0,
                    "prompting_turns": 1,
                    "active_capacity_consumers": 1,
                    "queued_prompts": 9,
                    "provider_concurrency": {
                        "codex": {
                            "connected_runtimes": 1,
                            "idle_sessions": 0,
                            "prompting_turns": 1,
                            "active_capacity_consumers": 1,
                            "queued_prompts": 9,
                        }
                    },
                    "quiescing": False,
                    "prompting": True,
                    "message": "1 ACP session working, 9 prompts queued",
                }
            )
            manager.list_runtimes.return_value = [runtime]
            ctx.services = {"instance_agent": manager}
            ctx.store.list_sessions_for_workshop.return_value = ([session], 1)

            activity = local_dimension(ctx, "activity")

            self.assertEqual(activity["active_capacity_consumers"], 1)
            self.assertEqual(activity["queued_prompts"], 9)
            self.assertEqual(activity["capacity"]["consumed"], 1)
            self.assertEqual(activity["sessions"][0]["queued"], 9)
            self.assertTrue(activity["sessions"][0]["capacity_consuming"])
            self.assertEqual(
                activity["capacity_consumer_links"],
                [
                    {
                        "kind": "session",
                        "session_id": "session-backlog",
                        "href": "/agent?session=session-backlog",
                        "state": "working",
                        "slots": 1,
                        "consumer_id": "session:session-backlog",
                    }
                ],
            )
            self.assertNotIn("queued_prompts", activity["capacity_policy"]["consumes"])
            self.assertIn(
                "queued_prompts", activity["capacity_policy"]["does_not_consume"]
            )

    def test_local_activity_ignores_stale_running_dispatch_without_live_turn(
        self,
    ) -> None:
        from pa.fleet.overview import local_dimension

        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="local")
            ctx = MagicMock(settings=settings)
            stale = MagicMock(
                state="running",
                target_instance_id="local",
                authority_instance_id="local",
            )
            stale.public_dict.return_value = {
                "dispatch_id": "dispatch-stale",
                "card_id": "card-done",
                "session_id": "session-gone",
                "target_instance_id": "local",
                "authority_instance_id": "local",
                "state": "running",
                "progress": {
                    "latest": {
                        "phase": "turn_ended",
                        "summary": "Historical turn output.",
                    },
                    "freshness": {"last_activity_at": "2026-08-04T00:00:00Z"},
                },
                "dispatch_completion": {"completed": False},
                "card_reconciliation": {"state": "not_requested"},
            }
            dispatch_store = MagicMock()
            dispatch_store.list.return_value = [stale]
            ctx.services = {"dispatch_store": dispatch_store}
            ctx.store.list_sessions_for_workshop.return_value = ([], 0)

            activity = local_dimension(ctx, "activity")

            self.assertEqual(activity["state"], "idle")
            self.assertIsNone(activity["current_dispatch"])
            self.assertEqual(activity["capacity"]["consumed"], 0)
            self.assertEqual(activity["capacity_consumer_links"], [])

    def test_overview_refresh_defaults_off_under_pytest(self) -> None:
        from pa.fleet.overview import overview_refresh_enabled

        self.assertIn("PYTEST_CURRENT_TEST", os.environ)
        self.assertFalse(overview_refresh_enabled())
        with patch.dict(os.environ, {"PA_FLEET_OVERVIEW_REFRESH": "1"}):
            self.assertTrue(overview_refresh_enabled())
        with patch.dict(os.environ, {"PA_FLEET_OVERVIEW_REFRESH": "0"}, clear=False):
            self.assertFalse(overview_refresh_enabled())

    def test_required_readiness_ignores_stale_inconsistent_sync(self) -> None:
        from pa.fleet.overview import field, required_readiness

        stale_mismatch = {
            "reachability": field(
                "stale",
                {"health": "up"},
                observed_at="2026-08-16T16:00:00+00:00",
            ),
            "status": field(
                "timeout", None, duration_ms=4000, error="status exceeded 4s deadline"
            ),
            "sync": {
                **field(
                    "timeout",
                    None,
                    duration_ms=4000,
                    error="sync exceeded 4s deadline",
                ),
                "state": "stale",
                "last_attempt_state": "timeout",
                "value": {
                    "consistent": False,
                    "head": "old",
                    "projection_head": "older",
                },
            },
        }
        stale_mismatch["reachability"]["last_attempt_state"] = "fresh"
        self.assertEqual(required_readiness(stale_mismatch), "timeout")

        live_mismatch = {
            "reachability": field("fresh", {"health": "up"}),
            "status": field("fresh", {"version": "1.0.11"}),
            "sync": field(
                "fresh",
                {"consistent": False, "head": "a", "projection_head": "b"},
            ),
        }
        self.assertEqual(required_readiness(live_mismatch), "error")

    def test_background_refresh_skips_recent_failures_and_probes_the_rest(
        self,
    ) -> None:
        from pa.fleet.overview import (
            field,
            should_skip_background_probe,
        )

        recent = field(
            "timeout", None, duration_ms=4000, error="deadline"
        )
        self.assertTrue(should_skip_background_probe(recent))
        old = field(
            "timeout",
            None,
            duration_ms=4000,
            error="deadline",
            attempted_at="2026-08-16T00:00:00+00:00",
        )
        self.assertFalse(should_skip_background_probe(old))

    async def test_refresh_required_dimensions_probes_active_instances(self) -> None:
        from pa.fleet.overview import refresh_required_dimensions

        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="local")
            ctx = MagicMock(settings=settings)
            active = FleetInstance(
                instance_id="remote",
                name="remote",
                url="http://remote:8080",
                lifecycle_state="active",
            )
            removed = FleetInstance(
                instance_id="gone",
                name="gone",
                url="http://gone:8080",
                lifecycle_state="removed",
            )
            probed = []

            async def fake_probe(_ctx, inst, dimension, *, force=False):
                probed.append((inst.instance_id, dimension, force))
                return {"state": "fresh"}

            with patch(
                "pa.fleet.overview.probe_dimension", side_effect=fake_probe
            ):
                await refresh_required_dimensions(ctx, [active, removed])

            self.assertEqual(
                probed,
                [
                    ("remote", "reachability", False),
                    ("remote", "status", False),
                    ("remote", "sync", False),
                ],
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
        self.assertIn('" slots used · " + promptBacklog', script)
        self.assertIn("escapeHtml(capacityPresentation.summary)", script)
        self.assertIn('return presentation.summary + " · " + presentation.source', script)
        self.assertNotIn(' used</strong>', script)
        self.assertIn('name="install_timeout"', template)

    def test_update_is_modal_and_restores_isolated_persisted_instance_jobs(self) -> None:
        root = Path(__file__).parents[1]
        script = (root / "src/pa/server/static/js/fleet.js").read_text()
        template = (root / "src/pa/server/templates/pages/fleet.html").read_text()
        style = (root / "src/pa/server/static/style.css").read_text()
        self.assertIn('<dialog class="fleet-update-dialog"', template)
        self.assertIn('id="pa-fleet-update-progress"', template)
        self.assertIn("restoreFleetUpdate(fleetUpdateInstanceId)", script)
        self.assertIn("job.instance_id !== fleetUpdateInstanceId", script)
        self.assertIn("generation !== fleetUpdateGeneration", script)
        self.assertIn("closeFleetUpdateWatcher()", script)
        self.assertIn("job.progress_percent", script)
        self.assertIn(".fleet-update-dialog::backdrop", style)

    def test_live_health_is_single_flight_generation_safe_and_terminal(self) -> None:
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
        self.assertIn("function edgeVisualStatus(edge, snapshot)", script)
        self.assertIn("providerLabel", script)
        self.assertIn("seq !== liveStatusSeq", script)
        self.assertIn("patch.generation !== seq", script)
        self.assertIn("function createFleetSnapshot", script)
        self.assertIn("function applyFleetDimensionUpdate", script)
        self.assertIn("awaiting server result", script)
        self.assertIn("Object.assign({}, previous", script)
        self.assertIn("Health check failed", script)
        self.assertIn("performance.measure", script)
        self.assertIn('" · Pending: " + pending.join', script)
        self.assertIn('" · Failed: " + failed.join', script)
        self.assertIn('(node.name || node.id) + " / " + dimension', script)
        self.assertIn('id="pa-fleet-refresh"', template)

    def test_page_refresh_is_single_flight_abort_safe_and_generation_fenced(self) -> None:
        root = Path(__file__).parents[1]
        script = (root / "src/pa/server/static/js/fleet.js").read_text()
        self.assertIn("if (fleetPageRefreshRequest && fleetPageRefreshUrl === url)", script)
        self.assertIn("new AbortController()", script)
        self.assertIn("controller.abort()", script)
        self.assertIn("htmx.swap(target, html", script)
        self.assertIn("isExpectedHtmxAbort(error)", script)
        self.assertIn("X-PA-Navigation-Generation", script)
        self.assertIn("generation !== fleetPageRefreshGeneration", script)
        self.assertIn("evt.detail.shouldSwap = false", script)
        self.assertNotIn("suppressExpectedFleetHtmxError", script)
        self.assertIn("Fleet page refresh failed", script)
        self.assertIn("abortFleetPageRefresh();\n    teardownFleetOverview();", script)

    def test_topology_is_accessible_responsive_and_has_no_js_equivalent(self) -> None:
        root = Path(__file__).parents[1]
        script = (root / "src/pa/server/static/js/fleet.js").read_text()
        template = (root / "src/pa/server/templates/pages/fleet.html").read_text()
        style = (root / "src/pa/server/static/style.css").read_text()
        self.assertIn('aria-label="Fleet instance and activity topology"', template)
        self.assertIn('aria-label="Topology viewport controls"', template)
        self.assertIn('data-fleet-topology-action="zoom-in"', template)
        self.assertIn('data-fleet-topology-action="zoom-out"', template)
        self.assertIn('data-fleet-topology-action="reset"', template)
        self.assertIn('data-fleet-topology-action="fit"', template)
        self.assertIn("<noscript>", template)
        self.assertIn('id="pa-fleet-edge-list"', template)
        self.assertIn('id="pa-fleet-instances"', template)
        self.assertIn('tabindex="0" role="button"', script)
        self.assertIn('event.key !== "Enter" && event.key !== " "', script)
        self.assertIn("function fleetTopologyLayout(nodes, containerWidth)", script)
        self.assertIn('mode = "stacked"', script)
        self.assertIn('mode = "grid"', script)
        self.assertIn("host.getBoundingClientRect().width", script)
        self.assertIn("new ResizeObserver", script)
        self.assertIn("function FleetTopologyController(host)", script)
        self.assertIn("destroyFleetTopologyController", script)
        self.assertIn('document.body.addEventListener("htmx:afterSwap"', script)
        self.assertNotIn('document.body.addEventListener("htmx:after:swap"', script)
        self.assertIn('document.addEventListener("htmx:historyRestore"', script)
        self.assertIn('window.addEventListener("popstate"', script)
        self.assertIn("topologyViewportAfterZoom", script)
        self.assertIn("data-fleet-pan-surface", script)
        self.assertIn("pointercancel", script)
        self.assertIn("lostpointercapture", script)
        self.assertIn("focused.focus({ preventScroll: true })", script)
        self.assertIn('data-fleet-edge-item="', script)
        self.assertIn("selectedFleetItem.edgeId", script)
        self.assertIn("parallelOffset", script)
        self.assertIn("canonicalFrom", script)
        self.assertIn('data-fleet-layer="nodes"', script)
        self.assertIn('data-fleet-layer="edges"', script)
        self.assertIn('data-fleet-layer="labels"', script)
        self.assertIn('data-fleet-layer="interactions"', script)
        self.assertIn("topologyNodeBoundaryPoint", script)
        self.assertIn("fleet-node-halo", script)
        self.assertIn("fleet-edge-halo", script)
        self.assertIn("!current.selection", script)
        self.assertIn('edge.count > 1 ? " ×" + edge.count', script)
        self.assertIn("@media (max-width: 1050px)", style)
        self.assertIn("@media (max-width: 900px)", style)
        self.assertIn("@media (prefers-reduced-motion: reduce)", style)
        self.assertIn(".fleet-edge-stale line", style)
        self.assertIn("vector-effect: non-scaling-stroke", style)
        self.assertIn(".fleet-node.fleet-selected .fleet-node-halo", style)
        self.assertIn(".fleet-edge.fleet-selected .fleet-edge-hit", style)
        self.assertIn("stroke-width: 18", style)
        self.assertIn(".fleet-edge-hit", style)
        self.assertIn(".fleet-selected", style)
        self.assertIn("@media (prefers-reduced-motion: reduce)", style)
        self.assertNotIn("min-width: 44rem", style)
        self.assertLess(
            template.index('id="pa-fleet-edge-list"'),
            template.index('id="pa-fleet-detail"'),
        )


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
            repository = Repository(
                id="repo-1", url="https://github.com/petersky/pa.git", name="PA"
            )
            store.list_project_repositories.return_value = [(
                repository,
                ProjectRepository(project_id=project.id, repository_id=repository.id),
            )]
            store.project_working_directory.return_value = "/srv/pa/remote"
            store.update_card.return_value = updated

            ctx = MagicMock()
            ctx.settings = settings
            ctx.store = store
            wake_started = threading.Event()
            wake_release = threading.Event()
            worker = MagicMock()

            def slow_wake() -> None:
                wake_started.set()
                wake_release.wait(1.0)

            worker.wake.side_effect = slow_wake
            ctx.services = {
                "fleet_registry": fleet,
                "dispatch_worker": worker,
                "writer_lock": MagicMock(),
            }
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
                started = time.perf_counter()
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
                admission_elapsed = time.perf_counter() - started

            self.assertLess(admission_elapsed, 0.2)
            wake_release.set()
            await asyncio.sleep(0.02)
            self.assertTrue(wake_started.is_set())

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
            ctx.services = {"fleet_registry": fleet, "writer_lock": MagicMock()}
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
            ctx.services = {"fleet_registry": fleet, "writer_lock": MagicMock()}
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
            ctx.services = {"fleet_registry": fleet, "writer_lock": MagicMock()}
            ctx.require_service.side_effect = lambda name: ctx.services[name]
            ctx.register_service.side_effect = lambda name, value: (
                ctx.services.__setitem__(name, value)
            )
            request = MagicMock()
            request.app.state.ctx = ctx
            request.headers = {"idempotency-key": "prompt-failure"}
            peer = AsyncMock(
                side_effect=[
                    {
                        "session": {
                            "id": "remote-session",
                            "title": "Remote smoke",
                        },
                        "configuration": {
                            "state": "ready",
                            "effective": {
                                "model_id": None,
                                "mode_id": "agent-full-access",
                                "reasoning": None,
                                "config": {},
                            },
                        },
                    },
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
                    RemoteAgentStartBody(
                        title="Remote smoke",
                        message="Start work",
                        mode_id="agent-full-access",
                    ),
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
            self.assertEqual(record.request_payload["mode_id"], "agent-full-access")
            self.assertEqual(
                peer.await_args_list[0].kwargs["body"]["mode_id"],
                "agent-full-access",
            )
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

    async def test_agent_proxy_does_not_duplicate_openapi_operation_ids(self) -> None:
        from pa.modules.fleet import router

        app = FastAPI()
        app.include_router(router, prefix="/api")
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            app.openapi()

        duplicates = [
            warning
            for warning in caught
            if "Duplicate Operation ID" in str(warning.message)
        ]
        self.assertEqual(duplicates, [])

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

    async def test_agent_proxy_preserves_structured_recovery_and_retry_after(
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
            request.headers.get.return_value = "application/json"
            request.body = AsyncMock(return_value=b"")
            detail = {
                "code": "agent_recovery_in_progress",
                "message": "PA is restoring durable agent sessions.",
                "recoverable": True,
                "retry_after_ms": 250,
            }

            async def upstream_handler(_request: httpx.Request) -> httpx.Response:
                return httpx.Response(
                    503,
                    json={"detail": detail},
                    headers={"Retry-After": "2"},
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
                    "sessions",
                )

            self.assertEqual(response.status_code, 503)
            self.assertEqual(response.headers["retry-after"], "2")
            self.assertEqual(json.loads(response.body)["detail"], detail)

    async def test_agent_proxy_invalidates_cached_activity_after_mutation(self) -> None:
        from pa.fleet.overview import cache_for, field
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
            cache = cache_for(settings.data_dir)
            cache.put(
                "mini-1",
                "activity",
                field("fresh", {"active_sessions": 1}),
            )
            cache.put(
                "mini-1",
                "repositories",
                field("fresh", {"active_leases": 1}),
            )
            ctx = MagicMock(settings=settings)
            ctx.require_service.return_value = fleet
            request = MagicMock()
            request.app.state.ctx = ctx
            request.method = "POST"
            request.query_params.multi_items.return_value = []
            request.headers.get.return_value = "application/json"
            request.body = AsyncMock(return_value=b"{}")

            async def upstream_handler(_request: httpx.Request) -> httpx.Response:
                return httpx.Response(200, json={"status": "closed"})

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
                    "sessions/remote-session/close",
                )

            self.assertEqual(response.status_code, 200)
            self.assertIsNone(cache.get("mini-1", "activity"))
            self.assertIsNone(cache.get("mini-1", "repositories"))

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

    async def test_agent_proxy_closes_idle_upstream_after_downstream_disconnect(
        self,
    ) -> None:
        from pa.core.sse_observability import sse_connections
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
            request.query_params.get.side_effect = lambda name: {
                "client_id": "tab-1"
            }.get(name)
            request.headers.get.side_effect = lambda name: {
                "accept": "text/event-stream"
            }.get(name)
            request.body = AsyncMock(return_value=b"")
            request.is_disconnected = AsyncMock(return_value=True)

            class IdleEventStream(httpx.AsyncByteStream):
                def __init__(self) -> None:
                    self.closed = False

                async def __aiter__(self):
                    await asyncio.Event().wait()
                    yield b"unreachable"

                async def aclose(self) -> None:
                    self.closed = True

            stream = IdleEventStream()

            async def upstream_handler(_request: httpx.Request) -> httpx.Response:
                return httpx.Response(
                    200,
                    stream=stream,
                    headers={"content-type": "text/event-stream"},
                )

            upstream_client = httpx.AsyncClient(
                transport=httpx.MockTransport(upstream_handler)
            )
            sse_connections.reset_for_tests()
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
                    "session-events",
                )
                body = b"".join([chunk async for chunk in response.body_iterator])

            self.assertEqual(body, b"")
            self.assertTrue(stream.closed)
            snapshot = sse_connections.snapshot()
            self.assertEqual(snapshot["active"], 0)
            self.assertEqual(snapshot["cancelled"], 2)
            self.assertTrue(snapshot["paired"]["balanced"])

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

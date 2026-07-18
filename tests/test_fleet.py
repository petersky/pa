"""Tests for fleet registry, join wiring, and remote install helpers."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx

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
        settings = Settings(data_dir=self.data_dir, host="0.0.0.0", port=8080, instance_url="")
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
            self.assertIn(issue["action"], {"set_instance_url", "set_bind_all", "ensure_sync_token"})
        warnings = readiness_warnings(settings)
        self.assertTrue(any("loopback" in w.lower() or "127.0.0.1" in w for w in warnings))

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
        self.assertFalse(any(i["id"] == "missing_instance_url" for i in readiness_issues(settings)))
        self.assertFalse(any(i["id"] == "loopback_bind" for i in readiness_issues(settings)))

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
                patch("pa.fleet.remote_install._connect_ssh", AsyncMock(return_value=mock_conn)),
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
            blob = "\n".join(result.log_lines) + (data_dir / "fleet_jobs" / f"{job.job_id}.json").read_text()
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

            async def fake_get(url, headers=None):
                if url.endswith("/api/health"):
                    return FakeResp(200)
                if url.endswith("/api/agent/providers"):
                    host = "a" if "://a:" in url else "b"
                    return FakeResp(
                        200,
                        [{"id": host, "display_name": host.upper(), "available": True}],
                    )
                if url.endswith("/api/status"):
                    return FakeResp(200, {"version": "0.2.5"})
                if url.endswith("/api/fleet/peer-update-check"):
                    return FakeResp(200, {"available_version": "0.2.6", "upgrade_available": True})
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
            # health + providers + status + update check for each instance
            self.assertEqual(mock_client.get.await_count, 8)


class RemoteOperationsTests(unittest.IsolatedAsyncioTestCase):
    async def test_card_dispatch_returns_remote_session_and_records_audit(self) -> None:
        from pa.modules.fleet import RemoteAgentStartBody, start_remote_agent_work

        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(
                data_dir=Path(tmp),
                instance_id="controller-1",
                instance_name="controller",
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
            store.update_card.return_value = updated

            ctx = MagicMock()
            ctx.settings = settings
            ctx.store = store
            ctx.require_service.return_value = fleet
            request = MagicMock()
            request.app.state.ctx = ctx

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
            ):
                result = await start_remote_agent_work(
                    request,
                    "mini-1",
                    RemoteAgentStartBody(card_id=card.id, provider="codex"),
                )

            self.assertEqual(result["session"]["session"]["id"], "remote-session")
            create_call = peer.await_args_list[0]
            self.assertEqual(create_call.args[3], "sessions")
            self.assertEqual(
                create_call.kwargs["body"]["cwd"],
                "/Users/petersky/repos/petersky/pa",
            )
            self.assertEqual(create_call.kwargs["body"]["label"], "card:card-1")
            prompt_call = peer.await_args_list[1]
            self.assertIn("# Card: Implement remote control", prompt_call.kwargs["body"]["message"])
            store.update_card.assert_called_once()
            store.add_knowledge.assert_called_once()

    async def test_dispatch_preserves_session_when_initial_prompt_fails(self) -> None:
        from fastapi import HTTPException

        from pa.modules.fleet import RemoteAgentStartBody, start_remote_agent_work

        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(
                data_dir=Path(tmp),
                instance_id="controller-1",
                instance_name="controller",
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
            ctx.require_service.return_value = fleet
            request = MagicMock()
            request.app.state.ctx = ctx
            peer = AsyncMock(
                side_effect=[
                    {"session": {"id": "remote-session", "title": "Remote smoke"}},
                    HTTPException(status_code=503, detail="provider unavailable"),
                ]
            )

            with (
                patch("pa.modules.fleet.require_user", return_value=object()),
                patch("pa.modules.fleet._peer_agent_json", peer),
            ):
                result = await start_remote_agent_work(
                    request,
                    "mini-1",
                    RemoteAgentStartBody(title="Remote smoke", message="Start work"),
                )

            self.assertEqual(result["session"]["session"]["id"], "remote-session")
            self.assertEqual(result["prompt_error"], "provider unavailable")
            entry = store.add_knowledge.call_args.args[0]
            self.assertIn("Initial prompt failed", entry.summary)
            self.assertIn("prompt-error", entry.tags)

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

            async def upstream_handler(upstream_request: httpx.Request) -> httpx.Response:
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

    async def test_agent_proxy_disables_read_timeout_only_for_session_events(self) -> None:
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


if __name__ == "__main__":
    unittest.main()

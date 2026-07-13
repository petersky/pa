"""Tests for fleet registry, join wiring, and remote install helpers."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from pa.config import Settings
from pa.domain.instance_config import load_instance_config, update_instance_config
from pa.fleet.join import (
    apply_join_response,
    ensure_sync_token,
    owner_public_url,
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
        warnings = readiness_warnings(settings)
        self.assertTrue(any("loopback" in w.lower() or "127.0.0.1" in w for w in warnings))

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


if __name__ == "__main__":
    unittest.main()

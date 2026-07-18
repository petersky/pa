"""Fleet update job, authentication, recovery, and peer workflow tests."""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx

from pa.config import Settings
from pa.fleet.registry import FleetRegistry
from pa.fleet.update import (
    FleetUpdateJobStore,
    FleetUpdateRequest,
    UpdatePhase,
    recover_update_jobs,
    run_update_job,
)
from pa.fleet.update import _peer_json
from pa.modules.fleet import _require_instance, fleet_instance_update_events
from pa.update.channels import (
    ReleaseInfo,
    compare_versions,
    resolve_release,
    resolve_uv_binary,
)


class FleetUpdateStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.data_dir = Path(self.tmp.name)
        self.instance = SimpleNamespace(
            instance_id="peer-1", name="mini", url="http://mini:8080"
        )

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_rejects_concurrent_update_for_instance(self) -> None:
        store = FleetUpdateJobStore(self.data_dir)
        first = store.create(self.instance, FleetUpdateRequest(), "release")
        with self.assertRaisesRegex(RuntimeError, first.job_id):
            store.create(self.instance, FleetUpdateRequest(), "release")

    def test_persists_audit_without_token_or_credentials(self) -> None:
        store = FleetUpdateJobStore(self.data_dir)
        job = store.create(
            self.instance, FleetUpdateRequest(target_version="0.2.6"), "release"
        )
        store.event(job, UpdatePhase.PREFLIGHT, "Checking peer")
        text = (self.data_dir / "fleet_update_jobs" / f"{job.job_id}.json").read_text()
        self.assertNotIn("token", text.lower())
        self.assertNotIn("authorization", text.lower())
        reloaded = FleetUpdateJobStore(self.data_dir).get(job.job_id)
        self.assertEqual(reloaded.phase, UpdatePhase.PREFLIGHT)
        self.assertEqual(reloaded.events[-1]["message"], "Checking peer")

    def test_restart_recovery_resumes_nonterminal_job(self) -> None:
        settings = Settings(data_dir=self.data_dir, sync_token="secret")
        fleet = FleetRegistry(self.data_dir, settings.fleet_id)
        fleet.upsert_instance(
            __import__("pa.domain.models", fromlist=["FleetInstance"]).FleetInstance(
                instance_id="peer-1", name="mini", url="http://mini:8080"
            )
        )
        store = FleetUpdateJobStore(self.data_dir)
        job = store.create(self.instance, FleetUpdateRequest(), "release")
        store.event(job, UpdatePhase.RESTARTING, "Restarting")
        with patch("pa.fleet.update.start_update_job") as start:
            recover_update_jobs(settings, fleet, FleetUpdateJobStore(self.data_dir))
        self.assertEqual(start.call_count, 1)
        self.assertEqual(start.call_args.args[2].job_id, job.job_id)

    def test_event_sequence_survives_tail_truncation_and_reconnect(self) -> None:
        store = FleetUpdateJobStore(self.data_dir)
        job = store.create(self.instance, FleetUpdateRequest(), "release")
        for index in range(502):
            store.event(job, UpdatePhase.PREFLIGHT, f"event-{index + 1}")

        self.assertEqual(len(job.events), 500)
        self.assertEqual([event["seq"] for event in job.events[:2]], [3, 4])
        self.assertEqual(job.events[-1]["seq"], 502)

        first_connection = store.events_after(job, 500)
        self.assertEqual([event["seq"] for event in first_connection], [501, 502])
        reconnect = store.events_after(job, 501)
        self.assertEqual([event["seq"] for event in reconnect], [502])
        self.assertEqual(
            [event["seq"] for event in first_connection[:-1]]
            + [event["seq"] for event in reconnect],
            [501, 502],
        )

    def test_legacy_events_receive_stable_sequences_on_reload(self) -> None:
        store = FleetUpdateJobStore(self.data_dir)
        job = store.create(self.instance, FleetUpdateRequest(), "release")
        job.events = [
            {"phase": "preflight", "message": "one"},
            {"phase": "quiescing", "message": "two"},
        ]
        job.next_event_seq = 1
        store.persist(job)

        reloaded_store = FleetUpdateJobStore(self.data_dir)
        reloaded = reloaded_store.get(job.job_id)
        self.assertEqual([event["seq"] for event in reloaded.events], [1, 2])
        reloaded_store.event(reloaded, UpdatePhase.INSTALLING, "three")
        self.assertEqual([event["seq"] for event in reloaded.events], [1, 2, 3])


class FleetUpdateWorkflowTests(unittest.IsolatedAsyncioTestCase):
    async def _run(self, responses, *, force=False, target="0.2.6"):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        settings = Settings(data_dir=Path(tmp.name), sync_token="fleet-secret")
        store = FleetUpdateJobStore(Path(tmp.name))
        instance = SimpleNamespace(
            instance_id="peer-1", name="mini", url="http://mini:8080"
        )
        job = store.create(
            instance,
            FleetUpdateRequest(target_version=target, force=force, health_timeout=10),
            "release",
        )
        with (
            patch("pa.fleet.update._peer_json", AsyncMock(side_effect=responses)),
            patch("pa.fleet.update.asyncio.sleep", AsyncMock()),
            patch("pa.fleet.update.time.monotonic", side_effect=[0, 1, 20]),
        ):
            return await run_update_job(settings, store, job)

    async def test_explicit_valid_upgrade_verifies_reported_version(self) -> None:
        job = await self._run(
            [
                {"instance_id": "peer-1", "version": "0.2.5"},
                {"done": True},
                {"target_version": "0.2.6"},
                {"instance_id": "peer-1", "version": "0.2.6"},
            ]
        )
        self.assertEqual(job.phase, UpdatePhase.SUCCEEDED)
        self.assertEqual(job.verified_version, "0.2.6")

    async def test_explicit_equal_version_fails_before_quiesce(self) -> None:
        job = await self._run(
            [{"instance_id": "peer-1", "version": "0.2.5"}],
            target="0.2.5",
        )
        self.assertEqual(job.phase, UpdatePhase.FAILED)
        self.assertIn("must be newer", job.error)

    async def test_explicit_downgrade_fails_before_quiesce(self) -> None:
        job = await self._run(
            [{"instance_id": "peer-1", "version": "0.2.5"}],
            target="0.2.4",
        )
        self.assertEqual(job.phase, UpdatePhase.FAILED)
        self.assertIn("current 0.2.5", job.error)

    def test_semantic_version_comparison_handles_prereleases(self) -> None:
        self.assertLess(compare_versions("0.2.6-beta.1", "0.2.6"), 0)
        self.assertGreater(compare_versions("0.2.6", "0.2.6-rc.2"), 0)
        self.assertEqual(compare_versions("v0.2.6", "0.2.6.0"), 0)

    async def test_quiesce_timeout_requires_force(self) -> None:
        job = await self._run(
            [
                {"instance_id": "peer-1", "version": "0.2.5"},
                {"done": True, "error": "timed out with 1 active session"},
            ]
        )
        self.assertEqual(job.phase, UpdatePhase.FAILED)
        self.assertIn("force=true", job.error)

    async def test_version_mismatch_is_actionable_failure(self) -> None:
        job = await self._run(
            [
                {"instance_id": "peer-1", "version": "0.2.5"},
                {"done": True},
                {"target_version": "0.2.6"},
                {"instance_id": "peer-1", "version": "0.2.5"},
            ]
        )
        self.assertEqual(job.phase, UpdatePhase.FAILED)
        self.assertIn("expected 0.2.6", job.error)

    async def test_peer_identity_must_match_registered_target(self) -> None:
        job = await self._run([{"instance_id": "other", "version": "0.2.5"}])
        self.assertEqual(job.phase, UpdatePhase.FAILED)
        self.assertIn("identity", job.error)

    async def test_absent_available_version_fails_before_drain_or_install(self) -> None:
        job = await self._run(
            [
                {"instance_id": "peer-1", "version": "0.2.5"},
                {"available_version": None, "upgrade_available": False},
            ],
            target=None,
        )
        self.assertEqual(job.phase, UpdatePhase.FAILED)
        self.assertIn("did not report an available version", job.error)

    async def test_dispatch_transport_errors_continue_to_exact_verification(
        self,
    ) -> None:
        for error in (
            httpx.ConnectError("peer stopped before response"),
            httpx.ReadError("peer restarted during response"),
            httpx.RemoteProtocolError("peer closed protocol during restart"),
        ):
            with self.subTest(error=type(error).__name__):
                job = await self._run(
                    [
                        {"instance_id": "peer-1", "version": "0.2.5"},
                        {"done": True},
                        error,
                        {"instance_id": "peer-1", "version": "0.2.5"},
                    ]
                )
                self.assertEqual(job.phase, UpdatePhase.FAILED)
                self.assertEqual(job.expected_version, "0.2.6")
                self.assertIn("expected 0.2.6", job.error)

    async def test_connect_error_can_succeed_only_after_exact_verification(
        self,
    ) -> None:
        job = await self._run(
            [
                {"instance_id": "peer-1", "version": "0.2.5"},
                {"done": True},
                httpx.ConnectError("peer restarted before response"),
                {"instance_id": "peer-1", "version": "0.2.6"},
            ]
        )
        self.assertEqual(job.phase, UpdatePhase.SUCCEEDED)
        self.assertEqual(job.verified_version, job.expected_version)

    async def _run_resumed(self, phase: UpdatePhase):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        settings = Settings(data_dir=Path(tmp.name), sync_token="fleet-secret")
        store = FleetUpdateJobStore(Path(tmp.name))
        instance = SimpleNamespace(
            instance_id="peer-1", name="mini", url="http://mini:8080"
        )
        job = store.create(
            instance, FleetUpdateRequest(target_version="0.2.6"), "release"
        )
        job.phase = phase
        job.current_version = "0.2.5"
        job.expected_version = "0.2.6"
        store.persist(job)
        job = FleetUpdateJobStore(Path(tmp.name)).get(job.job_id)

        responses = []
        if phase == UpdatePhase.PREFLIGHT:
            responses.append({"done": True})
        if phase in {UpdatePhase.PREFLIGHT, UpdatePhase.QUIESCING}:
            responses.append({"target_version": "0.2.6"})
        responses.append({"instance_id": "peer-1", "version": "0.2.6"})
        peer = AsyncMock(side_effect=responses)
        with (
            patch("pa.fleet.update._peer_json", peer),
            patch("pa.fleet.update.asyncio.sleep", AsyncMock()),
            patch("pa.fleet.update.time.monotonic", side_effect=[0, 1, 20]),
        ):
            result = await run_update_job(settings, store, job)
        return result, peer

    async def test_resume_from_preflight_skips_preflight_only(self) -> None:
        job, peer = await self._run_resumed(UpdatePhase.PREFLIGHT)
        urls = [call.args[2] for call in peer.await_args_list]
        self.assertEqual(job.phase, UpdatePhase.SUCCEEDED)
        self.assertNotIn("/api/fleet/peer-update-check", urls)
        self.assertEqual(sum(url.endswith("/api/agent/quiesce") for url in urls), 1)
        self.assertEqual(sum(url.endswith("/api/fleet/peer-update") for url in urls), 1)

    async def test_resume_from_quiescing_skips_preflight_and_drain(self) -> None:
        job, peer = await self._run_resumed(UpdatePhase.QUIESCING)
        urls = [call.args[2] for call in peer.await_args_list]
        self.assertEqual(job.phase, UpdatePhase.SUCCEEDED)
        self.assertFalse(any(url.endswith("/api/agent/quiesce") for url in urls))
        self.assertEqual(sum(url.endswith("/api/fleet/peer-update") for url in urls), 1)

    async def test_resume_after_install_dispatch_is_verification_only(self) -> None:
        for phase in (
            UpdatePhase.INSTALLING,
            UpdatePhase.RESTARTING,
            UpdatePhase.VERIFYING,
        ):
            with self.subTest(phase=phase):
                job, peer = await self._run_resumed(phase)
                urls = [call.args[2] for call in peer.await_args_list]
                self.assertEqual(job.phase, UpdatePhase.SUCCEEDED)
                self.assertFalse(
                    any(url.endswith("/api/agent/quiesce") for url in urls)
                )
                self.assertFalse(
                    any(url.endswith("/api/fleet/peer-update") for url in urls)
                )

    async def test_legacy_recovery_without_expected_version_fails_safely(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        settings = Settings(data_dir=Path(tmp.name), sync_token="fleet-secret")
        store = FleetUpdateJobStore(Path(tmp.name))
        instance = SimpleNamespace(
            instance_id="peer-1", name="mini", url="http://mini:8080"
        )
        job = store.create(instance, FleetUpdateRequest(), "release")
        job.phase = UpdatePhase.INSTALLING
        store.persist(job)
        peer = AsyncMock()
        with patch("pa.fleet.update._peer_json", peer):
            result = await run_update_job(settings, store, job)
        self.assertEqual(result.phase, UpdatePhase.FAILED)
        self.assertIn("durable expected version", result.error)
        peer.assert_not_awaited()


class FleetUpdateSSETests(unittest.IsolatedAsyncioTestCase):
    async def test_sse_reconnect_uses_monotonic_sequence_ids(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = FleetUpdateJobStore(Path(tmp))
            instance = SimpleNamespace(
                instance_id="peer-1", name="mini", url="http://mini:8080"
            )
            job = store.create(instance, FleetUpdateRequest(), "release")
            for index in range(501):
                store.event(job, UpdatePhase.PREFLIGHT, f"event-{index + 1}")
            store.event(job, UpdatePhase.SUCCEEDED, "done")

            request = MagicMock()
            request.state.user = object()
            request.query_params = {"after": "500"}
            request.headers = {}
            request.app.state.ctx.require_service.return_value = store
            response = await fleet_instance_update_events(request, "peer-1", job.job_id)
            chunks = []
            async for chunk in response.body_iterator:
                chunks.append(chunk.decode() if isinstance(chunk, bytes) else chunk)
            body = "".join(chunks)

            request.query_params = {}
            request.headers = {"last-event-id": "501"}
            reconnect_response = await fleet_instance_update_events(
                request, "peer-1", job.job_id
            )
            reconnect_chunks = []
            async for chunk in reconnect_response.body_iterator:
                reconnect_chunks.append(
                    chunk.decode() if isinstance(chunk, bytes) else chunk
                )
            reconnect_body = "".join(reconnect_chunks)

        self.assertEqual(body.count("id: 501\n"), 1)
        self.assertEqual(body.count("id: 502\n"), 1)
        self.assertNotIn("id: 500\n", body)
        self.assertLess(body.index("id: 501\n"), body.index("id: 502\n"))
        self.assertNotIn("id: 501\n", reconnect_body)
        self.assertEqual(reconnect_body.count("id: 502\n"), 1)


class FleetPeerAuthTests(unittest.TestCase):
    def test_peer_endpoint_rejects_user_session(self) -> None:
        request = MagicMock()
        request.state.instance_authenticated = False
        with self.assertRaisesRegex(Exception, "authentication required"):
            _require_instance(request)

    def test_peer_endpoint_accepts_sync_authenticated_request(self) -> None:
        request = MagicMock()
        request.state.instance_authenticated = True
        self.assertIsNone(_require_instance(request))


class DisposablePeerIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_controller_calls_disposable_peer_with_sync_token(self) -> None:
        from fastapi import FastAPI, Header, HTTPException

        peer = FastAPI()

        @peer.get("/api/status")
        async def status(authorization: str | None = Header(default=None)):
            if authorization != "Bearer fleet-secret":
                raise HTTPException(status_code=401)
            return {"instance_id": "peer-1", "version": "0.2.6"}

        settings = Settings(
            data_dir=Path(tempfile.mkdtemp()), sync_token="fleet-secret"
        )
        transport = httpx.ASGITransport(app=peer)
        async with httpx.AsyncClient(
            transport=transport, base_url="http://peer"
        ) as client:
            result = await _peer_json(client, "GET", "http://peer/api/status", settings)
        self.assertEqual(result, {"instance_id": "peer-1", "version": "0.2.6"})


class UvResolutionTests(unittest.TestCase):
    def test_service_environment_resolves_home_local_uv(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            uv = Path(tmp) / ".local" / "bin" / "uv"
            uv.parent.mkdir(parents=True)
            uv.write_text("#!/bin/sh\n")
            uv.chmod(0o755)
            with (
                patch.dict("os.environ", {"HOME": tmp}, clear=True),
                patch("pa.update.channels.Path.home", return_value=Path(tmp)),
                patch("pa.update.channels.shutil.which", return_value=None),
            ):
                self.assertEqual(resolve_uv_binary(), str(uv))

    def test_missing_uv_has_actionable_error(self) -> None:
        with (
            patch.dict("os.environ", {}, clear=True),
            patch("pa.update.channels.Path.is_file", return_value=False),
            patch("pa.update.channels.shutil.which", return_value=None),
        ):
            with self.assertRaisesRegex(RuntimeError, "PA_UV_BIN"):
                resolve_uv_binary()


class FleetReleaseResolutionTests(unittest.TestCase):
    def test_dev_uses_channel_resolved_branch_install_spec(self) -> None:
        channel = MagicMock()
        channel.latest.return_value = ReleaseInfo(
            version="dev",
            install_spec="git+https://github.com/petersky/pa.git@feature/dev-fleet",
            tag="feature/dev-fleet",
            track="dev",
        )
        with patch("pa.update.channels.get_channel", return_value=channel):
            release = resolve_release("dev", "dev", repo="petersky/pa")
        self.assertEqual(
            release.install_spec,
            "git+https://github.com/petersky/pa.git@feature/dev-fleet",
        )
        self.assertNotIn("@vdev", release.install_spec)

    def test_release_and_beta_keep_version_tag_install_specs(self) -> None:
        release = resolve_release("release", "0.2.6", repo="petersky/pa")
        beta = resolve_release("beta", "0.2.7-beta.1", repo="petersky/pa")
        self.assertTrue(release.install_spec.endswith("@v0.2.6"))
        self.assertTrue(beta.install_spec.endswith("@v0.2.7-beta.1"))

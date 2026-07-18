"""Fleet update job, authentication, recovery, and peer workflow tests."""

from __future__ import annotations

import asyncio
import tempfile
import threading
from datetime import UTC, datetime, timedelta
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pa.modules.fleet as fleet_module

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
from pa.install.metadata import (
    InstallMetadata,
    load_install_metadata,
    save_install_metadata,
)
from pa.modules.fleet import _require_instance, fleet_instance_update_events, peer_update
from pa.packaging.uv import resolve_uv_binary
from pa.update.channels import (
    GitHubTrackChannel,
    ReleaseInfo,
    compare_versions,
    resolve_release,
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

    def test_two_stores_atomically_exclude_same_instance(self) -> None:
        stores = [
            FleetUpdateJobStore(self.data_dir),
            FleetUpdateJobStore(self.data_dir),
        ]
        barrier = threading.Barrier(2)
        outcomes = []

        def create(store):
            barrier.wait()
            try:
                outcomes.append(
                    (
                        "created",
                        store.create(
                            self.instance, FleetUpdateRequest(), "release"
                        ).job_id,
                    )
                )
            except RuntimeError as exc:
                outcomes.append(("blocked", str(exc)))

        threads = [threading.Thread(target=create, args=(store,)) for store in stores]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(
            sorted(result for result, _ in outcomes), ["blocked", "created"]
        )
        self.assertEqual(outcomes[0][1], outcomes[1][1])

    def test_terminal_persisted_job_releases_cross_store_exclusion(self) -> None:
        first_store = FleetUpdateJobStore(self.data_dir)
        first = first_store.create(self.instance, FleetUpdateRequest(), "release")
        first_store.event(first, UpdatePhase.FAILED, "terminal")
        second = FleetUpdateJobStore(self.data_dir).create(
            self.instance, FleetUpdateRequest(), "release"
        )
        self.assertNotEqual(first.job_id, second.job_id)

    def test_cross_store_reconciliation_preserves_live_job_identity(self) -> None:
        store = FleetUpdateJobStore(self.data_dir)
        original = store.create(self.instance, FleetUpdateRequest(), "release")
        other = SimpleNamespace(
            instance_id="peer-2", name="linux", url="http://linux:8080"
        )

        external_store = FleetUpdateJobStore(self.data_dir)
        external = external_store.create(other, FleetUpdateRequest(), "release")
        created = store.create(
            SimpleNamespace(
                instance_id="peer-3", name="studio", url="http://studio:8080"
            ),
            FleetUpdateRequest(),
            "release",
        )

        self.assertIs(store.get(original.job_id), original)
        self.assertEqual(store.get(external.job_id).job_id, external.job_id)
        self.assertIs(store.get(created.job_id), created)
        store.event(original, UpdatePhase.PREFLIGHT, "still live")
        self.assertIs(store.get(original.job_id), original)
        self.assertEqual(store.get(original.job_id).events[-1]["message"], "still live")

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
    async def _run(
        self,
        responses,
        *,
        force=False,
        target="0.2.6",
        channel="release",
        install_statuses=None,
    ):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        settings = Settings(data_dir=Path(tmp.name), sync_token="fleet-secret")
        store = FleetUpdateJobStore(Path(tmp.name))
        instance = SimpleNamespace(
            instance_id="peer-1", name="mini", url="http://mini:8080"
        )
        job = store.create(
            instance,
            FleetUpdateRequest(
                target_version=target,
                channel=channel,
                force=force,
                health_timeout=10,
            ),
            channel,
        )
        remaining = iter(responses)
        operation_statuses = iter(install_statuses or [{"status": "installed"}])

        async def peer(_client, method, url, _settings, **_kwargs):
            if method == "GET" and "/api/fleet/peer-update/" in url:
                return next(operation_statuses)
            value = next(remaining)
            if isinstance(value, Exception):
                raise value
            return value

        with (
            patch("pa.fleet.update._peer_json", AsyncMock(side_effect=peer)),
            patch("pa.fleet.update.asyncio.sleep", AsyncMock()),
            patch("pa.fleet.update.time.monotonic", side_effect=[0, 1, 20]),
        ):
            return await run_update_job(settings, store, job)

    async def test_slow_install_does_not_consume_health_timeout(self) -> None:
        job = await self._run(
            [
                {"instance_id": "peer-1", "version": "0.2.5"},
                {"done": True},
                {"target_version": "0.2.6"},
                {"instance_id": "peer-1", "version": "0.2.6"},
            ],
            install_statuses=[
                {"status": "installing"},
                {"status": "installing"},
                {"status": "installed"},
            ],
        )
        self.assertEqual(job.phase, UpdatePhase.SUCCEEDED)
        self.assertIsNotNone(job.install_deadline)
        self.assertIsNotNone(job.health_deadline)

    async def test_expired_install_deadline_is_distinct_from_health_timeout(
        self,
    ) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        settings = Settings(data_dir=Path(tmp.name), sync_token="fleet-secret")
        store = FleetUpdateJobStore(Path(tmp.name))
        instance = SimpleNamespace(instance_id="peer-1", name="mini", url="http://mini")
        job = store.create(
            instance, FleetUpdateRequest(target_version="0.2.6"), "release"
        )
        job.phase = UpdatePhase.WAITING_INSTALL
        job.expected_version = "0.2.6"
        job.install_deadline = datetime.now(UTC) - timedelta(seconds=1)
        store.persist(job)
        with patch("pa.fleet.update.asyncio.sleep", AsyncMock()):
            result = await run_update_job(settings, store, job)
        self.assertEqual(result.phase, UpdatePhase.FAILED)
        self.assertIn("installation timed out", result.error)
        self.assertNotIn("health verification", result.error)

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

    async def test_semantically_equivalent_reported_version_succeeds(self) -> None:
        job = await self._run(
            [
                {"instance_id": "peer-1", "version": "0.2.5"},
                {"done": True},
                {"target_version": "0.2.6"},
                {"instance_id": "peer-1", "version": "0.2.6.0"},
            ]
        )
        self.assertEqual(job.phase, UpdatePhase.SUCCEEDED)
        self.assertEqual(compare_versions(job.verified_version, "0.2.6"), 0)

    async def test_dev_verifies_immutable_installed_revision(self) -> None:
        old_revision = "a" * 40
        expected_revision = "b" * 40
        job = await self._run(
            [
                {
                    "instance_id": "peer-1",
                    "version": "0.2.5",
                    "install_revision": old_revision,
                },
                {
                    "available_version": "dev",
                    "upgrade_available": True,
                    "target_identity": expected_revision,
                },
                {"done": True},
                {
                    "target_version": "dev",
                    "target_identity": expected_revision,
                },
                {
                    "instance_id": "peer-1",
                    "version": "0.2.6",
                    "installed_version": "0.2.6.0",
                    "install_channel": "dev",
                    "install_revision": expected_revision,
                },
            ],
            target=None,
            channel="dev",
        )
        self.assertEqual(job.phase, UpdatePhase.SUCCEEDED)
        self.assertEqual(job.verified_identity, expected_revision)

    async def test_dev_noop_revision_fails_before_quiesce(self) -> None:
        revision = "c" * 40
        job = await self._run(
            [
                {
                    "instance_id": "peer-1",
                    "version": "0.2.5",
                    "install_revision": revision,
                },
                {
                    "available_version": "dev",
                    "upgrade_available": True,
                    "target_identity": revision,
                },
            ],
            target=None,
            channel="dev",
        )
        self.assertEqual(job.phase, UpdatePhase.FAILED)
        self.assertIn("already has dev revision", job.error)

    async def test_dev_wrong_revision_fails_verification(self) -> None:
        expected_revision = "d" * 40
        wrong_revision = "e" * 40
        job = await self._run(
            [
                {
                    "instance_id": "peer-1",
                    "version": "0.2.5",
                    "install_revision": "a" * 40,
                },
                {
                    "available_version": "dev",
                    "upgrade_available": True,
                    "target_identity": expected_revision,
                },
                {"done": True},
                {
                    "target_version": "dev",
                    "target_identity": expected_revision,
                },
                {
                    "instance_id": "peer-1",
                    "version": "0.2.6",
                    "installed_version": "0.2.6",
                    "install_channel": "dev",
                    "install_revision": wrong_revision,
                },
            ],
            target=None,
            channel="dev",
        )
        self.assertEqual(job.phase, UpdatePhase.FAILED)
        self.assertIn(expected_revision, job.error)
        self.assertIn(wrong_revision, job.error)

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
        if phase == UpdatePhase.INSTALLING:
            responses.append({"target_version": "0.2.6"})
        responses.append({"instance_id": "peer-1", "version": "0.2.6"})
        remaining = iter(responses)

        async def peer_response(_client, method, url, _settings, **_kwargs):
            if method == "GET" and "/api/fleet/peer-update/" in url:
                return {"status": "installed"}
            return next(remaining)

        peer = AsyncMock(side_effect=peer_response)
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

    async def test_resume_from_ambiguous_installing_retries_idempotent_dispatch(
        self,
    ) -> None:
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
                dispatch_count = sum(
                    url.endswith("/api/fleet/peer-update") for url in urls
                )
                self.assertEqual(
                    dispatch_count, 1 if phase == UpdatePhase.INSTALLING else 0
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


class PeerUpdateIdempotencyTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.settings = Settings(data_dir=Path(self.tmp.name))
        self.request = MagicMock()
        self.request.state.instance_authenticated = True
        self.request.app.state.ctx.settings = self.settings
        self.release = ReleaseInfo(
            version="0.2.6", install_spec="pa==0.2.6", tag="v0.2.6"
        )
        self.body = {
            "operation_id": "job-123",
            "channel": "release",
            "target_version": "0.2.6",
        }
        fleet_module._peer_update_task = None
        fleet_module._peer_update_task_operation_id = None

    async def asyncTearDown(self) -> None:
        task = fleet_module._peer_update_task
        if task and not task.done():
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        fleet_module._peer_update_task = None
        fleet_module._peer_update_task_operation_id = None
        self.tmp.cleanup()

    def _persist_installing(self) -> None:
        fleet_module._write_peer_operation(
            self.settings,
            "job-123",
            {
                "status": "installing",
                "target_version": "0.2.6",
                "target_identity": "v0.2.6",
                "channel": "release",
                "error": None,
            },
        )

    async def test_active_installing_replay_returns_without_duplicate_task(self) -> None:
        self._persist_installing()
        blocker = asyncio.Event()
        active = asyncio.create_task(blocker.wait())
        fleet_module._peer_update_task = active
        fleet_module._peer_update_task_operation_id = "job-123"
        with patch("pa.update.channels.resolve_release") as resolve:
            result = await peer_update(self.request, self.body)
        self.assertEqual(result["status"], "installing")
        self.assertIs(fleet_module._peer_update_task, active)
        resolve.assert_not_called()

    async def test_stale_installing_record_resumes_local_work(self) -> None:
        from pa.update.runner import UpdateResult

        self._persist_installing()
        update_result = UpdateResult(
            current="0.2.6",
            latest="0.2.6",
            upgrade_available=True,
            release=self.release,
        )
        with (
            patch("pa.update.channels.resolve_release", return_value=self.release),
            patch("pa.modules.fleet._peer_has_exact_release", return_value=False),
            patch("pa.update.runner.apply_update", return_value=update_result) as apply,
            patch("pa.cli.service.restart"),
            patch("pa.instance.quiesce.request_skip_quiesce"),
        ):
            await peer_update(self.request, self.body)
            await fleet_module._peer_update_task
        apply.assert_called_once()
        operation = fleet_module._read_peer_operation(self.settings, "job-123")
        self.assertEqual(operation["status"], "restarting")

    async def test_stale_installing_exact_target_resumes_restart_without_reinstall(
        self,
    ) -> None:
        self._persist_installing()
        with (
            patch("pa.update.channels.resolve_release", return_value=self.release),
            patch("pa.modules.fleet._peer_has_exact_release", return_value=True),
            patch("pa.update.runner.apply_update") as apply,
            patch("pa.cli.service.restart") as restart,
            patch("pa.instance.quiesce.request_skip_quiesce"),
        ):
            await peer_update(self.request, self.body)
            await fleet_module._peer_update_task
        apply.assert_not_called()
        restart.assert_called_once()
        operation = fleet_module._read_peer_operation(self.settings, "job-123")
        self.assertEqual(operation["status"], "restarting")


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
    def test_noninteractive_path_resolves_local_uv(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            for shaped_home in (
                Path(tmp) / "Users" / "alice",
                Path(tmp) / "home" / "alice",
            ):
                with self.subTest(home=shaped_home):
                    uv = shaped_home / ".local" / "bin" / "uv"
                    uv.parent.mkdir(parents=True)
                    uv.write_text("#!/bin/sh\n")
                    uv.chmod(0o755)
                    with (
                        patch.dict(
                            "os.environ", {"HOME": str(shaped_home)}, clear=True
                        ),
                        patch("pa.packaging.uv.Path.home", return_value=shaped_home),
                        patch("pa.packaging.uv.shutil.which", return_value=None),
                    ):
                        self.assertEqual(resolve_uv_binary(), str(uv.resolve()))

    def test_running_uv_tool_install_resolves_uv_when_home_is_unrelated(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            install_home = Path(tmp) / "Users" / "alice"
            uv = install_home / ".local" / "bin" / "uv"
            uv.parent.mkdir(parents=True)
            uv.write_text("#!/bin/sh\n")
            uv.chmod(0o755)
            python = (
                install_home
                / ".local"
                / "share"
                / "uv"
                / "tools"
                / "pa"
                / "bin"
                / "python"
            )
            with (
                patch.dict("os.environ", {}, clear=True),
                patch("pa.packaging.uv.sys.executable", str(python)),
                patch(
                    "pa.packaging.uv.Path.home", return_value=Path(tmp) / "wrong-home"
                ),
                patch("pa.packaging.uv.shutil.which", return_value=None),
            ):
                self.assertEqual(resolve_uv_binary(), str(uv.resolve()))

    def test_normal_path_resolution(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            uv = Path(tmp) / "bin" / "uv"
            uv.parent.mkdir()
            uv.write_text("#!/bin/sh\n")
            uv.chmod(0o755)
            with (
                patch.dict("os.environ", {}, clear=True),
                patch("pa.packaging.uv.Path.home", return_value=Path(tmp) / "home"),
                patch("pa.packaging.uv.sys.executable", "/runtime/python"),
                patch("pa.packaging.uv.sys.argv", ["pa"]),
                patch("pa.packaging.uv.shutil.which", return_value=str(uv)),
            ):
                self.assertEqual(resolve_uv_binary(), str(uv.resolve()))

    def test_missing_uv_has_actionable_error(self) -> None:
        with (
            patch.dict("os.environ", {}, clear=True),
            patch("pa.packaging.uv.Path.is_file", return_value=False),
            patch("pa.packaging.uv.shutil.which", return_value=None),
        ):
            with self.assertRaisesRegex(RuntimeError, "PA_UV_BIN"):
                resolve_uv_binary()


class FleetReleaseResolutionTests(unittest.TestCase):
    def test_dev_channel_resolves_branch_to_immutable_commit(self) -> None:
        revision = "2" * 40
        channel = GitHubTrackChannel("dev", "petersky/pa")
        with (
            patch("pa.update.channels._ref_from_channels_json", return_value="main"),
            patch("pa.update.channels._resolve_github_revision", return_value=revision),
        ):
            release = channel.latest()
        self.assertEqual(release.revision, revision)
        self.assertTrue(release.install_spec.endswith(f"@{revision}"))

    def test_dev_uses_channel_resolved_branch_install_spec(self) -> None:
        channel = MagicMock()
        revision = "f" * 40
        channel.latest.return_value = ReleaseInfo(
            version="dev",
            install_spec=f"git+https://github.com/petersky/pa.git@{revision}",
            tag="feature/dev-fleet",
            track="dev",
            revision=revision,
        )
        with patch("pa.update.channels.get_channel", return_value=channel):
            release = resolve_release("dev", "dev", repo="petersky/pa")
        self.assertEqual(
            release.install_spec,
            f"git+https://github.com/petersky/pa.git@{revision}",
        )
        self.assertNotIn("@vdev", release.install_spec)
        self.assertEqual(release.revision, revision)

    def test_dev_exact_revision_builds_immutable_install_spec(self) -> None:
        revision = "1" * 40
        release = resolve_release("dev", "dev", repo="petersky/pa", revision=revision)
        self.assertTrue(release.install_spec.endswith(f"@{revision}"))
        self.assertEqual(release.revision, revision)

    def test_release_and_beta_keep_version_tag_install_specs(self) -> None:
        release = resolve_release("release", "0.2.6", repo="petersky/pa")
        beta = resolve_release("beta", "0.2.7-beta.1", repo="petersky/pa")
        self.assertTrue(release.install_spec.endswith("@v0.2.6"))
        self.assertTrue(beta.install_spec.endswith("@v0.2.7-beta.1"))

    def test_install_metadata_persists_dev_source_revision(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            revision = "3" * 40
            save_install_metadata(
                Path(tmp),
                InstallMetadata(
                    version="0.2.6",
                    channel="dev",
                    source_revision=revision,
                ),
            )
            loaded = load_install_metadata(Path(tmp))
        self.assertEqual(loaded.channel, "dev")
        self.assertEqual(loaded.source_revision, revision)

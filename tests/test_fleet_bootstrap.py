"""Contract tests for durable fleet machine onboarding."""

from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from pa.fleet.bootstrap import (
    BootstrapJobStore,
    BootstrapPhase,
    BootstrapRequest,
    BootstrapState,
    PhaseState,
    TargetDiscovery,
    accept_bootstrap_input,
    build_bootstrap_plan,
    _probe_github_repositories,
    run_bootstrap_job,
)
from pa.fleet.remote_install import (
    InstallJobStatus,
    InstallJobStore,
    RemoteInstallRequest,
    _connect_ssh,
    _install_script_url,
)


def _request(**changes) -> BootstrapRequest:
    values = {
        "target": "peter@mini",
        "host": "mini",
        "user": "peter",
        "instance_name": "mini",
        "instance_url": "http://mini:8080",
        "realm": "default",
    }
    values.update(changes)
    return BootstrapRequest.model_validate(values)


def _discovery(**changes) -> TargetDiscovery:
    values = {
        "target": "peter@mini",
        "host": "mini",
        "user": "peter",
        "port": 22,
        "host_key_fingerprint": "SHA256:trusted",
        "host_key_algorithm": "ssh-ed25519",
        "host_key_known": False,
        "host_key_state": "unknown",
    }
    values.update(changes)
    return TargetDiscovery.model_validate(values)


class BootstrapRequestTests(unittest.TestCase):
    def test_automatic_placement_requires_worker_profile(self) -> None:
        with self.assertRaisesRegex(ValueError, "automatic_placement"):
            _request(automatic_placement=True, worker_profile="manual")

    def test_replace_requires_explicit_confirmation(self) -> None:
        with self.assertRaisesRegex(ValueError, "confirm_replace"):
            _request(existing_install_action="replace")
        request = _request(existing_install_action="replace", confirm_replace=True)
        self.assertTrue(request.confirm_replace)

    def test_plan_discloses_mutation_privilege_and_interaction(self) -> None:
        plan = build_bootstrap_plan(
            _request(providers=["codex"], github_transport="ssh"),
            _discovery(),
        )
        by_phase = {item["phase"]: item for item in plan}
        self.assertFalse(by_phase["resolve_target"]["mutation"])
        self.assertIn(
            "host_key_confirmation",
            by_phase["resolve_target"]["required_interactions"],
        )
        self.assertTrue(by_phase["install_pa"]["privileged"])
        self.assertIn(
            "device_or_browser_login",
            by_phase["provider_auth"]["required_interactions"],
        )

    def test_github_probe_accepts_noninteractive_gh_token_auth(self) -> None:
        connection = MagicMock()
        connection.run = AsyncMock(
            return_value=MagicMock(exit_status=0)
        )
        connection.wait_closed = AsyncMock()

        async def exercise():
            with patch(
                "pa.fleet.bootstrap._connect_ssh",
                new=AsyncMock(return_value=connection),
            ):
                return await _probe_github_repositories(
                    _request(
                        github_transport="https",
                        repositories=["petersky/pa"],
                    ),
                    {},
                )

        evidence = asyncio.run(exercise())
        self.assertTrue(evidence["authenticated"])
        self.assertEqual(evidence["auth_mode"], "verified_gh_cli")
        command = connection.run.await_args.args[0]
        self.assertIn("gh auth status", command)
        self.assertIn("agent_github_token_enabled", command)
        self.assertIn("github.json", command)
        self.assertNotIn("token-that-must-not-leak", command)


class BootstrapJobStoreTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.data_dir = Path(self.temp.name)

    def tearDown(self) -> None:
        self.temp.cleanup()

    def _create(self, store: BootstrapJobStore, **changes):
        request = _request(**changes)
        return store.create(
            request,
            idempotency_key="setup-mini-1",
            actor="user:local",
            authority_instance_id="authority-1",
            authority_url="http://authority:8080",
            discovery=_discovery(),
            secrets={
                "password": "password-should-never-persist",
                "passphrase": "passphrase-should-never-persist",
            },
        )

    def test_persists_full_job_but_never_secrets(self) -> None:
        store = BootstrapJobStore(self.data_dir)
        job, duplicate = self._create(store)
        self.assertFalse(duplicate)
        snapshot = (
            self.data_dir / "fleet_bootstrap_jobs" / f"{job.job_id}.json"
        ).read_text()
        self.assertNotIn("password-should-never-persist", snapshot)
        self.assertNotIn("passphrase-should-never-persist", snapshot)
        restored = BootstrapJobStore(self.data_dir).get(job.job_id)
        self.assertIsNotNone(restored)
        self.assertEqual(restored.schema_version, 1)
        self.assertEqual(len(restored.phases), len(BootstrapPhase))

    def test_idempotency_returns_same_job_and_rejects_conflict(self) -> None:
        store = BootstrapJobStore(self.data_dir)
        first, duplicate = self._create(store)
        second, duplicate = self._create(store)
        self.assertTrue(duplicate)
        self.assertEqual(first.job_id, second.job_id)
        with self.assertRaisesRegex(ValueError, "different bootstrap request"):
            self._create(store, instance_name="other")

    def test_restart_turns_running_job_into_retryable_checkpoint(self) -> None:
        store = BootstrapJobStore(self.data_dir)
        job, _ = self._create(store)
        job.state = BootstrapState.RUNNING
        job.phase_record(BootstrapPhase.PREFLIGHT_HOST).state = PhaseState.RUNNING
        store.save(job)

        recovered = BootstrapJobStore(self.data_dir).get(job.job_id)
        self.assertEqual(recovered.state, BootstrapState.RETRYABLE)
        self.assertIn("resume", recovered.readiness_reason.lower())

    def test_host_key_input_requires_exact_explicit_confirmation(self) -> None:
        store = BootstrapJobStore(self.data_dir)
        job, _ = self._create(store)
        from pa.fleet.bootstrap import RequiredInput

        job.state = BootstrapState.WAITING_INPUT
        job.required_input = RequiredInput(
            kind="host_key",
            prompt="confirm",
            phase=BootstrapPhase.RESOLVE_TARGET,
            details={"fingerprint": "SHA256:trusted"},
        )
        store.save(job)
        with self.assertRaisesRegex(ValueError, "exact fingerprint"):
            accept_bootstrap_input(
                store,
                job,
                kind="host_key",
                value="SHA256:attacker",
                confirmed=True,
            )
        accepted = accept_bootstrap_input(
            store,
            job,
            kind="host_key",
            value="SHA256:trusted",
            confirmed=True,
        )
        self.assertEqual(accepted.request.host_key_policy, "pinned")
        self.assertEqual(
            accepted.phase_record(BootstrapPhase.RESOLVE_TARGET).state,
            PhaseState.PENDING,
        )

    def test_secret_input_is_memory_only_and_audited_without_value(self) -> None:
        store = BootstrapJobStore(self.data_dir)
        job, _ = self._create(store)
        from pa.fleet.bootstrap import RequiredInput

        job.required_input = RequiredInput(
            kind="ssh_password",
            prompt="password",
            phase=BootstrapPhase.PREFLIGHT_HOST,
        )
        secret = "ssh-password-visible-nowhere"
        accept_bootstrap_input(
            store,
            job,
            kind="ssh_password",
            value=secret,
        )
        self.assertEqual(store.secrets.get(job.job_id)["password"], secret)
        snapshot = (
            self.data_dir / "fleet_bootstrap_jobs" / f"{job.job_id}.json"
        ).read_text()
        self.assertNotIn(secret, snapshot)
        self.assertNotIn(secret, "\n".join(event.message for event in job.log_events))


class SecureRemoteInstallTests(unittest.IsolatedAsyncioTestCase):
    async def test_resumed_join_uses_persisted_preflight_executable(self) -> None:
        from pa.config import Settings
        from pa.fleet.registry import FleetRegistry

        with tempfile.TemporaryDirectory() as tmp:
            data_dir = Path(tmp)
            settings = Settings(
                data_dir=data_dir,
                instance_url="http://owner:8080",
                subscribed_realms=["default"],
                sync_token="sync-token",
            )
            fleet = FleetRegistry(data_dir, settings.fleet_id)
            store = BootstrapJobStore(data_dir)
            job, _ = store.create(
                _request(existing_install_action="join_only"),
                idempotency_key="resume-join",
                actor="user:local",
                authority_instance_id="authority-1",
                authority_url="http://owner:8080",
                discovery=_discovery(),
            )
            job.phase_record(BootstrapPhase.RESOLVE_TARGET).state = PhaseState.SUCCEEDED
            job.phase_record(BootstrapPhase.PREFLIGHT_HOST).state = PhaseState.SUCCEEDED
            job.checkpoints[BootstrapPhase.PREFLIGHT_HOST.value] = {
                "pa": "/home/peter/.local/bin/pa",
            }
            store.save(job)
            captured: list[RemoteInstallRequest] = []

            async def fail_after_capture(
                settings, fleet, legacy_store, legacy_job, remote, **kwargs
            ):
                captured.append(remote)
                legacy_job.status = InstallJobStatus.FAILED
                legacy_job.error = "captured"
                return legacy_job

            with patch(
                "pa.fleet.bootstrap.run_install_job",
                side_effect=fail_after_capture,
            ):
                await run_bootstrap_job(
                    settings,
                    fleet,
                    store,
                    job,
                    domain_store=MagicMock(),
                    author_instance_id="authority-1",
                )

            self.assertEqual(len(captured), 1)
            self.assertEqual(
                captured[0].pa_executable,
                "/home/peter/.local/bin/pa",
            )

    async def test_strict_connection_does_not_disable_known_hosts(self) -> None:
        request = RemoteInstallRequest(
            host="mini",
            user="peter",
            instance_name="mini",
            instance_url="http://mini:8080",
        )
        connection = MagicMock()
        with patch("asyncssh.connect", AsyncMock(return_value=connection)) as connect:
            self.assertIs(await _connect_ssh(request), connection)
        kwargs = connect.await_args.kwargs
        self.assertNotIn("known_hosts", kwargs)

    async def test_pinned_connection_refuses_changed_fingerprint(self) -> None:
        request = RemoteInstallRequest(
            host="mini",
            user="peter",
            instance_name="mini",
            instance_url="http://mini:8080",
            host_key_policy="pinned",
            host_key_fingerprint="SHA256:expected",
        )
        key = MagicMock()
        key.get_fingerprint.return_value = "SHA256:actual"
        with (
            patch(
                "asyncssh.get_server_host_key",
                AsyncMock(return_value=key),
            ),
            patch("asyncssh.connect", AsyncMock()) as connect,
        ):
            with self.assertRaisesRegex(ValueError, "fingerprint mismatch"):
                await _connect_ssh(request)
        connect.assert_not_awaited()

    def test_release_ref_pins_installer_source(self) -> None:
        request = RemoteInstallRequest(
            host="mini",
            user="peter",
            instance_name="mini",
            instance_url="http://mini:8080",
            release_ref="abc123",
        )
        self.assertIn("/abc123/scripts/install-remote.sh", _install_script_url(request))

    def test_legacy_log_replaces_exact_one_shot_secrets(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = InstallJobStore(Path(tmp))
            request = RemoteInstallRequest(
                host="mini",
                user="peter",
                instance_name="mini",
                instance_url="http://mini:8080",
                password="literal-password",
                passphrase="literal-passphrase",
            )
            job = store.create(request)
            job.append("bad remote echoed literal-password and literal-passphrase")
            store._persist(job)
            blob = (
                json.dumps(job.to_public_dict())
                + (Path(tmp) / "fleet_jobs" / f"{job.job_id}.json").read_text()
            )
            self.assertNotIn("literal-password", blob)
            self.assertNotIn("literal-passphrase", blob)

from __future__ import annotations

import asyncio
import tempfile
from pathlib import Path
from types import SimpleNamespace
from typing import ClassVar, Self
from unittest.mock import patch

import pytest

from pa.cli import service
from pa.config import Settings
from pa.fleet import credentials as credentials_module
from pa.fleet.credentials import (
    CredentialRotationStore,
    RotationConflict,
    apply_overlap,
    revoke_previous,
)


def settings_for(root: Path, *, peers: list[str] | None = None) -> Settings:
    return Settings(
        data_dir=root,
        instance_name="test",
        sync_token="old-fleet-secret-value",
        peers=peers or [],
    )


def test_systemd_uses_protected_credential_without_plaintext() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        settings = settings_for(Path(tmp))
        unit = service.render_systemd_unit(settings, Path("/usr/bin/pa"))
        assert "old-fleet-secret-value" not in unit
        assert "Environment=PA_SYNC_TOKEN=" not in unit
        assert "LoadCredential=pa_sync_token:" in unit
        assert "Environment=PA_SYNC_TOKEN_FILE=%d/pa_sync_token" in unit


def test_launchd_contains_only_private_credential_path() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        settings = settings_for(Path(tmp))
        plist = service.render_plist(settings, Path("/usr/bin/pa")).decode()
        assert "old-fleet-secret-value" not in plist
        assert "PA_SYNC_TOKEN_FILE" in plist
        assert str(service.sync_credential_path(settings)) in plist


def test_credential_file_enforces_mode_and_replaces_value() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        settings = settings_for(Path(tmp))
        path = service.install_sync_credential(settings)
        assert path is not None
        assert path.read_text().strip() == settings.sync_token
        assert path.stat().st_mode & 0o777 == 0o600
        path.chmod(0o644)
        service.install_sync_credential(settings)
        assert path.stat().st_mode & 0o777 == 0o600


def test_legacy_detection_never_returns_credential() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        unit = Path(tmp) / "pa.service"
        unit.write_text("Environment=PA_SYNC_TOKEN=do-not-print-this\n")
        with (
            patch.object(service, "_plist_path", return_value=Path(tmp) / "missing"),
            patch.object(service, "_systemd_unit_path", return_value=unit),
        ):
            assert service.legacy_plaintext_sync_credential() is True


def test_rotation_is_private_audited_and_idempotent() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        settings = settings_for(Path(tmp), peers=["http://peer-a", "http://peer-b"])
        store = CredentialRotationStore(settings.data_dir)
        first = store.begin(settings, idempotency_key="operator-1", peers=settings.peers)
        duplicate = store.begin(
            settings, idempotency_key="operator-1", peers=settings.peers
        )
        assert duplicate["operation_id"] == first["operation_id"]
        assert store.path.stat().st_mode & 0o777 == 0o600
        public = store.public(first)
        assert public is not None
        assert "old_token" not in public and "new_token" not in public
        assert set(public["peers"]) == set(settings.peers)
        with pytest.raises(RotationConflict):
            store.begin(settings, idempotency_key="concurrent", peers=[])


def test_overlap_rollback_and_revocation_persist_without_manual_edits() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        settings = settings_for(Path(tmp))
        old = settings.sync_token
        apply_overlap(settings, "new-fleet-secret-value-that-is-long-enough")
        assert settings.sync_token_previous == [old]
        assert service.sync_credential_path(settings).read_text().strip() == settings.sync_token
        from pa.domain.instance_config import load_instance_config

        persisted = load_instance_config(settings.data_dir)
        assert persisted is not None and persisted.sync_token_previous == [old]
        import hashlib

        fingerprint = hashlib.sha256(old.encode()).hexdigest()[:12]
        revoke_previous(settings, fingerprint)
        assert settings.sync_token_previous == []


class _Response:
    def __init__(self, status_code: int = 200) -> None:
        self.status_code = status_code


class _Client:
    outcomes: ClassVar[dict[str, object]] = {}

    def __init__(self, **_kwargs: object) -> None:
        pass

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def post(self, url: str, **_kwargs: object) -> _Response:
        outcome = self.outcomes.get(url, _Response())
        if isinstance(outcome, BaseException):
            raise outcome
        assert isinstance(outcome, _Response)
        return outcome


def _request(settings: Settings) -> SimpleNamespace:
    ctx = SimpleNamespace(settings=settings)
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(ctx=ctx)))


def test_rollout_tracks_online_offline_and_resumes_after_interruption() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        settings = settings_for(Path(tmp), peers=["http://online", "http://offline"])
        store = CredentialRotationStore(settings.data_dir)
        state = store.begin(settings, idempotency_key="resume", peers=settings.peers)
        _Client.outcomes = {
            "http://online/api/fleet/credentials/apply": _Response(),
            "http://offline/api/fleet/credentials/apply": OSError("offline"),
        }
        with patch.object(credentials_module.httpx, "AsyncClient", _Client):
            public = asyncio.run(
                credentials_module.rollout(_request(settings), state)
            )
        assert public["status"] == "waiting_for_peers"
        assert public["peers"]["http://online"]["state"] == "updated"
        assert public["peers"]["http://offline"]["state"] == "offline"
        assert settings.sync_token_previous == ["old-fleet-secret-value"]

        persisted = store.load()
        assert persisted is not None
        _Client.outcomes = {
            "http://offline/api/fleet/credentials/apply": _Response(),
        }
        with patch.object(credentials_module.httpx, "AsyncClient", _Client):
            resumed = asyncio.run(
                credentials_module.rollout(_request(settings), persisted)
            )
        assert resumed["status"] == "ready_to_revoke"
        assert resumed["peers"]["http://online"]["attempts"] == 1
        assert resumed["peers"]["http://offline"]["attempts"] == 2

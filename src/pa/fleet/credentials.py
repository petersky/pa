"""Audited, resumable fleet sync-credential rotation state."""

from __future__ import annotations

import hashlib
import os
import secrets
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from pa.auth.middleware import require_user
from pa.config import Settings
from pa.core.io import atomic_write_json
from pa.core.logging import redact_log_text
from pa.domain.instance_config import update_instance_config

router = APIRouter()
PEER_TIMEOUT = 5.0


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _fingerprint(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()[:12] if token else "none"


class RotationConflict(RuntimeError):
    pass


class CredentialRotationStore:
    def __init__(self, data_dir: Path) -> None:
        self.root = data_dir / "credentials"
        self.path = self.root / "fleet-sync-rotation.json"
        self.lock_path = self.root / "fleet-sync-rotation.lock"

    @contextmanager
    def locked(self) -> Iterator[None]:
        self.root.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.lock_path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            import fcntl

            fcntl.flock(fd, fcntl.LOCK_EX)
            yield
        finally:
            os.close(fd)

    def load(self) -> dict[str, Any] | None:
        if not self.path.exists():
            return None
        import json

        try:
            value = json.loads(self.path.read_text())
        except OSError, ValueError:
            return None
        return value if isinstance(value, dict) else None

    def save(self, state: dict[str, Any]) -> None:
        atomic_write_json(self.path, state, mode=0o600)
        self.path.chmod(0o600)

    @staticmethod
    def public(state: dict[str, Any] | None) -> dict[str, Any] | None:
        if state is None:
            return None
        return {
            key: value
            for key, value in state.items()
            if key not in {"new_token", "old_token"}
        }

    def begin(
        self, settings: Settings, *, idempotency_key: str, peers: list[str]
    ) -> dict[str, Any]:
        with self.locked():
            existing = self.load()
            if existing and existing.get("status") not in {"revoked", "rolled_back"}:
                if existing.get("idempotency_key") == idempotency_key:
                    return existing
                raise RotationConflict(
                    f"rotation {existing.get('operation_id')} is already active"
                )
            old = settings.sync_token
            if not old:
                raise ValueError("fleet sync credential is not configured")
            new = secrets.token_urlsafe(48)
            state: dict[str, Any] = {
                "operation_id": secrets.token_hex(16),
                "idempotency_key": idempotency_key,
                "status": "rolling_out",
                "created_at": _now(),
                "updated_at": _now(),
                "old_fingerprint": _fingerprint(old),
                "new_fingerprint": _fingerprint(new),
                "old_token": old,
                "new_token": new,
                "peers": {peer: {"state": "pending", "attempts": 0} for peer in peers},
                "audit": [{"at": _now(), "action": "rotation_started"}],
            }
            self.save(state)
            return state


def apply_overlap(settings: Settings, new_token: str) -> None:
    old = settings.sync_token
    previous = [
        item
        for item in [old, *settings.sync_token_previous]
        if item and item != new_token
    ]
    settings.sync_token = new_token
    settings.sync_token_previous = list(dict.fromkeys(previous))[:2]
    update_instance_config(
        settings.data_dir,
        sync_token=settings.sync_token,
        sync_token_previous=settings.sync_token_previous,
    )
    from pa.cli.service import install_sync_credential

    install_sync_credential(settings)


def revoke_previous(settings: Settings, fingerprint: str) -> None:
    settings.sync_token_previous = [
        token
        for token in settings.sync_token_previous
        if _fingerprint(token) != fingerprint
    ]
    update_instance_config(
        settings.data_dir, sync_token_previous=settings.sync_token_previous
    )


class RotationRequest(BaseModel):
    idempotency_key: str = Field(min_length=1, max_length=200)


class ApplyRequest(BaseModel):
    operation_id: str = Field(min_length=1, max_length=100)
    token: str = Field(min_length=32, max_length=512)


class RevokeRequest(BaseModel):
    operation_id: str = Field(min_length=1, max_length=100)
    fingerprint: str = Field(min_length=12, max_length=64)


async def rollout(request: Request, state: dict[str, Any]) -> dict[str, Any]:
    settings = request.app.state.ctx.settings
    store = CredentialRotationStore(settings.data_dir)
    for peer, progress in state["peers"].items():
        if progress.get("state") == "updated":
            continue
        progress["attempts"] = int(progress.get("attempts", 0)) + 1
        progress["last_attempt_at"] = _now()
        try:
            async with httpx.AsyncClient(timeout=PEER_TIMEOUT) as client:
                response = await client.post(
                    f"{peer.rstrip('/')}/api/fleet/credentials/apply",
                    json={"operation_id": state["operation_id"], "token": state["new_token"]},
                    headers={"Authorization": f"Bearer {state['old_token']}"},
                )
            if response.status_code >= 400:
                raise RuntimeError(f"peer rejected credential update ({response.status_code})")
            progress.update(state="updated", updated_at=_now())
            progress.pop("error", None)
        except (httpx.HTTPError, OSError, RuntimeError) as exc:
            progress.update(state="offline", error=redact_log_text(exc))
        state["updated_at"] = _now()
        store.save(state)
    state["status"] = "ready_to_revoke" if all(
        item.get("state") == "updated" for item in state["peers"].values()
    ) else "waiting_for_peers"
    if settings.sync_token != state["new_token"]:
        apply_overlap(settings, state["new_token"])
    state["audit"].append({"at": _now(), "action": "rollout_attempted"})
    store.save(state)
    return store.public(state) or {}


@router.get("/fleet/credentials/rotation")
def rotation_status(request: Request) -> dict[str, Any]:
    require_user(request)
    store = CredentialRotationStore(request.app.state.ctx.settings.data_dir)
    return store.public(store.load()) or {"status": "idle"}


@router.post("/fleet/credentials/rotate")
async def rotate(request: Request, body: RotationRequest) -> dict[str, Any]:
    require_user(request)
    settings = request.app.state.ctx.settings
    store = CredentialRotationStore(settings.data_dir)
    try:
        state = store.begin(settings, idempotency_key=body.idempotency_key.strip(), peers=list(dict.fromkeys(settings.peers)))
    except RotationConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return await rollout(request, state)


@router.post("/fleet/credentials/rotation/retry")
async def retry(request: Request) -> dict[str, Any]:
    require_user(request)
    store = CredentialRotationStore(request.app.state.ctx.settings.data_dir)
    state = store.load()
    if not state or state.get("status") in {"revoked", "rolled_back"}:
        raise HTTPException(status_code=409, detail="no active credential rotation")
    return await rollout(request, state)


@router.post("/fleet/credentials/apply")
def apply_peer(request: Request, body: ApplyRequest) -> dict[str, Any]:
    if not request.state.instance_authenticated:
        raise HTTPException(status_code=403, detail="fleet instance authentication required")
    apply_overlap(request.app.state.ctx.settings, body.token)
    return {"operation_id": body.operation_id, "state": "updated"}


@router.post("/fleet/credentials/rotation/revoke")
async def finish(request: Request, force: bool = False) -> dict[str, Any]:
    require_user(request)
    settings = request.app.state.ctx.settings
    store = CredentialRotationStore(settings.data_dir)
    state = store.load()
    if not state or state.get("status") in {"revoked", "rolled_back"}:
        raise HTTPException(status_code=409, detail="no active credential rotation")
    pending = [peer for peer, item in state["peers"].items() if item.get("state") != "updated"]
    if pending and not force:
        raise HTTPException(status_code=409, detail="offline peers remain; retry or explicitly force revocation")
    for peer, progress in state["peers"].items():
        if progress.get("state") != "updated":
            continue
        try:
            async with httpx.AsyncClient(timeout=PEER_TIMEOUT) as client:
                response = await client.post(
                    f"{peer.rstrip('/')}/api/fleet/credentials/revoke",
                    json={"operation_id": state["operation_id"], "fingerprint": state["old_fingerprint"]},
                    headers={"Authorization": f"Bearer {state['new_token']}"},
                )
            if response.status_code >= 400:
                raise RuntimeError(f"peer rejected revocation ({response.status_code})")
            progress["revoked_at"] = _now()
        except (httpx.HTTPError, OSError, RuntimeError) as exc:
            if not force:
                progress["error"] = redact_log_text(exc)
                store.save(state)
                raise HTTPException(status_code=503, detail="peer revocation incomplete; safe retry is available") from exc
    revoke_previous(settings, state["old_fingerprint"])
    state.update(
        status="revoked_with_offline_peers" if pending else "revoked",
        revoked_at=_now(),
    )
    state["audit"].append({"at": state["revoked_at"], "action": "old_credential_revoked", "forced": force})
    store.save(state)
    return store.public(state) or {}


@router.post("/fleet/credentials/revoke")
def revoke_peer(request: Request, body: RevokeRequest) -> dict[str, Any]:
    if not request.state.instance_authenticated:
        raise HTTPException(status_code=403, detail="fleet instance authentication required")
    revoke_previous(request.app.state.ctx.settings, body.fingerprint)
    return {"operation_id": body.operation_id, "state": "revoked"}


@router.post("/fleet/credentials/rotation/rollback")
async def rollback(request: Request) -> dict[str, Any]:
    require_user(request)
    settings = request.app.state.ctx.settings
    store = CredentialRotationStore(settings.data_dir)
    state = store.load()
    if not state or state.get("status") in {"revoked", "rolled_back"}:
        raise HTTPException(status_code=409, detail="no reversible credential rotation")
    for peer, progress in state["peers"].items():
        if progress.get("state") != "updated":
            continue
        try:
            async with httpx.AsyncClient(timeout=PEER_TIMEOUT) as client:
                response = await client.post(
                    f"{peer.rstrip('/')}/api/fleet/credentials/apply",
                    json={"operation_id": state["operation_id"], "token": state["old_token"]},
                    headers={"Authorization": f"Bearer {state['new_token']}"},
                )
            if response.status_code >= 400:
                raise RuntimeError("peer rejected rollback")
            progress["rolled_back_at"] = _now()
        except (httpx.HTTPError, OSError, RuntimeError) as exc:
            progress["rollback_error"] = redact_log_text(exc)
            store.save(state)
            raise HTTPException(status_code=503, detail="peer rollback incomplete; retry recovery") from exc
    apply_overlap(settings, state["old_token"])
    revoke_previous(settings, state["new_fingerprint"])
    state.update(status="rolled_back", rolled_back_at=_now())
    state["audit"].append({"at": state["rolled_back_at"], "action": "rotation_rolled_back"})
    store.save(state)
    return store.public(state) or {}

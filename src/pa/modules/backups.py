from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field

from pa.auth.middleware import get_principal_id, require_user
from pa.backup.models import BackupConfig
from pa.backup.service import BackupError, BackupService, validate_destination
from pa.core.context import AppContext
from pa.core.contracts import Module
from pa.domain.instance_config import update_instance_config

router = APIRouter()


class TriggerRequest(BaseModel):
    idempotency_key: str | None = Field(default=None, min_length=1, max_length=200)


class RestoreStartRequest(BaseModel):
    backup_id: str
    confirm_instance_id: str


def _service(request: Request) -> BackupService:
    return request.app.state.ctx.require_service("backup_service")


def _admin(request: Request):
    user = require_user(request)
    if getattr(user, "role", None) != "admin":
        raise HTTPException(
            status_code=403,
            detail={
                "code": "backup_admin_required",
                "message": "Only an administrator may change backup or restore state.",
            },
        )
    return user


def _raise(exc: BackupError) -> None:
    status = 404 if exc.code.endswith("not_found") else 409
    if exc.code in {
        "unsafe_destination",
        "unsafe_recursive_destination",
        "confirmation_mismatch",
        "instance_mismatch",
    }:
        status = 422
    raise HTTPException(status_code=status, detail=exc.detail()) from exc


@router.get("/backups/status")
def backup_status(request: Request) -> dict[str, Any]:
    return _service(request).status()


@router.get("/backups")
def list_backups(request: Request, verify: bool = False) -> list[dict[str, Any]]:
    if verify:
        _admin(request)
    return [
        item.public_dict() for item in _service(request).list_backups(verify=verify)
    ]


@router.get("/backups/config")
def backup_config(request: Request) -> dict[str, Any]:
    status = _service(request).status()
    return {
        key: status[key]
        for key in ("configured", "effective", "sources", "destination_health")
    }


@router.patch("/backups/config")
def update_backup_config(request: Request, body: dict[str, Any]) -> dict[str, Any]:
    _admin(request)
    service = _service(request)
    unknown = set(body) - set(BackupConfig.model_fields)
    if unknown:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "unknown_backup_config",
                "message": "Unknown backup fields: " + ", ".join(sorted(unknown)),
            },
        )
    try:
        config = BackupConfig.model_validate({**service.config.model_dump(), **body})
        validate_destination(service.settings, config.destination_dir)
    except (ValueError, BackupError) as exc:
        if isinstance(exc, BackupError):
            _raise(exc)
        raise HTTPException(
            status_code=422,
            detail={"code": "invalid_backup_config", "message": str(exc)},
        ) from exc
    updates = {
        "backup_" + key: (str(value) if isinstance(value, Path) else value)
        for key, value in config.model_dump().items()
    }
    update_instance_config(
        service.settings.data_dir,
        instance_id=service.settings.instance_id,
        instance_name=service.settings.instance_name,
        data_dir=str(service.settings.data_dir),
        fleet_id=service.settings.fleet_id,
        **updates,
    )
    for key, value in updates.items():
        if key == "backup_destination_dir":
            value = Path(value)
        setattr(service.settings, key, value)
    service.apply_config(config)
    return service.status()


@router.post("/backups", status_code=201)
async def trigger_backup(request: Request, body: TriggerRequest) -> dict[str, Any]:
    _admin(request)
    key = request.headers.get("Idempotency-Key") or body.idempotency_key
    if not key or len(key) > 200:
        raise HTTPException(
            status_code=422,
            detail={
                "code": "idempotency_key_required",
                "message": "Provide a stable Idempotency-Key of 1–200 characters.",
            },
        )
    runtime = request.app.state.ctx.require_service("async_runtime")
    run = await runtime.run_blocking(
        "backup.manual",
        _service(request).run_backup,
        trigger="manual",
        idempotency_key=key,
        timeout=max(300.0, _service(request).config.interval_seconds),
    )
    if run.status == "failed":
        raise HTTPException(
            status_code=503,
            detail={"code": "backup_failed", "message": run.failure_reason},
        )
    return run.model_dump(mode="json")


@router.get("/backups/{backup_id}")
def inspect_backup(request: Request, backup_id: str) -> dict[str, Any]:
    try:
        return _service(request).inspect_backup(backup_id).public_dict()
    except BackupError as exc:
        _raise(exc)


@router.post("/backups/{backup_id}/verify")
async def verify_backup(request: Request, backup_id: str) -> dict[str, Any]:
    _admin(request)
    runtime = request.app.state.ctx.require_service("async_runtime")
    try:
        record = await runtime.run_blocking(
            "backup.verify",
            _service(request).verify_backup,
            backup_id,
            timeout=300.0,
        )
    except BackupError as exc:
        _raise(exc)
    if not record.verified:
        raise HTTPException(
            status_code=409,
            detail={
                "code": "backup_verification_failed",
                "message": record.verification_error,
            },
        )
    return record.public_dict()


@router.delete("/backups/{backup_id}", status_code=204)
def delete_backup(request: Request, backup_id: str) -> None:
    _admin(request)
    try:
        _service(request).delete_backup(backup_id)
    except BackupError as exc:
        _raise(exc)


@router.get("/backups/{backup_id}/download")
def download_backup(request: Request, backup_id: str) -> FileResponse:
    _admin(request)
    try:
        path = _service(request).download_path(backup_id)
    except BackupError as exc:
        _raise(exc)
    return FileResponse(
        path,
        media_type="application/vnd.pa.metadata-backup",
        filename=path.name,
    )


@router.get("/backups/{backup_id}/export-info")
def backup_export_info(request: Request, backup_id: str) -> dict[str, Any]:
    _admin(request)
    try:
        path = _service(request).download_path(backup_id)
    except BackupError as exc:
        _raise(exc)
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return {
        "backup_id": backup_id,
        "filename": path.name,
        "size_bytes": path.stat().st_size,
        "sha256": digest.hexdigest(),
        "download_url": f"/api/backups/{backup_id}/download",
        "authorization": "PA administrator session required",
    }


@router.post("/backups/restores", status_code=202)
def initiate_restore(request: Request, body: RestoreStartRequest) -> dict[str, Any]:
    _admin(request)
    try:
        restore = _service(request).initiate_restore(
            body.backup_id,
            requested_by=get_principal_id(request),
            confirm_instance_id=body.confirm_instance_id,
        )
    except BackupError as exc:
        _raise(exc)
    return restore.model_dump(mode="json")


@router.get("/backups/restores/{restore_id}")
def restore_status(request: Request, restore_id: str) -> dict[str, Any]:
    try:
        return _service(request).get_restore(restore_id).model_dump(mode="json")
    except BackupError as exc:
        _raise(exc)


class BackupsModule(Module):
    @property
    def name(self) -> str:
        return "backups"

    @property
    def version(self) -> str:
        return "1.0.0"

    @property
    def description(self) -> str:
        return "Scheduled verified metadata backups and guarded restore workflows"

    def on_load(self, ctx: AppContext) -> None:
        ctx.register_service("backup_service", BackupService(ctx.settings, ctx.store))

    async def on_startup(self, app, ctx: AppContext) -> None:
        await ctx.require_service("backup_service").start(
            ctx.require_service("async_runtime")
        )

    async def on_shutdown(self, app, ctx: AppContext) -> None:
        await ctx.require_service("backup_service").stop()

    def api_routers(self):
        return [("/api", router, ["backups"])]

    def register_mcp(self, mcp, ctx) -> None:
        from pa.mcp.tools.backups import register_mcp

        register_mcp(mcp, ctx)

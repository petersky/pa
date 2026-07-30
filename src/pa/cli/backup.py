from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated, Any
from uuid import uuid4

import httpx
import typer

from pa.auth.users import UserDirectory
from pa.config import get_settings
from pa.core.writer_lock import DataDirAlreadyOwnedError, DataDirWriterLock
from pa.mcp.local_api import local_pa_url, request_local_pa

backup_app = typer.Typer(
    help="Scheduled verified metadata backups and guarded restore",
    no_args_is_help=True,
)


def _json(value: Any) -> None:
    typer.echo(json.dumps(value, indent=2, default=str))


def _api(method: str, path: str, *, json_body: dict | None = None, params=None):
    return request_local_pa(get_settings(), method, path, json=json_body, params=params)


@backup_app.command("status")
def status(as_json: Annotated[bool, typer.Option("--json")] = False) -> None:
    """Show schedule, destination health, storage, and recent history."""
    data = _api("GET", "/api/backups/status")
    if as_json:
        _json(data)
        return
    destination = data["destination_health"]
    typer.echo(f"Enabled:       {data['effective']['enabled']}")
    typer.echo(f"Interval:      {data['effective']['interval_seconds']} seconds")
    typer.echo(f"Next run:      {data.get('next_scheduled_run') or '—'}")
    typer.echo(f"Last attempt:  {data.get('last_attempt') or '—'}")
    typer.echo(f"Last success:  {data.get('last_success') or '—'}")
    typer.echo(f"Destination:   {destination['path']}")
    typer.echo(
        f"Health:        {'writable' if destination['writable'] else destination.get('error') or 'unhealthy'}"
    )
    typer.echo(
        f"Stored:        {data['backup_count']} backups / {data['storage_used_bytes']} bytes"
    )
    typer.echo(f"Failures:      {data['consecutive_failures']}")


@backup_app.command("run")
def run(
    idempotency_key: Annotated[
        str | None, typer.Option(help="Stable retry key")
    ] = None,
) -> None:
    """Trigger an immediate idempotent online backup."""
    result = _api(
        "POST",
        "/api/backups",
        json_body={"idempotency_key": idempotency_key or f"cli:{uuid4()}"},
    )
    _json(result)


@backup_app.command("list")
def list_cmd(
    verify: Annotated[bool, typer.Option(help="Re-run full verification")] = False,
    as_json: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """List retained backups and verification status."""
    records = _api("GET", "/api/backups", params={"verify": verify})
    if as_json:
        _json(records)
        return
    if not records:
        typer.echo("No backups.")
        return
    for item in records:
        state = "verified" if item["verified"] else "CORRUPT"
        typer.echo(
            f"{item['backup_id']}  {item['created_at']}  "
            f"{item['size_bytes']} bytes  {state}"
        )


@backup_app.command("inspect")
def inspect(backup_id: str) -> None:
    """Inspect and verify a backup manifest."""
    _json(_api("GET", f"/api/backups/{backup_id}"))


@backup_app.command("verify")
def verify(backup_id: str) -> None:
    """Re-run integrity and manifest verification."""
    _json(_api("POST", f"/api/backups/{backup_id}/verify"))


@backup_app.command("delete")
def delete(
    backup_id: str,
    yes: Annotated[bool, typer.Option("--yes", "-y")] = False,
) -> None:
    """Delete one explicit backup, preserving the last known-good copy."""
    if not yes and not typer.confirm(f"Delete backup {backup_id}?"):
        raise typer.Abort()
    _api("DELETE", f"/api/backups/{backup_id}")
    typer.echo(f"Deleted {backup_id}.")


@backup_app.command("config")
def config(
    enabled: Annotated[bool | None, typer.Option("--enabled/--disabled")] = None,
    interval_seconds: Annotated[int | None, typer.Option()] = None,
    retention_count: Annotated[int | None, typer.Option()] = None,
    retention_max_age_seconds: Annotated[int | None, typer.Option()] = None,
    retention_max_total_bytes: Annotated[int | None, typer.Option()] = None,
    destination_dir: Annotated[Path | None, typer.Option()] = None,
    run_on_startup: Annotated[
        bool | None, typer.Option("--run-on-startup/--no-run-on-startup")
    ] = None,
    startup_min_age_seconds: Annotated[int | None, typer.Option()] = None,
    verification_level: Annotated[str | None, typer.Option()] = None,
    compression: Annotated[
        bool | None, typer.Option("--compression/--no-compression")
    ] = None,
    io_limit_mib_per_second: Annotated[float | None, typer.Option()] = None,
    concurrency: Annotated[int | None, typer.Option()] = None,
    alert_after_failures: Annotated[int | None, typer.Option()] = None,
    jitter_seconds: Annotated[int | None, typer.Option()] = None,
    patch: Annotated[
        str | None, typer.Option(help="Additional JSON object for optional limits")
    ] = None,
) -> None:
    """Show configuration or validate and persist supplied fields."""
    changes = {
        key: value
        for key, value in {
            "enabled": enabled,
            "interval_seconds": interval_seconds,
            "retention_count": retention_count,
            "retention_max_age_seconds": retention_max_age_seconds,
            "retention_max_total_bytes": retention_max_total_bytes,
            "destination_dir": str(destination_dir.resolve())
            if destination_dir
            else None,
            "run_on_startup": run_on_startup,
            "startup_min_age_seconds": startup_min_age_seconds,
            "verification_level": verification_level,
            "compression": compression,
            "io_limit_mib_per_second": io_limit_mib_per_second,
            "concurrency": concurrency,
            "alert_after_failures": alert_after_failures,
            "jitter_seconds": jitter_seconds,
        }.items()
        if value is not None
    }
    if patch:
        try:
            extra = json.loads(patch)
        except json.JSONDecodeError as exc:
            raise typer.BadParameter(f"invalid JSON patch: {exc}") from exc
        if not isinstance(extra, dict):
            raise typer.BadParameter("patch must be a JSON object")
        changes.update(extra)
    if changes:
        _json(_api("PATCH", "/api/backups/config", json_body=changes))
    else:
        _json(_api("GET", "/api/backups/config"))


@backup_app.command("export")
def export(
    backup_id: str,
    output: Annotated[Path, typer.Option("-o", "--output")],
) -> None:
    """Download an authorized verified backup archive."""
    settings = get_settings()
    token = UserDirectory(settings.data_dir).ensure_default_user().cli_token
    with httpx.stream(
        "GET",
        f"{local_pa_url(settings)}/api/backups/{backup_id}/download",
        headers={"Authorization": f"Bearer {token}"},
        timeout=300,
    ) as response:
        response.raise_for_status()
        output = output.expanduser().resolve()
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = output.with_name(f".{output.name}.{uuid4().hex}.tmp")
        try:
            with temporary.open("xb") as handle:
                for chunk in response.iter_bytes():
                    handle.write(chunk)
            temporary.chmod(0o600)
            temporary.replace(output)
        finally:
            temporary.unlink(missing_ok=True)
    typer.echo(f"Exported {backup_id} to {output}.")


@backup_app.command("restore-initiate")
def restore_initiate(backup_id: str) -> None:
    """Create the guarded maintenance request while PA is running."""
    settings = get_settings()
    _json(
        _api(
            "POST",
            "/api/backups/restores",
            json_body={
                "backup_id": backup_id,
                "confirm_instance_id": settings.instance_id,
            },
        )
    )


@backup_app.command("restore-status")
def restore_status(restore_id: str) -> None:
    """Monitor a guarded restore request."""
    _json(_api("GET", f"/api/backups/restores/{restore_id}"))


@backup_app.command("restore")
def restore(
    backup_id: str,
    request_id: Annotated[str | None, typer.Option()] = None,
    yes: Annotated[bool, typer.Option("--yes", "-y")] = False,
) -> None:
    """Restore while the PA writer is stopped, with an automatic rollback point."""
    settings = get_settings()
    if not yes:
        typer.echo(
            "This replaces the local projection, durable sync refs, and event-log objects."
        )
        if not typer.confirm(
            f"Confirm restore of {backup_id} onto instance {settings.instance_id}?"
        ):
            raise typer.Abort()
    writer = DataDirWriterLock(settings.data_dir)
    try:
        writer.acquire()
    except DataDirAlreadyOwnedError as exc:
        typer.echo(
            f"{exc}\nStop PA first (`pa stop`) and retry; restore never overwrites a running writer.",
            err=True,
        )
        raise typer.Exit(2) from exc
    try:
        from pa.backup.service import BackupService

        result = BackupService(settings, None).restore_offline(
            backup_id, request_id=request_id
        )
    finally:
        writer.release()
    _json(result.model_dump(mode="json"))
    if result.status == "failed":
        raise typer.Exit(1)

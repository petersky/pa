from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer

from pa.config import get_settings
from pa.mcp.local_api import LocalPAServerUnavailable, request_local_pa

telemetry_app = typer.Typer(help="Resource telemetry, history, and retention")


def _request(method: str, path: str, *, body: dict | None = None):
    try:
        return request_local_pa(get_settings(), method, path, json=body)
    except LocalPAServerUnavailable as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc


def _print(payload) -> None:
    typer.echo(json.dumps(payload, indent=2, sort_keys=True))


@telemetry_app.command("live")
def live(
    session: Annotated[
        str | None, typer.Option("--session", help="Limit to one session ID")
    ] = None,
) -> None:
    """Read fresh instance or session telemetry."""
    path = "/api/telemetry/live"
    if session:
        path += f"?scope_type=session&scope_id={session}"
    _print(_request("GET", path))


@telemetry_app.command("status")
def status() -> None:
    """Inspect collection, storage, drops, and retention state."""
    _print(_request("GET", "/api/telemetry/health"))


@telemetry_app.command("query")
def query(
    range: Annotated[
        str, typer.Option("--range", help="15m, 1h, 6h, 24h, 7d, 30d")
    ] = "1h",
    session: Annotated[str | None, typer.Option("--session")] = None,
    metric: Annotated[list[str] | None, typer.Option("--metric")] = None,
) -> None:
    """Query bounded, server-aggregated historical series."""
    _print(
        _request(
            "POST",
            "/api/telemetry/query",
            body={
                "range": range,
                "scope_type": "session" if session else None,
                "scope_ids": [session] if session else [],
                "metrics": metric or [],
            },
        )
    )


@telemetry_app.command("configure")
def configure(
    enabled: Annotated[bool | None, typer.Option("--enabled/--disabled")] = None,
    live_interval: Annotated[float | None, typer.Option("--live-interval")] = None,
    persistence_interval: Annotated[
        float | None, typer.Option("--persistence-interval")
    ] = None,
    raw_retention_hours: Annotated[
        float | None, typer.Option("--raw-retention-hours")
    ] = None,
    rollup_retention_hours: Annotated[
        float | None, typer.Option("--rollup-retention-hours")
    ] = None,
    max_database_bytes: Annotated[
        int | None, typer.Option("--max-database-bytes")
    ] = None,
    database_path: Annotated[Path | None, typer.Option("--database-path")] = None,
    per_session: Annotated[
        bool | None, typer.Option("--per-session/--no-per-session")
    ] = None,
    ui_refresh: Annotated[float | None, typer.Option("--ui-refresh")] = None,
    default_range: Annotated[str | None, typer.Option("--default-range")] = None,
) -> None:
    """Validate and persist telemetry collection and retention settings."""
    values = {
        "enabled": enabled,
        "live_interval_seconds": live_interval,
        "persistence_interval_seconds": persistence_interval,
        "raw_retention_hours": raw_retention_hours,
        "rollup_retention_hours": rollup_retention_hours,
        "max_database_bytes": max_database_bytes,
        "database_path": str(database_path) if database_path else None,
        "per_session_enabled": per_session,
        "ui_refresh_seconds": ui_refresh,
        "default_report_range": default_range,
    }
    body = {key: value for key, value in values.items() if value is not None}
    if not body:
        _print(_request("GET", "/api/telemetry/config"))
        return
    _print(_request("PATCH", "/api/telemetry/config", body=body))


@telemetry_app.command("prune")
def prune(
    compact: Annotated[bool, typer.Option(help="VACUUM after bounded pruning")] = False,
) -> None:
    """Trigger safe deterministic pruning or compaction."""
    _print(
        _request(
            "POST",
            "/api/telemetry/maintenance",
            body={"action": "compact" if compact else "prune"},
        )
    )


@telemetry_app.command("export")
def export_slice(
    output: Annotated[Path, typer.Option("--output", "-o")],
    range: Annotated[str, typer.Option("--range")] = "15m",
    session: Annotated[str | None, typer.Option("--session")] = None,
) -> None:
    """Write a bounded, redacted diagnostic slice."""
    path = f"/api/telemetry/export?range={range}"
    if session:
        path += f"&scope_type=session&scope_id={session}"
    payload = _request("GET", path)
    output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    typer.echo(str(output))

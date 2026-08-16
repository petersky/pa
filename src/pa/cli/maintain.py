from __future__ import annotations

import json
from typing import Annotated

import typer

from pa.config import get_settings
from pa.mcp.local_api import LocalPAServerUnavailable, request_local_pa

maintain_app = typer.Typer(help="Local cruft cleanup and SQLite maintenance")


def _print(payload) -> None:
    typer.echo(json.dumps(payload, indent=2, sort_keys=True))


@maintain_app.command("status")
def status() -> None:
    """Show the last maintenance sweep and retention settings."""
    try:
        payload = request_local_pa(
            get_settings(), "GET", "/api/instance/maintenance"
        )
    except LocalPAServerUnavailable as exc:
        settings = get_settings()
        _print(
            {
                "available": False,
                "running": False,
                "server_error": str(exc),
                "interval_seconds": settings.maintenance_interval_seconds,
                "transcript_retention_days": settings.transcript_retention_days,
                "mutation_operation_retention_days": (
                    settings.mutation_operation_retention_days
                ),
            }
        )
        return
    _print(payload)


@maintain_app.command("run")
def run(
    local: Annotated[
        bool,
        typer.Option(
            "--local",
            help="Run against this data dir even if the PA server is up",
        ),
    ] = False,
) -> None:
    """Prune closed-session transcripts, old mutation receipts, and dispatch evidence."""
    if not local:
        try:
            _print(
                request_local_pa(
                    get_settings(),
                    "POST",
                    "/api/instance/maintenance/run",
                    timeout_seconds=120.0,
                )
            )
            return
        except LocalPAServerUnavailable:
            pass
    from pa.domain.store import get_store
    from pa.execution.dispatch import DispatchStore
    from pa.instance.maintenance import run_maintenance

    settings = get_settings()
    store = get_store(settings)
    dispatch = DispatchStore(settings.data_dir)
    try:
        _print(run_maintenance(settings, store, dispatch))
    finally:
        dispatch.close()

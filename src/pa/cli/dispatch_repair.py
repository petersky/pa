"""Dispatch ledger maintenance commands."""

from __future__ import annotations

import json
from typing import Annotated

import typer

from pa.config import get_settings

dispatch_app = typer.Typer(help="Inspect and repair the dispatch ledger")


def render_report(report: dict, *, as_json: bool) -> str:
    if as_json:
        return json.dumps(report, indent=2, sort_keys=True, default=str)
    done = report["applied"]
    lines = [
        ("Applied" if done else "Dry run (nothing changed; pass --apply to act)")
        + ": " + (", ".join(f"{k}={v}" for k, v in sorted(report["counts"].items())) or "nothing to do")
    ]
    for entry in report["actions"]:
        marker = (
            "note" if entry["action"] in {"skip", "needs_operator"}
            else "done" if entry["applied"] else "would"
        )
        lines.append(
            f"  [{marker}] {entry['action']:<22} dispatch={entry['dispatch_id']} "
            f"card={entry['card_id'] or '-'} state={entry['state']} {entry['reason']}"
        )
    return "\n".join(lines)


@dispatch_app.command("repair-stuck")
def repair_stuck_command(
    apply: Annotated[
        bool,
        typer.Option(
            "--apply",
            help="Perform the repair. Requires the PA server to be stopped. "
            "Without it this is a dry run that changes nothing.",
        ),
    ] = False,
    stale_days: Annotated[
        int,
        typer.Option(help="Fail remote running dispatches with no progress for this many days"),
    ] = 30,
    as_json: Annotated[bool, typer.Option("--json", help="Print the full report as JSON")] = False,
) -> None:
    """Close dispatch records that nothing will ever finish.

    Targets records whose card is already Done, whose session is closed or
    blocked, remote running records with no progress for --stale-days, and legacy
    acknowledged records. Closes them through the ledger's atomic repair
    primitive without inferring any outcome. Dry-run unless --apply is given.
    """
    from pa.core.writer_lock import DataDirAlreadyOwnedError, DataDirWriterLock
    from pa.domain.store import get_store
    from pa.execution.dispatch import DispatchStore
    from pa.execution.dispatch_repair import repair_stuck_dispatches

    settings = get_settings()
    lock = DataDirWriterLock(settings.data_dir)
    server_running = False
    try:
        lock.acquire()
    except DataDirAlreadyOwnedError as exc:
        if apply:
            typer.echo(f"{exc}\nStop the PA server before using --apply.", err=True)
            raise typer.Exit(1) from exc
        server_running = True
    ledger = None
    try:
        # A dry run reads the ledger read-only so it can never contend with, or
        # write beside, a running server.
        ledger = DispatchStore(settings.data_dir, read_only=not apply)
        report = repair_stuck_dispatches(
            ledger, get_store(settings), instance_id=settings.instance_id,
            apply=apply, stale_days=stale_days,
        )
        report["server_running"] = server_running
        typer.echo(render_report(report, as_json=as_json))
        if server_running:
            typer.echo(
                "Note: the server is running, so this dry run read its data directory "
                "without the writer lock.", err=True,
            )
    finally:
        if ledger is not None:
            ledger.close()
        lock.release()

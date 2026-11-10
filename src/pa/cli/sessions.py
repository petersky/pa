"""Session maintenance commands."""

from __future__ import annotations

import json
from typing import Annotated

import typer

from pa.config import get_settings

sessions_app = typer.Typer(help="Inspect and repair agent sessions")


def render_repair(report: dict, *, as_json: bool) -> str:
    if as_json:
        return json.dumps(report, indent=2, sort_keys=True)
    done = report["applied"]
    lines = [
        ("Applied" if done else "Dry run (nothing changed; pass --apply to act)")
        + ": " + (", ".join(f"{k}={v}" for k, v in sorted(report["counts"].items())) or "nothing to do")
    ]
    for entry in report["actions"]:
        tail = {k: v for k, v in entry.items()
                if k not in {"session_id", "card_id", "status", "action", "applied"} and v is not None}
        marker = (
            "note" if entry["action"] in {"skip", "needs_operator"}
            else "done" if entry.get("applied") else "would"
        )
        lines.append(
            f"  [{marker}] {entry['action']:<14} session={entry['session_id']} "
            f"card={entry['card_id'] or '-'} status={entry['status']}"
            + "".join(f" {k}={v}" for k, v in tail.items())
        )
    return "\n".join(lines)


@sessions_app.command("repair-recovery")
def repair_recovery_command(
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
        typer.Option(help="Close unlinked quiesced/disconnected sessions idle this many days"),
    ] = 7,
    as_json: Annotated[bool, typer.Option("--json", help="Print the full report as JSON")] = False,
) -> None:
    """Re-evaluate blocked and zombie sessions under the current recovery rules.

    Reclaims dead owners' worktree leases (saving their work to refs/pa/rescue),
    closes sessions on finished cards through the audited close path, and re-arms
    sessions whose block is now resolvable. Dry-run unless --apply is given.
    """
    from pa.core.writer_lock import DataDirAlreadyOwnedError, DataDirWriterLock
    from pa.domain.store import get_store
    from pa.instance.recovery_repair import repair_recovery
    from pa.repository.workspace import WorkspaceManager

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
    try:
        if settings.workspace_root is None:
            typer.echo("workspace_root is not configured", err=True)
            raise typer.Exit(1)
        store = get_store(settings)
        report = repair_recovery(
            settings, store, WorkspaceManager(settings, store),
            apply=apply, stale_days=stale_days,
        )
        report["server_running"] = server_running
        typer.echo(render_repair(report, as_json=as_json))
        if server_running:
            typer.echo(
                "Note: the server is running, so this dry run read its data directory "
                "without the writer lock.", err=True,
            )
    finally:
        lock.release()

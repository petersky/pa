"""Fleet notification CLI with stable human and JSON output."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Annotated, Any
from uuid import uuid4

import typer
from rich.console import Console
from rich.table import Table

from pa.config import get_settings
from pa.mcp.local_api import (
    LocalPARequestError,
    LocalPAServerUnavailable,
    request_local_pa,
)

notifications_app = typer.Typer(
    help="List and act on fleet notifications",
    no_args_is_help=True,
)


def _request(
    method: str, path: str, *, params: dict | None = None, body: dict | None = None
):
    try:
        return request_local_pa(get_settings(), method, path, params=params, json=body)
    except LocalPARequestError as exc:
        detail = exc.detail if isinstance(exc.detail, dict) else {}
        destination = detail.get("destination")
        message = detail.get("message") or str(exc)
        if destination:
            message = f"{message} Destination: {destination}"
        typer.echo(message, err=True)
        raise typer.Exit(2 if exc.status == 409 else 1) from exc
    except LocalPAServerUnavailable as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc


def _age(value: str | None) -> str:
    if not value:
        return "—"
    try:
        stamp = datetime.fromisoformat(value)
    except ValueError:
        return "—"
    seconds = max(0, int((datetime.now(UTC) - stamp).total_seconds()))
    if seconds < 60:
        return "now"
    if seconds < 3600:
        return f"{seconds // 60}m"
    if seconds < 86400:
        return f"{seconds // 3600}h"
    return f"{seconds // 86400}d"


def _emit_json(value: Any) -> None:
    typer.echo(json.dumps(value, indent=2, sort_keys=True))


@notifications_app.command("list")
def list_notifications(
    realm: Annotated[str | None, typer.Option(help="Realm ID")] = None,
    type: Annotated[str | None, typer.Option(help="Notification type")] = None,
    priority: Annotated[str | None, typer.Option(help="Priority")] = None,
    unread: Annotated[
        bool | None, typer.Option("--unread/--read", help="Read state")
    ] = None,
    outstanding: Annotated[
        bool | None,
        typer.Option("--outstanding/--not-outstanding", help="Outstanding state"),
    ] = None,
    resolved: Annotated[
        bool | None, typer.Option("--resolved/--unresolved", help="Resolution state")
    ] = None,
    limit: Annotated[int, typer.Option(min=1, max=200)] = 50,
    offset: Annotated[int, typer.Option(min=0)] = 0,
    json_output: Annotated[
        bool, typer.Option("--json", help="Emit machine-readable JSON")
    ] = False,
    color: Annotated[
        bool | None, typer.Option("--color/--no-color", help="Force or disable color")
    ] = None,
) -> None:
    """List authorized notifications with pagination and automation-safe output."""
    result = _request(
        "GET",
        "/api/notifications",
        params={
            "realm": realm,
            "type": type,
            "priority": priority,
            "unread": unread,
            "outstanding": outstanding,
            "resolved": resolved,
            "limit": limit,
            "offset": offset,
        },
    )
    if json_output:
        _emit_json(result)
        return
    console = Console(no_color=color is False, force_terminal=color is True)
    table = Table(show_header=True, header_style="bold")
    for heading in ("ID", "Priority", "Type", "Source", "Age", "Title", "State"):
        table.add_column(heading)
    for item in result.get("items", []):
        interaction = item.get("interaction") or {}
        source = (
            item.get("source_instance_name")
            or item.get("source_instance_id")
            or "local"
        )
        if item.get("card_id"):
            source += f"/card:{item['card_id'][:8]}"
        table.add_row(
            item.get("id", "")[:12],
            item.get("priority", ""),
            item.get("type", ""),
            source,
            _age(item.get("updated_at")),
            item.get("title", ""),
            interaction.get("state")
            or ("resolved" if item.get("resolved_at") else "notice"),
        )
    console.print(table)
    if result.get("next_offset") is not None:
        console.print(f"Next page: --offset {result['next_offset']}")
    console.print(f"Outstanding: {result.get('outstanding_count', 0)}")


@notifications_app.command("view")
def view_notification(
    notification_id: Annotated[str, typer.Argument(help="Notification ID")],
    json_output: Annotated[bool, typer.Option("--json")] = False,
    color: Annotated[bool | None, typer.Option("--color/--no-color")] = None,
) -> None:
    """View one notification, response contract, routing, and audit state."""
    result = _request("GET", f"/api/notifications/{notification_id}")
    if json_output:
        _emit_json(result)
        return
    console = Console(no_color=color is False, force_terminal=color is True)
    console.print(f"[bold]{result['title']}[/bold]")
    console.print(result.get("body") or result.get("summary") or "")
    console.print(
        f"ID: {result['id']}  Type: {result['type']}  Priority: {result['priority']}"
    )
    routing = result.get("routing") or {}
    if routing.get("response_mode") == "remote":
        console.print(
            f"[yellow]Complete on owning instance:[/yellow] {routing.get('destination')}"
        )
    interaction = result.get("interaction") or {}
    if interaction:
        console.print(f"Interaction: {interaction.get('state')}")
        for choice in interaction.get("choices") or []:
            console.print(f"  {choice['id']}: {choice['label']}")


def _mutation(notification_id: str, action: str, key: str | None) -> None:
    result = _request(
        "POST",
        f"/api/notifications/{notification_id}/{action}",
        body={"idempotency_key": key or str(uuid4())},
    )
    typer.echo(f"{action.capitalize()}d {result['id']} (version {result['version']})")


@notifications_app.command("acknowledge")
def acknowledge_notification(
    notification_id: Annotated[str, typer.Argument()],
    idempotency_key: Annotated[str | None, typer.Option()] = None,
) -> None:
    """Idempotently acknowledge one notification."""
    _mutation(notification_id, "acknowledge", idempotency_key)


@notifications_app.command("resolve")
def resolve_notification(
    notification_id: Annotated[str, typer.Argument()],
    idempotency_key: Annotated[str | None, typer.Option()] = None,
) -> None:
    """Idempotently resolve one notification."""
    _mutation(notification_id, "resolve", idempotency_key)


@notifications_app.command("respond")
def respond_notification(
    notification_id: Annotated[str, typer.Argument()],
    choice: Annotated[str | None, typer.Option(help="Choice ID")] = None,
    value: Annotated[str | None, typer.Option(help="Freeform response")] = None,
    fields_json: Annotated[
        str | None, typer.Option("--fields-json", help="Structured JSON object")
    ] = None,
    cancel: Annotated[bool, typer.Option(help="Cancel the request")] = False,
    idempotency_key: Annotated[str | None, typer.Option()] = None,
    json_output: Annotated[bool, typer.Option("--json")] = False,
) -> None:
    """Respond by choice, freeform text, structured fields, or cancellation."""
    supplied = sum(
        [choice is not None, value is not None, fields_json is not None, cancel]
    )
    if supplied != 1:
        typer.echo(
            "Provide exactly one of --choice, --value, --fields-json, or --cancel.",
            err=True,
        )
        raise typer.Exit(2)
    fields = None
    if fields_json is not None:
        try:
            fields = json.loads(fields_json)
        except json.JSONDecodeError as exc:
            typer.echo(f"Invalid --fields-json: {exc}", err=True)
            raise typer.Exit(2) from exc
        if not isinstance(fields, dict):
            typer.echo("--fields-json must decode to an object.", err=True)
            raise typer.Exit(2)
    result = _request(
        "POST",
        f"/api/notifications/{notification_id}/respond",
        body={
            "idempotency_key": idempotency_key or str(uuid4()),
            "choice_id": choice,
            "value": value,
            "fields": fields,
            "cancel": cancel,
        },
    )
    if json_output:
        _emit_json(result)
    else:
        typer.echo(
            f"Response delivered; state={result.get('interaction', {}).get('state')}"
        )

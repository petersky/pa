"""CLI commands for durable card dispatch through the running PA server."""

from __future__ import annotations

import json
from collections.abc import Callable
from typing import Annotated, Any
from uuid import uuid4

import httpx
import typer

from pa.auth.users import UserDirectory
from pa.cli.dispatch_wait import (
    DEFAULT_POLL_INTERVAL_SECONDS,
    DEFAULT_WAIT_TIMEOUT_SECONDS,
    DispatchFetchError,
    KeepAwakeError,
    wait_for_dispatches,
)
from pa.config import Settings, get_settings
from pa.workloads import CANONICAL_WORKLOAD_PROFILES

card_app = typer.Typer(help="Card execution and durable dispatch")
DEFAULT_MESSAGE = "Execute this card completely."


class CardCommandError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


def _base_url(settings: Settings) -> str:
    host = settings.host if settings.host not in ("0.0.0.0", "::") else "127.0.0.1"
    return f"http://{host}:{settings.port}"


def _error_detail(response: httpx.Response) -> str:
    try:
        payload = response.json()
    except ValueError:
        return response.text.strip() or response.reason_phrase
    detail = payload.get("detail") if isinstance(payload, dict) else payload
    if not isinstance(detail, dict):
        return str(detail or payload)
    message = str(detail.get("message") or detail.get("code") or detail)
    code = detail.get("code")
    if code and code not in message:
        message = f"{message} ({code})"
    if detail.get("recoverable"):
        message += " Retry after resolving the reported condition."
    return message


def _request(
    settings: Settings,
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    body: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout_seconds: float = 10.0,
    unknown_outcome_idempotency_key: str | None = None,
) -> dict[str, Any] | list[dict[str, Any]]:
    """Use CLI bearer auth; bearer API requests are CSRF-exempt by design."""
    user = UserDirectory(settings.data_dir).ensure_default_user()
    request_headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {user.cli_token}",
        **(headers or {}),
    }
    try:
        response = httpx.request(
            method,
            f"{_base_url(settings)}{path}",
            params=params,
            json=body,
            headers=request_headers,
            timeout=timeout_seconds,
        )
    except httpx.TimeoutException as exc:
        if unknown_outcome_idempotency_key:
            raise CardCommandError(
                "PA did not answer before the dispatch admission deadline. The "
                "request outcome is unknown; inspect dispatches or retry with the "
                f"same idempotency key: {unknown_outcome_idempotency_key}"
            ) from exc
        raise CardCommandError(
            f"The local PA server at {_base_url(settings)} did not answer before "
            "the request deadline."
        ) from exc
    except httpx.HTTPError as exc:
        raise CardCommandError(
            f"Could not reach the local PA server at {_base_url(settings)}. "
            "Start PA for this PA_DATA_DIR and retry."
        ) from exc
    if response.status_code >= 400:
        raise CardCommandError(
            f"PA rejected the request ({response.status_code}): {_error_detail(response)}",
            status_code=response.status_code,
        )
    try:
        payload = response.json()
    except ValueError as exc:
        raise CardCommandError("PA returned an invalid non-JSON response.") from exc
    if not isinstance(payload, (dict, list)):
        raise CardCommandError("PA returned an unexpected response.")
    return payload


def _resolve_instance(settings: Settings, value: str) -> dict[str, Any]:
    payload = _request(settings, "GET", "/api/fleet/instances")
    instances = payload if isinstance(payload, list) else []
    exact = [item for item in instances if item.get("instance_id") == value]
    named = [
        item
        for item in instances
        if str(item.get("name") or "").casefold() == value.casefold()
    ]
    matches = exact or named
    if len(matches) == 1:
        return matches[0]
    if len(matches) > 1:
        ids = ", ".join(str(item.get("instance_id")) for item in matches)
        raise CardCommandError(
            f"Instance name {value!r} is ambiguous; use an ID: {ids}"
        )
    available = ", ".join(
        f"{item.get('name')} ({item.get('instance_id')})" for item in instances
    )
    suffix = (
        f" Available instances: {available}"
        if available
        else " No instances are registered."
    )
    raise CardCommandError(f"Fleet instance not found: {value}.{suffix}")


def _run(command: Callable[[], None]) -> None:
    try:
        command()
    except CardCommandError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc


def _state_line(dispatch: dict[str, Any]) -> str:
    target = dispatch.get("target_instance_name") or dispatch.get(
        "target_instance_id", "unknown"
    )
    line = f"{dispatch.get('dispatch_id', 'unknown')}  {dispatch.get('state', 'unknown')}  target={target}"
    events = dispatch.get("events")
    if isinstance(events, list) and events and events[-1].get("message"):
        line += f"  {events[-1]['message']}"
    return line


def _render_wait_result(
    result: dict[str, Any], *, quiet: bool, json_output: bool
) -> None:
    if quiet:
        return
    if json_output:
        typer.echo(json.dumps(result, sort_keys=True))
        return
    typer.echo("Dispatch summary:")
    for item in result["dispatches"]:
        target = f" target={item['target']}" if item.get("target") else ""
        typer.echo(
            f"  {item['dispatch_id']}  {item['state']}  {item['outcome']}{target}"
        )


def _execute_dispatch_wait(
    dispatch_ids: list[str],
    *,
    settings: Settings,
    timeout_seconds: float,
    poll_interval_seconds: float,
    keep_awake: bool,
    quiet: bool,
    json_output: bool,
) -> None:
    def fetch(dispatch_id: str) -> dict[str, Any]:
        try:
            payload = _request(
                settings, "GET", f"/api/fleet/dispatch-jobs/{dispatch_id}"
            )
        except CardCommandError as exc:
            raise DispatchFetchError(str(exc), status_code=exc.status_code) from exc
        if not isinstance(payload, dict):
            raise DispatchFetchError("PA returned an invalid dispatch response.")
        return payload

    try:
        result = wait_for_dispatches(
            dispatch_ids,
            fetch=fetch,
            timeout_seconds=timeout_seconds,
            poll_interval_seconds=poll_interval_seconds,
            keep_awake=keep_awake,
            emit=None if quiet or json_output else typer.echo,
        )
    except KeepAwakeError as exc:
        raise CardCommandError(str(exc)) from exc
    _render_wait_result(result, quiet=quiet, json_output=json_output)
    if result["exit_code"]:
        raise typer.Exit(int(result["exit_code"]))


@card_app.command("dispatch-wait")
def dispatch_wait(
    dispatch_ids: Annotated[
        list[str], typer.Argument(help="One or more durable dispatch IDs")
    ],
    timeout: Annotated[
        float,
        typer.Option(
            "--timeout",
            min=0.1,
            help="Maximum total wait in seconds",
        ),
    ] = float(DEFAULT_WAIT_TIMEOUT_SECONDS),
    poll_interval: Annotated[
        float,
        typer.Option(
            "--poll-interval",
            min=0.05,
            help="Public API polling interval in seconds",
        ),
    ] = DEFAULT_POLL_INTERVAL_SECONDS,
    keep_awake: Annotated[
        bool,
        typer.Option(
            "--keep-awake",
            help="Prevent macOS system/display/idle sleep while waiting",
        ),
    ] = False,
    quiet: Annotated[
        bool, typer.Option("--quiet", "-q", help="Suppress all output")
    ] = False,
    json_output: Annotated[
        bool, typer.Option("--json", help="Emit one machine-readable result")
    ] = False,
) -> None:
    """Wait until every durable dispatch reaches a terminal state."""
    if quiet and json_output:
        raise typer.BadParameter("--quiet and --json cannot be combined")
    normalized = list(dict.fromkeys(value.strip() for value in dispatch_ids))
    if any(not value for value in normalized):
        raise typer.BadParameter("Dispatch IDs cannot be empty")
    _run(
        lambda: _execute_dispatch_wait(
            normalized,
            settings=get_settings(),
            timeout_seconds=timeout,
            poll_interval_seconds=poll_interval,
            keep_awake=keep_awake,
            quiet=quiet,
            json_output=json_output,
        )
    )


@card_app.command("dispatch")
def dispatch_card(
    card_id: Annotated[str, typer.Argument(help="Card ID")],
    instance: Annotated[
        str, typer.Option("--instance", help="Target instance name or ID")
    ],
    project: Annotated[
        str | None, typer.Option("--project", help="Override inferred project ID")
    ] = None,
    provider: Annotated[str | None, typer.Option(help="Agent provider")] = None,
    model: Annotated[
        str | None, typer.Option("--model", help="Provider model ID")
    ] = None,
    mode: Annotated[str | None, typer.Option("--mode", help="Provider mode ID")] = None,
    effort: Annotated[str | None, typer.Option(help="Reasoning effort")] = None,
    profile: Annotated[
        str | None,
        typer.Option(
            "--profile",
            help=(
                "Workspace profile: "
                + ", ".join(CANONICAL_WORKLOAD_PROFILES)
                + "; legacy code maps to repository"
            ),
        ),
    ] = None,
    message: Annotated[str, typer.Option(help="Initial instruction")] = DEFAULT_MESSAGE,
    idempotency_key: Annotated[
        str | None, typer.Option("--idempotency-key", help="Stable retry key")
    ] = None,
    priority: Annotated[
        int, typer.Option(min=-10, max=10, help="Requested queue priority")
    ] = 0,
    wait: Annotated[
        bool, typer.Option("--wait", help="Wait for this dispatch to become terminal")
    ] = False,
    keep_awake: Annotated[
        bool,
        typer.Option(
            "--keep-awake",
            help="Prevent macOS system/display/idle sleep while waiting",
        ),
    ] = False,
    timeout: Annotated[
        float,
        typer.Option(
            "--timeout",
            min=0.1,
            help="Maximum wait in seconds when --wait is used",
        ),
    ] = float(DEFAULT_WAIT_TIMEOUT_SECONDS),
) -> None:
    """Durably queue a card on a fleet instance."""
    if keep_awake and not wait:
        raise typer.BadParameter("--keep-awake requires --wait")

    def execute() -> None:
        settings = get_settings()
        card = _request(settings, "GET", f"/api/cards/{card_id}")
        if not isinstance(card, dict):
            raise CardCommandError("PA returned an invalid card response.")
        target = _resolve_instance(settings, instance)
        key = (idempotency_key or str(uuid4())).strip()
        if not key:
            raise CardCommandError("Idempotency key cannot be empty.")
        body = {
            "card_id": card_id,
            "project_id": project or card.get("project_id"),
            "provider": provider,
            "model_id": model,
            "mode_id": mode,
            "effort": effort,
            "message": message.strip(),
        }
        if profile is not None:
            body["execution_contract"] = {
                "version": 1,
                "profile": profile,
                "confirmed": profile.strip() != "automatic",
                "requirements": {},
            }
        if priority:
            body["priority"] = priority
        body = {name: value for name, value in body.items() if value is not None}
        if not body["message"]:
            raise CardCommandError("Initial instruction cannot be empty.")
        result = _request(
            settings,
            "POST",
            f"/api/fleet/instances/{target['instance_id']}/agent/start",
            body=body,
            headers={"Idempotency-Key": key},
            timeout_seconds=30.0,
            unknown_outcome_idempotency_key=key,
        )
        dispatch = result.get("dispatch") if isinstance(result, dict) else None
        if not isinstance(dispatch, dict):
            raise CardCommandError("PA did not return a durable dispatch record.")
        typer.echo(
            f"{'Recovered' if result.get('duplicate') else 'Queued'} durable card dispatch."
        )
        typer.echo(f"  {_state_line(dispatch)}")
        typer.echo(f"  Idempotency key: {key}")
        if not wait:
            typer.echo(f"  Inspect: pa card dispatch-get {dispatch['dispatch_id']}")
            return
        _execute_dispatch_wait(
            [str(dispatch["dispatch_id"])],
            settings=settings,
            timeout_seconds=timeout,
            poll_interval_seconds=DEFAULT_POLL_INTERVAL_SECONDS,
            keep_awake=keep_awake,
            quiet=False,
            json_output=False,
        )

    _run(execute)


@card_app.command("dispatch-list")
def dispatch_list(
    instance: Annotated[
        str | None, typer.Option("--instance", help="Filter by instance")
    ] = None,
    limit: Annotated[int, typer.Option(min=1, max=500)] = 100,
) -> None:
    """List durable dispatches."""

    def execute() -> None:
        settings = get_settings()
        params: dict[str, Any] = {"limit": limit}
        if instance:
            params["target_instance_id"] = _resolve_instance(settings, instance)[
                "instance_id"
            ]
        result = _request(settings, "GET", "/api/fleet/dispatch-jobs", params=params)
        if not isinstance(result, list):
            raise CardCommandError("PA returned an invalid dispatch list.")
        if not result:
            typer.echo("No durable dispatches found.")
        for dispatch in result:
            typer.echo(_state_line(dispatch))

    _run(execute)


@card_app.command("dispatch-get")
def dispatch_get(
    dispatch_id: Annotated[str, typer.Argument(help="Dispatch ID")],
) -> None:
    """Show one durable dispatch and its latest status."""

    def execute() -> None:
        result = _request(
            get_settings(), "GET", f"/api/fleet/dispatch-jobs/{dispatch_id}"
        )
        if not isinstance(result, dict):
            raise CardCommandError("PA returned an invalid dispatch.")
        typer.echo(_state_line(result))
        if result.get("last_error"):
            typer.echo(f"Error: {result['last_error']}")
        typer.echo(f"Retryable: {'yes' if result.get('can_retry') else 'no'}")
        typer.echo(f"Cancellable: {'yes' if result.get('can_cancel') else 'no'}")

    _run(execute)


def _dispatch_action(dispatch_id: str, action: str) -> None:
    result = _request(
        get_settings(),
        "POST",
        f"/api/fleet/dispatch-jobs/{dispatch_id}/{action}",
        body={},
    )
    if not isinstance(result, dict):
        raise CardCommandError("PA returned an invalid dispatch.")
    typer.echo(_state_line(result))


@card_app.command("dispatch-retry")
def dispatch_retry(
    dispatch_id: Annotated[str, typer.Argument(help="Dispatch ID")],
) -> None:
    """Retry a recoverable failed or cancelled dispatch."""
    _run(lambda: _dispatch_action(dispatch_id, "retry"))


@card_app.command("dispatch-cancel")
def dispatch_cancel(
    dispatch_id: Annotated[str, typer.Argument(help="Dispatch ID")],
) -> None:
    """Cancel a dispatch before prompt acceptance."""
    _run(lambda: _dispatch_action(dispatch_id, "cancel"))

"""Structured CLI facade for PA's authoritative Browser session manager."""

from __future__ import annotations

import json
import os
from typing import Annotated, Any
from uuid import UUID

import typer

from pa.config import get_settings
from pa.mcp.local_api import request_local_pa

browser_app = typer.Typer(
    help="Control an isolated PA browser through the authenticated local API",
    no_args_is_help=True,
)


def _session_id(value: str | None) -> str:
    if value:
        candidate = value
    else:
        try:
            candidate = str(
                json.loads(os.environ.get("PA_EXECUTION_CONTEXT", "{}")).get(
                    "session_id", ""
                )
            )
        except ValueError:
            candidate = ""
    try:
        UUID(candidate)
    except ValueError as exc:
        raise typer.BadParameter(
            "Supply --session with the full canonical PA agent session UUID."
        ) from exc
    return candidate


def _call(
    operation: str,
    *,
    session: str | None,
    browser_handle: str | None = None,
    **payload: Any,
) -> dict[str, Any]:
    body = {
        "agent_session_id": _session_id(session),
        "browser_handle": browser_handle,
        **payload,
    }
    return request_local_pa(
        get_settings(), "POST", f"/api/browser/{operation}", json=body
    )


def _emit(value: Any) -> None:
    typer.echo(json.dumps(value, ensure_ascii=False, sort_keys=True))


@browser_app.command("capabilities")
def capabilities() -> None:
    """Print stable action names, key/button semantics, and safety limits."""
    _emit(request_local_pa(get_settings(), "GET", "/api/browser/capabilities"))


@browser_app.command("attach")
def attach(
    session: Annotated[str | None, typer.Option("--session")] = None,
    url: Annotated[str, typer.Option()] = "about:blank",
    width: Annotated[int, typer.Option(min=320, max=7680)] = 1440,
    height: Annotated[int, typer.Option(min=240, max=4320)] = 900,
    scale: Annotated[float, typer.Option(min=0.25, max=4)] = 1,
    share_handle: Annotated[str | None, typer.Option()] = None,
) -> None:
    """Attach the session's isolated browser or redeem an explicit share handle."""
    _emit(
        _call(
            "attach",
            session=session,
            url=url,
            width=width,
            height=height,
            device_scale_factor=scale,
            share_handle=share_handle,
        )
    )


@browser_app.command("open")
def open_url(
    url: str,
    session: Annotated[str | None, typer.Option("--session")] = None,
    browser_handle: Annotated[str | None, typer.Option()] = None,
    operation_id: Annotated[str | None, typer.Option()] = None,
) -> None:
    """Navigate, with an optional operation ID for safe transport retry."""
    _emit(
        _call(
            "open",
            session=session,
            browser_handle=browser_handle,
            url=url,
            operation_id=operation_id,
        )
    )


@browser_app.command("snapshot")
def snapshot(
    session: Annotated[str | None, typer.Option("--session")] = None,
    browser_handle: Annotated[str | None, typer.Option()] = None,
) -> None:
    """Return snapshot-first content and revision-bound element references."""
    _emit(_call("snapshot", session=session, browser_handle=browser_handle))


@browser_app.command("click")
def click(
    selector: Annotated[str | None, typer.Option()] = None,
    ref: Annotated[str | None, typer.Option()] = None,
    x: Annotated[float | None, typer.Option()] = None,
    y: Annotated[float | None, typer.Option()] = None,
    button: Annotated[str, typer.Option()] = "left",
    count: Annotated[int, typer.Option(min=1, max=3)] = 1,
    modifier: Annotated[list[str] | None, typer.Option()] = None,
    session: Annotated[str | None, typer.Option("--session")] = None,
    browser_handle: Annotated[str | None, typer.Option()] = None,
    operation_id: Annotated[str | None, typer.Option()] = None,
) -> None:
    """Click a selector/ref (preferred) or bounded viewport coordinates."""
    _emit(
        _call(
            "click",
            session=session,
            browser_handle=browser_handle,
            selector=selector,
            ref=ref,
            x=x,
            y=y,
            button=button,
            click_count=count,
            modifiers=modifier or [],
            operation_id=operation_id,
        )
    )


@browser_app.command("actions")
def actions(
    sequence: Annotated[
        str,
        typer.Argument(
            help="JSON ordered action list; raw JavaScript/CDP is not accepted"
        ),
    ],
    session: Annotated[str | None, typer.Option("--session")] = None,
    browser_handle: Annotated[str | None, typer.Option()] = None,
    operation_id: Annotated[str | None, typer.Option()] = None,
) -> None:
    """Atomically execute bounded pointer/key/wheel/pause primitives."""
    try:
        parsed = json.loads(sequence)
    except ValueError as exc:
        raise typer.BadParameter("sequence must be valid JSON") from exc
    if not isinstance(parsed, list):
        raise typer.BadParameter("sequence must be a JSON list")
    _emit(
        _call(
            "actions",
            session=session,
            browser_handle=browser_handle,
            actions=parsed,
            operation_id=operation_id,
        )
    )

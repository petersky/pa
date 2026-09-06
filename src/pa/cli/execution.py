"""Execution selection controls routed through the owning PA HTTP API."""

from __future__ import annotations

import json
from uuid import uuid4

import typer

from pa.cli.card import CardCommandError, _request, _run
from pa.config import get_settings
from pa.execution.selection import ExecutionPreferences

execution_app = typer.Typer(
    help="Preview, start, and inspect provider-neutral execution selection"
)


def preferences(value: str) -> dict:
    try:
        return ExecutionPreferences.model_validate_json(value).model_dump(mode="json")
    except ValueError as exc:
        raise CardCommandError(f"Invalid execution preferences: {exc}") from exc


def stable_key(value: str) -> str:
    if not value.strip() or len(value) > 200:
        raise CardCommandError(
            "A nonempty stable idempotency key of at most 200 characters is required"
        )
    return value


def send(method, path, body=None):
    result = _request(
        get_settings(),
        method,
        path,
        body=body,
        headers={"Idempotency-Key": str(uuid4())},
        timeout_seconds=30,
    )
    typer.echo(json.dumps(result, indent=2))


@execution_app.command("catalog")
def catalog(refresh: bool = False) -> None:
    """Read cached capability evidence, or explicitly request bounded discovery."""
    _run(
        lambda: send(
            "POST" if refresh else "GET",
            "/api/execution/catalog/refresh" if refresh else "/api/execution/catalog",
        )
    )


@execution_app.command("preview")
def preview(
    selection_json: str = "{}",
    card_id: str | None = None,
    project_id: str | None = None,
) -> None:
    """Preview local execution without applying settings. Use card dispatch --preview for fleet placement."""
    _run(
        lambda: send(
            "POST",
            "/api/execution/preview",
            {
                "execution_preferences": preferences(selection_json),
                "card_id": card_id,
                "project_id": project_id,
            },
        )
    )


@execution_app.command("start")
def start(
    selection_json: str = "{}",
    title: str | None = None,
    project_id: str | None = None,
    idempotency_key: str = typer.Option(
        ..., help="Stable key for this standalone session creation"
    ),
) -> None:
    """Create a standalone session; card work uses card dispatch and its durable fleet contract."""
    _run(
        lambda: send(
            "POST",
            "/api/agent/sessions",
            {
                "execution_preferences": preferences(selection_json),
                "title": title,
                "project_id": project_id,
                "label": "execution-cli:" + stable_key(idempotency_key),
            },
        )
    )


@execution_app.command("receipt")
def receipt(decision_id: str, offset: int = 0, limit: int = 100) -> None:
    """Read the owned decision, native confirmations, and recorded attempts."""
    _run(
        lambda: send(
            "GET",
            f"/api/execution/decisions/{decision_id}?offset={offset}&limit={limit}",
        )
    )


@execution_app.command("linked-attempt")
def linked_attempt(session_id: str, selection_json: str, idempotency_key: str) -> None:
    """Cross an explicit idle standalone context boundary; dispatch successors use card dispatch."""
    _run(
        lambda: send(
            "POST",
            f"/api/execution/sessions/{session_id}/boundary",
            {
                "execution_preferences": preferences(selection_json),
                "idempotency_key": idempotency_key,
            },
        )
    )


@execution_app.command("settings")
def settings(
    session_id: str,
    selection_json: str,
    expected_version: str,
    idempotency_key: str,
    defer: bool = False,
) -> None:
    """Request supported native changes with concurrency control, optionally deferred until the next turn."""
    _run(
        lambda: send(
            "POST",
            f"/api/execution/sessions/{session_id}/settings",
            {
                "execution_preferences": preferences(selection_json),
                "expected_version": expected_version,
                "idempotency_key": idempotency_key,
                "defer": defer,
            },
        )
    )


@execution_app.command("policy")
def policy(
    policy_json: str | None = None, expected_revision: int | None = None
) -> None:
    """Read policy, or save administrator-owned native rules with revision checking."""

    def execute():
        if policy_json is None:
            send("GET", "/api/execution/policy")
            return
        from pa.execution.selection import SelectionPolicy

        try:
            value = SelectionPolicy.model_validate_json(policy_json).model_dump(
                mode="json"
            )
        except ValueError as exc:
            raise CardCommandError(f"Invalid selection policy: {exc}") from exc
        if expected_revision is None:
            raise CardCommandError("Saving policy requires --expected-revision")
        send(
            "PUT",
            "/api/execution/policy",
            {"policy": value, "expected_revision": expected_revision},
        )

    _run(execute)

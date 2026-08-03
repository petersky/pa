from __future__ import annotations

import json
from typing import Annotated
from uuid import uuid4

import typer

from pa.config import get_settings
from pa.mcp.local_api import LocalPAServerUnavailable, request_local_pa

goal_app = typer.Typer(help="Durable goal management")


def _emit(value) -> None:
    typer.echo(json.dumps(value, indent=2, default=str))


def _request(method: str, path: str, *, params=None, body=None, headers=None):
    try:
        return request_local_pa(
            get_settings(),
            method,
            path,
            params=params,
            json=body,
            headers=headers,
            timeout_seconds=10,
        )
    except LocalPAServerUnavailable as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc


@goal_app.command("list")
def list_goals(
    realm: Annotated[str | None, typer.Option()] = None,
    state: Annotated[str | None, typer.Option()] = None,
) -> None:
    """List durable goals."""
    _emit(_request("GET", "/api/goals", params={"realm": realm, "state": state}))


@goal_app.command("show")
def show_goal(goal_id: str) -> None:
    """Show a goal and its attributable event ledger."""
    _emit(_request("GET", f"/api/goals/{goal_id}"))


@goal_app.command("create")
def create_goal(
    objective: Annotated[str, typer.Argument()],
    criterion: Annotated[
        list[str], typer.Option("--criterion", help="Required success criterion")
    ],
    verification: Annotated[
        str, typer.Option(help="Verification method for each criterion")
    ] = "manual audit",
    evidence_requirement: Annotated[
        str, typer.Option(help="Required evidence for each criterion")
    ] = "auditor evidence",
    project_id: Annotated[str | None, typer.Option()] = None,
    idempotency_key: Annotated[str | None, typer.Option()] = None,
) -> None:
    """Create a goal with structured success criteria."""
    key = idempotency_key or str(uuid4())
    body = {
        "objective": objective,
        "project_id": project_id,
        "criteria": [
            {
                "description": item,
                "verification_method": verification,
                "evidence_requirement": evidence_requirement,
            }
            for item in criterion
        ],
    }
    _emit(
        _request(
            "POST",
            "/api/goals",
            params={"expected_version": 0, "policy_revision": 1},
            body=body,
            headers={"Idempotency-Key": key},
        )
    )


def _transition(
    goal_id: str,
    state: str,
    reason: str,
    version: int,
    policy_revision: int,
    fencing_token: int | None,
) -> None:
    headers = {"Idempotency-Key": str(uuid4())}
    if fencing_token is not None:
        headers["X-PA-Goal-Fencing-Token"] = str(fencing_token)
    _emit(
        _request(
            "POST",
            f"/api/goals/{goal_id}/transition",
            params={"expected_version": version, "policy_revision": policy_revision},
            body={"state": state, "reason": reason},
            headers=headers,
        )
    )


@goal_app.command("pause")
def pause_goal(
    goal_id: str,
    version: Annotated[int, typer.Option()],
    policy_revision: Annotated[int, typer.Option()] = 1,
    reason: Annotated[str, typer.Option()] = "operator pause",
    fencing_token: Annotated[int | None, typer.Option()] = None,
) -> None:
    """Pause an active durable goal."""
    _transition(goal_id, "paused", reason, version, policy_revision, fencing_token)


@goal_app.command("resume")
def resume_goal(
    goal_id: str,
    version: Annotated[int, typer.Option()],
    policy_revision: Annotated[int, typer.Option()] = 1,
    reason: Annotated[str, typer.Option()] = "operator resume",
    fencing_token: Annotated[int | None, typer.Option()] = None,
) -> None:
    """Resume a paused or waiting goal."""
    _transition(goal_id, "active", reason, version, policy_revision, fencing_token)


@goal_app.command("stop")
def stop_goal(
    goal_id: str,
    version: Annotated[int, typer.Option()],
    policy_revision: Annotated[int, typer.Option()] = 1,
    reason: Annotated[str, typer.Option()] = "operator stop",
    fencing_token: Annotated[int | None, typer.Option()] = None,
) -> None:
    """Abandon a durable goal."""
    _transition(goal_id, "abandoned", reason, version, policy_revision, fencing_token)


@goal_app.command("events")
def goal_events(goal_id: str) -> None:
    """Show the immutable, policy-attributed mutation ledger."""
    payload = _request("GET", f"/api/goals/{goal_id}")
    _emit(payload.get("events", []) if isinstance(payload, dict) else payload)

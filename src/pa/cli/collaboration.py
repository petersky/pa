from __future__ import annotations

import json
from typing import Annotated

import typer

from pa.collaboration.models import (
    CollaborationPolicy,
    PlanLifecycle,
    PolicyScope,
    PolicyStrategy,
)
from pa.config import get_settings
from pa.mcp.local_api import request_local_pa

collaboration_app = typer.Typer(
    help="Inspect and configure collaboration-mode policy and commands",
    no_args_is_help=True,
)


def _print(payload, *, as_json: bool) -> None:
    if as_json:
        typer.echo(json.dumps(payload, indent=2, sort_keys=True))
        return
    if isinstance(payload, list):
        if not payload:
            typer.echo("No collaboration policies configured.")
            return
        for item in payload:
            typer.echo(
                f"{item['id']}  {item['scope_type']}:{item['scope_id']}  "
                f"{item['strategy']}  v{item['version']}"
            )
        return
    typer.echo(f"Session:        {payload.get('session_id', '-')}")
    typer.echo(f"Current mode:   {payload.get('current_mode', '-')}")
    typer.echo("Supported:      " + ", ".join(payload.get("supported_modes") or []))
    typer.echo(
        f"Execution mode: {payload.get('execution_mode_id') or 'unchanged/provider default'}"
    )
    decision = payload.get("policy_decision") or {}
    typer.echo(f"Policy source:  {decision.get('source') or 'deterministic fallback'}")
    typer.echo(f"Rationale:      {decision.get('rationale') or '-'}")
    pending = payload.get("pending_transition")
    typer.echo(
        "Pending:        "
        + (
            f"{pending.get('requested_mode')} ({pending.get('reason')})"
            if pending
            else "none"
        )
    )


@collaboration_app.command("inspect")
def inspect_session(
    session_id: str,
    json_output: Annotated[
        bool, typer.Option("--json", help="Emit stable JSON output")
    ] = False,
) -> None:
    """Inspect effective policy and mode state for one session."""
    payload = request_local_pa(
        get_settings(),
        "GET",
        f"/api/agent/sessions/{session_id}/collaboration",
    )
    _print(payload, as_json=json_output)


@collaboration_app.command("commands")
def list_commands(
    session_id: str,
    json_output: Annotated[
        bool, typer.Option("--json", help="Emit stable JSON output")
    ] = False,
) -> None:
    """List the active provider and PA-native command catalog."""
    payload = request_local_pa(
        get_settings(), "GET", f"/api/agent/sessions/{session_id}/commands"
    )
    if json_output:
        typer.echo(json.dumps(payload, indent=2, sort_keys=True))
        return
    typer.echo(
        f"Session {session_id} — catalog generation {payload.get('generation') or 'loading'}"
    )
    for command in payload.get("commands") or []:
        state = command.get("availability", "available")
        reason = (
            f" — {command.get('disabled_reason')}"
            if command.get("disabled_reason")
            else ""
        )
        typer.echo(
            f"/{command['name']:<22} {command['origin']:<8} {state}{reason}\n"
            f"  {command.get('description') or ''}"
        )


@collaboration_app.command("policy-list")
def policy_list(
    json_output: Annotated[
        bool, typer.Option("--json", help="Emit stable JSON output")
    ] = False,
) -> None:
    """List configured policy records and their precedence scopes."""
    payload = request_local_pa(
        get_settings(), "GET", "/api/agent/collaboration/policies"
    )
    _print(payload, as_json=json_output)


@collaboration_app.command("policy-set")
def policy_set(
    policy_id: str,
    scope: Annotated[PolicyScope, typer.Option(help="Policy scope")],
    scope_id: Annotated[str, typer.Option(help="Stable scope entity ID")],
    strategy: Annotated[PolicyStrategy, typer.Option(help="Selection strategy")],
    provider: Annotated[
        str | None, typer.Option(help="Optional provider filter")
    ] = None,
    mandatory_mode: Annotated[
        str | None, typer.Option(help="Mandatory default or plan constraint")
    ] = None,
    deny_agent_transitions: Annotated[
        bool, typer.Option(help="Deny agent-requested transitions")
    ] = False,
    max_plan_turns: Annotated[int, typer.Option(min=1, max=20)] = 3,
    plan_expiry_minutes: Annotated[int, typer.Option(min=1, max=10_080)] = 60,
    expected_version: Annotated[int | None, typer.Option()] = None,
    json_output: Annotated[
        bool, typer.Option("--json", help="Emit stable JSON output")
    ] = False,
) -> None:
    """Create or version-check an effective collaboration policy."""
    policy = CollaborationPolicy(
        id=policy_id,
        scope_type=scope,
        scope_id=scope_id,
        provider=provider,
        strategy=strategy,
        mandatory_mode=mandatory_mode,
        allow_agent_transitions=not deny_agent_transitions,
        lifecycle=PlanLifecycle(
            max_turns=max_plan_turns, expires_minutes=plan_expiry_minutes
        ),
    )
    payload = request_local_pa(
        get_settings(),
        "PUT",
        f"/api/agent/collaboration/policies/{policy_id}",
        json={
            "policy": policy.model_dump(mode="json"),
            "expected_version": expected_version,
        },
    )
    if json_output:
        typer.echo(json.dumps(payload, indent=2, sort_keys=True))
    else:
        typer.echo(
            f"Saved {payload['id']} ({payload['scope_type']}:{payload['scope_id']}) "
            f"at version {payload['version']}."
        )

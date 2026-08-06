from __future__ import annotations

import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import httpx
import pytest
from mcp.server.mcpserver import MCPServer
from pydantic import ValidationError

from pa.acp.environment import (
    ASSIGNED_SERVICE_AUTHORITY_INSTANCE_ENV,
    ASSIGNED_SERVICE_AUTHORITY_URL_ENV,
    ASSIGNED_SERVICE_CREDENTIAL_ENV,
    ASSIGNED_SERVICE_DISPATCH_ENV,
    ASSIGNED_SERVICE_MODE_ENV,
    ASSIGNED_SERVICE_SESSION_ENV,
    assigned_service_mcp_environment,
    assigned_service_session_capability,
    sanitize_provider_environment,
)
from pa.domain.models import AgentSession
from pa.goals.advanced_models import ProviderGoalProgress, ProviderRunState
from pa.goals.models import AssignedServiceGoalProposalCreate
from pa.instance.agent_session import AgentSessionManager, AgentSessionRecoveryError
from pa.mcp.local_api import LocalPAServerUnavailable, request_local_pa
from pa.mcp.server import (
    ASSIGNED_SERVICE_TOOL_ALLOWLIST,
    ToolAllowlistProxy,
    assigned_service_mcp_mode,
)
from pa.modules.agent_providers import AgentProvidersModule
from pa.modules.fleet import FleetModule
from pa.modules.goals import GoalsModule


class FakeMcp:
    def __init__(self) -> None:
        self.functions: dict[str, object] = {}

    def tool(self, *args, **kwargs):
        del args, kwargs

        def register(fn):
            self.functions[fn.__name__] = fn
            return fn

        return register


def _schema_property_names(value) -> set[str]:
    names: set[str] = set()
    if isinstance(value, dict):
        properties = value.get("properties")
        if isinstance(properties, dict):
            names.update(properties)
        for nested in value.values():
            names.update(_schema_property_names(nested))
    elif isinstance(value, list):
        for nested in value:
            names.update(_schema_property_names(nested))
    return names


@pytest.mark.asyncio
async def test_assigned_mcp_registration_is_exact_and_identity_free() -> None:
    mcp = MCPServer("assigned-goal-schema")
    restricted = ToolAllowlistProxy(mcp, ASSIGNED_SERVICE_TOOL_ALLOWLIST)
    ctx = SimpleNamespace(
        settings=SimpleNamespace(),
        services={},
        require_service=lambda _name: MagicMock(),
    )
    GoalsModule().register_mcp(restricted, ctx)
    FleetModule().register_mcp(restricted, ctx)
    # This module has direct provider install/configure mutators. Registering it
    # proves the allowlist is enforced at the MCP server boundary.
    AgentProvidersModule().register_mcp(restricted, ctx)

    tools = await mcp.list_tools()
    assert {tool.name for tool in tools} == ASSIGNED_SERVICE_TOOL_ALLOWLIST
    forbidden = {
        "goal_id",
        "work_package_id",
        "run_id",
        "session_id",
        "dispatch_id",
        "provider",
        "provider_id",
        "target_instance_id",
        "authority_instance_id",
        "fencing_token",
        "actor_principal",
        "proposer_principal",
        "proposer_role",
        "auditor_principal",
        "service_role",
        "progress_credential",
        "credential",
        "token",
    }
    for tool in tools:
        assert forbidden.isdisjoint(_schema_property_names(tool.input_schema)), tool.name


def test_incomplete_assigned_mode_never_falls_back_to_full_registration() -> None:
    cases = (
        {ASSIGNED_SERVICE_MODE_ENV: "1"},
        {
            ASSIGNED_SERVICE_MODE_ENV: "1",
            ASSIGNED_SERVICE_DISPATCH_ENV: "dispatch-only",
        },
        {ASSIGNED_SERVICE_SESSION_ENV: "session-without-mode"},
    )
    for environment in cases:
        with (
            patch.dict(os.environ, environment, clear=True),
            pytest.raises(RuntimeError, match="assigned MCP session binding"),
        ):
            assigned_service_mcp_mode()


def test_every_assigned_proposal_action_rejects_nested_identity_assertions() -> None:
    actions = (
        {
            "kind": "request_operator",
            "prompt": "Choose one bounded answer.",
            "allow_freeform": True,
        },
        {
            "kind": "revise_strategy",
            "summary": "Revise the bounded strategy.",
        },
        {
            "kind": "transition_goal",
            "state": "blocked",
            "reason": "Wait for bounded evidence.",
        },
    )
    for action in actions:
        with pytest.raises(ValidationError, match="extra_forbidden"):
            AssignedServiceGoalProposalCreate.model_validate(
                {
                    "action": {
                        **action,
                        "authority_instance_id": "forged-authority",
                        "progress_credential": "forged-token",
                    },
                    "rationale": "Exercise strict nested validation.",
                    "expected_goal_version": 1,
                    "policy_revision": 1,
                }
            )


def test_assigned_progress_tool_uses_only_the_bound_local_route() -> None:
    delegate = FakeMcp()
    restricted = ToolAllowlistProxy(delegate, ASSIGNED_SERVICE_TOOL_ALLOWLIST)
    settings = SimpleNamespace()
    local_api = MagicMock(return_value={"sequence": 4})
    with patch("pa.mcp.local_api.request_local_pa", local_api):
        FleetModule().register_mcp(restricted, SimpleNamespace(settings=settings))
        result = delegate.functions["report_assigned_dispatch_progress"](
            phase="testing",
            summary="Exact assigned probe passed.",
            idempotency_key="assigned-progress-4",
        )

    assert result == {"sequence": 4}
    call = local_api.call_args
    assert call.args == (
        settings,
        "POST",
        "/api/goal-assigned-session/progress",
    )
    assert call.kwargs["json"]["idempotency_key"] == "assigned-progress-4"
    assert "dispatch_id" not in call.kwargs["json"]


def test_assigned_local_api_derives_exact_capability_without_owner_token() -> None:
    dispatch_id = "dispatch-bound"
    session_id = "session-bound"
    target_id = "target-b"
    secret = "target-session-secret"
    captured: dict = {}

    def route(method, url, **kwargs):
        captured.update(method=method, url=url, kwargs=kwargs)
        request = httpx.Request(method, url, headers=kwargs["headers"])
        return httpx.Response(
            200,
            request=request,
            headers={"X-PA-Instance-ID": target_id},
            json={"goal": {"objective": "bounded"}},
        )

    settings = SimpleNamespace(
        data_dir=Path("/unused"),
        host="127.0.0.1",
        port=9123,
        session_secret=secret,
    )
    with (
        patch.dict(
            os.environ,
            {
                **assigned_service_mcp_environment(
                    dispatch_id=dispatch_id,
                    session_id=session_id,
                ),
                "PA_INSTANCE_ID": target_id,
                "PA_LOCAL_API_URL": "http://target-b.test",
                "PA_LOCAL_API_TOKEN": "owner-token-must-not-be-used",
            },
            clear=True,
        ),
        patch("pa.mcp.local_api.UserDirectory") as users,
        patch("pa.mcp.local_api.httpx.request", side_effect=route),
    ):
        result = request_local_pa(
            settings,
            "GET",
            "/api/goal-assigned-session/goal",
        )

    assert result == {"goal": {"objective": "bounded"}}
    users.assert_not_called()
    expected = assigned_service_session_capability(
        secret=secret,
        dispatch_id=dispatch_id,
        session_id=session_id,
        target_instance_id=target_id,
    )
    headers = captured["kwargs"]["headers"]
    assert headers["Authorization"] == f"GoalSession {expected}"
    assert headers["X-PA-Assigned-Dispatch-ID"] == dispatch_id
    assert headers["X-PA-Assigned-Session-ID"] == session_id
    assert "owner-token-must-not-be-used" not in repr(captured)


def test_assigned_local_api_rejects_generic_mutation_before_auth_or_http() -> None:
    settings = SimpleNamespace(
        data_dir=Path("/unused"),
        host="127.0.0.1",
        port=9123,
        session_secret="target-session-secret",
    )
    with (
        patch.dict(
            os.environ,
            {
                **assigned_service_mcp_environment(
                    dispatch_id="dispatch-bound",
                    session_id="session-bound",
                ),
                "PA_INSTANCE_ID": "target-b",
            },
            clear=True,
        ),
        patch("pa.mcp.local_api.UserDirectory") as users,
        patch("pa.mcp.local_api.httpx.request") as outbound,
        pytest.raises(
            LocalPAServerUnavailable,
            match="cannot invoke this ordinary PA tool",
        ),
    ):
        request_local_pa(settings, "POST", "/api/goals", json={})

    users.assert_not_called()
    outbound.assert_not_called()


@pytest.mark.asyncio
async def test_runtime_rederives_assignment_for_fresh_and_recovered_bridges(
    tmp_path: Path,
) -> None:
    settings = SimpleNamespace(
        data_dir=tmp_path, workspace_root=tmp_path / "workspaces"
    )
    store = MagicMock()
    store.next_transcript_seq.return_value = 1
    manager = AgentSessionManager(settings, store)
    expected = assigned_service_mcp_environment(
        dispatch_id="dispatch-bound",
        session_id="session-bound",
    )
    calls: list[str] = []

    def resolve(session: AgentSession) -> dict[str, str]:
        calls.append(session.id)
        return expected

    manager.assigned_mcp_environment_resolver = resolve
    session = AgentSession(
        id="session-bound",
        agent_name="codex",
        dispatch_id="dispatch-bound",
    )
    fresh = await manager._new_runtime(session, mcp_private_env=dict(expected))
    recovered = await manager._new_runtime(session)

    assert fresh.mcp_private_env == expected
    assert recovered.mcp_private_env == expected
    assert calls == ["session-bound", "session-bound"]
    assert expected.keys().isdisjoint(fresh.agent_env)
    assert repr(expected) not in session.model_dump_json()


@pytest.mark.asyncio
async def test_runtime_rejects_unbacked_or_conflicting_assignment(
    tmp_path: Path,
) -> None:
    settings = SimpleNamespace(
        data_dir=tmp_path, workspace_root=tmp_path / "workspaces"
    )
    store = MagicMock()
    store.next_transcript_seq.return_value = 1
    manager = AgentSessionManager(settings, store)
    session = AgentSession(id="session-bound", agent_name="codex")
    forged = assigned_service_mcp_environment(
        dispatch_id="forged-dispatch",
        session_id=session.id,
    )
    with pytest.raises(AgentSessionRecoveryError, match="not backed"):
        await manager._new_runtime(session, mcp_private_env=forged)

    manager.assigned_mcp_environment_resolver = lambda _session: {
        **forged,
        ASSIGNED_SERVICE_DISPATCH_ENV: "durable-dispatch",
    }
    with pytest.raises(AgentSessionRecoveryError, match="conflicts"):
        await manager._new_runtime(session, mcp_private_env=forged)


def test_runtime_provider_environment_cannot_receive_assignment_material() -> None:
    from pa.instance.agent_session import AgentSessionRuntime

    session = AgentSession(id="session-bound", agent_name="codex")
    runtime = AgentSessionRuntime(
        SimpleNamespace(settings=SimpleNamespace(), store=SimpleNamespace()),
        session,
        agent_env={
            ASSIGNED_SERVICE_MODE_ENV: "1",
            ASSIGNED_SERVICE_DISPATCH_ENV: "dispatch-bound",
            ASSIGNED_SERVICE_SESSION_ENV: "session-bound",
            ASSIGNED_SERVICE_CREDENTIAL_ENV: "paas1.private",
            ASSIGNED_SERVICE_AUTHORITY_URL_ENV: "https://authority.invalid",
            ASSIGNED_SERVICE_AUTHORITY_INSTANCE_ENV: "authority-a",
            "PUBLIC_PROVIDER_VALUE": "visible",
        },
        mcp_private_env=assigned_service_mcp_environment(
            dispatch_id="dispatch-bound",
            session_id="session-bound",
        ),
        initial_transcript_seq=0,
    )

    merged = sanitize_provider_environment({}, runtime._merged_agent_env(None))
    assert merged["PUBLIC_PROVIDER_VALUE"] == "visible"
    assert ASSIGNED_SERVICE_TOOL_ALLOWLIST
    for name in (
        ASSIGNED_SERVICE_MODE_ENV,
        ASSIGNED_SERVICE_DISPATCH_ENV,
        ASSIGNED_SERVICE_SESSION_ENV,
        ASSIGNED_SERVICE_CREDENTIAL_ENV,
        ASSIGNED_SERVICE_AUTHORITY_URL_ENV,
        ASSIGNED_SERVICE_AUTHORITY_INSTANCE_ENV,
    ):
        assert name not in merged


def test_generic_provider_goal_progress_contract_is_unchanged() -> None:
    mcp = FakeMcp()
    settings = SimpleNamespace()
    local_api = MagicMock(return_value={"provider_runs": []})
    with patch("pa.mcp.local_api.request_local_pa", local_api):
        GoalsModule().register_mcp(mcp, SimpleNamespace(settings=settings))
        result = mcp.functions["ingest_provider_goal_progress"](
            "goal-1",
            ProviderGoalProgress(
                run_id="run-1",
                state=ProviderRunState.RUNNING,
                summary="Provider progress remains compatible.",
            ),
            expected_autonomy_version=3,
            goal_version=7,
            policy_revision=2,
            idempotency_key="provider-progress",
            authority_instance_id="authority-a",
            progress_credential="provider-goal-run-token",
            fencing_token=9,
        )

    assert result == {"provider_runs": []}
    call = local_api.call_args
    assert call.args == (
        settings,
        "POST",
        "/api/goals/goal-1/providers/progress",
    )
    assert call.kwargs["headers"] == {
        "Idempotency-Key": "provider-progress",
        "X-PA-Authority-Instance": "authority-a",
        "Authorization": "GoalRun provider-goal-run-token",
        "X-PA-Goal-Fencing-Token": "9",
    }
    assert "goal_run_credential" not in call.kwargs

"""The reconciliation adapter delegates authority and never drives a restart."""
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from pa.mcp.tools import agent_chat


def registered():
    tools = {}
    def tool():
        def register(fn):
            tools[fn.__name__] = fn
            return fn
        return register
    ctx = SimpleNamespace(settings=object())
    with patch("pa.mcp.local_api.request_local_pa") as request:
        agent_chat.register_mcp(SimpleNamespace(tool=tool), ctx)
    return tools, ctx, request


@pytest.mark.parametrize("effect", ["unknown", "confirmed"])
def test_exact_receipt_and_version_delegated_without_effect_reinterpretation(effect):
    tools, ctx, request = registered()
    observed = {"handoff_id": "receipt", "effect": effect, "replayed": False}
    request.return_value = observed
    assert tools["reconcile_agent_restart_handoff"]("session", "receipt", 3) is observed
    request.assert_called_once_with(ctx.settings, "POST",
        "/api/agent/sessions/session/restart-handoffs/receipt/reconcile",
        json={"expected_version": 3}, timeout_seconds=15.0)


def test_path_components_escaped_and_no_principal_owner_or_prompt_payload():
    tools, ctx, request = registered()
    tools["reconcile_agent_restart_handoff"]("s?x=1", "r#fragment", 3)
    assert request.call_args.args[2] == (
        "/api/agent/sessions/s%3Fx%3D1/restart-handoffs/r%23fragment/reconcile")
    assert request.call_args.kwargs["json"] == {"expected_version": 3}


@pytest.mark.parametrize("error", [RuntimeError("unknown outcome"), ValueError("stale version")])
def test_owner_errors_propagate_without_retry_or_fallback(error):
    tools, ctx, request = registered()
    request.side_effect = error
    with pytest.raises(type(error)) as raised:
        tools["reconcile_agent_restart_handoff"]("session", "receipt", 3)
    assert raised.value is error
    assert request.call_count == 1


def test_assigned_service_allowlist_remains_closed():
    from pa.mcp.server import ASSIGNED_SERVICE_TOOL_ALLOWLIST
    assert "reconcile_agent_restart_handoff" not in ASSIGNED_SERVICE_TOOL_ALLOWLIST


def test_unknown_transport_outcome_preserves_original_lookup_guidance():
    from pa.mcp.local_api import LocalPAUnknownOutcome
    tools, ctx, request = registered()
    error = LocalPAUnknownOutcome("audit acknowledgement unknown",
        operation_id="original-audit", correlation_id="correlation",
        endpoint="/api/agent/sessions/session/restart-handoffs/receipt/reconcile")
    request.side_effect = error
    with pytest.raises(LocalPAUnknownOutcome) as raised:
        tools["reconcile_agent_restart_handoff"]("session", "receipt", 3)
    assert raised.value is error
    assert raised.value.recovery_action == "get_operation_outcome"
    assert request.call_count == 1

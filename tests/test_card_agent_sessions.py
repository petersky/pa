from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from pa.core.ui.card_executions import build_card_execution_index
from pa.domain.models import AgentSession
from pa.execution.dispatch import DispatchRecord
from pa.modules.fleet import RemoteAgentStartBody
from pa.modules.items import _card_session_view


def session(**changes) -> AgentSession:
    values = {
        "id": "session-1",
        "agent_name": "codex",
        "origin_instance_id": "local",
        "status": "idle",
        "updated_at": datetime.now(UTC),
    }
    values.update(changes)
    return AgentSession(**values)


def test_card_session_states_cover_active_resumable_unavailable_and_failed() -> None:
    assert _card_session_view(session(), "local")["state"] == "active"
    resumable = _card_session_view(
        session(status="closed", external_session_id="provider-thread"), "local"
    )
    assert resumable["state"] == "resumable"
    assert resumable["selectable"] is True
    assert (
        _card_session_view(
            session(status="disconnected", external_session_id="provider-thread"),
            "local",
        )["state"]
        == "resumable"
    )
    assert (
        _card_session_view(session(status="closed", external_session_id=None), "local")[
            "state"
        ]
        == "unavailable"
    )
    assert (
        _card_session_view(session(origin_instance_id="remote"), "local")["state"]
        == "unavailable"
    )
    assert (
        _card_session_view(session(status="recovery_blocked"), "local")["state"]
        == "failed"
    )


def test_card_agent_template_defines_fresh_and_multi_session_controls() -> None:
    from pathlib import Path

    root = Path(__file__).parents[1] / "src" / "pa" / "server"
    template = (root / "templates" / "partials" / "card-detail-agent.html").read_text()
    script = (root / "static" / "js" / "spa.js").read_text()
    widget = (root / "static" / "js" / "agent-chat.js").read_text()
    assert "Start new session" in template
    assert "data-card-agent-select" in template
    assert "data-resume" in template
    assert "fresh=true" in template
    assert "recoverSession(selectedId)" in script
    assert "body.fresh = true" in widget
    assert "this.recoverSession(this.sessionId)" in widget
    assert 'data-acw-restart hidden>Resume session' in (
        root / "templates" / "partials" / "agent" / "chat-widget.html"
    ).read_text()


def test_session_list_order_supports_deterministic_default_selection() -> None:
    now = datetime.now(UTC)
    sessions = [
        session(id="newest", updated_at=now),
        session(id="older", updated_at=now - timedelta(minutes=1)),
    ]
    views = [_card_session_view(item, "local") for item in sessions]
    assert (
        next(view for view in views if view["state"] == "active")["session"].id
        == "newest"
    )


def test_remote_dispatch_without_local_session_is_one_visible_execution() -> None:
    record = DispatchRecord(
        mutation_id="mutation-1",
        idempotency_key="key-1",
        request_fingerprint="fingerprint-1",
        placement_request_fingerprint="fingerprint-1",
        card_id="card-1",
        authority_instance_id="macbook",
        authority_url="http://macbook.test",
        target_instance_id="monica",
        target_instance_name="Monica",
        placement_policy="best_match",
        request_payload={"provider": "codex", "model_id": "gpt-5"},
    )
    record.state = "running"
    record.session_id = "remote-session-1"
    ctx = type("Context", (), {"settings": type("Settings", (), {"instance_id": "macbook"})(), "services": {}})()

    result = build_card_execution_index(ctx, sessions=[], dispatches=[record])

    assert len(result["executions"]) == 1
    assert result["executions"][0]["remote_only"] is True
    assert result["executions"][0]["exact_href"] == "/agent?session=remote-session-1&instance=monica"
    assert result["primary_action"]["label"] == "View running work"
    assert result["exclusive_active"] is True
    assert result["parallel_start"]["requires_reason"] is True


def test_concurrent_remote_start_requires_an_explicit_reason() -> None:
    with pytest.raises(ValidationError, match="concurrent_reason"):
        RemoteAgentStartBody(allow_concurrent=True)
    body = RemoteAgentStartBody(
        allow_concurrent=True, concurrent_reason="Independent release validation"
    )
    assert body.concurrent_reason == "Independent release validation"

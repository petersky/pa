from datetime import UTC, datetime, timedelta

from pa.domain.models import AgentSession
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

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1] / "src" / "pa" / "server"


def test_agent_sidebar_exposes_opt_in_history_controls() -> None:
    template = (ROOT / "templates" / "pages" / "agent.html").read_text()

    assert "data-agent-history-toggle" in template
    assert "Show closed sessions" in template
    assert "data-agent-session-search" in template
    assert 'data-session-live="true"' in template


def test_agent_sidebar_loads_and_selects_durable_history() -> None:
    script = (ROOT / "static" / "js" / "agent-chat.js").read_text()

    assert 'includeClosed ? "/history?limit=500" : "/sessions"' in script
    assert 'this.api("/history/" + sessionId)' in script
    assert "filterSessionList" in script
    assert 'li.dataset.sessionLive !== "false"' in script
    assert "if (!historical) self.connectSSE();" in script


def test_blocked_session_surfaces_retry_and_close_guidance() -> None:
    template = (
        ROOT / "templates" / "partials" / "agent" / "chat-widget.html"
    ).read_text()
    script = (ROOT / "static" / "js" / "agent-chat.js").read_text()

    assert "data-acw-recovery-action" in template
    assert "data-acw-retry" in template
    assert '"/sessions/" + this.sessionId + "/retry"' in script
    assert "end it from the Session menu" in script

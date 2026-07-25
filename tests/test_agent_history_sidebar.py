from pathlib import Path


ROOT = Path(__file__).resolve().parents[1] / "src" / "pa" / "server"


def test_agent_sidebar_exposes_opt_in_history_controls() -> None:
    template = (ROOT / "templates" / "pages" / "agent.html").read_text()

    assert "data-agent-history-toggle" in template
    assert "Show closed sessions" in template
    assert "data-agent-session-search" in template
    assert "live_session_ids" in template
    assert "data-agent-session-close" in template
    assert "Forget" in template
    assert "data-agent-end-all" in template


def test_agent_sidebar_loads_and_selects_durable_history() -> None:
    script = (ROOT / "static" / "js" / "agent-chat.js").read_text()

    assert 'includeClosed ? "/history?limit=500" : "/sessions"' in script
    assert 'self.api("/history/" + sessionId)' in script
    assert "filterSessionList" in script
    assert 'li.dataset.sessionLive !== "false"' in script
    assert 'csrfFetch("/sessions/close-all"' in script
    assert "data-agent-session-close" in script
    assert '"/api/fleet/session-route/" + encodeURIComponent(sessionId)' in script


def test_agent_deep_link_survives_refresh_and_back_forward_without_close() -> None:
    script = (ROOT / "static" / "js" / "agent-chat.js").read_text()
    switch_block = script.split("AgentChatWidget.prototype.switchSession", 1)[1].split(
        "AgentChatWidget.prototype.setApiBase", 1
    )[0]

    assert 'url.searchParams.set("session", this.sessionId)' in script
    assert 'url.searchParams.set("instance", this.ownerInstanceId)' in script
    assert 'window.addEventListener("popstate"' in script
    assert "root._acw.switchSession(sessionId, true, instanceId" in script
    assert (
        "this.openSession(this.sessionId, this.ownerInstanceId, { replace: true })"
        in script
    )
    assert '"/close"' not in switch_block


def test_agent_page_exposes_non_destructive_recovery_action() -> None:
    template = (
        ROOT / "templates" / "partials" / "agent" / "chat-widget.html"
    ).read_text()
    script = (ROOT / "static" / "js" / "agent-chat.js").read_text()

    assert "data-acw-recover" in template
    assert '"/recover"' in script
    assert 'route.state === "owner_unreachable"' in script
    assert 'route.state === "missing"' in script


def test_blocked_session_surfaces_retry_and_close_guidance() -> None:
    template = (
        ROOT / "templates" / "partials" / "agent" / "chat-widget.html"
    ).read_text()
    script = (ROOT / "static" / "js" / "agent-chat.js").read_text()

    assert "data-acw-recovery-action" in template
    assert "data-acw-retry" in template
    assert '"/sessions/" + this.sessionId + "/retry"' in script
    assert "end it from the Session menu" in script

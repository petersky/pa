from pathlib import Path


ROOT = Path(__file__).resolve().parents[1] / "src" / "pa" / "server"


def test_agent_sidebar_exposes_opt_in_history_controls() -> None:
    template = (ROOT / "templates" / "pages" / "agent.html").read_text()
    widget = (
        ROOT / "templates" / "partials" / "agent" / "chat-widget.html"
    ).read_text()

    assert "data-agent-history-toggle" in template
    assert "Show closed sessions" in template
    assert 'role="switch"' in template
    assert 'aria-checked="false"' in template
    assert "data-agent-session-search" in template
    assert "live_session_ids" in template
    assert "data-agent-session-close" in template
    assert "Forget" in template
    assert "data-agent-end-all" in template
    assert "agent-session-title-tooltip" in template
    assert 'role="tooltip"' in template
    for label in ("Instance", "Provider", "Model", "Mode", "Status"):
        assert f"<dt>{label}</dt>" in template
    assert "data-acw-recover" in widget
    assert "data-acw-history" in widget


def test_agent_sidebar_loads_and_selects_durable_history() -> None:
    script = (ROOT / "static" / "js" / "agent-chat.js").read_text()

    assert 'includeClosed ? "/history?limit=500" : "/sessions"' in script
    assert 'self.api("/history/" + encodeURIComponent(sessionId))' in script
    assert "filterSessionList" in script
    assert 'li.dataset.sessionLive !== "false"' in script
    assert 'csrfFetch("/sessions/close-all"' in script
    assert "data-agent-session-close" in script
    assert '"/api/fleet/session-route/" + encodeURIComponent(sessionId)' in script
    assert "this.openSession(sessionId, ownerInstanceId" in script
    assert "retryAfterStartupRecovery" in script
    assert "resolveSessionNotLive" in script
    assert "clearSelectedSession" in script
    assert '"/sessions/" + encodeURIComponent(targetSessionId) + "/recover"' in script
    assert 'code === "session_deleted"' in script
    assert 'getAttribute("aria-checked") === "true"' in script
    assert "updateSessionTitleTooltips" in script
    assert 'item.setAttribute("aria-label"' in script
    assert 'metadataField("Instance"' in script


def test_agent_session_layout_regression_contracts_cover_density_and_viewports() -> None:
    """Keep long fixture values readable in expanded/collapsed Workshop layouts.

    The selectors are shared by server-rendered rows and live refreshes. The two
    page density states are the existing expanded and collapsed sidebar classes;
    viewport contracts cover desktop, tablet, and narrow widths.
    """
    css = (ROOT / "static" / "style.css").read_text()
    template = (ROOT / "templates" / "pages" / "agent.html").read_text()
    script = (ROOT / "static" / "js" / "agent-chat.js").read_text()

    assert "--pa-sidebar-left-width: clamp(300px, 28vw, 420px)" in css
    assert "@media (min-width: 769px)" in css
    assert "@media (max-width: 768px)" in css
    assert "@media (max-width: 430px)" in css
    assert ".page-agent.is-sidebar-collapsed .page-sidebar-left" in css
    assert "-webkit-line-clamp: 3" in css
    assert "font-size: 0.8rem" in css
    assert 'data-title-clamped="true"' in css
    assert "overflow-wrap: anywhere" in css
    assert "grid-template-columns: 1fr" in css
    assert "min-height: 2.5rem" in css
    assert "agent-session-actions" in template
    assert "agent-session-actions" in script


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

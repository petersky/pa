"""Regression coverage for Fleet's bounded remote activity ownership."""

from pathlib import Path

from pa.core.sse_observability import SSEConnectionRegistry


def test_sse_observability_pairs_active_proxy_legs() -> None:
    registry = SSEConnectionRegistry(over_age_seconds=60)
    downstream = registry.open(
        endpoint="/api/fleet/instances/{instance_id}/agent/session-events",
        direction="downstream",
        client_id="tab-1",
        peer_id="peer-1",
        session_scope="all_live",
        paired_id="pair-1",
    )
    upstream = registry.open(
        endpoint="/api/agent/session-events",
        direction="upstream",
        client_id="tab-1",
        peer_id="peer-1",
        session_scope="all_live",
        paired_id="pair-1",
    )
    active = registry.snapshot()
    assert active["active"] == 2
    assert active["paired"]["downstream"] == 1
    assert active["paired"]["upstream"] == 1
    assert active["paired"]["balanced"]
    registry.close(upstream, "cancelled")
    registry.close(downstream, "cancelled")
    closed = registry.snapshot()
    assert closed["active"] == 0
    assert closed["cancelled"] == 2


def test_layout_emits_explicit_section_lifecycle_before_visibility_change() -> None:
    source = (
        Path(__file__).parents[1]
        / "src"
        / "pa"
        / "server"
        / "static"
        / "js"
        / "layout.js"
    ).read_text()
    before = source.index('"pa:section-will-change"')
    visibility = source.index('root.querySelectorAll("[data-section]")')
    after = source.index('"pa:section-changed"')
    assert before < visibility < after
    assert "root.dataset.activeSection = sectionId" in source


def test_fleet_uses_one_multiplexed_transport_and_live_only_cursors() -> None:
    source = (
        Path(__file__).parents[1]
        / "src"
        / "pa"
        / "server"
        / "static"
        / "js"
        / "fleet.js"
    ).read_text()
    assert '"/session-events?client_id="' in source
    assert '"/session-events/capabilities"' in source
    assert 'new BroadcastChannel("pa-fleet-activity-v1")' in source
    assert "session.live !== true || session.orphan === true" in source
    assert (
        '"/sessions/" + encodeURIComponent(session.id) +\n        "/events'
        not in source
    )
    assert "error.status === 404 || error.status === 410" in source
    assert "scheduleLegacyRemotePoll" in source
    assert "15000" in source
    assert "widgetRoot._acw.useExternalEventTransport(true)" in source
    assert "remoteActivityEventTypes.forEach" in source


def test_fleet_consumes_section_and_navigation_teardown_events() -> None:
    source = (
        Path(__file__).parents[1]
        / "src"
        / "pa"
        / "server"
        / "static"
        / "js"
        / "fleet.js"
    ).read_text()
    assert 'addEventListener("pa:section-will-change"' in source
    assert 'addEventListener("pa:section-changed"' in source
    assert 'addEventListener("htmx:beforeRequest"' in source
    assert 'stopRemoteActivity("pagehide", true)' in source
    assert 'stopRemoteActivity("page-suspended", true)' in source
    assert 'layout.dataset.activeSection === "operations"' in source
    assert "if (!select || !remoteOperationsSectionActive)" in source

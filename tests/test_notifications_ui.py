from pathlib import Path

ROOT = Path(__file__).parents[1]


def test_bell_panel_accessibility_live_updates_and_draft_preservation_contract() -> (
    None
):
    chrome = (ROOT / "src/pa/server/templates/partials/chrome-actions.html").read_text()
    script = (ROOT / "src/pa/server/static/js/notifications.js").read_text()
    styles = (ROOT / "src/pa/server/static/style.css").read_text()

    assert 'aria-label="Open notifications"' in chrome
    assert 'role="dialog"' in chrome
    assert 'aria-live="polite"' in chrome
    assert "data-notification-count hidden" in chrome
    assert 'data-notification-filter="outstanding"' in chrome
    assert "var drafts = new Map()" in script
    assert "data-notification-send-fields" in script
    assert 'new EventSource("/api/cards/events")' in script
    assert "setInterval" in script
    assert 'event.key === "Escape"' in script
    assert "if (flyout.hidden) return;" in script
    assert "@media (max-width: 640px)" in styles

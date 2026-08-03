from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_chat_widget_exposes_accessible_command_typeahead():
    template = (
        ROOT / "src/pa/server/templates/partials/agent/chat-widget.html"
    ).read_text()
    script = (ROOT / "src/pa/server/static/js/agent-chat.js").read_text()
    assert 'role="listbox"' in template
    assert 'aria-autocomplete="list"' in template
    assert 'raw.charAt(0) !== "/"' in script
    assert 'raw.indexOf("//") === 0' in script
    assert 'e.key === "ArrowDown"' in script
    assert 'e.key === "Escape"' in script
    assert "aria-activedescendant" in script


def test_recognized_command_executes_instead_of_prompt_submission():
    script = (ROOT / "src/pa/server/static/js/agent-chat.js").read_text()
    assert "this.commandInvocation(rawText.trim())" in script
    assert '"/commands/execute"' in script
    assert "if (invocation)" in script
    assert "this.executeCommand(invocation.command" in script


def test_settings_and_dispatch_surfaces_distinguish_collaboration_mode():
    settings = (ROOT / "src/pa/server/templates/pages/settings.html").read_text()
    fleet = (ROOT / "src/pa/server/templates/pages/fleet.html").read_text()
    assert "Collaboration-mode policy" in settings
    assert 'name="collaboration_mode"' in fleet
    assert 'name="mode_id"' in fleet
    assert "never changes sandbox" in settings

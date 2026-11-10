"""`/` and `$` menus in the real chat widget."""
import asyncio
import base64
import json
import subprocess
import time

import httpx
import pytest

from pa.browser.manager import BrowserManager, _browser_executable
from pa.browser.session import BrowserScope, BrowserSessionManager
from pa.domain.models import AgentSession, CardCreate
from tests.test_card_status_browser import operator_server  # noqa: F401
from tests.visualization_chat_fixture import build_visualization_app

CATALOG = {
    "generation": 1,
    "commands": [
        {"name": "review", "origin": "provider", "provider": "codex", "kind": "command", "description": "Review changes"},
        {"name": "rename", "origin": "provider", "provider": "codex", "kind": "command", "description": "Rename", "input_hint": "new name"},
        {"name": "pa:mode", "origin": "pa", "kind": "command", "description": "Mode", "input_hint": "default | plan", "input_required": True},
        {"name": "$seed", "origin": "provider", "provider": "codex", "kind": "skill", "description": "Make a seed"},
        {"name": "$google-drive:google-docs", "origin": "provider", "provider": "codex", "kind": "skill", "description": "Docs"},
    ],
}


@pytest.fixture
def operator_app(tmp_path):
    return build_visualization_app(tmp_path)


@pytest.mark.skipif(not _browser_executable(), reason="managed Chromium is not installed")
@pytest.mark.asyncio
async def test_slash_and_dollar_menus_complete_text_without_running_anything(operator_server, tmp_path):  # noqa: F811
    app, base = operator_server
    browser = BrowserManager(tmp_path / "browser")
    manager = BrowserSessionManager(browser, instance_id="composer-triggers-test")
    scope = BrowserScope("user:test", "composer-triggers", "composer-triggers-test")
    async with httpx.AsyncClient(base_url=base, follow_redirects=False) as client:
        await client.get("/fixture")
    try:
        await manager.attach(scope, url=base + "/agent?session=viz-session", width=1280, height=900)
        page = manager.resolve(scope).page

        async def wait_for(expression):
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                if await page.evaluate(expression):
                    return
                await asyncio.sleep(.05)
            pytest.fail("Browser condition timed out: " + expression)

        await wait_for("!!(document.querySelector('[data-agent-chat]')._acw && document.querySelector('[data-acw-input]'))")
        # A real catalog refresh can land at any moment, so the fixed catalog is re-applied on every keystroke.
        catalog = json.dumps(CATALOG)
        type_text = f"""(([text, caret]) => {{ const w = document.querySelector('[data-agent-chat]')._acw;
            w.commandCatalog = {catalog}; w.commandCatalogSession = w.sessionId;
            const i = document.querySelector('[data-acw-input]'); i.disabled = false;
            i.value = text; i.setSelectionRange(caret, caret); i.dispatchEvent(new Event('input', {{bubbles: true}})); }})"""
        titles = "[...document.querySelectorAll('[data-acw-command-options] .acw-command-title')].map(e => e.textContent)"
        menu_open = "!document.querySelector('[data-acw-command-menu]').hidden"

        await page.evaluate(f"{type_text}(['/', 1])")
        assert await page.evaluate(menu_open)
        assert await page.evaluate(titles) == ["/pa:mode", "/rename", "/review"]  # skills never appear under "/"

        await page.evaluate(f"{type_text}(['$', 1])")
        assert await page.evaluate(titles) == ["$google-drive:google-docs", "$seed"]

        await page.evaluate(f"{type_text}(['$zzz', 4])")
        assert not await page.evaluate(menu_open)  # quiet when nothing matches: "$zzz" is ordinary text
        await page.evaluate(f"{type_text}(['costs $5', 8])")
        assert not await page.evaluate(menu_open)
        await page.evaluate(f"{type_text}(['/etc/hosts', 10])")
        assert await page.evaluate(titles) == []  # a path is not a command, and Enter will send it

        # A skill is completed in place, mid-sentence, without running anything.
        await page.evaluate(f"{type_text}(['use $se now', 7])")
        assert await page.evaluate(titles) == ["$seed"]
        # Type and click in one task: a catalog refresh landing in between would legitimately re-render the menu.
        await page.evaluate(f"({type_text})(['use $se now', 7]); document.querySelector('[data-acw-command-options] .acw-command-option').click()")
        assert await page.evaluate("document.querySelector('[data-acw-input]').value") == "use $seed now"
        assert await page.evaluate("document.querySelector('[data-acw-input]').selectionStart") == 10
        assert not await page.evaluate(menu_open)

        # A command with optional input waits for it instead of running immediately.
        await page.evaluate(f"{type_text}(['/ren', 4])")
        assert "Optional input: new name" in await page.evaluate("document.querySelector('.acw-command-input').textContent")
        await page.evaluate(f"({type_text})(['/ren', 4]); document.querySelector('[data-acw-command-options] .acw-command-option').click()")
        assert await page.evaluate("document.querySelector('[data-acw-input]').value") == "/rename "
        assert await page.evaluate("document.body.textContent.includes('Running /rename')") is False

        # Tab completes the highlighted skill as well.
        await page.evaluate(f"""({type_text})(['$goo', 4]);
            document.querySelector('[data-acw-input]').dispatchEvent(new KeyboardEvent('keydown', {{key: 'Tab', bubbles: true, cancelable: true}}))""")
        assert await page.evaluate("document.querySelector('[data-acw-input]').value") == "$google-drive:google-docs "
    finally:
        await manager.close()
        await browser.close()


PIXEL = "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk+M9QDwADhgGAWjR9awAAAABJRU5ErkJggg=="


@pytest.mark.skipif(not _browser_executable(), reason="managed Chromium is not installed")
@pytest.mark.asyncio
async def test_at_mentions_search_the_server_and_complete_or_attach(operator_server, tmp_path):  # noqa: F811
    app, base = operator_server
    workspace = tmp_path / "repo"
    (workspace / "src/app").mkdir(parents=True)
    (workspace / "docs").mkdir()
    (workspace / "src/app/server.py").write_text("print()\n")
    (workspace / "docs/shot.png").write_bytes(base64.b64decode(PIXEL))
    subprocess.run(["git", "init", "-q", str(workspace)], check=True)
    browser = BrowserManager(tmp_path / "browser")
    manager = BrowserSessionManager(browser, instance_id="composer-mentions-test")
    scope = BrowserScope("user:test", "composer-mentions", "composer-mentions-test")
    async with httpx.AsyncClient(base_url=base, follow_redirects=False) as client:
        await client.get("/fixture")
    store = app.state.ctx.store
    session = store.get_session("viz-session")
    store.save_session(AgentSession(id="planning-chat", agent_name="codex", purpose="chat", principal_id=session.principal_id,
                                    title="Release planning"))
    store.create_card(CardCreate(title="Fix release pipeline"))
    # The workspace must be a trusted root for the image to be served back.
    try:
        await manager.attach(scope, url=base + "/agent?session=viz-session", width=1280, height=900)
        page = manager.resolve(scope).page

        def pin_workspace():
            # Opening the page re-saves the session; keep pointing it at the test workspace.
            current = store.get_session("viz-session")
            if current.cwd != str(workspace):
                current.cwd = str(workspace)
                store.save_session(current)

        async def wait_for(expression):
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                pin_workspace()
                if await page.evaluate(expression):
                    return
                await asyncio.sleep(.05)
            pytest.fail("Browser condition timed out: " + expression)

        await wait_for("!!(document.querySelector('[data-agent-chat]')._acw && document.querySelector('[data-acw-input]'))")
        pin_workspace()
        type_text = """(([text, caret]) => { const i = document.querySelector('[data-acw-input]'); i.disabled = false;
            i.value = text; i.setSelectionRange(caret, caret); i.dispatchEvent(new Event('input', {bubbles: true})); })"""
        titles = "[...document.querySelectorAll('[data-acw-command-options] .acw-command-title')].map(e => e.textContent)"
        value = "document.querySelector('[data-acw-input]').value"
        menu_open = "!document.querySelector('[data-acw-command-menu]').hidden"
        first = "document.querySelector('[data-acw-command-options] .acw-command-option').click()"

        async def mention(text, caret, title):
            """Type and wait for a result; retype if the fixture reset the working directory mid-search."""
            for _ in range(6):
                pin_workspace()
                # An unchanged query never restarts a search, so clear the token before retyping it.
                await page.evaluate(f"{type_text}(['', 0])")
                await page.evaluate(f"{type_text}([{text!r}, {caret}])")
                deadline = time.monotonic() + 3
                while time.monotonic() < deadline:
                    if title in await page.evaluate(titles):
                        return
                    await asyncio.sleep(.05)
            pytest.fail(f"No mention result {title!r} for {text!r}")

        # A file completes to its relative path.
        await mention("@serv", 5, "@src/app/server.py")
        await page.evaluate(first)
        assert await page.evaluate(value) == "@src/app/server.py "

        # A conversation completes to a labelled id; the kind prefix narrows the search.
        await mention("talk to @session:rel", 19, "Release planning")
        assert await page.evaluate(titles) == ["Release planning"]
        await page.evaluate(first)
        assert await page.evaluate(value) == "talk to @session:planning-chat "

        # A card likewise.
        await mention("@card:pipe", 10, "Fix release pipeline")
        await page.evaluate(first)
        assert (await page.evaluate(value)).startswith("@card:") and (await page.evaluate(value)).endswith(" ")

        # An image is attached instead of typed, and its token is removed from the text.
        await mention("look at @shot now", 13, "@docs/shot.png")
        assert await page.evaluate("document.querySelector('.acw-command-origin').textContent") == "image"
        await page.evaluate(first)
        assert await page.evaluate(value) == "look at now"
        await wait_for("document.querySelector('[data-agent-chat]')._acw.pendingImages.length === 1")
        assert await page.evaluate("document.querySelector('[data-agent-chat]')._acw.pendingImages[0].name") == "shot.png"

        # Plain text that merely contains "@" never shows a menu.
        for text, caret in (("mail ada@example.com", 20), ("@nothingmatchesthis", 19)):
            await page.evaluate(f"{type_text}([{text!r}, {caret}])")
            await asyncio.sleep(.5)
            assert not await page.evaluate(menu_open)
    finally:
        await manager.close()
        await browser.close()


@pytest.mark.skipif(not _browser_executable(), reason="managed Chromium is not installed")
@pytest.mark.asyncio
async def test_bang_runs_a_command_and_shares_or_keeps_the_output(operator_server, tmp_path):  # noqa: F811
    app, base = operator_server
    browser = BrowserManager(tmp_path / "browser")
    manager = BrowserSessionManager(browser, instance_id="composer-shell-test")
    scope = BrowserScope("user:test", "composer-shell", "composer-shell-test")
    async with httpx.AsyncClient(base_url=base, follow_redirects=False) as client:
        await client.get("/fixture")
    try:
        await manager.attach(scope, url=base + "/agent?session=viz-session", width=1280, height=900)
        page = manager.resolve(scope).page

        async def wait_for(expression):
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                if await page.evaluate(expression):
                    return
                await asyncio.sleep(.05)
            pytest.fail("Browser condition timed out: " + expression)

        await wait_for("!!(document.querySelector('[data-agent-chat]')._acw && document.querySelector('[data-acw-input]'))")
        # The server side is covered elsewhere: stub the two requests so this proves the composer's behaviour.
        await page.evaluate("""(() => {
            const w = document.querySelector('[data-agent-chat]')._acw;
            window.__calls = [];
            w.sessionId = 'viz-session'; w.sessionClosed = false;
            w.api = (path, opts) => { window.__calls.push(['api', path, JSON.parse(opts.body || 'null')]);
              const body = JSON.parse(opts.body);
              const result = {id: 'run-' + window.__calls.length, command: body.command, cwd: '/w', exit_code: 0, signal: null,
                timed_out: false, cancelled: false, output: 'hello\\n', truncated: false, omitted_chars: 0, duration_ms: 40,
                sent_to_agent: body.send_to_agent};
              if (body.send_to_agent) result.prompt = 'I ran this in the terminal:\\n\\n$ ' + body.command;
              return Promise.resolve(result); };
            w.apiWithTimeout = (path, ms, opts) => { window.__calls.push(['prompt', path, JSON.parse(opts.body)]);
              const error = new Error('stubbed'); error.status = 400; return Promise.reject(error); };
        })()""")
        type_text = """(text => { const i = document.querySelector('[data-acw-input]'); i.disabled = false;
            i.value = text; i.setSelectionRange(text.length, text.length); i.dispatchEvent(new Event('input', {bubbles: true})); })"""
        enter = "document.querySelector('[data-acw-input]').dispatchEvent(new KeyboardEvent('keydown', {key: 'Enter', bubbles: true, cancelable: true}))"
        calls = "window.__calls.map(c => [c[0], c[2].command || c[2].message, c[2].send_to_agent])"
        menu_text = "document.querySelector('[data-acw-command-state]').textContent"

        # The menu explains what Enter will do, and says how the choice flips.
        await page.evaluate(f"{type_text}('!git status')")
        assert await page.evaluate("!document.querySelector('[data-acw-command-menu]').hidden")
        hint = await page.evaluate(menu_text)
        assert "shared with the agent" in hint and "!! keeps it here" in hint

        # `!cmd` shares by default: the command runs, then the agent gets the output as an ordinary prompt.
        await page.evaluate(enter)
        await wait_for("window.__calls.length === 2")
        assert await page.evaluate(calls) == [["api", "git status", True], ["prompt", "I ran this in the terminal:\n\n$ git status", None]]

        # `!!cmd` keeps the output local and renders it as a result bubble; nothing goes to the agent.
        await page.evaluate("window.__calls.length = 0; document.querySelector('[data-agent-chat]')._acw.submissionPending = false")
        await page.evaluate(f"{type_text}('!!echo hello')")
        assert "keeps it here" not in await page.evaluate(menu_text) and "shares it with the agent" in await page.evaluate(menu_text)
        await page.evaluate(enter)
        await wait_for("!!document.querySelector('.acw-msg-shell')")
        assert await page.evaluate(calls) == [["api", "echo hello", False]]
        bubble = await page.evaluate("document.querySelector('.acw-msg-shell .acw-bubble-shell').textContent")
        assert "$ echo hello" in bubble and "hello" in bubble and "exit 0" in bubble
        assert await page.evaluate("document.querySelector('.acw-msg-shell').dataset.shellStatus") == "ok"
        assert await page.evaluate("document.querySelector('[data-acw-input]').value") == ""

        # The backslash escape sends literal text; with the feature off, `!` is ordinary text too.
        for text, sent in (("\\!important", "!important"), ("!ls", "!ls")):
            if text == "!ls":
                await page.evaluate("document.querySelector('[data-agent-chat]').dataset.shell = 'off'")
            await page.evaluate("window.__calls.length = 0; document.querySelector('[data-agent-chat]')._acw.submissionPending = false")
            await page.evaluate(f"{type_text}({text!r})")
            await page.evaluate(enter)
            await wait_for("window.__calls.length === 1")
            assert await page.evaluate(calls) == [["prompt", sent, None]]

        # A recorded result replays from the transcript (once, even if it is delivered twice).
        await page.evaluate("""(() => { const w = document.querySelector('[data-agent-chat]')._acw;
            const event = {seq: 0, type: 'shell_result', payload: {id: 'replayed', command: 'make test', cwd: '/w', exit_code: 2,
              signal: null, timed_out: false, cancelled: false, output: 'boom\\n', truncated: false, duration_ms: 1200}};
            w.handleEvent(event, true); w.handleEvent(event, true); })()""")
        assert await page.evaluate("document.querySelectorAll('.acw-msg-shell').length") == 2  # the earlier `!!echo hello` plus this one
        assert await page.evaluate("[...document.querySelectorAll('.acw-msg-shell')].pop().dataset.shellStatus") == "failed"
    finally:
        await manager.close()
        await browser.close()

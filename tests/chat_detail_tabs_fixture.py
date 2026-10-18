"""Real chat controls with synthetic in-memory replies; no provider or PA state."""
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader


ROOT = Path(__file__).parents[1]


def build_detail_tabs_fixture():
    app = FastAPI()
    env = Environment(loader=FileSystemLoader(ROOT / "src/pa/server/templates"))
    widget = env.get_template("partials/agent/chat-widget.html")
    content = widget.render(session_id="fixture", auto_start=False, telemetry_enabled=True,
                            telemetry_session_header="collapsed", telemetry_ui_refresh_seconds=60)
    other = widget.render(session_id="other", auto_start=False, compact=True, telemetry_enabled=False)
    actions = env.get_template("partials/chrome-actions.html").render(
        agent_startup={"complete": True}, appearance="light", telemetry_enabled=False)
    scripts = ''.join(f'<script src="/static/js/{name}.js" defer></script>' for name in
                      ("execution-selection", "conversation-issues", "conversation-threads", "agent-chat"))

    @app.get("/", response_class=HTMLResponse)
    async def page():
        return '''<!doctype html><html lang="en" data-theme="pa" data-appearance="light">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Chat detail controls fixture</title>
<link rel="stylesheet" href="/static/style.css">
<link id="theme" rel="stylesheet" href="/static/themes/pa/light.css">
<style>main {padding: 1rem; max-width: 1100px; margin: auto;} .fixture-chat {height: 650px; display: flex;}
#other-widget {height: 320px; display: flex; margin-top: 2rem;}</style>
<script type="application/json" src="https://cdn.jsdelivr.net/npm/marked@15.0.7/marked.min.js" data-loaded="1"></script>
<script type="application/json" src="https://cdn.jsdelivr.net/npm/dompurify@3.2.4/dist/purify.min.js" data-loaded="1"></script>
''' + scripts + '<body><header class="site-header"><span>PA · UI fixture</span>' + actions + '''</header>
<main><div id="primary-widget" class="fixture-chat">''' + content + '''</div><div id="other-widget">''' + other + '''</div></main>
<script>
window.fixtureCalls = [];
window.addEventListener('DOMContentLoaded', () => {
  const widget = document.querySelector('#primary-widget [data-agent-chat]')._acw;
  window.fixtureWidget = widget;
  window.fixtureIssues = {revision: 1, issues: [{id: 'one', title: 'First issue', detail: '', group: '', deferred: false}], requests: []};
  window.fixtureSnapshot = {session: {id: 'fixture', updated_at: 'v1', agent_name: 'codex', model_id: 'confirmed-model', mode_id: 'workspace-write'},
    execution_selection: {requested: {harness: 'codex', model: 'fixture-model'}, provider_confirmation: {state: 'confirmed', effective: {model_id: 'fixture-model'}}}};
  widget.api = async (path, options = {}) => {
    window.fixtureCalls.push({path, method: options.method || 'GET'});
    if (path.endsWith('/execution-catalog')) return {candidates: [{model: 'fixture-model', options: {}}]};
    if (path.endsWith('/execution-settings')) return {state: 'applied'};
    if (path.endsWith('/conversation-capabilities')) return {native_branch_available: true, copy_available: true, steering_reason: 'Synthetic fixture.'};
    if (path.endsWith('/branch')) return {session: {id: 'fixture-branch', status: 'idle'}};
    if (path.endsWith('/issues')) {
      let issues = window.fixtureIssues;
      if (options.method === 'PUT') window.fixtureIssues = issues = {...issues, revision: issues.revision + 1, issues: JSON.parse(options.body).issues};
      return structuredClone(issues);
    }
    return window.fixtureSnapshot;
  };
  widget.applyOptionSnapshot = snap => window.PAExecutionSelection.renderSession(widget, snap);
  window.PAExecutionSelection.renderSession(widget, window.fixtureSnapshot);
  widget.composerReadyMs = 12;
  widget.renderTimings();
  widget.issues.ready();
  widget.renderModelsModes(window.fixtureSnapshot);
  window.fixtureReady = true;
});
</script></body></html>'''

    app.mount("/static", StaticFiles(directory=ROOT / "src/pa/server/static"), name="static")
    return app

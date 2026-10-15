"""Model-specific catalog and delayed acknowledgments; no provider is launched."""
import asyncio
import copy
import tempfile
from datetime import UTC, datetime
from pathlib import Path

import uvicorn
from fastapi import Request
from fastapi.responses import HTMLResponse, JSONResponse
from jinja2 import Environment, FileSystemLoader

from pa.execution.selection_catalog import candidates_from_advertisement
from tests.agent_queue_fixture import build_queue_fixture

MODELS = ['gpt-6.1-sol', 'gpt-6-astra', 'gpt-6-luna', 'unknown-model', 'no-effort']
LEVELS = ['low', 'medium', 'high', 'xhigh', 'max', 'ultra']


def build_effort_fixture(directory):
    app = build_queue_fixture(directory)
    app.router.routes = [r for r in app.router.routes if getattr(r, 'path', '') not in {'/', '/api/agent/sessions/{session_id}'}]
    app.state.calls = []
    app.state.catalog_delay = 0
    app.state.option_delays = {}
    app.state.model_delay = 0
    app.state.defaults_model = MODELS[0]
    app.state.model_ack = True
    app.state.native_pending = False
    app.state.current_model = MODELS[0]
    app.state.current_effort = 'ultra'
    app.state.use_receipt = False
    app.state.catalog_reads = 0
    env = Environment(loader=FileSystemLoader(Path(__file__).parents[1] / 'src/pa/server/templates'))
    widget = env.get_template('partials/agent/chat-widget.html').render(session_id='live-fixture', current_instance_id='live-fixture', telemetry_enabled=False)
    selection = env.get_template('partials/execution-preferences.html').render(selection_purpose='new-session',selection_open=True)

    def advertisement(model=None):
        model = model or app.state.current_model
        levels = LEVELS[:-1] if model == 'gpt-6-luna' else LEVELS
        return {'models': {'currentModelId':model, 'availableModels': [
            {'modelId':f'{m}[{e}]'} for m in MODELS[:3] for e in (LEVELS[:-1] if m == 'gpt-6-luna' else LEVELS)
        ] + [{'modelId':m} for m in MODELS[3:]]}, 'config_options': [
            {'id':'model', 'category':'model', 'name':'Model', 'currentValue':model, 'options':[{'value':m} for m in MODELS]},
            {'id':'reasoning_effort', 'category':'thought_level', 'name':'Effort', 'currentValue':app.state.current_effort,
             'options':[{'value':v} for v in levels] if model not in MODELS[3:] else []},
        ]}

    def candidates():
        values = [c.model_dump(mode='json') for c in candidates_from_advertisement(instance_id='live-fixture', harness='codex',
            advertisement=advertisement(), readiness='ready', observed_at=datetime.now(UTC), source='fixture')]
        for c in values:
            if c['model'] == 'no-effort': c['reasoning'] = {'support':'unsupported', 'values':[]}
        values.append({**values[-1], 'harness':'cursor','model':'unknown-model','default':True,'reasoning':{'support':'unknown','values':[]}})
        return values

    @app.get('/', response_class=HTMLResponse)
    async def page():
        return '''<!doctype html><html lang="en" data-pa-instance-id="live-fixture" data-theme="pa" data-appearance="light">
        <meta name="viewport" content="width=device-width,initial-scale=1"><title>Isolated effort selector</title>
        <link rel="stylesheet" href="/static/style.css"><link rel="stylesheet" href="/static/themes/pa/light.css">
        <script src="/static/js/native-effort.js"></script><script src="/static/js/execution-selection.js" defer></script>
        <script src="/static/js/agent-chat-drafts.js" defer></script><script src="/static/js/agent-chat-draft-widget.js" defer></script>
        <script src="/static/js/session-recovery.js" defer></script><script src="/static/js/agent-chat.js" defer></script>
        <h1>Model effort fixture</h1><main class="page-agent">''' + widget + '''</main>'''

    @app.get('/new', response_class=HTMLResponse)
    async def new_page():
        return '<html><script src="/static/js/native-effort.js"></script><script src="/static/js/execution-selection.js" defer></script><form>' + selection + '</form></html>'

    @app.get('/api/execution/defaults')
    async def defaults():
        return {'layers':[{'source':'user','preferences':{'harness':{'intent':'required','value':'codex'},'model':{'intent':'required','value':app.state.defaults_model},'reasoning':{'intent':'required','value':'ultra'}}}]}

    @app.post('/api/execution/preview')
    async def preview():
        return {}

    @app.get('/api/agent/provider-options/{provider}')
    async def provider_options(provider: str):
        await asyncio.sleep(app.state.option_delays.get(provider, 0))
        if provider == 'cursor':
            return {'models':{'availableModels':[{'modelId':'unknown-model'}]}, 'config_options':[]}
        return advertisement()

    @app.get('/defaults', response_class=HTMLResponse)
    async def defaults_page():
        # Execute the shipped Settings handler against a minimal, local form.
        source = (Path(__file__).parents[1]/'src/pa/server/templates/pages/settings.html').read_text()
        start = source.index('<script>\n(function () {\n  var form = document.getElementById("pa-agent-prefs-form")')
        script = source[start:source.index('</script>',start)+9]
        return '''<html><script src="/static/js/native-effort.js"></script><form id="pa-agent-prefs-form" data-instance-provider="codex">
        <select name="user_provider"><option>codex</option><option>cursor</option></select>
        <select name="global_provider"><option>codex</option><option>cursor</option></select>
        <fieldset data-settings-defaults-scope="user">
        <select data-settings-default-provider><option>codex</option><option>cursor</option></select>
        <select data-settings-default-model data-saved-value="gpt-6.1-sol[high]"></select>
        <select data-settings-default-mode></select>
        <select data-settings-default-effort data-saved-value="ultra"></select>
        <div data-settings-default-related></div></fieldset>
        <p id="pa-agent-prefs-status"></p></form>''' + script + '</html>'

    @app.get('/api/agent/sessions/{session_id}')
    async def snapshot(session_id: str):
        a = advertisement()
        session = app.state.runtime.session.model_dump(mode='json')
        session.update(id=session_id, model_id=app.state.current_model, config_json={'values':{'model':app.state.current_model, 'reasoning_effort':app.state.current_effort}}, updated_at='2026-10-15T05:00:00Z')
        result = {'session':session,'connected':True,'prompting':False,'queue':[], **a}
        if app.state.use_receipt:
            selected = {'harness':'codex','connection':'default','model':app.state.current_model,'reasoning':app.state.current_effort}
            result['execution_selection'] = {'requested':selected, 'provider_confirmation':{'state':'confirmed','effective':{'model_id':app.state.current_model,'reasoning':app.state.current_effort}}}
            session['config_json']['execution_selection'] = {'selected':selected}
        return result

    @app.get('/api/agent/sessions/{session_id}/execution-catalog')
    async def catalog(session_id: str):
        app.state.catalog_reads += 1
        result = {'candidates':copy.deepcopy([c for c in candidates() if c['harness']=='codex']), 'session_version':'2026-10-15T05:00:00Z'}
        await asyncio.sleep(app.state.catalog_delay)
        return result

    @app.get('/api/execution/catalog')
    async def global_catalog():
        return {'instance_id':'live-fixture','candidates':candidates()}

    @app.put('/api/agent/sessions/{session_id}/config')
    async def config(session_id: str, request: Request):
        body = await request.json()
        app.state.calls.append((session_id,'config',body))
        if body['config_id'] == 'model': await asyncio.sleep(app.state.model_delay)
        if body['config_id'] == 'model' and app.state.model_ack:
            app.state.current_model = body['value']
            app.state.current_effort = 'medium'
        elif body['config_id'] == 'reasoning_effort':
            if app.state.current_model == 'gpt-6-luna' and body['value'] == 'ultra':
                return JSONResponse({'detail':'Unsupported effort'},status_code=400)
            app.state.current_effort = body['value']
        return body

    @app.post('/api/agent/sessions/{session_id}/execution-settings')
    async def settings(session_id: str, request: Request):
        body = await request.json(); app.state.calls.append((session_id,'native',body))
        return {'state':'pending' if app.state.native_pending else 'applied'}

    return app


if __name__ == '__main__':
    with tempfile.TemporaryDirectory(prefix='pa-effort-') as directory:
        uvicorn.run(build_effort_fixture(Path(directory)),host='127.0.0.1',port=8094,log_level='warning')

"""Real isolated PA settings, with local capability evidence and no ACP processes."""
from pathlib import Path
import tempfile

from fastapi.responses import JSONResponse
from pa.config import Settings
from pa.core.kernel import Kernel
from pa.core.preferences import get_preferences_store, SurfaceAgentPrefs
from pa.domain.store import reset_store
from pa.instance.agent_session import reset_instance_agent


def build_settings_app(directory, *, personal=True):
    # A prior test app may have closed and removed its TemporaryDirectory.
    # Bind this app's disabled agent/workspace reader to its own store instead
    # of reusing that process singleton and its expired database path.
    reset_store()
    reset_instance_agent()
    settings = Settings(data_dir=directory / 'data', workspace_root=directory / 'workspaces',
                        agent_enabled=False, peers=[], sync_token='isolated-settings-fixture',
                        agent_provider='codex', telemetry_enabled=False)
    # Seed only this throwaway fixture before its server starts.
    get_preferences_store(settings.data_dir).update(
        agent_provider='cursor', agent_auto_approve_permissions=True,
        agent_surfaces={'chat.default': SurfaceAgentPrefs(provider='codex', model_id='model-a',
            mode_id='workspace-write', effort='high', config={'unadvertised':'keep'})})
    if personal:
        get_preferences_store(settings.data_dir, user_id='local').update(
            agent_provider='openinterpreter', agent_auto_approve_permissions=False,
            agent_surfaces={'chat.default': SurfaceAgentPrefs(config={'private-option':'keep'},
                execution_preferences={'connection': {'intent':'preferred','value':'default'}}),
                'chat.card': SurfaceAgentPrefs(provider='cursor', model_id='card-model', config={'card-option':'keep'})})
    app = Kernel.boot(settings=settings).build_app()
    app.state.settings_writes = []
    app.state.reject_settings = False
    app.state.reject_preview = False
    app.state.catalog_empty = False
    @app.middleware('http')
    async def capabilities(request, call_next):
        path = request.url.path
        if path == '/api/agent/preferences' and request.method == 'PUT':
            app.state.settings_writes.append(await request.json())
            if app.state.reject_settings:
                return JSONResponse({'detail':'fixture rejection'}, status_code=403)
        if path == '/api/agent/preferences/preview' and app.state.reject_preview:
            return JSONResponse({'detail':'preview rejection'}, status_code=403)
        if path.startswith('/api/agent/provider-options/'):
            return JSONResponse({'models': {'availableModels':[{'modelId':x} for x in ['model-a','model-b']]},
                'modes': {'availableModes':[{'id':'workspace-write'},{'id':'read-only'}]},
                'config_options': []})
        if path == '/api/execution/catalog':
            return JSONResponse({'instance_id':settings.instance_id,'candidates':[] if app.state.catalog_empty else [
                {'instance_id':settings.instance_id,'harness':'codex','connection':'default','model':model,
                 'default':model=='model-a','reasoning':{'support':'supported','values':['high','low']}}
                for model in ['model-a','model-b']]})
        return await call_next(request)
    return app


def create_app():
    return build_settings_app(Path(tempfile.mkdtemp(prefix='pa-settings-scope-')))

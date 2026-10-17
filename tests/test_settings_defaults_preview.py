"""Read-only settings previews reuse new-session precedence and preserve stored intent."""
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from pa.config import Settings
from pa.core.preferences import SurfaceAgentPrefs, get_preferences_store
from pa.execution.selection import merge_preferences
from pa.execution.selection_service import preference_layers
from pa.modules.agent_chat import PreferencesBody, agent_defaults_preview, preview_agent_preferences


@pytest.mark.parametrize('scope', ['user', 'global'])
def test_preview_matches_session_layers_and_never_persists(tmp_path, scope):
    settings = Settings(data_dir=tmp_path/'data', workspace_root=tmp_path/'workspaces', agent_provider='codex')
    instance = get_preferences_store(settings.data_dir)
    user = get_preferences_store(settings.data_dir, user_id='alice')
    instance.update(agent_provider='cursor', agent_auto_approve_permissions=True,
        agent_surfaces={'chat.default': SurfaceAgentPrefs(provider='codex', model_id='instance-model',
            effort='high', mode_id='workspace-write', config={'unadvertised':'keep'})})
    user.update(agent_provider='openinterpreter', agent_auto_approve_permissions=False,
        agent_surfaces={'chat.default': SurfaceAgentPrefs(
            execution_preferences={'model':{'intent':'preferred','value':'advanced-model'}})})
    original = [store.path.read_bytes() for store in (instance, user)]
    principal = 'user:alice'
    expected, sources = merge_preferences(preference_layers(settings,
        principal=principal if scope=='user' else None, surface='chat.default'))
    result = agent_defaults_preview(settings, principal, scope=scope)
    assert (result['values'], result['sources']) == (expected, sources)
    assert result['mode']=='workspace-write'
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(ctx=SimpleNamespace(settings=settings))))
    body = PreferencesBody(scope=scope, agent_surfaces={'chat.default': {'model_id':'staged-model', 'mode_id':'read-only'}})
    with patch('pa.modules.agent_chat._user_id', return_value='alice'), patch('pa.modules.agent_chat.get_principal_id', return_value=principal):
        preview = preview_agent_preferences(request, body)
    assert preview['values']['model']['value']=='staged-model'
    assert preview['mode']=='read-only'
    assert preview['sources']['model']==('user_legacy' if scope=='user' else 'installation_surface_legacy')
    assert [store.path.read_bytes() for store in (instance, user)] == original


def test_preview_preserves_explicit_automatic_and_advanced_precedence(tmp_path):
    settings = Settings(data_dir=tmp_path/'data', workspace_root=tmp_path/'workspaces')
    instance = get_preferences_store(settings.data_dir)
    user = get_preferences_store(settings.data_dir, user_id='alice')
    instance.update(agent_surfaces={'chat.default':SurfaceAgentPrefs(model_id='instance-model', effort='high')})
    user.update(agent_surfaces={'chat.default':SurfaceAgentPrefs(model_id='legacy-model',
        execution_preferences={'model':{'intent':'preferred','value':'advanced-model'},
                               'reasoning':{'intent':'automatic'}})})
    preview = agent_defaults_preview(settings, 'user:alice')
    assert preview['values']['model']=={'intent':'preferred','value':'advanced-model'}
    assert preview['values']['reasoning']['intent']=='automatic'
    assert preview['sources']['model']=='user' and preview['sources']['reasoning']=='user'

"""Isolated restart acceptance server with a tool-free ACP wire provider.

Run with PA_OWNER_API_URL=http://127.0.0.1:8097 and uvicorn factory mode.
The restart hook terminates only this fixture process; the validation harness
starts it again against the same isolated data. Owner probes are real.
"""
import os
import signal
from pathlib import Path

from pa.acp.providers.registry import register_provider
from pa.config import Settings
from pa.core.kernel import Kernel
from tests.selection_browser_app import FixtureProvider


def create_app():
    import pa.cli.service

    root = Path(__file__).resolve().parents[1] / '.dev' / 'restart-validation'
    settings = Settings(
        data_dir=root / 'data', workspace_root=root / 'workspaces',
        instance_id='831156d3-f921-4b8b-a6ec-7a47bf9279b8',
        instance_name='Isolated restart acceptance',
        instance_url='http://127.0.0.1:8097', port=8097,
        sync_token='isolated-restart-fixture-only', peers=[],
        agent_provider='codex', agent_enabled=True,
        card_summary_auth_source='dedicated', card_summary_api_key='',
        card_summary_anthropic_api_key='',
    )
    def restart_fixture(actual_settings, **kwargs):
        assert actual_settings.data_dir == settings.data_dir
        os.kill(os.getpid(), signal.SIGTERM)
        return {'status': 'requested', 'operation_id': kwargs.get('operation_id')}
    pa.cli.service.request_restart = restart_fixture
    register_provider(FixtureProvider('codex'))
    return Kernel.boot(settings=settings).build_app()

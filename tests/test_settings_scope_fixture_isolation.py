"""A new UI fixture must never inherit an expired fixture's agent/store."""
import tempfile
from pathlib import Path

import httpx
import pytest

from pa.config import Settings
from pa.core.kernel import Kernel
from pa.domain.store import reset_store
from pa.instance.agent_session import get_instance_agent, reset_instance_agent
from tests.ux_guidance_fixture import build_ux_app


@pytest.mark.asyncio
async def test_agent_partial_uses_current_fixture_after_prior_directory_expires(tmp_path):
    reset_store()
    reset_instance_agent()
    try:
        # Normal teardown of an earlier disabled-agent test app leaves the
        # process singleton reachable after its disposable directory expires.
        with tempfile.TemporaryDirectory(dir=tmp_path) as expired:
            expired = Path(expired)
            settings = Settings(
                data_dir=expired / 'data', workspace_root=expired / 'workspaces',
                agent_enabled=False, telemetry_enabled=False, peers=[],
            )
            prior_kernel = Kernel.boot(settings=settings)
            prior = get_instance_agent(settings, prior_kernel.ctx.store)
            prior_path = Path(prior.store.db_path)
            assert prior_path.exists()
        assert not prior_path.exists()

        app = build_ux_app(tmp_path / 'current')
        async with app.router.lifespan_context(app):
            ctx = app.state.ctx
            manager = ctx.services['instance_agent']
            assert manager is not prior
            assert manager.store is ctx.store
            assert manager.workspace_manager.store is ctx.store
            assert manager.settings.data_dir == ctx.settings.data_dir
            card = app.state.ux_cards['Turn ended']
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url='http://127.0.0.1'
            ) as client:
                response = await client.get(f'/partials/cards/{card.id}/agent')
            assert response.status_code == 200
            assert 'data-execution-id=' in response.text
            assert 'data-session-id="ux-worker"' in response.text
    finally:
        reset_instance_agent()
        reset_store()

"""Disposable PA journey fixture; no real providers, peers or user preferences.

Manual check: uv run uvicorn tests.ux_guidance_fixture:create_app --factory
             --host 127.0.0.1 --port 8097
"""
from pathlib import Path
from contextlib import asynccontextmanager
import tempfile

from pa.domain.models import AgentSession, CardCreate
from pa.execution.dispatch import DispatchRecord
from tests.settings_scope_fixture import build_settings_app


def build_ux_app(directory):
    app = build_settings_app(directory)
    original_lifespan = app.router.lifespan_context

    @asynccontextmanager
    async def lifespan(app):
        async with original_lifespan(app):
            seed_journeys(app)
            yield

    app.router.lifespan_context = lifespan
    return app


def seed_journeys(app):
    ctx = app.state.ctx
    store = ctx.store
    # Seed inside the disposable server's writer, before it accepts requests.
    cards = {}
    for name, lane in [('Not started', 'inbox'), ('Turn ended', 'active'), ('Delivered', 'done')]:
        cards[name] = store.create_card(CardCreate(
            title=name, lane=lane, auto_enrich=False,
            body=('Delivered evidence: [Regression report](https://example.test/report/build-a). '
                  'Subject revision: build-a.' if name == 'Delivered' else 'Journey fixture.')))
    ended = cards['Turn ended']
    store.save_session(AgentSession(
        id='ux-worker', agent_name='codex', card_id=ended.id, dispatch_id='ux-ended',
        title='Journey worker', status='idle', model_id='model-a', mode_id='workspace-write',
        origin_instance_id=ctx.settings.instance_id))
    ctx.services['dispatch_store'].put(DispatchRecord(
        dispatch_id='ux-ended', mutation_id='ux-ended', card_id=ended.id,
        authority_instance_id=ctx.settings.instance_id, authority_url='http://127.0.0.1:8097',
        target_instance_id=ctx.settings.instance_id, session_id='ux-worker', state='completed'))
    app.state.ux_cards = cards


def create_app():
    return build_ux_app(Path(tempfile.mkdtemp(prefix='pa-ux-guidance-')))

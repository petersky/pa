"""Portable original Home fixture in the full PA chat page and authenticated API."""
from pathlib import Path
import tempfile

import uvicorn
from fastapi.responses import RedirectResponse, JSONResponse

from pa.config import Settings
from pa.core.kernel import Kernel
from pa.domain.store import reset_store
from pa.instance.agent_session import reset_instance_agent
from tests.test_visualizations import seed, marker, FIXTURES


def build_visualization_app(directory: Path):
    directory = directory.resolve()
    reset_store()
    reset_instance_agent()
    settings = Settings(data_dir=directory/"data",workspace_root=directory/"workspaces",
                        agent_enabled=False,peers=[],sync_token="isolated-viz-fixture")
    kernel = Kernel.boot(settings=settings)
    app = kernel.build_app()
    # Retain raw reference, normal mode, failures and code examples in real history.
    from pa.domain.models import TranscriptEvent
    def seed_history():
        session, source, text = seed(app,directory/"owned")
        store = app.state.ctx.store
        events = []
        wrapper = directory/"owned"/"pa-attention-preview.html"
        wrapper.write_bytes((FIXTURES/wrapper.name).read_bytes())
        for seq, value in enumerate([
        marker(source,title="Default Home preview"),
        marker(wrapper,title="Standalone Home export"),
        marker("output/missing.html",title="Missing concept"),
        marker("output/denied.html",title="Denied concept"),
        '\ue200visualize\ue202{"title":"Future concept","path":"future.html","version":9}\ue201',
        '\ue200visualize\ue202{"title":"Invalid concept","wide":"yes"}\ue201',
        "```text\n"+text+"\n```",
        ],start=5):
            events.append(TranscriptEvent(session_id=session.id,seq=seq,event_type="agent_message_chunk",
                payload={"message_id":f"example-{seq}","text":value,"phase":"final"}))
        from pa.visualizations import VisualizationCapture, VisualizationStore
        VisualizationCapture(VisualizationStore(settings,store),session).consume(events)
        store.append_transcript_events(events)
        source.unlink()  # Only durable runtime capture can supply the preview.
        wrapper.unlink()
        return session

    @app.get("/fixture")
    async def fixture_entry():
        if not getattr(app.state, "viz_seeded", False):
            import asyncio
            await asyncio.to_thread(seed_history)
            app.state.viz_seeded = True
        # Startup ran with providers disabled; this enables only the real chat UI.
        settings.agent_enabled = True
        manager = app.state.ctx.require_service("instance_agent")
        manager._accepting = True
        manager._quiescing = False
        manager._startup_complete = True
        return RedirectResponse("/agent?session=viz-session")

    app.router.routes.insert(0, app.router.routes.pop())  # Before PA's page catch-all.

    @app.middleware("http")
    async def denied_fixture(request, call_next):
        if request.url.path.endswith("/visualizations") and "Denied concept" in request.query_params.get("reference", ""):
            return JSONResponse({"detail":"Fixture preview access denied"},status_code=403)
        return await call_next(request)
    return app


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="pa-viz-chat-") as directory:
        uvicorn.run(build_visualization_app(Path(directory)),host="127.0.0.1",port=18085)

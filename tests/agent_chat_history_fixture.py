"""Serve isolated synthetic history for browser verification; no PA service writes.

Run from the checkout: uv run python -m tests.agent_chat_history_fixture
"""
import asyncio
import json
import tempfile
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
from unittest.mock import MagicMock

from pa.domain.models import AgentSession
from pa.domain.projection import CardProjection
from pa.modules.agent_chat import get_agent_session_history
from tests.test_agent_chat_history_reconstruction import FINAL_TEXT, fixture_events


def main():
    with tempfile.TemporaryDirectory(prefix="pa-chat-fixture-") as directory:
        store = CardProjection(Path(directory) / "fixture.db")
        session = AgentSession(id="history-fixture", agent_name="codex", status="idle", purpose="chat")
        store.append_transcript_events(fixture_events())
        store.save_session(session)
        store.save_session(AgentSession(id="other-fixture", agent_name="codex", status="idle", purpose="chat"))
        manager = MagicMock()
        manager.store = store
        manager.get.return_value = None
        request = MagicMock()
        request.app.state.ctx.require_service.return_value = manager
        request.app.state.ctx.services = {}
        request.app.state.ctx.store = store
        request.app.state.ctx.settings.instance_id = "isolated-fixture"
        request.app.state.ctx.settings.instance_name = "isolated-fixture"

        class Handler(SimpleHTTPRequestHandler):
            def log_message(self, *_args):
                pass

            def do_GET(self):
                parsed = urlsplit(self.path)
                if parsed.path == "/fixture-expected":
                    return self.json({"final": FINAL_TEXT})
                if parsed.path.startswith("/api/fleet/session-route/"):
                    return self.json({"state": "live", "live": True, "api_base": "/api/agent"})
                if parsed.path.startswith("/api/agent/history/"):
                    query = parse_qs(parsed.query)
                    kwargs = {k: int(query[k][0]) for k in ("limit", "after_seq", "before_seq") if k in query}
                    kwargs["message_boundaries"] = query.get("message_boundaries") == ["true"]
                    return self.json(asyncio.run(get_agent_session_history(request, parsed.path.rsplit("/", 1)[-1], **kwargs)))
                if parsed.path.startswith("/api/agent/sessions/"):
                    selected = store.get_session(parsed.path.split("/")[4]) or session
                    return self.json({"session": selected.model_dump(mode="json"), "connected": True,
                        "prompting": False, "queue": [], "presentation": {"purpose": "chat", "archive": {"archived": False}}})
                if parsed.path.startswith("/api/"):
                    return self.json({})
                return super().do_GET()

            def json(self, value):
                content = json.dumps(value).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(content)))
                self.end_headers()
                self.wfile.write(content)

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        print(f"http://127.0.0.1:{server.server_port}/tests/agent_chat_history_browser_harness.html", flush=True)
        server.serve_forever()


if __name__ == "__main__":
    main()

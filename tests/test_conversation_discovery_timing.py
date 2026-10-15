import asyncio
import json
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from fastapi import Response

from pa.domain.models import AgentSession, CardCreate
from pa.domain.projection import CardProjection
from pa.instance.agent_session import AgentSessionRuntime
from pa.instance.quiesce import QueuedPrompt
from pa.modules.items import list_card_summaries_api


def test_bounded_301_card_discovery_never_reads_bodies_or_history(tmp_path, monkeypatch):
    store = CardProjection(tmp_path / "cards.db")
    for i in range(301):
        store.create_card(CardCreate(title=f"History {i}", summary=f"Summary {i}", body="private historical body " * 100), via_log=False)
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(ctx=SimpleNamespace(store=store, settings=SimpleNamespace(primary_realm="default")))))
    monkeypatch.setattr(store, "list_cards", lambda *a, **k: (_ for _ in ()).throw(AssertionError("full cards")))
    monkeypatch.setattr(store, "list_transcript_events", lambda *a, **k: (_ for _ in ()).throw(AssertionError("history")))
    response = Response()
    first = list_card_summaries_api(request, response, limit=30, offset=0, q="")
    second = list_card_summaries_api(request, Response(), limit=30, offset=30, q="")
    assert len(first["items"]) == 30 and first["next_offset"] == 30
    assert not ({i["id"] for i in first["items"]} & {i["id"] for i in second["items"]})
    assert "private historical body" not in json.dumps(first)
    assert all("body" not in i and "history_url" in i for i in first["items"])
    last = list_card_summaries_api(request, Response(), limit=30, offset=300, q="")
    assert len(last["items"]) == 1 and last["next_offset"] is None
    assert "card_summaries;dur=" in response.headers["Server-Timing"]


def test_first_useful_event_timing_is_session_scoped_and_emitted_once():
    runtime = AgentSessionRuntime.__new__(AgentSessionRuntime)
    runtime.session = AgentSession(id="local", agent_name="codex", external_session_id="native")
    runtime.connection = None
    runtime._in_flight = QueuedPrompt(id="prompt", message="test")
    runtime._turn_agent_events = []
    runtime._turn_timing = {"prompt_id": "prompt", "accept_to_provider_ms": 10}
    runtime._provider_started_monotonic = time.monotonic() - .05
    runtime._append_transcript = MagicMock()
    runtime._report_progress = AsyncMock()
    runtime._save_session_preserving_external_browser_async = AsyncMock()
    event = {"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": "Useful reply"}}
    asyncio.run(runtime._on_acp_update("other", event))
    assert not runtime._append_transcript.called
    asyncio.run(runtime._on_acp_update("native", event))
    asyncio.run(runtime._on_acp_update("native", event))
    timings = [c.args[1] for c in runtime._append_transcript.call_args_list if c.args[0] == "prompt_timing"]
    assert len(timings) == 1
    assert timings[0]["provider_generation_ms"] >= 50
    assert timings[0]["accept_to_useful_ms"] == timings[0]["provider_generation_ms"] + 10
    assert runtime.session.metrics_json["turn_timing"]["prompt_id"] == "prompt"

"""Provider metadata and display-only tool evidence regressions."""
import asyncio
import json
import shutil
import subprocess
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
from pydantic import ValidationError

from pa.acp.client import normalize_session_update, _resolve_session_load_target
from pa.acp.session_titles import apply_provider_title
from pa.config import Settings
from pa.domain.models import AgentSession
from pa.domain.projection import CardProjection
from pa.instance.agent_session import AgentSessionManager, AgentSessionRuntime


def test_title_identity_timestamp_manual_and_restore(tmp_path):
    store = CardProjection(tmp_path / "titles.db")
    session = AgentSession(id="local", external_session_id="provider", agent_name="codex")
    assert not apply_provider_title(session, "Cross session", external_session_id="other")
    for title in (None, "", "   ", {}, ["bad"], "a" * 513):
        assert not apply_provider_title(session, title, external_session_id="provider")
    assert apply_provider_title(session, "Provider title", external_session_id="provider", updated_at="2026-10-13T12:00:00Z")
    assert not apply_provider_title(session, "Old title", external_session_id="provider", updated_at="2026-10-12T12:00:00Z")
    store.save_session(session)
    manager = AgentSessionManager(Settings(data_dir=tmp_path), store)
    asyncio.run(manager.name_session("local", title="My title", automatic=False))
    session = store.get_session("local")
    assert apply_provider_title(session, "New automatic title", external_session_id="provider")
    assert session.title == "My title"
    store.save_session(session)
    restored = asyncio.run(manager.name_session("local", title=None, automatic=True))
    assert restored.title == "New automatic title"
    assert store.get_session("local").config_json["naming"]["mode"] == "automatic"


def test_runtime_title_event_persists_and_rejects_cross_session():
    runtime = AgentSessionRuntime.__new__(AgentSessionRuntime)
    runtime.session = AgentSession(id="local", agent_name="codex", external_session_id="native")
    runtime._in_flight = None
    runtime.connection = None
    runtime._save_session_preserving_external_browser_async = AsyncMock()
    runtime._append_transcript = MagicMock()
    runtime._report_progress = AsyncMock()
    asyncio.run(runtime._on_acp_update("other", {"sessionUpdate": "session_info_update", "title": "Wrong"}))
    runtime._append_transcript.assert_not_called()
    asyncio.run(runtime._on_acp_update("native", {"sessionUpdate": "session_info_update", "title": "Correct"}))
    assert runtime.session.title == "Correct"
    runtime._save_session_preserving_external_browser_async.assert_awaited_once()
    assert runtime._append_transcript.call_args.args[1]["title"] == "Correct"


def test_list_metadata_is_exact_session_only():
    observed = []
    conn = SimpleNamespace(list_sessions=AsyncMock(return_value={"sessions": [
        {"sessionId": "other", "cwd": "/other", "title": "Wrong"},
        {"sessionId": "wanted", "cwd": "/wanted", "title": "Restored"},
    ]}))
    assert asyncio.run(_resolve_session_load_target(conn, session_id="wanted", cwd="/cwd", on_info=observed.append)) == ("wanted", "/wanted")
    assert observed == [{"sessionId": "wanted", "cwd": "/wanted", "title": "Restored"}]


def test_normalization_keeps_falsy_input_and_full_output():
    text = "x" * 2001
    normalized = normalize_session_update({"sessionUpdate": "tool_call_update", "toolCallId": "call", "rawInput": False, "rawOutput": text, "_meta": {"terminal_output_delta": {"data": "delta"}}})
    assert normalized["raw_input"] is False
    assert normalized["raw_output"] == text
    assert normalized["raw"]["_meta"]["terminal_output_delta"]["data"] == "delta"
    assert Settings().terminal_output_char_limit == 1000
    for limit in (0, 99, 100001, "bad"):
        with pytest.raises(ValidationError):
            Settings(terminal_output_char_limit=limit)


@pytest.mark.skipif(not shutil.which("node"), reason="node required")
def test_terminal_order_replay_streaming_isolation_and_safe_text():
    result = subprocess.run(["node", "tests/agent_terminal_node_harness.js"], capture_output=True, text=True)
    assert result.returncode == 0, result.stderr


def test_canonical_tool_evidence_survives_retention_redacted(tmp_path):
    from pa.domain.models import TranscriptEvent
    store = CardProjection(tmp_path / "tools.db")
    payload = normalize_session_update({
        "sessionUpdate": "tool_call_update", "toolCallId": "call",
        "rawInput": {"command": "echo fixture", "password": "do-not-retain"},
        "rawOutput": {"text": "x" * 5000, "token": "do-not-retain"},
        "_meta": {"terminal_output_delta": {"data": "streamed output", "terminal_id": "call"}},
    })
    store.append_transcript_events([TranscriptEvent(session_id="s", seq=1, event_type="tool_call_update", payload=payload)])
    restored = store.list_transcript_events("s")[0].payload
    assert "raw" not in restored and "raw_output" not in restored
    assert restored["tool_input"] == {"command": "echo fixture", "password": "[REDACTED]"}
    assert len(restored["tool_output"]["text"]) == 5000
    assert restored["tool_output"]["token"] == "[REDACTED]"
    assert restored["output_chunks"] == ["streamed output"]
    assert "do-not-retain" not in json.dumps(restored)

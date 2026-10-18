"""Fleet summaries preserve lifecycle truth without diagnostic hydration."""
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest

from pa.config import Settings
from pa.domain.models import AgentSession
from pa.domain.projection import CardProjection
from pa.fleet import overview


def fixture(root):
    store = CardProjection(root / "sessions.db")
    now = datetime(2026, 8, 1, tzinfo=UTC)
    for index, (realm, state) in enumerate([
        ("default", "idle"), ("default", "working"),
        ("default", "queued"), ("default", "recoverable"),
        ("default", "closed"), ("foreign", "working"),
    ]):
        store.save_session(AgentSession(
            id=f"session-{index}", agent_name="codex", realm_id=realm,
            status=state, title=f"Session {index}", card_id=f"card-{index}",
            updated_at=now + timedelta(seconds=index),
            config_json={"durable_runtime": {
                "lifecycle": "recoverable_interrupted",
                "queued_prompts": [{"id": "preserved", "source": "human"}],
                "queue_paused": True,
            }},
            recovery_json={"reason": "provider_disconnected"},
            execution_binding={"source": "fixture", "nested": {"value": 1}},
            metrics_json={"diagnostic": "x" * 65536},
        ))
    return store


@pytest.mark.parametrize("limit", [0, 2, 80])
def test_summary_preserves_selection_fields_order_count_and_full_reads(tmp_path, limit):
    store = fixture(tmp_path)
    full, full_count = store.list_sessions_for_workshop(realm_id="default", limit=limit)
    summary, count = store.list_sessions_for_workshop_summary(realm_id="default", limit=limit)
    assert count == full_count == 4
    assert [s.model_dump(exclude={"metrics_json"}) for s in summary] == [
        s.model_dump(exclude={"metrics_json"}) for s in full
    ]
    assert all(s.metrics_json == {} for s in summary)
    assert all(s.metrics_json["diagnostic"] == "x" * 65536 for s in full)
    assert store.get_session("session-1").metrics_json == {"diagnostic": "x" * 65536}
    if limit:
        assert summary[0].id == "session-1"
        summary[0].execution_binding["nested"]["value"] = 99
        assert store.get_session("session-1").execution_binding["nested"]["value"] == 1


def test_activity_summary_and_legacy_extension_have_identical_presentation(tmp_path, monkeypatch):
    store = fixture(tmp_path)
    settings = Settings(data_dir=tmp_path / "data", workspace_root=tmp_path / "workspaces",
                        primary_realm="default", instance_id="local", agent_enabled=False)
    build = overview.build_session_presentation
    monkeypatch.setattr(overview, "build_session_presentation", lambda *a, **k:
                        build(*a, **k, now=datetime(2026, 8, 2, tzinfo=UTC)))
    # A legacy extension implements only the original keyword contract.
    calls = []
    def legacy(*, realm_id, limit):
        calls.append((realm_id, limit))
        return store.list_sessions_for_workshop(realm_id=realm_id, limit=limit)
    expected = overview._local_activity(SimpleNamespace(
        settings=settings, services={}, store=SimpleNamespace(list_sessions_for_workshop=legacy)))
    original = store.list_sessions_for_workshop_summary
    summary_calls = []
    def summary(**kwargs):
        value = original(**kwargs)
        summary_calls.append(value)
        return value
    monkeypatch.setattr(store, "list_sessions_for_workshop_summary", summary)
    actual = overview._local_activity(SimpleNamespace(settings=settings, services={}, store=store))
    assert actual == expected
    assert calls == [("default", overview.WORKSHOP_SESSION_LIMIT)]
    assert len(summary_calls) == 1
    assert all(s.metrics_json == {} for s in summary_calls[0][0])
    assert actual["session_total"] == 4
    assert {s["id"] for s in actual["sessions"]} == {"session-0", "session-1", "session-2"}

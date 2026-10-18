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
    assert [s.model_dump(exclude={"metrics_json", "config_json"}) for s in summary] == [
        s.model_dump(exclude={"metrics_json", "config_json"}) for s in full
    ]
    for lean, complete in zip(summary, full):
        for key in ("configuration", "durable_runtime", "provisioning"):
            assert lean.config_json.get(key) == complete.config_json.get(key)
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


@pytest.mark.parametrize("control_mode", ["human", "automation"])
def test_summary_omits_unused_config_proofs_and_preserves_presentation(tmp_path, control_mode):
    from pa.execution.session_presentation import build_session_presentation

    store = fixture(tmp_path)
    session = store.get_session("session-1")
    session.control_mode = control_mode
    session.config_json.update({
        "configuration": {"effective": {"model_id": "fixture-model"}},
        "provisioning": {"action": "retained remedy"},
        "selection_proof": {"retained": "x" * 131072},
    })
    session.config_json["durable_runtime"].update({
        "pending_permissions": [{"id": "permission", "status": "pending"}],
        "pending_interactions": [{"id": "question", "status": "pending"}],
        "in_flight": {"id": "in-flight", "source": "human", "message": "preserved"},
    })
    store.save_session(session)
    full, count = store.list_sessions_for_workshop(realm_id="default", limit=80)
    lean, lean_count = store.list_sessions_for_workshop_summary(realm_id="default", limit=80)
    now = datetime(2026, 8, 2, tzinfo=UTC)
    assert count == lean_count
    assert [build_session_presentation(s, now=now) for s in lean] == [
        build_session_presentation(s, now=now) for s in full
    ]
    projected = next(s for s in lean if s.id == session.id)
    assert set(projected.config_json) == {"configuration", "durable_runtime", "provisioning"}
    assert all(projected.config_json[k] == session.config_json[k] for k in projected.config_json)
    # Both public full read modes retain complete config, including future keys.
    for with_metrics in (False, True):
        observed, _ = store.list_sessions_for_workshop(
            realm_id="default", limit=80, include_metrics=with_metrics)
        assert next(s for s in observed if s.id == session.id).config_json == session.config_json
    assert store.get_session(session.id).config_json == session.config_json


@pytest.mark.parametrize("raw", [
    '{}', '[invalid', 'null', '[]', '3', 'false', '"legacy"',
    '{"configuration":{"model":"first"},"configuration":{"model":"last"}}',
    r'{"config\u0075ration":{"model":"first"},"configuration":{"model":"last"}}',
    '{"durable_runtime":{"queue_paused":true},"durable_runtime":{"queue_paused":false}}',
    '{"configuration":true}', '{"configuration":999999999999999999999999999999999}',
    '{"provisioning":0}', '{"durable_runtime":[["queue_paused",false]]}',
    '{"configuration":{"model":"first","model":"last"},"unused":"omit"}',
])
def test_summary_preserves_legacy_config_decoder_semantics(tmp_path, raw):
    store = CardProjection(tmp_path / "fixture.db")
    store.save_session(AgentSession(id="legacy", agent_name="fixture", status="working"))
    # Only a disposable fixture receives historical serialized values.
    with store._conn() as conn:
        conn.execute("UPDATE agent_sessions SET config_json=? WHERE id='legacy'", (raw,))
    try:
        full, _ = store.list_sessions_for_workshop(realm_id="default", limit=80)
    except ValueError as exc:
        with pytest.raises(type(exc)):
            store.list_sessions_for_workshop_summary(realm_id="default", limit=80)
        return
    lean, _ = store.list_sessions_for_workshop_summary(realm_id="default", limit=80)
    assert lean[0].model_dump(exclude={"config_json", "metrics_json"}) == full[0].model_dump(
        exclude={"config_json", "metrics_json"})
    for key in ("configuration", "durable_runtime", "provisioning"):
        assert lean[0].config_json.get(key) == full[0].config_json.get(key)
        assert type(lean[0].config_json.get(key)) is type(full[0].config_json.get(key))

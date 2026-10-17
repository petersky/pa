"""Status is a passive owner read, with counts independent of retained payloads."""
from datetime import UTC, datetime
from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from typer.testing import CliRunner

from pa.cli.main import app
from pa.config import Settings
from pa.core.performance import OperationObservation, current_observation
from pa.domain.models import AgentSession, CardCreate, CardKind, CardLane, KnowledgeEntry, KnowledgeStatus
from pa.domain.projection import CardProjection
from pa.mcp.local_api import LocalPARequestError, LocalPAServerUnavailable


@pytest.mark.parametrize("active_knowledge", [0, 1, 5, 8])
def test_status_counts_match_legacy_all_realm_archive_and_recent_semantics(tmp_path, active_knowledge):
    store = CardProjection(tmp_path / "fixture.db")
    for index, lane in enumerate(CardLane):
        store.create_card(CardCreate(title=f"fixture {index}", lane=lane,
            kind=list(CardKind)[index % len(CardKind)], realm_id="other" if index % 2 else "default"))
    for index in range(4):
        store.save_session(AgentSession(id=f"s-{index}", agent_name="fixture",
            realm_id="other" if index % 2 else "default",
            archived_at=datetime.now(UTC) if index % 2 else None,
            status="closed" if index % 2 else "idle", metrics_json={"proof": "x" * 65536}))
    for index in range(active_knowledge):
        store.add_knowledge(KnowledgeEntry(summary=f"active {index}"))
    for status in KnowledgeStatus:
        if status != KnowledgeStatus.ACTIVE:
            store.add_knowledge(KnowledgeEntry(summary="excluded", status=status))
    expected = {"item_count": len(store.list_items()), "session_count": len(store.list_sessions()),
                "knowledge_count": len(store.list_knowledge(limit=5))}
    observation = OperationObservation()
    token = current_observation.set(observation)
    try:
        with patch.object(store, "_row_to_card", side_effect=AssertionError("card hydration")), \
             patch.object(store, "_row_to_session", side_effect=AssertionError("session hydration")), \
             patch.object(store, "_row_to_knowledge", side_effect=AssertionError("knowledge hydration")):
            assert store.status_inventory() == expected
    finally:
        current_observation.reset(token)
    assert observation.sqlite_rows == 1
    assert observation.sqlite_calls == 2  # Connection busy_timeout plus one SELECT.
    assert observation.sqlite_commit_ms == 0


def test_status_inventory_empty_and_committed_snapshot(tmp_path):
    store = CardProjection(tmp_path / "fixture.db")
    assert store.status_inventory() == {"item_count": 0, "session_count": 0, "knowledge_count": 0}
    # An ordinary uncommitted writer must not expose its partial count to the
    # separate status reader, nor require a Python mutation-lock acquisition.
    store.save_session(AgentSession(id="s", agent_name="fixture"))
    with ThreadPoolExecutor(max_workers=1) as pool, store._conn() as writer:
        writer.execute("BEGIN IMMEDIATE")
        writer.execute("DELETE FROM agent_sessions WHERE id='s'")
        assert pool.submit(store.status_inventory).result(timeout=5)["session_count"] == 1
    assert store.status_inventory()["session_count"] == 0


def owner_snapshot():
    return dict(version="owner-version", instance_name="owner-name", instance_id="owner-id",
        data_dir="/owner/data", server_url="http://owner:8080", binary=None, service_binary=None,
        installed_version=None, release_track="release", service={"installed": False, "running": False,
        "state": "unavailable", "backend": "fixture"}, bind={}, sync={"consistent": True}, debug=False,
        agent_enabled=True, fleet_id="fleet", realms=["default"], zone="zone", peer_count=2,
        module_count=11, item_count=12, session_count=13, knowledge_count=5)


def test_server_snapshot_uses_counts_and_retains_loaded_identity(tmp_path):
    from pa.status.info import build_status_snapshot
    store = CardProjection(tmp_path / "fixture.db")
    store.save_session(AgentSession(agent_name="fixture"))
    ctx = SimpleNamespace(settings=Settings(data_dir=tmp_path, instance_id="owner-id"),
                          store=store, services={"assets": SimpleNamespace(version="loaded-assets")})
    service = SimpleNamespace(installed=False, running=False, loaded=False,
                              backend="fixture", plist_path=tmp_path / "unit")
    with patch("pa.status.info.svc.get_status", return_value=service), \
         patch("pa.status.info.svc.service_supported", return_value=False), \
         patch("pa.status.info.svc.find_pa_binary", return_value=None), \
         patch("pa.status.info.svc.find_service_binary", return_value=None), \
         patch("pa.status.info.load_install_metadata", return_value=None), \
         patch("pa.status.info.sync_from_context", return_value=SimpleNamespace(as_dict=lambda: {"consistent": True})), \
         patch("pa.server.listeners.owner_channel_health", return_value={}), \
         patch.object(store, "list_items", side_effect=AssertionError("hydration")), \
         patch.object(store, "list_sessions", side_effect=AssertionError("hydration")), \
         patch.object(store, "list_knowledge", side_effect=AssertionError("hydration")):
        snapshot = build_status_snapshot(ctx, module_count=11)
    assert snapshot["instance_id"] == "owner-id"
    assert snapshot["asset_version"] == "loaded-assets"
    assert snapshot["session_count"] == 1 and snapshot["item_count"] == 0
    assert snapshot["knowledge_count"] == 0 and snapshot["module_count"] == 11


def test_cli_status_uses_owner_identity_without_kernel_or_settings_writes(tmp_path, monkeypatch):
    from pa.status.serving import HealthProbe
    settings = Settings(data_dir=tmp_path / "never-created", instance_name="cli-name", instance_id="cli-id")
    monkeypatch.setattr("pa.mcp.context.registration_context", lambda: SimpleNamespace(settings=settings))
    with patch("pa.cli.main.Kernel.boot", side_effect=AssertionError("writer bootstrap")), \
         patch("pa.cli.main.get_settings", side_effect=AssertionError("settings write")), \
         patch("pa.mcp.local_api.request_local_pa", return_value=owner_snapshot()) as read, \
         patch("pa.status.serving.probe_health", return_value=HealthProbe("http://owner/api/health", True, 1, 200)):
        result = CliRunner().invoke(app, ["status"])
    assert result.exit_code == 0, result.output
    read.assert_called_once_with(settings, "GET", "/api/status")
    assert "owner-version" in result.output and "owner-id" in result.output
    assert "cli-name" not in result.output and "cli-id" not in result.output
    assert "Sessions:    13" in result.output
    assert not settings.data_dir.exists()


@pytest.mark.parametrize("failure", [
    LocalPAServerUnavailable("fixture unavailable"),
    LocalPARequestError("private body", operation="GET", endpoint="/api/status", status=401, correlation_id="fixture"),
    LocalPARequestError("private body", operation="GET", endpoint="/api/status", status=500, correlation_id="fixture"),
])
def test_cli_status_unavailable_is_truthful_passive_and_does_not_expose_response(tmp_path, monkeypatch, failure):
    settings = Settings(data_dir=tmp_path / "never-created", instance_id="unattested-id")
    monkeypatch.setattr("pa.mcp.context.registration_context", lambda: SimpleNamespace(settings=settings))
    with patch("pa.cli.main.Kernel.boot", side_effect=AssertionError("writer bootstrap")), \
         patch("pa.cli.main.get_settings", side_effect=AssertionError("settings write")), \
         patch("pa.mcp.local_api.request_local_pa", side_effect=failure) as read:
        result = CliRunner().invoke(app, ["status"])
    assert result.exit_code == 1
    assert "unknown" in result.output
    assert "unattested-id" not in result.output and "private body" not in result.output
    read.assert_called_once()
    assert not settings.data_dir.exists()

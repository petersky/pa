import asyncio
import sqlite3
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from pa.config import Settings
from pa.domain.models import AgentSession
from pa.domain.projection import CardProjection
from pa.instance.agent_session import AgentSessionManager, AgentSessionRecoveryError


def _session(session_id: str, **updates) -> AgentSession:
    values = {
        "id": session_id,
        "agent_name": "codex",
        "status": "quiesced",
        "label": "default",
        "external_session_id": f"provider-{session_id}",
        "cwd": f"/workspace/{session_id}",
        "principal_id": "user:local",
        "authority_instance_id": "local",
        "config_json": {"configuration": {"mode": "agent"}},
    }
    values.update(updates)
    return AgentSession(**values)


def test_duplicate_default_migration_keeps_selected_identity(tmp_path: Path) -> None:
    db = tmp_path / "pa.db"
    store = CardProjection(db)
    selected = _session(
        "selected",
        config_json={"browser_default_selected": True},
    )
    store.save_session(selected)
    with sqlite3.connect(db) as conn:
        conn.execute("DROP INDEX idx_agent_sessions_one_default")
        replacement = _session(
            "replacement",
            created_at=datetime.now(UTC) + timedelta(seconds=1),
            updated_at=datetime.now(UTC) + timedelta(seconds=1),
        )
        conn.execute(
            """INSERT INTO agent_sessions
               (id, agent_name, external_session_id, realm_id, lifecycle_owner,
                status, cwd, label, config_json, metrics_json, created_at, updated_at)
               VALUES (?, ?, ?, 'default', 'standalone', ?, ?, ?, '{}', '{}', ?, ?)""",
            (
                replacement.id,
                replacement.agent_name,
                replacement.external_session_id,
                replacement.status,
                replacement.cwd,
                replacement.label,
                replacement.created_at.isoformat(),
                replacement.updated_at.isoformat(),
            ),
        )

    reopened = CardProjection(db)

    assert reopened.get_session_by_label("default").id == "selected"
    assert reopened.get_session("replacement").label is None
    with pytest.raises(sqlite3.IntegrityError):
        reopened.save_session(_session("third"))


def test_label_selection_is_deterministic_and_closed_is_terminal(tmp_path: Path) -> None:
    store = CardProjection(tmp_path / "pa.db")
    old_closed = _session("closed", status="closed")
    store.save_session(old_closed)

    assert store.get_session_by_label("default").id == "closed"


def test_attach_default_lazily_reuses_exact_quiesced_session(tmp_path: Path) -> None:
    existing = _session("retained")
    store = MagicMock()
    store.get_session_by_label.return_value = existing
    manager = AgentSessionManager(Settings(data_dir=tmp_path), store)
    runtime = MagicMock(session=existing, connected=True, _closed=False)
    manager.create_session = AsyncMock(return_value=runtime)

    result = asyncio.run(manager.attach_default(cwd="/wrong/new/workspace"))

    assert result.session.id == "retained"
    kwargs = manager.create_session.await_args.kwargs
    assert kwargs["existing"] is existing
    assert kwargs["resume_external_id"] == "provider-retained"
    assert existing.cwd == "/workspace/retained"
    assert existing.config_json["browser_default_selected"] is True


def test_attach_default_failure_retains_identity_and_exposes_action(tmp_path: Path) -> None:
    existing = _session("retained")
    store = MagicMock()
    store.get_session_by_label.return_value = existing
    store.get_session.return_value = existing
    manager = AgentSessionManager(Settings(data_dir=tmp_path), store)
    manager.create_session = AsyncMock(side_effect=RuntimeError("provider load failed"))

    with pytest.raises(AgentSessionRecoveryError, match="Retained default session retained"):
        asyncio.run(manager.attach_default())

    assert existing.status == "recoverable_interrupted"
    assert existing.label == "default"
    manager.create_session.assert_awaited_once()


def test_remote_default_is_retained_without_local_replacement(tmp_path: Path) -> None:
    existing = _session("remote", origin_instance_id="other-instance")
    store = MagicMock()
    store.get_session_by_label.return_value = existing
    manager = AgentSessionManager(
        Settings(data_dir=tmp_path, instance_id="local-instance"), store
    )
    manager.create_session = AsyncMock()

    with pytest.raises(AgentSessionRecoveryError, match="another instance"):
        asyncio.run(manager.attach_default())

    manager.create_session.assert_not_awaited()


def test_closed_default_allows_explicit_replacement_path(tmp_path: Path) -> None:
    closed = _session("closed", status="closed")
    store = MagicMock()
    store.get_session_by_label.return_value = closed
    manager = AgentSessionManager(Settings(data_dir=tmp_path), store)
    replacement = _session("replacement", status="idle")
    runtime = SimpleNamespace(session=replacement)
    manager.create_session = AsyncMock(return_value=runtime)

    result = asyncio.run(manager.attach_default())

    assert result.session.id == "replacement"
    assert manager.create_session.await_args.kwargs["existing"] is None


def test_concurrent_browser_attach_coalesces_to_one_recovery(tmp_path: Path) -> None:
    existing = _session("retained")
    store = MagicMock()
    store.get_session_by_label.return_value = existing
    manager = AgentSessionManager(Settings(data_dir=tmp_path), store)
    runtime = MagicMock(session=existing, connected=True, _closed=False)

    async def create(**_kwargs):
        await asyncio.sleep(0.01)
        manager._runtimes[existing.id] = runtime
        return runtime

    manager.create_session = AsyncMock(side_effect=create)

    async def attach_twice():
        return await asyncio.gather(manager.attach_default(), manager.attach_default())

    results = asyncio.run(attach_twice())

    assert [item.session.id for item in results] == ["retained", "retained"]
    manager.create_session.assert_awaited_once()

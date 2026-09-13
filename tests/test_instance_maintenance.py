"""Retention, indexes, and bounded lookups for busy local instances."""

from __future__ import annotations

import json
import asyncio
import sqlite3
import tempfile
import threading
import unittest
from contextlib import closing
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from pa.config import Settings
from pa.domain.models import (
    AgentSession,
    CardAttachment,
    CardCreate,
    CardUpdate,
    TranscriptEvent,
)
from pa.domain.projection import CardProjection
from pa.execution.dispatch import DispatchRecord, DispatchStore
from pa.instance.maintenance import (
    InstanceMaintenanceService,
    _maintain_database,
    run_maintenance,
)
from pa.sync.event_log import EventLog
from pa.sync.object_store import ObjectStore


def _sha256() -> str:
    return "a" * 64


class ProjectionLookupTests(unittest.TestCase):
    def test_parent_id_filter_and_lane_map_avoid_full_scans(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = CardProjection(Path(tmp) / "pa.db")
            parent = store.create_card(CardCreate(title="Parent"))
            child = store.create_card(
                CardCreate(title="Child", parent_id=parent.id)
            )
            store.create_card(CardCreate(title="Other"))

            children = store.list_cards(parent_id=parent.id)
            self.assertEqual([item.id for item in children], [child.id])
            lanes = store.list_card_lanes()
            self.assertEqual(lanes[parent.id], "inbox")
            self.assertEqual(len(lanes), 3)
            self.assertIn(child.id, lanes)

    def test_find_card_attachment_uses_json_each(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = CardProjection(Path(tmp) / "pa.db")
            card = store.create_card(CardCreate(title="Has file"))
            other = store.create_card(CardCreate(title="No file"))
            attachment = CardAttachment(
                card_id=card.id,
                filename="notes.txt",
                size=4,
                sha256=_sha256(),
                blob_ref=f"sha256:{_sha256()}",
                created_by_principal="test",
                created_by_instance="test",
            )
            with store._conn() as conn:
                conn.execute(
                    "UPDATE cards SET attachments=? WHERE id=?",
                    (json.dumps([attachment.model_dump(mode="json")]), card.id),
                )
                conn.execute(
                    "UPDATE cards SET attachments=? WHERE id=?",
                    (json.dumps([]), other.id),
                )

            found = store.find_card_attachment(
                attachment.attachment_id, attachment.filename
            )
            self.assertIsNotNone(found)
            found_card, found_item = found
            self.assertEqual(found_card.id, card.id)
            self.assertEqual(found_item.filename, "notes.txt")
            self.assertIsNone(
                store.find_card_attachment(attachment.attachment_id, "missing.txt")
            )

    def test_list_sessions_can_exclude_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = CardProjection(Path(tmp) / "pa.db")
            open_session = store.save_session(
                AgentSession(id="open", agent_name="codex", status="idle")
            )
            closed = store.save_session(
                AgentSession(id="closed", agent_name="codex", status="closed")
            )
            listed = store.list_sessions(exclude_statuses=("closed",))
            self.assertEqual([item.id for item in listed], [open_session.id])
            statuses = store.list_session_statuses()
            self.assertEqual(statuses[open_session.id], "idle")
            self.assertEqual(statuses[closed.id], "closed")

    def test_indexes_are_created_on_migrate(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = CardProjection(Path(tmp) / "pa.db")
            with store._conn() as conn:
                card_indexes = {
                    row["name"] for row in conn.execute("PRAGMA index_list(cards)")
                }
                session_indexes = {
                    row["name"]
                    for row in conn.execute("PRAGMA index_list(agent_sessions)")
                }
                knowledge_indexes = {
                    row["name"] for row in conn.execute("PRAGMA index_list(knowledge)")
                }
                mutation_indexes = {
                    row["name"]
                    for row in conn.execute("PRAGMA index_list(mutation_operations)")
                }
            self.assertIn("idx_cards_realm_parent", card_indexes)
            self.assertIn("idx_cards_realm_project_updated", card_indexes)
            self.assertIn("idx_agent_sessions_status_updated", session_indexes)
            self.assertIn("idx_knowledge_card", knowledge_indexes)
            self.assertIn("idx_mutation_operations_state_updated", mutation_indexes)

    def test_closed_transcript_and_mutation_prune_keep_live_rows(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = CardProjection(Path(tmp) / "pa.db")
            live = store.save_session(
                AgentSession(id="live", agent_name="codex", status="idle")
            )
            closed = store.save_session(
                AgentSession(id="old-closed", agent_name="codex", status="idle")
            )
            store.append_transcript_events(
                [
                    TranscriptEvent(
                        session_id=live.id, seq=1, event_type="message", payload={}
                    ),
                    TranscriptEvent(
                        session_id=closed.id, seq=1, event_type="message", payload={}
                    ),
                ]
            )
            store.close_session(closed.id, reason="test")
            old = datetime.now(UTC) - timedelta(days=30)
            with store._conn() as conn:
                conn.execute(
                    "UPDATE agent_sessions SET updated_at=? WHERE id=?",
                    (old.isoformat(), closed.id),
                )
                conn.execute(
                    """
                    INSERT INTO mutation_operations (
                        idempotency_key, operation, request_fingerprint, realm_id,
                        state, owner_token, recovery_state, created_at, updated_at
                    ) VALUES
                    ('old-ok', 'card.create', 'fp', 'default', 'succeeded', 'owner',
                     'pending', ?, ?),
                    ('pending', 'card.create', 'fp', 'default', 'pending', 'owner',
                     'pending', ?, ?)
                    """,
                    (old.isoformat(), old.isoformat(), old.isoformat(), old.isoformat()),
                )

            cutoff = datetime.now(UTC) - timedelta(days=14)
            self.assertEqual(store.prune_closed_session_transcripts(before=cutoff), 2)
            self.assertEqual(len(store.list_transcript_events(live.id)), 1)
            self.assertEqual(len(store.list_transcript_events(closed.id)), 0)
            self.assertIsNotNone(store.get_session(closed.id))

            self.assertEqual(store.prune_mutation_operations(before=cutoff), 1)
            with store._conn() as conn:
                keys = {
                    row["idempotency_key"]
                    for row in conn.execute(
                        "SELECT idempotency_key FROM mutation_operations"
                    )
                }
            self.assertEqual(keys, {"pending"})


class EventHistoryBoundTests(unittest.TestCase):
    def test_recent_entity_events_are_newest_first_and_bounded(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            log = EventLog(ObjectStore(root / "objects"), root / "refs", "authority")
            store = CardProjection(root / "pa.db", log)
            card = store.create_card(CardCreate(title="One"))
            store.update_card(card.id, CardUpdate(title="Two"))
            store.update_card(card.id, CardUpdate(title="Three"))
            events = log.recent_entity_events(
                "default", "card", card.id, limit=2, max_commits=50
            )
            self.assertEqual(len(events), 2)
            self.assertGreaterEqual(events[0].timestamp, events[1].timestamp)
            titles = [event.payload.get("title") for event in events]
            self.assertIn("Three", titles)


class DispatchCardFilterTests(unittest.TestCase):
    def test_list_filters_by_card_before_limit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = DispatchStore(Path(tmp))
            older = DispatchRecord(
                dispatch_id="keep-old",
                mutation_id="mut-old",
                idempotency_key="adm-old",
                card_id="keep",
                authority_instance_id="authority",
                authority_url="https://authority.example",
                target_instance_id="target",
                state="completed",
                updated_at=datetime.now(UTC) - timedelta(days=2),
            )
            newer_other = DispatchRecord(
                dispatch_id="other-new",
                mutation_id="mut-new",
                idempotency_key="adm-new",
                card_id="other",
                authority_instance_id="authority",
                authority_url="https://authority.example",
                target_instance_id="target",
                state="running",
                updated_at=datetime.now(UTC),
            )
            store.put(older)
            store.put(newer_other)
            listed = store.list(card_id="keep", limit=1)
            self.assertEqual([item.dispatch_id for item in listed], ["keep-old"])
            store.close()


class MaintenanceRunTests(unittest.TestCase):
    def test_run_maintenance_reports_prune_and_compact(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(
                data_dir=Path(tmp),
                transcript_retention_days=14,
                mutation_operation_retention_days=14,
            )
            store = CardProjection(Path(tmp) / "pa.db")
            closed = store.save_session(
                AgentSession(id="old", agent_name="codex", status="idle")
            )
            store.append_transcript_events(
                [
                    TranscriptEvent(
                        session_id=closed.id, seq=1, event_type="message", payload={}
                    )
                ]
            )
            store.close_session(closed.id, reason="test")
            old = datetime.now(UTC) - timedelta(days=30)
            with store._conn() as conn:
                conn.execute(
                    "UPDATE agent_sessions SET updated_at=? WHERE id=?",
                    (old.isoformat(), closed.id),
                )

            compact = {"events": 4, "receipts": 2}

            class _Dispatch:
                def compact(self, *, now=None):
                    return compact

            result = run_maintenance(settings, store, _Dispatch())
            self.assertGreaterEqual(result["transcript_events_deleted"], 1)
            self.assertEqual(result["dispatch_compact"], compact)
            self.assertIn("page_count", result["sqlite"])
            self.assertEqual(len(store.list_transcript_events(closed.id)), 0)
            self.assertIsNotNone(store.get_session(closed.id))

def _add_free_pages(path):
    with closing(sqlite3.connect(path)) as conn, conn:
        conn.execute("CREATE TABLE maintenance_evidence(id INTEGER PRIMARY KEY, body BLOB)")
        conn.executemany(
            "INSERT INTO maintenance_evidence VALUES (?, ?)",
            [(i, b"x" * 10_000) for i in range(100)],
        )
        conn.execute("DELETE FROM maintenance_evidence WHERE id != 1")


def test_sweep_vacuums_both_databases_without_invalidating_live_connections(tmp_path):
    settings = Settings(data_dir=tmp_path)
    store = CardProjection(settings.db_path)
    session = store.save_session(AgentSession(id="live", agent_name="codex"))
    paths = (store.db_path, store.transcripts.db_path)
    for path in paths:
        _add_free_pages(path)
    inodes = [path.stat().st_ino for path in paths]
    result = run_maintenance(settings, store)
    for key, path, inode in zip(("sqlite", "transcript_sqlite"), paths, inodes):
        report = result[key]
        assert report["status"] == "ok"
        assert report["compacted"]
        assert report["quick_check"] == "ok"
        assert report["after"]["bytes"] < report["before"]["bytes"]
        assert report["after"]["free_bytes"] == 0
        assert path.stat().st_ino == inode
        with closing(sqlite3.connect(path)) as conn:
            assert conn.execute(
                "SELECT id, length(body) FROM maintenance_evidence"
            ).fetchall() == [(1, 10_000)]
    assert store.get_session(session.id).id == session.id
    store.save_session(session.model_copy(update={"title": "Still writable"}))
    assert store.get_session(session.id).title == "Still writable"
    store.close_thread_connection()


def test_database_maintenance_defers_to_writer_and_retries(tmp_path):
    path = tmp_path / "busy.db"
    _add_free_pages(path)
    with closing(sqlite3.connect(path)) as writer:
        writer.execute("BEGIN IMMEDIATE")
        result = _maintain_database(path)
        assert result["status"] == "deferred"
        assert result["reason"] == "busy"
        assert not result["compacted"]
        writer.rollback()
    assert _maintain_database(path)["compacted"]


def test_database_maintenance_defers_when_wal_reader_pins_pages(tmp_path):
    path = tmp_path / "reader.db"
    _add_free_pages(path)
    with closing(sqlite3.connect(path)) as writer, closing(sqlite3.connect(path)) as reader:
        writer.execute("PRAGMA journal_mode=WAL")
        reader.execute("BEGIN")
        reader.execute("SELECT * FROM maintenance_evidence").fetchall()
        writer.execute("INSERT INTO maintenance_evidence VALUES (2, 'new')")
        writer.commit()
        result = _maintain_database(path)
        assert result["reason"] == "busy"
        assert not result["compacted"]
        reader.rollback()
    assert _maintain_database(path)["compacted"]


def test_database_maintenance_checks_space_and_avoids_redundant_vacuum(tmp_path):
    path = tmp_path / "small.db"
    _add_free_pages(path)
    with patch("pa.instance.maintenance.shutil.disk_usage", return_value=SimpleNamespace(free=0)):
        result = _maintain_database(path)
    assert result["reason"] == "insufficient_disk_space"
    assert not result["compacted"]
    assert _maintain_database(path)["compacted"]
    result = _maintain_database(path)
    assert not result["compacted"]
    assert result["status"] == "ok"
    assert result["quick_check"] == "ok"
    assert result["wal_busy"] == 0


def test_database_maintenance_does_not_recreate_missing_database(tmp_path):
    path = tmp_path / "missing.db"
    with pytest.raises(sqlite3.OperationalError):
        _maintain_database(path)
    assert not path.exists()


def test_database_maintenance_honors_shutdown(tmp_path):
    path = tmp_path / "stop.db"
    _add_free_pages(path)
    before = path.read_bytes()
    stop = threading.Event()
    stop.set()
    result = _maintain_database(path, stop=stop)
    assert result["reason"] == "stopping"
    assert path.read_bytes() == before


def test_shutdown_interrupts_in_progress_vacuum_without_losing_rows(tmp_path):
    path = tmp_path / "interrupt.db"
    _add_free_pages(path)
    with closing(sqlite3.connect(path)) as conn, conn:
        conn.execute("CREATE TABLE retained(id INTEGER PRIMARY KEY, value TEXT)")
        conn.executemany(
            "INSERT INTO retained VALUES (?, 'keep')", ((i,) for i in range(50_000))
        )
        conn.execute("INSERT INTO maintenance_evidence VALUES (2, zeroblob(2000000))")
        conn.execute("DELETE FROM maintenance_evidence WHERE id = 2")
    stop = threading.Event()
    connect = sqlite3.connect

    def observed_connection(*args, **kwargs):
        conn = connect(*args, **kwargs)
        conn.set_trace_callback(lambda sql: stop.set() if sql == "VACUUM" else None)
        return conn

    with patch("pa.instance.maintenance.sqlite3.connect", side_effect=observed_connection):
        result = _maintain_database(path, stop=stop)
    assert stop.is_set()
    assert result["reason"] == "stopping"
    assert not result["compacted"]
    with closing(connect(path)) as conn:
        assert conn.execute("PRAGMA quick_check").fetchone()[0] == "ok"
        assert conn.execute("SELECT COUNT(*) FROM retained").fetchone()[0] == 50_000
        assert conn.execute("PRAGMA freelist_count").fetchone()[0] > 0


def test_daily_maintenance_default_matches_persisted_and_runtime_configuration():
    from pa.configuration.registry import get_setting
    from pa.domain.instance_config import InstanceConfig

    assert Settings.model_fields["maintenance_interval_seconds"].default == 86400
    assert InstanceConfig().maintenance_interval_seconds == 86400
    assert get_setting("maintenance_interval_seconds").default == 86400


@pytest.mark.asyncio
async def test_service_runs_at_startup_and_again_after_daily_interval(tmp_path):
    service = InstanceMaintenanceService(Settings(data_dir=tmp_path), None, {})
    waits = []

    async def daily_tick(awaitable, *, timeout):
        awaitable.close()
        waits.append(timeout)
        if len(waits) == 1:
            raise TimeoutError
        service._closing = True

    with patch.object(service, "_call", new_callable=AsyncMock, return_value={}) as work:
        with patch("pa.instance.maintenance.asyncio.wait_for", side_effect=daily_tick):
            service.start()
            service.start()  # Duplicate lifecycle starts must not spawn another timer.
            await service._task
    assert work.await_count == 2
    assert waits == [86400.0, 86400.0]
    assert service.last_finished_at is not None
    await service.close()


@pytest.mark.asyncio
async def test_cancelled_manual_request_keeps_single_worker_until_completion(tmp_path):
    from pa.core.async_runtime import AsyncRuntime

    started = threading.Event()
    release = threading.Event()

    def sweep(*args, **kwargs):
        started.set()
        assert release.wait(timeout=5)
        return {"sqlite": {"compacted": True}}

    runtime = AsyncRuntime()
    await runtime.start()
    service = InstanceMaintenanceService(
        Settings(data_dir=tmp_path), None, {}, async_runtime=runtime
    )
    try:
        with patch("pa.instance.maintenance.run_maintenance", side_effect=sweep) as work:
            first = asyncio.create_task(service.run_once())
            assert await asyncio.to_thread(started.wait, 5)
            first.cancel()
            with pytest.raises(asyncio.CancelledError):
                await first
            second = asyncio.create_task(service.run_once())
            await asyncio.sleep(0)
            assert work.call_count == 1
            release.set()
            assert (await second)["sqlite"]["compacted"]
            assert work.call_count == 1
            assert service.last_result["sqlite"]["compacted"]
    finally:
        release.set()
        await service.close()
        await runtime.close()


@pytest.mark.asyncio
async def test_service_shutdown_signals_and_drains_active_worker(tmp_path):
    started = threading.Event()

    def sweep(*args, stop, **kwargs):
        started.set()
        assert stop.wait(timeout=5)
        return {"sqlite": {"status": "deferred", "reason": "stopping"}}

    service = InstanceMaintenanceService(Settings(data_dir=tmp_path), None, {})
    with patch("pa.instance.maintenance.run_maintenance", side_effect=sweep):
        service.start()
        assert await asyncio.to_thread(started.wait, 5)
        await service.close()
    assert service._active_run.done()
    assert service._task.done()
    assert service.last_result["sqlite"]["reason"] == "stopping"


@pytest.mark.asyncio
async def test_failed_sweep_does_not_stop_periodic_maintenance(tmp_path):
    service = InstanceMaintenanceService(Settings(data_dir=tmp_path), None, {})
    waits = []

    async def tick(awaitable, *, timeout):
        awaitable.close()
        waits.append(timeout)
        if len(waits) == 1:
            assert service.last_error == "database failure"
            raise TimeoutError
        service._closing = True

    with patch.object(service, "_call", new_callable=AsyncMock,
                      side_effect=[RuntimeError("database failure"), {}]) as work:
        with patch("pa.instance.maintenance.asyncio.wait_for", side_effect=tick):
            service.start()
            await service._task
    assert work.await_count == 2
    assert service.last_error is None
    await service.close()

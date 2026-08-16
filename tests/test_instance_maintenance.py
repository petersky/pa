"""Retention, indexes, and bounded lookups for busy local instances."""

from __future__ import annotations

import json
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

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
from pa.instance.maintenance import run_maintenance
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

from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
import threading
import unittest
import zlib
from datetime import UTC, datetime, timedelta
from pathlib import Path

from pa.domain.models import AgentSession, TranscriptEvent
from pa.domain.projection import CardProjection


class TranscriptStorageTests(unittest.TestCase):
    def test_large_payload_is_redacted_deduplicated_and_verified(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = CardProjection(Path(tmp) / "pa.db")
            body = "repeated " * 2000
            payload = {
                "text": body,
                "authorization": "Bearer ultra-private-token",
                "raw_output": body,
                "provider_envelope": {"token": "secret"},
            }
            store.append_transcript_events([
                TranscriptEvent(session_id="s", seq=i, event_type="output", payload=payload)
                for i in (1, 2)
            ])
            events = store.list_transcript_events("s")
            self.assertEqual(events[0].payload["text"], body)
            self.assertEqual(events[0].payload["authorization"], "[REDACTED]")
            self.assertNotIn("raw_output", events[0].payload)
            metrics = store.transcript_storage_metrics()
            self.assertEqual(metrics["objects"], 1)
            self.assertEqual(metrics["references"], 2)
            self.assertEqual(metrics["deduplicated_references"], 1)
            with store._conn() as conn:
                mirror = json.loads(conn.execute(
                    "SELECT payload FROM agent_transcript_events WHERE session_id='s' LIMIT 1"
                ).fetchone()[0])
            self.assertEqual(mirror["storage"], "cold")
            self.assertNotIn(body, json.dumps(mirror))

    def test_corrupt_and_missing_objects_degrade_without_losing_provenance(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = CardProjection(Path(tmp) / "pa.db")
            event = TranscriptEvent(session_id="s", seq=1, event_type="output", payload={"text": "x" * 5000})
            store.append_transcript_events([event])
            digest = next(iter(store.transcripts.referenced_hashes()))
            path = store.transcripts._object_path(digest)
            path.write_bytes(zlib.compress(b"changed"))
            loaded = store.list_transcript_events("s")[0]
            self.assertEqual(loaded.id, event.id)
            self.assertEqual(loaded.payload["availability"], "corrupt")
            path.unlink()
            self.assertEqual(store.list_transcript_events("s")[0].payload["availability"], "missing")
            self.assertEqual(store.transcript_storage_metrics()["missing_objects"], 1)

    def test_legacy_migration_preserves_order_attribution_completion_and_hash(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "pa.db"
            first = CardProjection(db)
            event = TranscriptEvent(session_id="legacy", seq=7, event_type="turn_completed", payload={"text": "done"})
            # Simulate a pre-upgrade row by removing the new representation.
            with first._conn() as conn:
                conn.execute("INSERT INTO agent_transcript_events VALUES(?,?,?,?,?,?)", (
                    event.id, event.session_id, event.seq, event.event_type,
                    json.dumps(event.payload), event.created_at.isoformat()))
            first.transcripts.db_path.unlink()
            restarted = CardProjection(db)
            migrated = restarted.list_transcript_events("legacy")[0]
            self.assertEqual((migrated.session_id, migrated.seq, migrated.event_type), ("legacy", 7, "turn_completed"))
            expected = hashlib.sha256(b'{"text":"done"}').hexdigest()
            with restarted.transcripts._conn() as conn:
                self.assertEqual(conn.execute("SELECT payload_hash FROM transcript_events").fetchone()[0], expected)

    def test_retention_keeps_independent_completion_evidence_and_is_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = CardProjection(Path(tmp) / "pa.db")
            session = store.save_session(AgentSession(id="s", agent_name="codex"))
            store.append_transcript_events([TranscriptEvent(session_id="s", seq=1, event_type="turn_completed", payload={"result": "ok"})])
            store.close_session(session.id, reason="done")
            old = datetime.now(UTC) - timedelta(days=30)
            with store._conn() as conn: conn.execute("UPDATE agent_sessions SET updated_at=? WHERE id='s'", (old.isoformat(),))
            self.assertEqual(store.prune_closed_session_transcripts(before=datetime.now(UTC)), 2)
            self.assertEqual(store.prune_closed_session_transcripts(before=datetime.now(UTC)), 0)
            self.assertEqual(store.list_transcript_events("s"), [])
            self.assertGreaterEqual(store.transcript_storage_metrics()["audit_evidence"], 1)

    def test_wal_metadata_reads_continue_during_transcript_ingestion(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = CardProjection(Path(tmp) / "pa.db")
            store.save_session(AgentSession(id="s", agent_name="codex"))
            failures = []
            def ingest() -> None:
                try:
                    for batch in range(20):
                        store.append_transcript_events([TranscriptEvent(session_id="s", seq=batch * 100 + i + 1, event_type="chunk", payload={"text": "x" * 64}) for i in range(100)])
                except Exception as exc: failures.append(exc)
            worker = threading.Thread(target=ingest)
            worker.start()
            while worker.is_alive():
                self.assertIsNotNone(store.get_session("s"))
            worker.join()
            self.assertEqual(failures, [])
            with sqlite3.connect(store.transcripts.db_path) as conn:
                self.assertEqual(conn.execute("PRAGMA journal_mode").fetchone()[0], "wal")


if __name__ == "__main__":
    unittest.main()

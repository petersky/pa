from __future__ import annotations

import asyncio
import re
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

from fastapi.testclient import TestClient

from pa.acp.client import AgentConnection, normalize_session_update
from pa.config import Settings, reset_settings
from pa.core.kernel import Kernel
from pa.domain.models import (
    AgentSession,
    KnowledgeEntry,
    KnowledgeKind,
    KnowledgeStatus,
    TranscriptEvent,
)
from pa.domain.projection import CardProjection
from pa.domain.store import reset_store
from pa.instance.agent_session import reset_instance_agent
from pa.knowledge.capture import (
    TRANSFORMATION_VERSION,
    assemble_canonical_transcript,
    audit_knowledge_records,
    capture_from_updates,
    promote_from_transcript,
    regenerate_knowledge,
)

CANONICAL = (
    "# Result\n\n"
    "**Codex** can't invent spaces in `refresh-all`.\n\n"
    "- URL: https://example.test/a-b?q=one%20two\n"
    "- Path: `/tmp/known-good/file.py`\n"
    "- UUID: `053fc9cf-f2c4-4f58-a05f-bf4aa3570cea`\n"
    "- SHA: `da171f016c52402367649ff7eaa347113cc4e14f`\n"
    "- Unicode: café — “quoted” 👩🏽‍💻 e\u0301\n\n"
    '```python\nprint("exact")\n```\n'
)


def _chunk(
    seq: int,
    text: str,
    *,
    event_id: str | None = None,
    phase: str = "final",
    message_id: str = "message-final",
    content_mode: str = "delta",
) -> TranscriptEvent:
    return TranscriptEvent(
        id=event_id or f"event-{seq}",
        session_id="session-1",
        seq=seq,
        event_type="agent_message_chunk",
        payload={
            "text": text,
            "phase": phase,
            "message_id": message_id,
            "content_mode": content_mode,
        },
    )


class CanonicalAssemblerTests(unittest.TestCase):
    def test_normalization_preserves_typed_final_replacement_semantics(self) -> None:
        normalized = normalize_session_update(
            {
                "sessionUpdate": "agent_message_chunk",
                "messageId": "final-message",
                "content": {"type": "text", "text": CANONICAL},
                "_meta": {
                    "codex": {
                        "phase": "final",
                        "contentMode": "replacement",
                        "final": True,
                    }
                },
            }
        )

        self.assertEqual(normalized["message_id"], "final-message")
        self.assertEqual(normalized["phase"], "final")
        self.assertEqual(normalized["content_mode"], "replacement")
        self.assertTrue(normalized["final"])

    def test_every_two_chunk_boundary_preserves_exact_canonical_text(self) -> None:
        for boundary in range(len(CANONICAL) + 1):
            with self.subTest(boundary=boundary):
                assembled = assemble_canonical_transcript(
                    [
                        _chunk(1, CANONICAL[:boundary]),
                        _chunk(2, CANONICAL[boundary:]),
                        TranscriptEvent(
                            session_id="session-1",
                            seq=3,
                            event_type="turn_completed",
                        ),
                    ]
                )
                self.assertEqual(assembled.text, CANONICAL)

    def test_character_chunks_preserve_markdown_unicode_and_identifiers(self) -> None:
        events = [_chunk(index, char) for index, char in enumerate(CANONICAL, 1)]
        events.append(
            TranscriptEvent(
                session_id="session-1",
                seq=len(events) + 1,
                event_type="turn_completed",
            )
        )

        assembled = assemble_canonical_transcript(events)

        self.assertEqual(assembled.text, CANONICAL)
        self.assertEqual(
            assembled.content_hash,
            assemble_canonical_transcript(list(events)).content_hash,
        )

    def test_final_replaces_partial_and_typed_replacement_replaces_deltas(self) -> None:
        events = [
            _chunk(1, "Planning text", phase="commentary"),
            _chunk(2, "stale partial", phase="response", message_id="partial"),
            TranscriptEvent(
                session_id="session-1",
                seq=3,
                event_type="agent_thought_chunk",
                payload={"text": "private reasoning"},
            ),
            TranscriptEvent(
                session_id="session-1",
                seq=4,
                event_type="tool_call",
                payload={"title": "Read file"},
            ),
            _chunk(5, "bad", content_mode="delta"),
            _chunk(6, "exact", content_mode="replace"),
            _chunk(7, " final", content_mode="delta"),
            TranscriptEvent(
                session_id="session-1",
                seq=8,
                event_type="turn_completed",
            ),
        ]

        assembled = assemble_canonical_transcript(events)

        self.assertEqual(assembled.text, "exact final")
        self.assertNotIn("Planning", assembled.text)
        self.assertNotIn("reasoning", assembled.text)
        self.assertNotIn("Read file", assembled.text)
        self.assertEqual(assembled.event_start, 6)
        self.assertEqual(assembled.event_end, 7)

    def test_duplicate_reconnect_delivery_and_pagination_order_are_idempotent(
        self,
    ) -> None:
        first = _chunk(10, "known-")
        second = _chunk(11, "good")
        completed = TranscriptEvent(
            session_id="session-1", seq=12, event_type="turn_completed"
        )

        replayed = assemble_canonical_transcript(
            [second, first, first.model_copy(deep=True), completed]
        )
        resumed = assemble_canonical_transcript([first, second, completed])

        self.assertEqual(replayed.text, "known-good")
        self.assertEqual(replayed, resumed)

    def test_typed_final_message_wins_over_partial_chunks(self) -> None:
        assembled = assemble_canonical_transcript(
            [
                _chunk(1, "partial", phase="response"),
                TranscriptEvent(
                    session_id="session-1",
                    seq=2,
                    event_type="agent_message_final",
                    payload={
                        "text": CANONICAL,
                        "message_id": "canonical-final",
                        "content_mode": "replace",
                    },
                ),
                TranscriptEvent(
                    session_id="session-1",
                    seq=3,
                    event_type="turn_completed",
                ),
            ]
        )

        self.assertEqual(assembled.text, CANONICAL)


class PromotionLifecycleTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = CardProjection(Path(self.tmp.name) / "pa.db")
        self.store.save_session(
            AgentSession(id="session-1", agent_name="codex", card_id="card-1")
        )
        self.store.append_transcript_events(
            [
                TranscriptEvent(
                    session_id="session-1",
                    seq=1,
                    event_type="user_message",
                    payload={"message": "Report the result"},
                ),
                _chunk(2, CANONICAL[:31]),
                _chunk(3, CANONICAL[31:]),
                TranscriptEvent(
                    session_id="session-1",
                    seq=4,
                    event_type="turn_completed",
                ),
            ]
        )

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def test_explicit_promotion_is_exact_scoped_audited_and_deduplicated(
        self,
    ) -> None:
        first = promote_from_transcript(
            self.store,
            session_id="session-1",
            actor="user:editor",
            kind=KnowledgeKind.DECISION,
            scope="project:pa",
            start_seq=1,
            end_seq=4,
            tags=["accepted"],
        )
        duplicate = promote_from_transcript(
            self.store,
            session_id="session-1",
            actor="user:editor",
            kind=KnowledgeKind.DECISION,
            scope="project:pa",
            start_seq=1,
            end_seq=4,
        )

        self.assertEqual(first.summary, CANONICAL)
        self.assertEqual(duplicate.id, first.id)
        self.assertEqual(first.status, KnowledgeStatus.ACTIVE)
        self.assertEqual(first.scope, "project:pa")
        self.assertIsNotNone(first.provenance)
        assert first.provenance is not None
        self.assertEqual(first.provenance.source_session_id, "session-1")
        self.assertEqual(first.provenance.source_event_start, 2)
        self.assertEqual(first.provenance.source_event_end, 3)
        self.assertEqual(first.provenance.actor, "user:editor")
        self.assertEqual(first.provenance.transformation, TRANSFORMATION_VERSION)
        audit = self.store.list_knowledge_audit(first.id)
        self.assertEqual(
            {event.action for event in audit},
            {"promoted", "promotion_deduplicated"},
        )

    def test_curated_summary_retains_exact_source_provenance(self) -> None:
        entry = promote_from_transcript(
            self.store,
            session_id="session-1",
            actor="user:editor",
            summary="Decision: keep transcripts in audit history.",
            kind=KnowledgeKind.DECISION,
            scope="realm",
        )

        self.assertEqual(entry.summary, "Decision: keep transcripts in audit history.")
        assert entry.provenance is not None
        self.assertNotEqual(entry.content_hash, entry.provenance.source_content_hash)

    def test_automatic_capture_is_disabled_or_pending_review(self) -> None:
        updates = [
            {
                "sessionUpdate": "agent_message_chunk",
                "messageId": "message-final",
                "content": {"type": "text", "text": CANONICAL},
                "_meta": {"codex": {"phase": "final"}},
            }
        ]

        disabled = capture_from_updates(
            self.store,
            session_id="session-1",
            item_id="card-1",
            updates=updates,
        )
        enabled = capture_from_updates(
            self.store,
            session_id="session-1",
            item_id="card-1",
            updates=updates,
            enabled=True,
            eligible=True,
        )

        self.assertIsNone(disabled)
        assert enabled is not None
        self.assertEqual(enabled.status, KnowledgeStatus.REVIEW)
        self.assertEqual(enabled.source, "generated")
        self.assertIn("pending-review", enabled.tags)
        self.assertEqual(self.store.list_knowledge(), [])

    def test_audit_and_regeneration_supersede_without_rewriting_evidence(
        self,
    ) -> None:
        corrupt = self.store.add_knowledge(
            KnowledgeEntry(
                session_id="session-1",
                summary="C od ex stored known -good text",
                source="acp_session",
                tags=["auto-capture"],
            )
        )

        report = audit_knowledge_records(self.store)
        regenerated = regenerate_knowledge(
            self.store, entry_id=corrupt.id, actor="user:operator"
        )
        original = self.store.get_knowledge(corrupt.id)

        self.assertEqual(report[0]["id"], corrupt.id)
        self.assertTrue(report[0]["recoverable"])
        self.assertIn("unintended-auto-capture", report[0]["reasons"])
        self.assertEqual(regenerated.summary, CANONICAL)
        self.assertEqual(regenerated.supersedes_id, corrupt.id)
        assert original is not None
        self.assertEqual(original.summary, "C od ex stored known -good text")
        self.assertEqual(original.status, KnowledgeStatus.SUPERSEDED)

    def test_unavailable_source_is_reported_and_never_guessed(self) -> None:
        corrupt = self.store.add_knowledge(
            KnowledgeEntry(
                session_id="missing-session",
                summary="C od ex",
                source="acp_session",
                tags=["auto-capture"],
            )
        )

        report = audit_knowledge_records(self.store)

        self.assertFalse(report[0]["recoverable"])
        with self.assertRaisesRegex(ValueError, "source transcript is unavailable"):
            regenerate_knowledge(self.store, entry_id=corrupt.id, actor="user:operator")


class AcpCapturePolicyTests(unittest.TestCase):
    def test_prompt_never_captures_by_default_and_only_queues_marked_candidates(
        self,
    ) -> None:
        async def run(enabled: bool, marked: bool):
            with tempfile.TemporaryDirectory() as tmp:
                settings = Settings(
                    data_dir=Path(tmp) / "data",
                    workspace_root=Path(tmp) / "workspaces",
                    memory_auto_capture_enabled=enabled,
                )
                store = MagicMock()
                connection = AgentConnection(settings, store)
                connection.session = AgentSession(
                    id="session-1",
                    agent_name="codex",
                    external_session_id="provider-session",
                )
                connection._conn = MagicMock()
                connection._conn.prompt = AsyncMock(
                    return_value=SimpleNamespace(stop_reason="end_turn", usage=None)
                )
                update = {
                    "sessionUpdate": "agent_message_chunk",
                    "content": {"type": "text", "text": "Eligible fact"},
                    "_meta": {
                        "codex": {"phase": "final"},
                        "pa": {"memory_candidate": marked},
                    },
                }
                connection._client = MagicMock()
                connection._client.drain_updates.return_value = [update]
                await connection.prompt("Do work")
                return connection.last_memory_candidate

        default_capture = asyncio.run(run(False, True))
        unmarked_capture = asyncio.run(run(True, False))
        marked_capture = asyncio.run(run(True, True))

        self.assertFalse(default_capture)
        self.assertFalse(unmarked_capture)
        self.assertTrue(marked_capture)


class KnowledgeApiTests(unittest.TestCase):
    def setUp(self) -> None:
        reset_settings()
        reset_store()
        reset_instance_agent()
        self.tmp = tempfile.TemporaryDirectory()
        self.settings = Settings(
            data_dir=Path(self.tmp.name) / "data",
            workspace_root=Path(self.tmp.name) / "workspaces",
            instance_id="memory-api-test",
            instance_name="Memory API test",
            agent_enabled=False,
        )
        self.kernel = Kernel.boot(settings=self.settings)
        self.app = self.kernel.build_app()
        self.store = self.kernel.ctx.store
        self.store.save_session(
            AgentSession(id="session-api", agent_name="codex", card_id="card-api")
        )
        self.store.append_transcript_events(
            [
                TranscriptEvent(
                    session_id="session-api",
                    seq=1,
                    event_type="user_message",
                    payload={"message": "Decide"},
                ),
                TranscriptEvent(
                    session_id="session-api",
                    seq=2,
                    event_type="agent_message_chunk",
                    payload={
                        "text": "# Decision\n\n**Keep** `exact-text`.",
                        "phase": "final",
                        "message_id": "final",
                    },
                ),
                TranscriptEvent(
                    session_id="session-api",
                    seq=3,
                    event_type="turn_completed",
                ),
            ]
        )

    def tearDown(self) -> None:
        reset_instance_agent()
        reset_store()
        reset_settings()
        self.tmp.cleanup()

    @staticmethod
    def _csrf(client: TestClient) -> str:
        page = client.get("/knowledge")
        match = re.search(r'<meta name="csrf-token" content="([^"]+)"', page.text)
        assert match is not None
        return match.group(1)

    def test_promotion_render_filter_audit_and_bulk_lifecycle_end_to_end(self) -> None:
        with TestClient(self.app) as client:
            token = self._csrf(client)
            promoted = client.post(
                "/api/knowledge/promote",
                headers={"X-CSRF-Token": token},
                json={
                    "session_id": "session-api",
                    "kind": "decision",
                    "scope": "project:pa",
                },
            )
            self.assertEqual(promoted.status_code, 201)
            entry_id = promoted.json()["id"]

            memory = client.get("/knowledge?source=promoted&kind=decision")
            self.assertEqual(memory.status_code, 200)
            self.assertIn("data-card-markdown", memory.text)
            self.assertIn("source-promoted", memory.text)
            self.assertIn("Source provenance", memory.text)
            self.assertIn(TRANSFORMATION_VERSION, memory.text)
            self.assertIn("# Decision", memory.text)

            replacement = client.get(
                "/partials/knowledge?source=promoted&kind=decision"
            )
            self.assertEqual(replacement.status_code, 200)
            self.assertEqual(
                replacement.text.count(
                    'class="card-markdown memory-markdown" data-card-markdown'
                ),
                1,
            )
            self.assertEqual(replacement.text.count("data-card-markdown-source"), 1)

            audit = client.get(f"/api/knowledge/{entry_id}/audit")
            self.assertEqual(audit.status_code, 200)
            self.assertEqual(audit.json()[0]["action"], "promoted")

            archived = client.post(
                "/api/knowledge/bulk",
                headers={"X-CSRF-Token": token},
                json={"ids": [entry_id], "action": "archive"},
            )
            self.assertEqual(archived.status_code, 200)
            self.assertEqual(archived.json()["updated"], [entry_id])
            self.assertEqual(
                self.store.get_knowledge(entry_id).status,
                KnowledgeStatus.ARCHIVED,
            )

    def test_malicious_markdown_is_escaped_in_server_fallback(self) -> None:
        malicious = (
            "# Safe heading\n\n**bold** [bad](javascript:alert(1)) "
            '<img src=x onerror="alert(1)"><script>alert(1)</script>'
        )
        with TestClient(self.app) as client:
            token = self._csrf(client)
            response = client.post(
                "/api/knowledge/promote",
                headers={"X-CSRF-Token": token},
                json={"session_id": "session-api", "summary": malicious},
            )
            self.assertEqual(response.status_code, 201)
            page = client.get("/knowledge")

        self.assertIn("data-card-markdown-source", page.text)
        self.assertNotIn("<script>alert(1)</script>", page.text)
        self.assertNotIn('<img src=x onerror="alert(1)">', page.text)
        self.assertIn("&lt;script&gt;alert(1)&lt;/script&gt;", page.text)

    def test_viewer_cannot_promote_or_change_lifecycle(self) -> None:
        users = self.kernel.ctx.require_service("users")
        local = users.get("local")
        assert local is not None
        local.role = "viewer"
        with TestClient(self.app) as client:
            token = self._csrf(client)
            denied = client.post(
                "/api/knowledge/promote",
                headers={"X-CSRF-Token": token},
                json={"session_id": "session-api"},
            )

        self.assertEqual(denied.status_code, 403)
        self.assertEqual(self.store.list_knowledge(), [])

    def test_memory_markdown_rebinds_after_supported_htmx_event(self) -> None:
        spa = (
            Path(__file__).parents[1]
            / "src"
            / "pa"
            / "server"
            / "static"
            / "js"
            / "spa.js"
        ).read_text()

        self.assertIn(
            'document.body.addEventListener("htmx:afterSwap", handleAfterSwap)',
            spa,
        )
        self.assertIn("observeMarkdownMutations", spa)
        self.assertIn("renderCardMarkdown(document)", spa)
        self.assertIn("{ allowEmbeddedMedia: false }", spa)


if __name__ == "__main__":
    unittest.main()

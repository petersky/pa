"""Shared compact-card, detail-dialog, and summary data regressions."""

from __future__ import annotations

import re
import sqlite3
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path

from fastapi.testclient import TestClient

from pa.config import Settings, reset_settings
from pa.core.kernel import Kernel
from pa.domain.card_summaries import MAX_CARD_SUMMARY_LENGTH, fallback_card_summary
from pa.domain.models import (
    CardCreate,
    CardLane,
    CardSummarySource,
    CardUpdate,
    AgentSession,
    KnowledgeEntry,
    KnowledgeKind,
    KnowledgeStatus,
    KnowledgeUpdate,
    ProjectCreate,
)
from pa.domain.projection import CardProjection
from pa.domain.session_selection import preferred_sessions_by_card
from pa.domain.store import reset_store
from pa.instance.agent_session import reset_instance_agent


class CardSummaryTests(unittest.TestCase):
    def test_fallback_is_plain_bounded_and_limited_to_three_sentences(self) -> None:
        body = (
            "## First [linked](https://example.test) sentence. "
            "Second sentence! Third sentence? Fourth sentence is omitted. "
            + "word "
            * 100
        )
        summary = fallback_card_summary(body)

        self.assertNotIn("##", summary)
        self.assertNotIn("https://", summary)
        self.assertIn("First linked sentence.", summary)
        self.assertNotIn("Fourth sentence", summary)
        self.assertLessEqual(len(summary), MAX_CARD_SUMMARY_LENGTH)

    def test_fallback_recomputes_but_curated_summary_becomes_stale(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = CardProjection(Path(tmp) / "pa.db")
            card = store.create_card(
                CardCreate(title="Summaries", body="Initial deterministic details.")
            )
            self.assertEqual(card.summary_source, CardSummarySource.FALLBACK)
            self.assertEqual(card.summary, "Initial deterministic details.")

            fallback = store.update_card(
                card.id, CardUpdate(body="Changed fallback details.")
            )
            assert fallback is not None
            self.assertEqual(fallback.summary, "Changed fallback details.")
            self.assertFalse(fallback.summary_stale)

            curated = store.update_card(
                card.id,
                CardUpdate(
                    summary="A deliberately curated summary.",
                    summary_source=CardSummarySource.AGENT,
                ),
            )
            assert curated is not None
            self.assertEqual(curated.summary_source, CardSummarySource.AGENT)

            stale = store.update_card(card.id, CardUpdate(body="New source details."))
            assert stale is not None
            self.assertEqual(stale.summary, "A deliberately curated summary.")
            self.assertTrue(stale.summary_stale)

    def test_existing_cards_are_backfilled_during_schema_migration(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "pa.db"
            now = datetime.now(UTC).isoformat()
            conn = sqlite3.connect(db)
            conn.execute(
                """
                CREATE TABLE cards (
                    id TEXT PRIMARY KEY, realm_id TEXT NOT NULL, kind TEXT NOT NULL,
                    title TEXT NOT NULL, body TEXT NOT NULL, lane TEXT NOT NULL,
                    parent_id TEXT, project_id TEXT, tags TEXT NOT NULL,
                    visibility TEXT NOT NULL, owner_principal TEXT,
                    preferred_instance TEXT, preferred_capabilities TEXT NOT NULL,
                    lease_holder_instance TEXT, lease_holder_principal TEXT,
                    lease_expires_at TEXT, created_by_principal TEXT,
                    created_by_instance TEXT, created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                )
                """
            )
            conn.execute(
                "INSERT INTO cards VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    "legacy-card",
                    "default",
                    "task",
                    "Legacy",
                    "Legacy body becomes durable summary text.",
                    "inbox",
                    None,
                    None,
                    "[]",
                    "realm",
                    None,
                    None,
                    "[]",
                    None,
                    None,
                    None,
                    None,
                    None,
                    now,
                    now,
                ),
            )
            conn.commit()
            conn.close()

            card = CardProjection(db).get_card("legacy-card")
            assert card is not None
            self.assertEqual(card.summary, "Legacy body becomes durable summary text.")
            self.assertEqual(card.summary_source, CardSummarySource.FALLBACK)
            self.assertFalse(card.summary_stale)
            self.assertEqual(card.summary_updated_at.isoformat(), now)

    def test_open_card_session_wins_over_a_newer_closed_session(self) -> None:
        now = datetime.now(UTC)
        closed = AgentSession(
            agent_name="codex",
            card_id="card-1",
            status="closed",
            updated_at=now,
        )
        open_session = AgentSession(
            agent_name="codex",
            card_id="card-1",
            status="working",
            updated_at=now - timedelta(minutes=5),
        )

        selected = preferred_sessions_by_card([closed, open_session])

        self.assertEqual(selected["card-1"].id, open_session.id)


class CuratedKnowledgeTests(unittest.TestCase):
    def test_memory_metadata_filters_and_lifecycle_are_durable(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = CardProjection(Path(tmp) / "pa.db")
            decision = store.add_knowledge(
                KnowledgeEntry(
                    summary="Use progressive disclosure for collection views.",
                    kind=KnowledgeKind.DECISION,
                    source="manual",
                    source_url="https://example.test/decision",
                    scope="project:pa",
                    owner="user:designer",
                    confidence=0.9,
                    tags=["ux", "navigation"],
                )
            )
            store.add_knowledge(
                KnowledgeEntry(
                    summary="Unrelated archived note", status=KnowledgeStatus.ARCHIVED
                )
            )

            active = store.list_knowledge(search="disclosure", kind="decision")
            self.assertEqual([entry.id for entry in active], [decision.id])
            self.assertEqual(active[0].scope, "project:pa")
            self.assertEqual(active[0].confidence, 0.9)
            duplicate = store.add_knowledge(
                KnowledgeEntry(
                    summary="  use progressive disclosure for collection views. ",
                    kind=KnowledgeKind.DECISION,
                    scope="project:pa",
                )
            )
            self.assertEqual(duplicate.id, decision.id)

            archived = store.update_knowledge(
                decision.id, KnowledgeUpdate(status=KnowledgeStatus.ARCHIVED)
            )
            assert archived is not None
            self.assertEqual(archived.status, KnowledgeStatus.ARCHIVED)
            self.assertEqual(store.list_knowledge(), [])

    def test_legacy_knowledge_schema_is_migrated_without_losing_content(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp) / "pa.db"
            now = datetime.now(UTC).isoformat()
            conn = sqlite3.connect(db)
            conn.execute(
                """CREATE TABLE knowledge (
                    id TEXT PRIMARY KEY, session_id TEXT, item_id TEXT, card_id TEXT,
                    summary TEXT NOT NULL, source TEXT NOT NULL, tags TEXT NOT NULL,
                    created_at TEXT NOT NULL
                )"""
            )
            conn.execute(
                "INSERT INTO knowledge VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                ("legacy", "session", None, None, "Keep this", "session", "[]", now),
            )
            conn.commit()
            conn.close()

            migrated = CardProjection(db).get_knowledge("legacy")
            assert migrated is not None
            self.assertEqual(migrated.summary, "Keep this")
            self.assertEqual(migrated.kind, KnowledgeKind.MEMORY)
            self.assertEqual(migrated.status, KnowledgeStatus.ACTIVE)
            self.assertEqual(migrated.updated_at.isoformat(), now)


class CoreWorkUiRouteTests(unittest.TestCase):
    def setUp(self) -> None:
        reset_settings()
        reset_store()
        reset_instance_agent()
        self.tmp = tempfile.TemporaryDirectory()
        self.settings = Settings(
            data_dir=Path(self.tmp.name),
            instance_id="ux-test",
            instance_name="UX test",
            agent_enabled=False,
        )
        self.app = Kernel.boot(settings=self.settings).build_app()

    def tearDown(self) -> None:
        reset_instance_agent()
        reset_store()
        reset_settings()
        self.tmp.cleanup()

    def test_home_and_collection_views_use_summaries_without_right_rails(self) -> None:
        with TestClient(self.app) as client:
            card = self.app.state.ctx.store.create_card(
                CardCreate(
                    title="Compact orchestration",
                    body="FULL BODY MUST STAY OUT OF COLLECTIONS",
                    summary="Concise durable summary.",
                    summary_source=CardSummarySource.MANUAL,
                )
            )
            home = client.get("/")
            self.assertEqual(home.status_code, 200)
            self.assertIn("Quick capture", home.text)
            self.assertIn("Needs attention", home.text)
            self.assertIn("Active work", home.text)
            self.assertIn("Recent outcomes", home.text)
            self.assertNotIn("page-sidebar-right", home.text)
            self.assertNotIn("FULL BODY MUST STAY OUT OF COLLECTIONS", home.text)

            collection = client.get("/partials/cards?lane=inbox")
            self.assertEqual(collection.status_code, 200)
            self.assertIn("Concise durable summary.", collection.text)
            self.assertIn("data-card-detail-link", collection.text)
            self.assertIn("data-card-move-to", collection.text)
            self.assertNotIn("FULL BODY MUST STAY OUT OF COLLECTIONS", collection.text)

            detail = client.get(f"/partials/cards/{card.id}/detail")
            self.assertEqual(detail.status_code, 200)
            self.assertIn("FULL BODY MUST STAY OUT OF COLLECTIONS", detail.text)
            self.assertIn("data-inline-edit-field", detail.text)
            self.assertIn("data-card-markdown", detail.text)
            self.assertNotIn("card-edit-surface", detail.text)
            self.assertNotIn("data-card-edit-open", detail.text)

    def test_memory_page_is_curated_filterable_and_not_a_transcript_dump(self) -> None:
        with TestClient(self.app) as client:
            self.app.state.ctx.store.add_knowledge(
                KnowledgeEntry(
                    summary="Keep the durable decision visible.",
                    kind=KnowledgeKind.DECISION,
                    source="promoted",
                    session_id="audit-session",
                )
            )
            response = client.get("/knowledge?kind=decision")

        self.assertEqual(response.status_code, 200)
        self.assertIn("Memory &amp; decisions", response.text)
        self.assertIn("Keep the durable decision visible.", response.text)
        self.assertIn("Session audit", response.text)
        self.assertIn(
            "Session transcripts stay in session audit history", response.text
        )
        self.assertNotIn("All sessions", response.text)
        self.assertNotIn("page-sidebar-right", response.text)

    def test_project_overview_has_progress_work_agents_and_explicit_settings(
        self,
    ) -> None:
        with TestClient(self.app) as client:
            project = self.app.state.ctx.store.create_project(
                ProjectCreate(
                    title="Orchestration UX", description="Make work legible."
                )
            )
            self.app.state.ctx.store.create_card(
                CardCreate(
                    title="Blocked navigation work",
                    summary="Waiting on a decision.",
                    project_id=project.id,
                    lane=CardLane.WAITING,
                )
            )
            response = client.get(f"/projects?project={project.id}")

        self.assertEqual(response.status_code, 200)
        self.assertIn("Project health", response.text)
        self.assertIn("Edit project settings", response.text)
        self.assertIn("Blocked navigation work", response.text)
        self.assertIn("Linked repositories &amp; worktrees", response.text)
        self.assertIn("Agents &amp; pull requests", response.text)

    def test_home_ignores_work_board_query_filters(self) -> None:
        with TestClient(self.app) as client:
            card = self.app.state.ctx.store.create_card(
                CardCreate(
                    title="Always visible command-center work",
                    summary="Active regardless of stale board filters.",
                    lane=CardLane.ACTIVE,
                )
            )

            response = client.get("/?q=no-match&blocked=blocked&kind=concern")

            self.assertEqual(response.status_code, 200)
            self.assertIn(card.title, response.text)

    def test_detail_save_preserves_summary_semantics_and_missing_cards_are_404(
        self,
    ) -> None:
        with TestClient(self.app) as client:
            card = self.app.state.ctx.store.create_card(
                CardCreate(
                    title="Curated card",
                    body="Original details.",
                    summary="A curated summary.",
                    summary_source=CardSummarySource.MANUAL,
                )
            )
            page = client.get("/")
            token_match = re.search(
                r'<meta name="csrf-token" content="([^"]+)"', page.text
            )
            assert token_match is not None
            form = {
                "title": card.title,
                "body": "Changed source details.",
                "summary": card.summary,
                "lane": card.lane.value,
            }

            saved = client.post(
                f"/partials/cards/{card.id}",
                headers={"X-CSRF-Token": token_match.group(1)},
                data=form,
            )

            self.assertEqual(saved.status_code, 200, saved.text)
            self.assertIn("Summary needs review", saved.text)
            updated = self.app.state.ctx.store.get_card(card.id)
            assert updated is not None
            self.assertEqual(updated.summary_source, CardSummarySource.MANUAL)
            self.assertTrue(updated.summary_stale)

            unchanged_at = updated.updated_at
            no_op = client.post(
                f"/partials/cards/{card.id}",
                headers={"X-CSRF-Token": token_match.group(1)},
                data=form,
            )
            self.assertEqual(no_op.status_code, 200, no_op.text)
            unchanged = self.app.state.ctx.store.get_card(card.id)
            assert unchanged is not None
            self.assertEqual(unchanged.updated_at, unchanged_at)

            missing = client.post(
                f"/partials/cards/{card.id}?realm=elsewhere",
                headers={"X-CSRF-Token": token_match.group(1)},
                data=form,
            )
            self.assertEqual(missing.status_code, 404)

    def test_detail_inline_forms_update_only_the_submitted_field(self) -> None:
        with TestClient(self.app) as client:
            card = self.app.state.ctx.store.create_card(
                CardCreate(
                    title="Inline editing",
                    body="Original **details**.",
                    summary="Original *summary*.",
                    summary_source=CardSummarySource.MANUAL,
                )
            )
            page = client.get("/")
            token_match = re.search(
                r'<meta name="csrf-token" content="([^"]+)"', page.text
            )
            assert token_match is not None

            saved = client.post(
                f"/partials/cards/{card.id}",
                headers={"X-CSRF-Token": token_match.group(1)},
                data={
                    "body": (
                        "Updated details with "
                        "![media](https://example.test/a.png)"
                    )
                },
            )

            self.assertEqual(saved.status_code, 200, saved.text)
            updated = self.app.state.ctx.store.get_card(card.id)
            assert updated is not None
            self.assertEqual(updated.title, card.title)
            self.assertEqual(updated.summary, card.summary)
            self.assertEqual(updated.lane, card.lane)
            self.assertIn("Updated details with", updated.body)
            self.assertIn('data-markdown-tab="edit"', saved.text)
            self.assertIn('data-markdown-tab="preview"', saved.text)
            self.assertIn("data-markdown-preview", saved.text)

    def test_work_filters_and_mobile_lane_controls_are_labeled(self) -> None:
        with TestClient(self.app) as client:
            response = client.get("/work?q=ship&blocked=blocked&updated=7")
            self.assertEqual(response.status_code, 200)
            for label in (
                "Search",
                "Project",
                "Kind",
                "Owner",
                "Instance",
                "Blocked state",
                "Tag",
                "Updated",
            ):
                self.assertIn(f"<span>{label}</span>", response.text)
            self.assertIn('name="q" value="ship"', response.text)
            self.assertIn('data-board-lane="active"', response.text)
            self.assertIn('aria-label="Work board"', response.text)
            self.assertNotIn("page-sidebar-right", response.text)

    def test_first_page_response_exposes_matching_csrf_for_mutation(self) -> None:
        with TestClient(self.app) as client:
            page = client.get("/")
            match = re.search(r'<meta name="csrf-token" content="([^"]+)"', page.text)
            self.assertIsNotNone(match)
            token = match.group(1) if match else ""
            self.assertEqual(token, client.cookies.get("pa_csrf"))

            created = client.post(
                "/api/cards",
                headers={"X-CSRF-Token": token},
                json={"title": "First-load mutation", "body": "Works safely."},
            )
            self.assertEqual(created.status_code, 201, created.text)
            self.assertEqual(created.json()["summary"], "Works safely.")

    def test_detail_agent_is_explicit_and_responsive_breakpoints_exist(self) -> None:
        root = Path(__file__).parents[1] / "src" / "pa" / "server"
        detail = (root / "templates" / "partials" / "card-detail.html").read_text()
        script = (root / "static" / "js" / "spa.js").read_text()
        markdown = (root / "static" / "js" / "agent-chat.js").read_text()
        css = (root / "static" / "style.css").read_text()

        self.assertIn("data-card-agent-start", detail)
        self.assertIn("auto_start=false", detail)
        self.assertIn('hx-preserve="true"', detail)
        self.assertIn("Selecting a card never starts work", detail)
        self.assertIn("data-inline-edit-open", detail)
        self.assertIn('data-markdown-tab="edit"', detail)
        self.assertIn('data-markdown-tab="preview"', detail)
        self.assertNotIn("card-edit-surface", detail)
        self.assertIn("history.pushState({ paCard", script)
        self.assertIn("renderCardMarkdown", script)
        self.assertIn("renderMarkdownInto(preview", script)
        self.assertIn('ADD_TAGS: ["audio", "iframe", "picture", "source", "track", "video"]', markdown)
        self.assertIn('"sandbox",', markdown)
        self.assertIn("Could not move card. Its original lane was restored.", script)
        self.assertIn("@media (max-width: 1000px)", css)
        self.assertIn("@media (max-width: 700px)", css)
        self.assertIn("width: 100vw", css)
        self.assertIn("prefers-reduced-motion", css)

    def test_core_icon_controls_and_async_surfaces_have_accessible_names(self) -> None:
        root = Path(__file__).parents[1] / "src" / "pa" / "server" / "templates"
        chrome = (root / "partials" / "chrome-actions.html").read_text()
        agent = (root / "partials" / "agent" / "chat-widget.html").read_text()
        memory = (root / "pages" / "knowledge.html").read_text()
        fleet = (root / "pages" / "fleet.html").read_text()

        self.assertIn('aria-label="Reconnect agent"', chrome)
        self.assertIn('aria-label="Toggle theme appearance"', chrome)
        self.assertIn('aria-label="Settings"', chrome)
        self.assertIn('aria-label="Tool activity"', agent)
        self.assertIn('aria-label="Session plans"', agent)
        self.assertIn('aria-live="polite"', memory)
        self.assertIn('aria-label="Filter memories"', memory)
        self.assertIn('aria-live="polite"', fleet)
        self.assertIn('aria-label="Update {{ inst.name }}"', fleet)
        self.assertIn('aria-label="Remove {{ inst.name }} from fleet"', fleet)


if __name__ == "__main__":
    unittest.main()

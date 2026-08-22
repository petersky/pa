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
from pa.domain.models import (
    AgentSession,
    CardCreate,
    CardLane,
    CardSummarySource,
    CardUpdate,
    KnowledgeEntry,
    KnowledgeKind,
    KnowledgeStatus,
    KnowledgeUpdate,
    ProjectCreate,
    RepositoryCreate,
)
from pa.domain.projection import CardProjection
from pa.domain.session_selection import preferred_sessions_by_card
from pa.domain.store import reset_store
from pa.instance.agent_session import reset_instance_agent


class CardSummaryTests(unittest.TestCase):
    def test_sessions_and_cards_have_durable_many_to_many_associations(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = CardProjection(Path(tmp) / "pa.db")
            first = store.create_card(CardCreate(title="First"))
            second = store.create_card(CardCreate(title="Second"))
            session = store.save_session(
                AgentSession(
                    id="session-many-cards",
                    agent_name="codex",
                    card_id=first.id,
                )
            )

            store.link_session_card(session.id, second.id, make_primary=True)

            self.assertEqual(
                store.list_card_ids_for_session(session.id), [first.id, second.id]
            )
            self.assertEqual(store.get_session(session.id).card_id, second.id)
            self.assertEqual(
                {item.id for item in store.list_sessions_for_cards({first.id})},
                {session.id},
            )
            self.assertEqual(
                {item.id for item in store.list_sessions_for_cards({second.id})},
                {session.id},
            )

            store.unlink_session_card(
                session.id,
                second.id,
                reason="associated_card_terminal",
                principal_id="system:session_lifecycle",
            )

            self.assertEqual(store.get_session(session.id).card_id, first.id)
            self.assertEqual(store.list_card_ids_for_session(session.id), [first.id])
            history = store.list_session_card_history(session.id)
            retired = next(item for item in history if item["card_id"] == second.id)
            self.assertIsNotNone(retired["linked_at"])
            self.assertIsNotNone(retired["retired_at"])
            self.assertEqual(retired["retired_reason"], "associated_card_terminal")
            self.assertEqual(
                retired["retired_by_principal"], "system:session_lifecycle"
            )
            self.assertNotIn(
                session.id,
                {item.id for item in store.list_sessions_for_cards({second.id})},
            )

            store.link_session_card(
                session.id,
                second.id,
                principal_id="user:relinker",
                make_primary=True,
            )

            intervals = [
                item
                for item in store.list_session_card_history(session.id)
                if item["card_id"] == second.id
            ]
            self.assertEqual(len(intervals), 2)
            self.assertEqual(
                intervals[0]["retired_reason"], "associated_card_terminal"
            )
            self.assertEqual(
                intervals[0]["retired_by_principal"], "system:session_lifecycle"
            )
            self.assertIsNone(intervals[1]["retired_at"])
            self.assertEqual(intervals[1]["linked_by_principal"], "user:relinker")

    def test_unsummarized_cards_are_pending_and_edits_are_stale(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = CardProjection(Path(tmp) / "pa.db")
            card = store.create_card(
                CardCreate(title="Summaries", body="Initial deterministic details.")
            )
            self.assertEqual(card.summary_source, CardSummarySource.FALLBACK)
            self.assertEqual(card.summary, "")
            self.assertEqual(card.summary_status.value, "pending")

            fallback = store.update_card(
                card.id, CardUpdate(body="Changed fallback details.")
            )
            assert fallback is not None
            self.assertEqual(fallback.summary, "")
            self.assertTrue(fallback.summary_stale)
            self.assertEqual(fallback.summary_status.value, "stale")

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

    def test_existing_cards_enter_pending_migration_without_startup_backfill(
        self,
    ) -> None:
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
            self.assertEqual(card.summary, "")
            self.assertEqual(card.summary_source, CardSummarySource.FALLBACK)
            self.assertFalse(card.summary_stale)
            self.assertEqual(card.summary_status.value, "pending")
            self.assertIsNone(card.summary_updated_at)

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
                    title="diagnose HTTP/2 naïve API 🧭",
                    body="FULL BODY MUST STAY OUT OF COLLECTIONS",
                    summary="Concise durable summary.",
                    summary_source=CardSummarySource.MANUAL,
                )
            )
            home = client.get("/")
            self.assertEqual(home.status_code, 200)
            self.assertNotIn("Quick capture", home.text)
            self.assertIn("data-new-card-open", home.text)
            self.assertIn('id="new-card-dialog"', home.text)
            self.assertIn("Needs attention", home.text)
            self.assertIn("In motion", home.text)
            self.assertIn("Recent outcomes", home.text)
            self.assertNotIn("page-sidebar-right", home.text)
            self.assertNotIn("FULL BODY MUST STAY OUT OF COLLECTIONS", home.text)

            collection = client.get("/partials/cards?lane=inbox")
            self.assertEqual(collection.status_code, 200)
            self.assertIn("Concise durable summary.", collection.text)
            self.assertIn("diagnose HTTP/2 naïve API 🧭", collection.text)
            self.assertNotIn("Diagnose Http/2 Naïve Api", collection.text)
            self.assertIn("data-card-detail-link", collection.text)
            self.assertIn("data-card-move-to", collection.text)
            self.assertIn(f'aria-label="Actions for {card.title}"', collection.text)
            self.assertIn(
                f'aria-label="Move {card.title} to Active"', collection.text
            )
            self.assertNotIn("FULL BODY MUST STAY OUT OF COLLECTIONS", collection.text)

            detail = client.get(f"/partials/cards/{card.id}/detail")
            self.assertEqual(detail.status_code, 200)
            self.assertIn("FULL BODY MUST STAY OUT OF COLLECTIONS", detail.text)
            self.assertIn("data-inline-edit-field", detail.text)
            self.assertIn("data-card-markdown", detail.text)
            self.assertNotIn("card-edit-surface", detail.text)
            self.assertNotIn("data-card-edit-open", detail.text)

    def test_board_menu_keyboard_and_live_refresh_contract_is_wired(self) -> None:
        script = (Path(__file__).parents[1] / "src/pa/server/static/js/spa.js").read_text()
        self.assertIn('event.key === "Escape"', script)
        self.assertIn('["ArrowDown", "Enter", " "]', script)
        self.assertIn('trigger.focus()', script)
        self.assertIn('"htmx:beforeSwap"', script)
        self.assertIn("boardRefreshFocus", script)

    def test_summary_diagnostics_and_disabled_ui_are_truthful_and_redacted(
        self,
    ) -> None:
        secret = "must-never-appear"
        with TestClient(self.app) as client:
            diagnostic = client.get("/api/cards/summary/diagnostics")
            self.assertEqual(diagnostic.status_code, 200)
            payload = diagnostic.json()
            self.assertEqual(payload["state"], "disabled")
            self.assertEqual(payload["effective_provider"], "openai")
            self.assertEqual(payload["effective_model"], "gpt-5-mini")
            self.assertEqual(payload["authentication_source"], "none")
            self.assertIn("PA_CARD_SUMMARY_API_KEY", payload["setup_guidance"])
            self.assertNotIn(secret, diagnostic.text)

            card = self.app.state.ctx.store.create_card(
                CardCreate(title="No provider", body=f"Description {secret}")
            )
            self.app.state.ctx.store.update_card(
                card.id,
                CardUpdate(
                    summary_status="disabled",
                    summary_failure_code="unconfigured",
                    summary_failure="Set PA_CARD_SUMMARY_API_KEY.",
                ),
            )
            detail = client.get(f"/partials/cards/{card.id}/detail")
            self.assertEqual(detail.status_code, 200)
            self.assertIn("Summary generation is disabled.", detail.text)
            self.assertIn("Authentication: none", detail.text)
            self.assertIn("Setup needed: unconfigured", detail.text)

            disabled_excerpt = "LEGACY DISABLED PREFIX MUST NOT RENDER"
            disabled = self.app.state.ctx.store.create_card(
                CardCreate(
                    title="Disabled legacy summary",
                    body=f"{disabled_excerpt} with complete description details.",
                    summary=disabled_excerpt,
                    summary_source=CardSummarySource.FALLBACK,
                )
            )
            self.app.state.ctx.store.update_card(
                disabled.id,
                CardUpdate(summary_status="disabled", summary_stale=True),
            )
            stale_excerpt = "LEGACY STALE PREFIX MUST NOT RENDER"
            stale = self.app.state.ctx.store.create_card(
                CardCreate(
                    title="Stale legacy summary",
                    body=f"{stale_excerpt} with complete description details.",
                    summary=stale_excerpt,
                    summary_source=CardSummarySource.FALLBACK,
                )
            )
            self.app.state.ctx.store.update_card(
                stale.id,
                CardUpdate(summary_status="stale", summary_stale=True),
            )

            collection = client.get("/partials/cards?lane=inbox")
            self.assertEqual(collection.status_code, 200)
            self.assertNotIn(disabled_excerpt, collection.text)
            self.assertNotIn(stale_excerpt, collection.text)
            self.assertIn("Summary generation is disabled.", collection.text)
            self.assertIn("Summary pending.", collection.text)

    def test_global_header_shows_the_serving_instance_beneath_the_pa_brand(
        self,
    ) -> None:
        with TestClient(self.app) as client:
            non_local = client.get("/")

            self.assertEqual(non_local.status_code, 200)
            self.assertRegex(
                non_local.text,
                r'class="brand-link">PA</a>\s*'
                r'<span class="brand-instance" data-pa-instance-name="UX test">',
            )
            self.assertIn("<pa-instance-identity", non_local.text)
            self.assertIn('instance-id="ux-test"', non_local.text)
            self.assertIn(
                '<span class="instance-identity-name">UX test</span>',
                non_local.text,
            )
            self.assertIn('src="/static/js/instance-identity.js', non_local.text)
            self.assertNotIn("instance-indicator", non_local.text)
            self.assertNotIn('aria-label="Instance:', non_local.text)
            self.assertRegex(
                non_local.text,
                r"data-new-card-open[\s\S]*?</button>\s*"
                r"(?!<span[^>]+instance)",
            )
            for page in self.app.state.ctx.require_service("pages").nav_pages():
                with self.subTest(page=page.path):
                    global_page = client.get(page.path)
                    self.assertEqual(global_page.status_code, 200)
                    self.assertIn(
                        'data-pa-instance-name="UX test"',
                        global_page.text,
                    )
                    self.assertNotIn("instance-indicator", global_page.text)

            self.app.state.ctx.settings.instance_name = "local"
            local = client.get("/")

            self.assertEqual(local.status_code, 200)
            self.assertIn('data-pa-instance-name="UX test"', local.text)
            self.assertIn(
                '<span class="instance-identity-name">UX test</span>',
                local.text,
            )
            self.assertNotIn('data-pa-instance-name="local"', local.text)

    def test_header_instance_label_has_responsive_quiet_type_contract(self) -> None:
        root = Path(__file__).parents[1] / "src" / "pa" / "server"
        css = (root / "static" / "style.css").read_text()
        spa = (root / "static" / "js" / "spa.js").read_text()

        brand_rules = css.split(".brand-block {", 1)[1].split("}", 1)[0]
        instance_rules = css.split(".brand-instance {", 1)[1].split("}", 1)[0]
        mobile_rules = css.split("@media (max-width: 700px)", 1)[1]

        self.assertIn("flex-direction: column", brand_rules)
        self.assertIn("flex: 0 0 auto", brand_rules)
        self.assertIn("font-size: 0.6875rem", instance_rules)
        self.assertIn("color: var(--pa-text-muted)", instance_rules)
        self.assertIn("white-space: nowrap", instance_rules)
        self.assertIn(
            ".header-start { min-width: 0; flex-wrap: nowrap; }", mobile_rules
        )
        self.assertIn(
            'document.querySelector("[data-pa-instance-name]")',
            spa,
        )
        self.assertNotIn(".instance-indicator", spa)

    def test_new_card_modal_exposes_compact_complete_creation_fields(self) -> None:
        with TestClient(self.app) as client:
            project = self.app.state.ctx.store.create_project(
                ProjectCreate(title="Selected project")
            )
            parent = self.app.state.ctx.store.create_card(
                CardCreate(title="Parent work")
            )

            response = client.get(f"/partials/cards/new?project={project.id}")

        self.assertEqual(response.status_code, 200)
        self.assertIn("data-new-card-form", response.text)
        self.assertIn(f'value="{project.id}" selected', response.text)
        self.assertIn(f'value="{parent.id}"', response.text)
        for field in (
            "title",
            "body",
            "summary",
            "kind",
            "lane",
            "project_id",
            "parent_id",
            "tags",
            "preferred_instance",
            "preferred_capabilities",
            "auto_enrich",
            "link_urls",
            "link_labels",
        ):
            self.assertIn(f'name="{field}"', response.text)
        self.assertIn("data-new-card-files", response.text)
        self.assertIn("Drop files here", response.text)
        self.assertIn('type="checkbox" name="auto_enrich"', response.text)

    def test_new_card_modal_creates_links_and_file_attachments(self) -> None:
        with TestClient(self.app) as client:
            project = self.app.state.ctx.store.create_project(
                ProjectCreate(title="Attachment project")
            )
            parent = self.app.state.ctx.store.create_card(
                CardCreate(title="Attachment parent")
            )
            page = client.get("/")
            token_match = re.search(
                r'<meta name="csrf-token" content="([^"]+)"', page.text
            )
            assert token_match is not None

            created = client.post(
                "/partials/cards/new?realm=default",
                headers={"X-CSRF-Token": token_match.group(1)},
                data={
                    "title": "Card with attachments",
                    "body": "Visual proof:\n\n![photo.png](attachment:drop-photo)",
                    "summary": "A manually curated summary.",
                    "kind": "goal",
                    "lane": "active",
                    "project_id": project.id,
                    "parent_id": parent.id,
                    "tags": "ui, upload, ui",
                    "preferred_instance": "instance-special",
                    "preferred_capabilities": "browser, gpu",
                    "link_urls": ["https://example.test/spec?q=card attachment"],
                    "link_labels": ["Design spec"],
                    "file_tokens": ["drop-photo", "picker-notes"],
                },
                files=[
                    ("files", ("photo.png", b"\x89PNG\r\n\x1a\nimage", "image/png")),
                    ("files", ("notes.txt", b"attachment notes", "text/plain")),
                ],
            )

            self.assertEqual(created.status_code, 201, created.text)
            card = self.app.state.ctx.store.get_card(created.json()["id"])
            assert card is not None
            self.assertEqual(card.kind.value, "goal")
            self.assertEqual(card.lane, CardLane.ACTIVE)
            self.assertEqual(card.project_id, project.id)
            self.assertEqual(card.parent_id, parent.id)
            self.assertEqual(card.tags, ["ui", "upload"])
            self.assertEqual(card.preferred_instance, "instance-special")
            self.assertEqual(card.preferred_capabilities, ["browser", "gpu"])
            self.assertEqual(card.summary, "A manually curated summary.")
            self.assertEqual(card.summary_source, CardSummarySource.MANUAL)
            self.assertIn("## Links", card.body)
            self.assertIn(
                "[Design spec](https://example.test/spec?q=card%20attachment)",
                card.body,
            )
            self.assertNotIn("attachment:drop-photo", card.body)
            self.assertIn("## Attachments", card.body)
            attachment_section = card.body.split("## Attachments", 1)[1]
            self.assertNotIn("photo.png", attachment_section)

            photo_match = re.search(
                r"(/card-attachments/[0-9a-f]{32}/photo\.png)", card.body
            )
            notes_match = re.search(
                r"(/card-attachments/[0-9a-f]{32}/notes\.txt)", card.body
            )
            assert photo_match is not None
            assert notes_match is not None
            photo = client.get(photo_match.group(1))
            notes = client.get(notes_match.group(1))

        self.assertEqual(photo.status_code, 200)
        self.assertEqual(photo.content, b"\x89PNG\r\n\x1a\nimage")
        self.assertEqual(photo.headers["content-type"], "image/png")
        self.assertIn("inline", photo.headers["content-disposition"])
        self.assertEqual(photo.headers["x-content-type-options"], "nosniff")
        self.assertEqual(notes.status_code, 200)
        self.assertEqual(notes.content, b"attachment notes")
        self.assertIn("attachment", notes.headers["content-disposition"])

    def test_invalid_attachment_link_does_not_leave_uploaded_files(self) -> None:
        with TestClient(self.app) as client:
            page = client.get("/")
            token_match = re.search(
                r'<meta name="csrf-token" content="([^"]+)"', page.text
            )
            assert token_match is not None
            response = client.post(
                "/partials/cards/new",
                headers={"X-CSRF-Token": token_match.group(1)},
                data={
                    "title": "Rejected link",
                    "link_urls": ["javascript:alert(1)"],
                    "file_tokens": ["pending"],
                },
                files=[("files", ("pending.txt", b"pending", "text/plain"))],
            )

        self.assertEqual(response.status_code, 422)
        attachment_root = Path(self.tmp.name) / "card-attachments"
        self.assertEqual(list(attachment_root.glob("*")), [])

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

    def test_home_does_not_guess_motion_from_active_lane(self) -> None:
        with TestClient(self.app) as client:
            card = self.app.state.ctx.store.create_card(
                CardCreate(
                    title="Always visible command-center work",
                    summary="Active regardless of stale board filters.",
                    lane=CardLane.ACTIVE,
                )
            )

            shell = client.get("/?q=no-match&blocked=blocked&kind=concern")
            response = client.get(
                "/partials/home/sections?q=no-match&blocked=blocked&kind=concern"
            )

            self.assertEqual(shell.status_code, 200)
            self.assertNotIn(card.title, shell.text)
            self.assertIn("Loading work in motion…", shell.text)
            self.assertEqual(response.status_code, 200)
            self.assertNotIn(card.title, response.text)
            self.assertIn("No autonomous work in motion", response.text)

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
                        "Updated details with ![media](https://example.test/a.png)"
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

    def test_done_lane_is_title_only_and_expands_filtered_results(self) -> None:
        with TestClient(self.app) as client:
            store = self.app.state.ctx.store
            omitted = []
            for index in range(12):
                card = store.create_card(
                    CardCreate(
                        title=f"Matching outcome {index:02d}",
                        summary=f"Done summary {index} must stay hidden.",
                        lane=CardLane.DONE,
                    )
                )
                if index < 2:
                    omitted.append(card.title)
            store.create_card(
                CardCreate(
                    title="Unrelated newest outcome",
                    summary="This card must not affect a filtered result limit.",
                    lane=CardLane.DONE,
                )
            )

            first_page = client.get("/partials/cards?lane=done&q=Matching")

            self.assertEqual(first_page.status_code, 200)
            self.assertEqual(first_page.text.count('<article class="compact-card'), 10)
            self.assertIn("Matching outcome 11", first_page.text)
            for title in omitted:
                self.assertNotIn(title, first_page.text)
            self.assertNotIn("Done summary", first_page.text)
            self.assertNotIn("Unrelated newest outcome", first_page.text)
            self.assertNotIn("compact-card-summary", first_page.text)
            self.assertNotIn("compact-card-meta", first_page.text)
            self.assertNotIn("compact-card-footer", first_page.text)
            self.assertIn("Showing 10 of 12", first_page.text)
            self.assertIn("Show 2 more", first_page.text)
            self.assertIn("data-done-show-more", first_page.text)
            self.assertIn("q=Matching", first_page.text)
            self.assertIn("limit=12", first_page.text)

            expanded = client.get("/partials/cards?lane=done&q=Matching&limit=12")

            self.assertEqual(expanded.status_code, 200)
            self.assertEqual(expanded.text.count('<article class="compact-card'), 12)
            for title in omitted:
                self.assertIn(title, expanded.text)
            self.assertNotIn("data-done-show-more", expanded.text)

    def test_first_page_response_exposes_matching_csrf_for_mutation(self) -> None:
        with TestClient(self.app) as client:
            page = client.get("/")
            match = re.search(r'<meta name="csrf-token" content="([^"]+)"', page.text)
            self.assertIsNotNone(match)
            token = match.group(1) if match else ""
            self.assertEqual(token, client.cookies.get("pa_csrf"))

            created = client.post(
                "/api/cards",
                headers={
                    "X-CSRF-Token": token,
                    "Idempotency-Key": "first-load-create",
                },
                json={"title": "First-load mutation", "body": "Works safely."},
            )
            self.assertEqual(created.status_code, 201, created.text)
            self.assertEqual(created.json()["summary"], "")
            self.assertEqual(created.json()["summary_status"], "pending")
            self.assertNotEqual(created.json()["summary"], "Works safely.")

    def test_card_project_change_simple_assign_change_and_clear(self) -> None:
        with TestClient(self.app) as client:
            first = self.app.state.ctx.store.create_project(
                ProjectCreate(title="First")
            )
            second = self.app.state.ctx.store.create_project(
                ProjectCreate(title="Second")
            )
            card = self.app.state.ctx.store.create_card(CardCreate(title="Movable"))
            page = client.get("/")
            token = re.search(
                r'<meta name="csrf-token" content="([^"]+)"', page.text
            ).group(1)

            assigned = client.post(
                f"/api/cards/{card.id}/project-change",
                headers={"X-CSRF-Token": token},
                json={"project_id": first.id},
            )
            self.assertEqual(assigned.status_code, 200, assigned.text)
            self.assertEqual(assigned.json()["status"], "changed")
            self.assertEqual(assigned.json()["card"]["project_id"], first.id)

            changed = client.post(
                f"/api/cards/{card.id}/project-change",
                headers={"X-CSRF-Token": token},
                json={"project_id": second.id},
            )
            self.assertEqual(changed.status_code, 200, changed.text)
            self.assertEqual(changed.json()["card"]["project_id"], second.id)

            cleared = client.post(
                f"/api/cards/{card.id}/project-change",
                headers={"X-CSRF-Token": token},
                json={"project_id": None},
            )
            self.assertEqual(cleared.status_code, 200, cleared.text)
            self.assertIsNone(cleared.json()["card"]["project_id"])

    def test_card_project_change_reviews_dependencies_and_cancel_preserves_card(
        self,
    ) -> None:
        with TestClient(self.app) as client:
            source = self.app.state.ctx.store.create_project(
                ProjectCreate(title="Source")
            )
            target = self.app.state.ctx.store.create_project(
                ProjectCreate(title="Target")
            )
            repository = self.app.state.ctx.store.create_repository(
                RepositoryCreate(url="https://example.test/source.git", name="source")
            )
            self.app.state.ctx.store.link_project_repository(source.id, repository.id)
            card = self.app.state.ctx.store.create_card(
                CardCreate(title="Dependent", project_id=source.id)
            )
            page = client.get("/")
            token = re.search(
                r'<meta name="csrf-token" content="([^"]+)"', page.text
            ).group(1)

            review = client.post(
                f"/api/cards/{card.id}/project-change",
                headers={"X-CSRF-Token": token},
                json={"project_id": target.id},
            )
            self.assertEqual(review.status_code, 200, review.text)
            self.assertEqual(review.json()["status"], "review_required")
            self.assertEqual(review.json()["impact"]["repository_count"], 1)
            self.assertFalse(review.json()["impact"]["migration_compatible"])
            self.assertEqual(
                self.app.state.ctx.store.get_card(card.id).project_id, source.id
            )

            rejected = client.post(
                f"/api/cards/{card.id}/project-change",
                headers={"X-CSRF-Token": token},
                json={"project_id": target.id, "decision": "migrate"},
            )
            self.assertEqual(rejected.status_code, 409)
            self.assertEqual(
                self.app.state.ctx.store.get_card(card.id).project_id, source.id
            )

            cancelled = client.post(
                f"/api/cards/{card.id}/project-change",
                headers={"X-CSRF-Token": token},
                json={"project_id": target.id, "decision": "cancel"},
            )
            self.assertEqual(cancelled.json()["status"], "cancelled")
            self.assertEqual(
                self.app.state.ctx.store.get_card(card.id).project_id, source.id
            )

    def test_card_project_change_compatible_migration_preserves_repository_links(
        self,
    ) -> None:
        with TestClient(self.app) as client:
            source = self.app.state.ctx.store.create_project(
                ProjectCreate(title="Source")
            )
            target = self.app.state.ctx.store.create_project(
                ProjectCreate(title="Target")
            )
            repository = self.app.state.ctx.store.create_repository(
                RepositoryCreate(url="https://example.test/shared.git", name="shared")
            )
            self.app.state.ctx.store.link_project_repository(source.id, repository.id)
            self.app.state.ctx.store.link_project_repository(target.id, repository.id)
            card = self.app.state.ctx.store.create_card(
                CardCreate(title="Compatible", project_id=source.id)
            )
            token = re.search(
                r'<meta name="csrf-token" content="([^"]+)"', client.get("/").text
            ).group(1)

            review = client.post(
                f"/api/cards/{card.id}/project-change",
                headers={"X-CSRF-Token": token},
                json={"project_id": target.id},
            )
            self.assertTrue(review.json()["impact"]["migration_compatible"])
            migrated = client.post(
                f"/api/cards/{card.id}/project-change",
                headers={"X-CSRF-Token": token},
                json={"project_id": target.id, "decision": "migrate"},
            )
            self.assertEqual(migrated.status_code, 200, migrated.text)
            self.assertEqual(migrated.json()["card"]["project_id"], target.id)
            self.assertEqual(
                len(self.app.state.ctx.store.list_project_repositories(source.id)), 1
            )
            self.assertEqual(
                len(self.app.state.ctx.store.list_project_repositories(target.id)), 1
            )

    def test_detail_agent_is_explicit_and_responsive_breakpoints_exist(self) -> None:
        root = Path(__file__).parents[1] / "src" / "pa" / "server"
        detail = (root / "templates" / "partials" / "card-detail.html").read_text()
        agent_detail = (
            root / "templates" / "partials" / "card-detail-agent.html"
        ).read_text()
        activity_detail = (
            root / "templates" / "partials" / "card-detail-activity.html"
        ).read_text()
        script = (root / "static" / "js" / "spa.js").read_text()
        markdown = (root / "static" / "js" / "agent-chat.js").read_text()
        css = (root / "static" / "style.css").read_text()
        new_card = (root / "templates" / "partials" / "card-new.html").read_text()

        self.assertIn("data-card-agent-start-new", agent_detail)
        self.assertIn("data-card-agent-select", agent_detail)
        self.assertIn("auto_start=false", agent_detail)
        self.assertIn('hx-preserve="true"', agent_detail)
        self.assertIn("Selecting a card never starts work", agent_detail)
        self.assertIn("data-inline-edit-open", detail)
        self.assertIn('data-markdown-tab="edit"', detail)
        self.assertIn('data-markdown-tab="preview"', detail)
        self.assertIn('role="tablist"', detail)
        self.assertIn('data-card-tab="summary"', detail)
        self.assertIn('data-card-tab="agent"', detail)
        self.assertIn('data-card-tab="activity"', detail)
        self.assertIn('data-card-tab-src="/partials/cards/', detail)
        self.assertIn("data-card-activity-filter", activity_detail)
        self.assertNotIn("card-edit-surface", detail)
        self.assertIn("paCardDepth", script)
        self.assertIn('url.searchParams.set("tab", name)', script)
        self.assertIn("loadCardTab", script)
        self.assertIn('event.key === "Home"', script)
        self.assertIn('event.key === "End"', script)
        self.assertIn("renderCardMarkdown", script)
        self.assertIn("renderMarkdownInto(preview", script)
        self.assertIn(
            'ADD_TAGS: ["audio", "iframe", "picture", "source", "track", "video"]',
            markdown,
        )
        self.assertIn('"sandbox",', markdown)
        self.assertIn("Could not move card. Its original lane was restored.", script)
        self.assertIn("new EventSource(", script)
        self.assertIn("/api/cards/events?realm=", script)
        self.assertIn('addEventListener("cards-changed"', script)
        self.assertIn('new CustomEvent("boardRefresh")', script)
        self.assertIn("data-new-card-description", new_card)
        self.assertIn("newCardFileMarkup", script)
        self.assertIn('textarea.addEventListener("drop"', script)
        self.assertIn('formData.append("files"', script)
        self.assertIn("new-card-dialog", css)
        self.assertIn("@media (max-width: 1000px)", css)
        self.assertIn("@media (max-width: 700px)", css)
        self.assertIn("width: 100vw", css)
        self.assertIn("position: sticky", css)
        self.assertIn("100dvh", css)
        self.assertIn("env(safe-area-inset-bottom)", css)
        self.assertIn("prefers-reduced-motion", css)

    def test_detail_tabs_lazy_load_agent_and_ordered_activity(self) -> None:
        with TestClient(self.app) as client:
            card = self.app.state.ctx.store.create_card(
                CardCreate(
                    title="A very long card title " * 12,
                    body="Summary loads before execution details.",
                    lane=CardLane.WAITING,
                )
            )
            self.app.state.ctx.store.save_session(
                AgentSession(
                    agent_name="codex",
                    card_id=card.id,
                    title="Lazy agent session",
                    status="working",
                    model_id="gpt-5",
                    mode_id="code",
                    cwd="/worktrees/card-tabs",
                    config_json={
                        "execution_context": {
                            "instance": {"name": "macmini"},
                            "approval_policy": "on-request",
                            "repositories": [
                                {
                                    "repository_url": "https://github.com/petersky/pa",
                                    "branch": "pa/card-tabs",
                                    "base_sha": "a" * 40,
                                }
                            ],
                        }
                    },
                )
            )

            summary = client.get(f"/partials/cards/{card.id}/detail")
            agent = client.get(f"/partials/cards/{card.id}/agent")
            activity = client.get(f"/partials/cards/{card.id}/activity")
            missing_agent = client.get("/partials/cards/missing/agent")
            missing_activity = client.get("/partials/cards/missing/activity")

        self.assertEqual(summary.status_code, 200)
        self.assertIn('data-card-tab="summary"', summary.text)
        self.assertIn('data-card-tab-panel="agent"', summary.text)
        self.assertNotIn("Blocked:", summary.text)
        self.assertIn("No operator-owned next step is recorded.", summary.text)
        self.assertNotIn("Lazy agent session", summary.text)
        self.assertEqual(agent.status_code, 200)
        self.assertIn("Lazy agent session", agent.text)
        self.assertIn("macmini", agent.text)
        self.assertIn("gpt-5", agent.text)
        self.assertIn("pa/card-tabs", agent.text)
        self.assertEqual(activity.status_code, 200)
        self.assertIn("Agent session working", activity.text)
        self.assertIn('data-activity-kind="card"', activity.text)
        self.assertIn('data-activity-kind="agent"', activity.text)
        self.assertEqual(missing_agent.status_code, 404)
        self.assertEqual(missing_activity.status_code, 404)

    def test_core_icon_controls_and_async_surfaces_have_accessible_names(self) -> None:
        root = Path(__file__).parents[1] / "src" / "pa" / "server" / "templates"
        chrome = (root / "partials" / "chrome-actions.html").read_text()
        agent = (root / "partials" / "agent" / "chat-widget.html").read_text()
        memory = (root / "pages" / "knowledge.html").read_text()
        fleet = (root / "pages" / "fleet.html").read_text()

        self.assertIn('aria-label="Open Sessions; agent is', chrome)
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

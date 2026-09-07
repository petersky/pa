from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from pa.domain.card_enrichment import (
    enrich_card,
    enrichment_request,
    advertised_capability_catalog,
    build_enrichment_update,
    explicit_enrichment_fields,
)
from pa.domain.models import (
    CardCreate,
    CardKind,
    CardUpdate,
    ProjectCreate,
    RepositoryCreate,
    ProjectRepo,
)
from pa.domain.projection import CardProjection


class CardEnrichmentTest(unittest.TestCase):
    def test_title_only_card_accepts_all_supported_suggestions(self) -> None:
        data = CardCreate(title="Fix intermittent deploys")

        update = build_enrichment_update(
            json.dumps(
                {
                    "description": (
                        "Investigate and stabilize intermittent deploy failures."
                    ),
                    "kind": "concern",
                    "project_id": "project-1",
                    "preferred_capabilities": ["github", "logs", "github"],
                    "tags": ["deploy", "reliability"],
                }
            ),
            explicit_fields=explicit_enrichment_fields(data),
            project_ids=["project-1"],
            advertised_capabilities=["github", "logs", "browser"],
        )

        self.assertEqual(
            update.body, "Investigate and stabilize intermittent deploy failures."
        )
        self.assertEqual(update.kind, CardKind.CONCERN)
        self.assertEqual(update.project_id, "project-1")
        self.assertEqual(update.preferred_capabilities, ["github", "logs"])
        self.assertEqual(update.tags, ["deploy", "reliability"])

    def test_invented_preferred_capabilities_are_dropped_without_catalog(self) -> None:
        update = build_enrichment_update(
            json.dumps(
                {
                    "description": "Investigate the live session.",
                    "kind": "task",
                    "preferred_capabilities": [
                        "agent-session-diagnostics",
                        "frontend-debugging",
                        "performance-profiling",
                    ],
                    "tags": ["investigation"],
                }
            ),
            explicit_fields=set(),
            project_ids=[],
            advertised_capabilities=[],
        )

        self.assertEqual(update.body, "Investigate the live session.")
        self.assertIsNone(update.preferred_capabilities)
        self.assertEqual(update.tags, ["investigation"])

    def test_catalog_filters_invented_labels_but_keeps_advertised_ones(self) -> None:
        update = build_enrichment_update(
            json.dumps(
                {
                    "preferred_capabilities": [
                        "browser",
                        "agent-session-diagnostics",
                        "frontend-debugging",
                    ],
                    "tags": ["ui"],
                }
            ),
            explicit_fields=set(),
            project_ids=[],
            advertised_capabilities=["browser", "capacity:4"],
        )

        self.assertEqual(update.preferred_capabilities, ["browser"])
        self.assertEqual(update.tags, ["ui"])

    def test_advertised_catalog_unions_local_settings_and_fleet_instances(self) -> None:
        ctx = SimpleNamespace(
            settings=SimpleNamespace(capabilities=["browser"]),
            services={
                "fleet_registry": SimpleNamespace(
                    list_instances=lambda: [
                        SimpleNamespace(capabilities=[]),
                        SimpleNamespace(capabilities=["gpu", " browser "]),
                    ]
                )
            },
        )

        self.assertEqual(
            advertised_capability_catalog(ctx),
            frozenset({"browser", "gpu"}),
        )

    def test_explicit_values_are_never_overwritten(self) -> None:
        data = CardCreate(
            title="Ship release",
            body="Use the approved release checklist.",
            kind=CardKind.GOAL,
            project_id="chosen",
            preferred_capabilities=["macos"],
            tags=["release"],
        )

        update = build_enrichment_update(
            '{"description":"replace","kind":"task","project_id":"other",'
            '"preferred_capabilities":["gpu"],"tags":["wrong"]}',
            explicit_fields=explicit_enrichment_fields(data),
            project_ids=["chosen", "other"],
        )

        self.assertFalse(update.model_fields_set)

    def test_rejects_unknown_project_and_invalid_kind(self) -> None:
        update = build_enrichment_update(
            "```json\n"
            '{"description":"Useful detail","kind":"idea",'
            '"project_id":"invented","preferred_capabilities":[],"tags":[]}'
            "\n```",
            explicit_fields=set(),
            project_ids=["real"],
        )

        self.assertEqual(update.body, "Useful detail")
        self.assertIsNone(update.kind)
        self.assertIsNone(update.project_id)

    def test_auto_enrich_is_route_only_and_defaults_on(self) -> None:
        enabled = CardCreate(title="Default")
        disabled = CardCreate(title="Opt out", auto_enrich=False)

        self.assertTrue(enabled.auto_enrich)
        self.assertFalse(disabled.auto_enrich)
        self.assertNotIn("auto_enrich", disabled.model_dump())

    def test_enrichment_cannot_create_governed_goal(self):
        update = build_enrichment_update(
            '{"kind":"goal"}', explicit_fields=set(), project_ids=[]
        )
        self.assertFalse(update.model_fields_set)

    def test_empty_card_is_rejected_but_description_only_is_allowed(self):
        with self.assertRaises(ValueError):
            CardCreate(title="  ", body="  ")
        self.assertEqual(CardCreate(body="User intent").title, "")


class CardEnrichmentLifecycleTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        from pa.sync.event_log import EventLog
        from pa.sync.object_store import ObjectStore

        root = Path(self.tmp.name)
        self.store = CardProjection(
            root / "pa.db",
            EventLog(ObjectStore(root / "objects"), root / "refs", "local"),
        )
        self.service = SimpleNamespace(suggest_card_fields=AsyncMock(), enqueue=Mock())
        self.ctx = SimpleNamespace(
            store=self.store,
            settings=SimpleNamespace(instance_id="local", capabilities=["browser"]),
            services={},
            require_service=lambda name: self.service,
        )

    async def test_description_only_generates_title_and_preserves_body(self):
        data = CardCreate(body="Investigate why deploys intermittently fail")
        card = self.store.create_card(data)
        self.service.suggest_card_fields.return_value = json.dumps(
            {
                "title": "Stabilize deployments",
                "description": "overwrite",
                "kind": "concern",
                "tags": ["deploy"],
                "preferred_capabilities": ["browser", "invented"],
            }
        )
        await enrich_card(
            self.ctx, card.id, card.realm_id, explicit_enrichment_fields(data)
        )
        result = self.store.get_card(card.id)
        self.assertEqual(result.title, "Stabilize deployments")
        self.assertEqual(result.body, data.body)
        self.assertEqual(result.kind, CardKind.CONCERN)
        self.assertEqual(result.preferred_capabilities, ["browser"])
        self.service.enqueue.assert_called_once_with(card.id, card.realm_id)

    async def test_edits_during_generation_are_preserved(self):
        data = CardCreate(title="Original")
        card = self.store.create_card(data)

        async def provider(*args):
            self.store.update_card(
                card.id, CardUpdate(body="User details", tags=["user"], kind="project")
            )
            return '{"description":"Agent details","tags":["agent"],"kind":"concern","title":"Wrong"}'

        self.service.suggest_card_fields.side_effect = provider
        await enrich_card(
            self.ctx, card.id, card.realm_id, explicit_enrichment_fields(data)
        )
        result = self.store.get_card(card.id)
        self.assertEqual(
            (result.title, result.body, result.tags, result.kind),
            ("Original", "User details", ["user"], CardKind.PROJECT),
        )

    async def test_failure_leaves_created_card_intact(self):
        card = self.store.create_card(CardCreate(title="Keep me"))
        self.service.suggest_card_fields.side_effect = RuntimeError("provider failed")
        await enrich_card(self.ctx, card.id, card.realm_id, {"title"})
        self.assertEqual(self.store.get_card(card.id), card)

    async def test_prompt_contains_field_explanations_and_project_repositories(self):
        project = self.store.create_project(
            ProjectCreate(
                title="PA",
                description="Personal assistant",
                tags=["python"],
                repos=[ProjectRepo(url="https://example.test/legacy")],
            )
        )
        repo = self.store.create_repository(
            RepositoryCreate(name="Core", url="https://example.test/core")
        )
        self.store.link_project_repository(project.id, repo.id)
        card = self.store.create_card(CardCreate(title="Fix PA", tags=["bug"]))
        request, projects, caps = enrichment_request(self.ctx, card, {"title", "tags"})
        context = json.loads(request.messages[1]["content"])
        self.assertEqual(context["projects"][0]["description"], "Personal assistant")
        self.assertEqual(
            {r["url"] for r in context["projects"][0]["repositories"]},
            {"https://example.test/core", "https://example.test/legacy"},
        )
        self.assertIn("task: actionable work", request.messages[0]["content"])
        self.assertIn("untrusted data", request.messages[0]["content"])
        self.assertNotIn("title", request.schema["properties"])
        self.assertNotIn("tags", request.schema["properties"])
        self.assertEqual(caps, ["browser"])

    async def test_no_request_when_all_fields_provided(self):
        data = CardCreate(
            title="Title",
            body="Body",
            kind="task",
            project_id="chosen",
            tags=["t"],
            preferred_capabilities=["browser"],
        )
        card = self.store.create_card(data)
        await enrich_card(
            self.ctx, card.id, card.realm_id, explicit_enrichment_fields(data)
        )
        self.service.suggest_card_fields.assert_not_called()

    async def test_version_conflict_does_not_overwrite_intervening_edit(self):
        data = CardCreate(title="Title")
        card = self.store.create_card(data)
        self.service.suggest_card_fields.return_value = '{"description":"Agent"}'
        original_update = self.store.update_card
        raced = False

        def update(*args, **kwargs):
            nonlocal raced
            if not raced:
                raced = True
                original_update(card.id, CardUpdate(body="User"))
            return original_update(*args, **kwargs)

        self.store.update_card = update
        await enrich_card(
            self.ctx, card.id, card.realm_id, explicit_enrichment_fields(data)
        )
        self.assertEqual(self.store.get_card(card.id).body, "User")


if __name__ == "__main__":
    unittest.main()

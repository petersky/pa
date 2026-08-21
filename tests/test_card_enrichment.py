from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

from pa.domain.card_enrichment import (
    _close_enrichment_session,
    advertised_capability_catalog,
    build_enrichment_update,
    explicit_enrichment_fields,
)
from pa.domain.models import AgentSession, CardCreate, CardKind
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


class CardEnrichmentLifecycleTest(unittest.IsolatedAsyncioTestCase):
    async def test_startup_orphan_is_durably_closed(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = CardProjection(Path(tmp) / "pa.db")
            session = store.save_session(
                AgentSession(
                    id="enrichment-orphan",
                    agent_name="codex",
                    label="card-enrichment:card-1",
                    status="disconnected",
                )
            )
            manager = SimpleNamespace(
                store=store,
                _runtimes={},
                reconcile_closed_sessions=AsyncMock(),
            )

            async def offload(_operation, call, *args, **kwargs):
                return call(*args, **kwargs)

            manager._offload = offload

            await _close_enrichment_session(manager, session.id, None)

            self.assertEqual(store.get_session(session.id).status, "closed")
            self.assertEqual(
                store.list_transcript_events(session.id)[0].event_type,
                "session_closed",
            )
            manager.reconcile_closed_sessions.assert_awaited_once_with([session.id])


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import json
import unittest

from pa.domain.card_enrichment import (
    build_enrichment_update,
    explicit_enrichment_fields,
)
from pa.domain.models import CardCreate, CardKind


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
        )

        self.assertEqual(
            update.body, "Investigate and stabilize intermittent deploy failures."
        )
        self.assertEqual(update.kind, CardKind.CONCERN)
        self.assertEqual(update.project_id, "project-1")
        self.assertEqual(update.preferred_capabilities, ["github", "logs"])
        self.assertEqual(update.tags, ["deploy", "reliability"])

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


if __name__ == "__main__":
    unittest.main()

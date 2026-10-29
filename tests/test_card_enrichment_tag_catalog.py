"""Enrichment needs the complete realm tag vocabulary, not card bodies."""
import json
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from pa.domain.card_enrichment import enrichment_request
from pa.domain.models import CardCreate, CardUpdate
from pa.domain.projection import CardProjection


@pytest.fixture
def catalog(tmp_path):
    store = CardProjection(tmp_path / "fixture.db")
    for realm, tags in [
        ("default", ["shared", "", "Z", "é", "🔎", "shared"]),
        ("default", ["alpha", "shared"]),
        ("other", ["foreign", "shared"]),
    ]:
        store.create_card(CardCreate(realm_id=realm, title="Fixture", body="x" * 65536, tags=tags))
    yield store
    store.close_thread_connection()


def test_enrichment_tag_context_does_not_hydrate_cards(catalog):
    card = catalog.list_cards(realm_id="default")[0]
    ctx = SimpleNamespace(store=catalog, settings=SimpleNamespace(capabilities=[]), services={})
    with patch.object(catalog, "list_cards", side_effect=AssertionError("Full card hydration")):
        request, _, _ = enrichment_request(ctx, card, {"title", "body"})
    assert json.loads(request.messages[1]["content"])["existing_tags"] == ["", "Z", "alpha", "shared", "é", "🔎"]


@pytest.mark.parametrize("realm", ["default", "other", "absent", ""])
def test_native_tag_query_matches_full_card_set_exactly(catalog, realm):
    expected = sorted({tag for card in catalog.list_cards(realm_id=realm) for tag in card.tags})
    with patch.object(catalog, "_row_to_card", side_effect=AssertionError("No card model needed")):
        assert catalog.list_card_tags(realm_id=realm) == expected


def test_tag_catalog_is_fresh_after_edit(catalog):
    card = catalog.list_cards(realm_id="other")[0]
    assert "foreign" in catalog.list_card_tags(realm_id="other")
    catalog.update_card(card.id, CardUpdate(tags=["new"]), realm_id="other")
    assert catalog.list_card_tags(realm_id="other") == ["new"]


def test_locked_tags_need_no_tag_catalog(catalog):
    card = catalog.list_cards(realm_id="default")[0]
    ctx = SimpleNamespace(store=catalog, settings=SimpleNamespace(capabilities=[]), services={})
    with patch.object(catalog, "list_card_tags", side_effect=AssertionError("Protected tags need no catalog")):
        request, _, _ = enrichment_request(ctx, card, {"title", "tags"})
    assert json.loads(request.messages[1]["content"])["existing_tags"] == []

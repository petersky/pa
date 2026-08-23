from __future__ import annotations

import tempfile
from pathlib import Path

from fastapi.testclient import TestClient

from pa.config import Settings, reset_settings
from pa.core.kernel import Kernel
from pa.domain.models import CardCreate
from pa.domain.store import reset_store
from pa.instance.agent_session import reset_instance_agent


def _app(tmp: str):
    reset_settings(); reset_store(); reset_instance_agent()
    return Kernel.boot(settings=Settings(data_dir=Path(tmp), auth_required=False, telemetry_enabled=False)).build_app()


def test_work_shell_does_not_embed_unselected_vocabulary_and_facet_is_bounded() -> None:
    with tempfile.TemporaryDirectory() as tmp, TestClient(_app(tmp)) as client:
        store = client.app.state.ctx.store
        for index in range(75):
            store.create_card(CardCreate(title=f"Card {index}", tags=[f"private-tag-{index:03d}"]))
        shell = client.get("/work")
        facet = client.get("/api/cards/facets?facet=tag&q=tag&limit=20")
        assert shell.status_code == 200
        assert "private-tag-074" not in shell.text
        assert facet.status_code == 200
        assert len(facet.json()["options"]) == 20
        assert facet.headers["x-pa-facet-options"] == "20"
        assert "facet" in facet.headers["server-timing"]


def test_multiple_tag_and_or_filters_and_special_char_url_round_trip() -> None:
    with tempfile.TemporaryDirectory() as tmp, TestClient(_app(tmp)) as client:
        store = client.app.state.ctx.store
        store.create_card(CardCreate(title="Both", tags=["alpha", "ops & qa"]))
        store.create_card(CardCreate(title="One", tags=["alpha"]))
        both = client.get("/partials/cards?lane=inbox&tag=ops%20%26%20qa&tag=alpha&tag_mode=and")
        either = client.get("/partials/cards?lane=inbox&tag=ops%20%26%20qa&tag=alpha&tag_mode=or")
        page = client.get("/work?tag=ops%20%26%20qa&tag=alpha&tag=alpha&tag_mode=and")
        assert "Both" in both.text and "One" not in both.text
        assert "Both" in either.text and "One" in either.text
        assert page.text.count('name="tag" value="alpha"') == 1
        assert 'name="tag" value="ops &amp; qa"' in page.text


def test_saved_views_are_scoped_versioned_audited_and_canonical() -> None:
    with tempfile.TemporaryDirectory() as tmp, TestClient(_app(tmp)) as client:
        client.get("/work")
        csrf = client.cookies.get("pa_csrf")
        headers = {"X-CSRF-Token": csrf, "Idempotency-Key": "saved-view-test"}
        created = client.post("/api/cards/saved-views", headers=headers, json={"name": "Triage", "query": "tag=z&tag=a&tag=z&blocked=blocked"})
        updated = client.post("/api/cards/saved-views", headers={**headers, "Idempotency-Key": "saved-view-test-2"}, json={"name": "Triage", "query": "tag=b"})
        assert created.status_code == 200
        assert created.json()["query"].endswith("tag=a&tag=z")
        assert updated.json()["id"] == created.json()["id"]
        assert updated.json()["version"] == 2
        with client.app.state.ctx.store._conn() as conn:
            assert conn.execute("SELECT COUNT(*) FROM work_saved_view_audit WHERE view_id=?", (created.json()["id"],)).fetchone()[0] == 2


def test_combobox_static_keyboard_and_accessibility_contract() -> None:
    script = Path("src/pa/server/static/js/work-filters.js").read_text()
    template = Path("src/pa/server/templates/pages/work.html").read_text()
    for behavior in ('"ArrowDown"', '"ArrowUp"', '"Enter"', '"Escape"', "aria-activedescendant", "AbortController"):
        assert behavior in script
    for contract in ('role="combobox"', 'role="listbox"', 'aria-live="polite"', "data-clear-tags", "Tag (no JavaScript fallback)"):
        assert contract in template

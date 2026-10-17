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
    return Kernel.boot(settings=Settings(data_dir=Path(tmp) / "data", workspace_root=Path(tmp) / "workspaces", agent_enabled=False, auth_required=False, telemetry_enabled=False)).build_app()


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


def test_applied_filter_chips_remove_one_value_and_preserve_shared_view() -> None:
    import html
    import re
    from urllib.parse import parse_qs, urlencode, urlsplit

    from pa.domain.models import ProjectCreate

    with tempfile.TemporaryDirectory() as tmp, TestClient(_app(tmp)) as client:
        project = client.app.state.ctx.store.create_project(ProjectCreate(title="Release & QA"))
        params = [
            ("q", "ship & review"), ("project", project.id), ("kind", "task"),
            ("owner", "user:me"), ("instance", "missing-instance"), ("blocked", "blocked"),
            ("tag", "ops & qa"), ("tag", "alpha"), ("tag_mode", "or"),
            ("updated", "14"), ("attention", "motion"), ("lane", "waiting"),
        ]
        page = client.get("/work?" + urlencode(params))
        assert page.status_code == 200
        assert '<details class="work-filter-disclosure">' in page.text  # Collapsed even with applied values.
        assert 'aria-label="9 applied"' in page.text
        assert "Project: Release &amp; QA" in page.text
        assert 'name="lane" value="waiting"' in page.text
        assert 'value="14" selected' in page.text
        chips = re.findall(r'href="([^"]+)" aria-label="Remove ([^"]+)" data-applied-filter="([^"]+)"', page.text)
        assert len(chips) == 11
        original = parse_qs(urlencode([("realm", "default"), *params]))
        original["tag"].sort()
        for link, label, key in chips:
            remaining = parse_qs(urlsplit(html.unescape(link)).query)
            expected = {k: list(v) for k, v in original.items()}
            if key == "tag":
                expected[key].remove(html.unescape(label).split(": ", 1)[1])
            else:
                del expected[key]
            assert remaining == expected, (key, remaining, expected)
            assert client.get(html.unescape(link)).status_code == 200


def test_last_tag_removal_clears_match_mode_and_empty_lanes_are_concise() -> None:
    import html
    import re
    from urllib.parse import parse_qs, urlsplit

    with tempfile.TemporaryDirectory() as tmp, TestClient(_app(tmp)) as client:
        page = client.get("/work?tag=ops%20%26%20qa&tag_mode=or&blocked=blocked")
        link = re.search(r'href="([^"]+)" aria-label="Remove Tag:', page.text).group(1)
        assert parse_qs(urlsplit(html.unescape(link)).query) == {"realm": ["default"], "blocked": ["blocked"]}
        empty = client.get("/partials/cards?lane=inbox")
        filtered = client.get("/partials/cards?lane=active&q=missing")
        assert 'data-total="0"' in empty.text
        assert "No inbox cards." in empty.text and "data-new-card-open" in empty.text
        assert "No active cards match these filters." in filtered.text
        assert "data-new-card-open" not in filtered.text
        assert "Try another filter" not in filtered.text
        for index in range(12):
            client.app.state.ctx.store.create_card(CardCreate(title=f"Result {index}"))
        results = client.get("/partials/cards?lane=inbox&q=Result")
        assert 'data-total="12"' in results.text
        assert "Showing 10 of 12 cards" in results.text


def test_show_more_preserves_all_tags_and_match_mode() -> None:
    import html
    import re
    from urllib.parse import parse_qs, urlsplit

    with tempfile.TemporaryDirectory() as tmp, TestClient(_app(tmp)) as client:
        store = client.app.state.ctx.store
        for index in range(12):
            store.create_card(CardCreate(title=f"Outcome {index}", tags=["alpha", "ops & qa"], lane="done"))
        query = "lane=done&tag=alpha&tag=ops%20%26%20qa&tag_mode=and"
        for suffix in ("", "&attention=outcome"):
            page = client.get("/partials/cards?" + query + suffix)
            links = re.findall(r'hx-get="([^"]+)"', page.text)
            link = next(html.unescape(link) for link in links if "limit=" in link)
            params = parse_qs(urlsplit(link).query)
            assert params["tag"] == ["alpha", "ops & qa"]
            assert params["tag_mode"] == ["and"]
            assert client.get(link).status_code == 200

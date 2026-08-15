"""Stable Work-page performance budgets and lifecycle regressions."""

from __future__ import annotations

import tempfile
import time
from pathlib import Path
from unittest.mock import Mock

from fastapi.testclient import TestClient

from pa.config import Settings, reset_settings
from pa.core.kernel import Kernel
from pa.domain.models import CardCreate, CardKind, CardLane
from pa.domain.store import reset_store
from pa.instance.agent_session import reset_instance_agent


def _app(tmp: str):
    reset_settings()
    reset_store()
    reset_instance_agent()
    settings = Settings(data_dir=Path(tmp), auth_required=False, telemetry_enabled=False)
    return Kernel.boot(settings=settings).build_app()


def test_large_history_keeps_work_shell_and_lane_payloads_bounded() -> None:
    with tempfile.TemporaryDirectory() as tmp, TestClient(_app(tmp)) as client:
        store = client.app.state.ctx.store
        for index in range(600):
            store.create_card(
                CardCreate(
                    title=f"Historical card {index:04d}",
                    body="historical detail " * 200,
                    lane=CardLane.DONE if index < 500 else CardLane.ACTIVE,
                )
            )

        shell = client.get("/work")
        lane = client.get("/partials/cards?lane=done")

        assert shell.status_code == 200
        assert "server-timing" in shell.headers
        assert "page_context" in shell.headers["server-timing"]
        assert "template" in shell.headers["server-timing"]
        assert int(shell.headers["x-pa-work-bytes"]) < 100_000
        assert len(lane.content) < 50_000
        assert lane.text.count('<article class="compact-card') == 10
        assert "historical detail" not in shell.text


def test_twenty_repeated_work_navigations_have_a_stable_server_budget() -> None:
    with tempfile.TemporaryDirectory() as tmp, TestClient(_app(tmp)) as client:
        store = client.app.state.ctx.store
        for index in range(120):
            store.create_card(CardCreate(title=f"Card {index:03d}"))

        durations = []
        sizes = []
        for _ in range(20):
            started = time.perf_counter()
            response = client.get("/work", headers={"HX-Request": "true"})
            durations.append(time.perf_counter() - started)
            sizes.append(len(response.content))

        assert max(sizes) == min(sizes)
        assert max(durations) < 1.0
        assert sum(durations[-5:]) / 5 < 2 * (sum(durations[:5]) / 5) + 0.02


def test_lane_batches_sessions_and_dispatch_progress_once() -> None:
    with tempfile.TemporaryDirectory() as tmp, TestClient(_app(tmp)) as client:
        store = client.app.state.ctx.store
        for index in range(40):
            store.create_card(CardCreate(title=f"Visible {index:02d}"))
        original = store.list_sessions_for_cards
        store.list_sessions_for_cards = Mock(wraps=original)
        dispatch_store = client.app.state.ctx.services["dispatch_store"]
        original_dispatch = dispatch_store.latest_by_card
        dispatch_store.latest_by_card = Mock(wraps=original_dispatch)

        response = client.get("/partials/cards?lane=inbox")

        assert response.status_code == 200
        assert store.list_sessions_for_cards.call_count == 1
        assert dispatch_store.latest_by_card.call_count == 1
        requested_ids = store.list_sessions_for_cards.call_args.args[0]
        assert len(requested_ids) == 10


def test_cards_partial_filters_in_sql_and_paginates_before_joins() -> None:
    with tempfile.TemporaryDirectory() as tmp, TestClient(_app(tmp)) as client:
        store = client.app.state.ctx.store
        for index in range(40):
            store.create_card(
                CardCreate(
                    title=f"Task {index:02d}",
                    kind=CardKind.TASK,
                    tags=["alpha"],
                    body="payload " * 80,
                )
            )
        store.create_card(CardCreate(title="Other kind", kind=CardKind.GOAL))
        store.list_cards = Mock(wraps=store.list_cards)
        page = store.list_card_work_projections(
            realm_id="default",
            kind=CardKind.TASK,
            tag="alpha",
            limit=10,
        )
        total = store.count_card_work_projections(
            realm_id="default",
            kind=CardKind.TASK,
            tag="alpha",
        )
        facets = store.list_card_filter_facets(realm_id="default")
        response = client.get("/partials/cards?lane=inbox&kind=task&tag=alpha")

        assert len(page) == 10
        assert all(not card.body for card in page)
        assert total == 40
        assert "alpha" in facets["tags"]
        assert response.status_code == 200
        assert response.text.count('<article class="compact-card') == 10
        store.list_cards.assert_not_called()


def test_work_sse_has_pre_swap_teardown_and_observable_resource_count() -> None:
    script = Path("src/pa/server/static/js/spa.js").read_text()
    assert 'document.body.addEventListener("htmx:beforeSwap"' in script
    assert "stopBoardLiveUpdates();" in script
    assert "window.__paWorkResources = { eventSources: 0 };" in script
    assert "window.__paWorkResources = { eventSources: 1 };" in script

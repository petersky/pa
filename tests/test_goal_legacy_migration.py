from __future__ import annotations

import json
import tempfile
from pathlib import Path

from fastapi.testclient import TestClient

from pa.config import Settings
from pa.core.kernel import Kernel
from pa.domain.models import CardCreate, CardKind
from pa.execution.orchestration import GoalPlan


def _app(path: Path):
    return Kernel.boot(
        settings=Settings(
            data_dir=path,
            instance_id="local",
            instance_name="Local",
            instance_url="http://pa.test:8080",
            agent_enabled=False,
            subscribed_realms=["default"],
            peers=[],
        )
    ).build_app()


def test_legacy_goal_migration_is_dry_run_auditable_and_idempotent() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp)
        app = _app(path)
        with TestClient(app) as client:
            card = app.state.ctx.store.create_card(
                CardCreate(kind=CardKind.GOAL, title="Legacy objective", body="Original context"),
                principal_id="user:legacy",
                instance_id="legacy-instance",
            )
            plan = app.state.ctx.require_service("orchestration_store").put(
                GoalPlan(goal_card_id=card.id)
            )
            assert client.get("/").status_code == 200
            csrf = client.cookies.get("pa_csrf")
            headers = {"X-CSRF-Token": csrf, "Idempotency-Key": "legacy-goals-v1"}

            dry_run = client.post(
                "/api/goals-migration?dry_run=true", headers=headers
            )
            assert dry_run.status_code == 200
            assert dry_run.json()["cards"][0]["plan_ids"] == [plan.id]
            assert app.state.ctx.store.get_card(card.id).kind is CardKind.GOAL

            migrated = client.post(
                "/api/goals-migration?dry_run=false", headers=headers
            )
            assert migrated.status_code == 200, migrated.text
            result = migrated.json()["migrated"][0]
            durable_id = result["durable_goal_id"]
            archived_card = app.state.ctx.store.get_card(card.id)
            assert archived_card.kind is CardKind.CONCERN
            assert "legacy-goal-archived" in archived_card.tags
            durable = client.get(f"/api/goals/{durable_id}").json()["goal"]
            assert durable["creation_source"] == f"legacy-card:{card.id}"
            assert card.id in durable["motivation"]
            archive = json.loads(
                (path / "goal_orchestration_archive.json").read_text()
            )
            assert archive[plan.id]["plan"]["goal_card_id"] == card.id

            replay = client.post(
                "/api/goals-migration?dry_run=false", headers=headers
            )
            assert replay.status_code == 200
            assert len(client.get("/api/goals").json()) == 1


def test_generic_card_modal_rejects_legacy_goal_creation() -> None:
    with tempfile.TemporaryDirectory() as tmp, TestClient(_app(Path(tmp))) as client:
        assert client.get("/").status_code == 200
        csrf = client.cookies.get("pa_csrf")
        response = client.post(
            "/partials/cards/new",
            headers={"X-CSRF-Token": csrf},
            data={"title": "Wrong path", "kind": "goal"},
        )
        assert response.status_code == 422
        assert "governed Goals workspace" in response.text

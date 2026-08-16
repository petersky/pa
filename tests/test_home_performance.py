"""Stable Home-page performance budgets for the deferred section shell."""

from __future__ import annotations

import tempfile
import time
from pathlib import Path

from fastapi.testclient import TestClient

from pa.config import Settings, reset_settings
from pa.core.kernel import Kernel
from pa.domain.models import CardCreate, CardLane
from pa.domain.store import reset_store
from pa.instance.agent_session import reset_instance_agent


def _app(tmp: str):
    reset_settings()
    reset_store()
    reset_instance_agent()
    settings = Settings(data_dir=Path(tmp), auth_required=False, telemetry_enabled=False)
    return Kernel.boot(settings=settings).build_app()


def test_large_history_keeps_home_shell_small_and_sections_bounded() -> None:
    with tempfile.TemporaryDirectory() as tmp, TestClient(_app(tmp)) as client:
        store = client.app.state.ctx.store
        for index in range(300):
            store.create_card(
                CardCreate(
                    title=f"Historical home card {index:04d}",
                    body="historical home detail " * 80,
                    lane=CardLane.DONE if index < 250 else CardLane.ACTIVE,
                )
            )

        shell = client.get("/")
        sections = client.get("/partials/home/sections")

        assert shell.status_code == 200
        assert "server-timing" in shell.headers
        assert "page_context" in shell.headers["server-timing"]
        assert "template" in shell.headers["server-timing"]
        assert int(shell.headers["x-pa-home-bytes"]) < 100_000
        assert "historical home detail" not in shell.text
        assert "Historical home card" not in shell.text
        assert "Loading actionable work…" in shell.text
        assert "data-attention-card" not in shell.text
        assert sections.status_code == 200
        assert sections.text.count("data-attention-card") <= 20
        assert "historical home detail" not in sections.text


def test_repeated_home_navigations_have_a_stable_server_budget() -> None:
    with tempfile.TemporaryDirectory() as tmp, TestClient(_app(tmp)) as client:
        store = client.app.state.ctx.store
        for index in range(80):
            store.create_card(CardCreate(title=f"Card {index:03d}"))

        durations = []
        sizes = []
        for _ in range(8):
            started = time.perf_counter()
            response = client.get("/", headers={"HX-Request": "true"})
            durations.append(time.perf_counter() - started)
            sizes.append(len(response.content))

        assert max(sizes) == min(sizes)
        assert max(durations) < 1.0
        assert sum(durations[-5:]) / 5 < 2 * (sum(durations[:5]) / 5) + 0.02
        assert "Loading actionable work…" in response.text
        assert "data-attention-card" not in response.text

"""Disposable page-context benchmark; never reads the installed PA data directory.

Run with uv run python scripts/benchmark_investigation_performance.py.
Set PYTHONPATH to a baseline checkout's src directory to compare the same fixture.
"""
from __future__ import annotations

import json
import statistics
import tempfile
import time
from pathlib import Path

from fastapi.testclient import TestClient

from pa.config import Settings
from pa.core.kernel import Kernel
from pa.domain.models import AgentSession, CardCreate, CardLane


def measure(call):
    call()
    samples = []
    for _ in range(7):
        start = time.perf_counter()
        call()
        samples.append((time.perf_counter() - start) * 1000)
    return {"median_ms": round(statistics.median(samples), 3),
            "max_ms": round(max(samples), 3)}


def main():
    with tempfile.TemporaryDirectory(prefix="pa-page-benchmark-") as root:
        settings = Settings(
            data_dir=Path(root) / "data", workspace_root=Path(root) / "workspaces",
            agent_enabled=False, telemetry_enabled=False, auth_required=False,
        )
        app = Kernel.boot(settings=settings).build_app()
        with TestClient(app) as client:
            store = app.state.ctx.store
            for index in range(300):
                store.create_card(CardCreate(
                    title=f"Historical card {index:04d}", body="fixture detail " * 1500,
                    lane=CardLane.DONE if index < 250 else CardLane.ACTIVE,
                    auto_enrich=False,
                ), via_log=False)
            for index in range(302):
                store.save_session(AgentSession(
                    id=f"session-{index}", agent_name="codex", status="closed",
                    purpose="automated_run" if index else "chat",
                    config_json={"fixture": "historical metadata " * 250},
                ))
            def get(path):
                response = client.get(path)
                response.raise_for_status()
                return response

            print(json.dumps({
                "fixture": {"cards": 300, "sessions": 302, "samples": 7},
                "home_partial": measure(lambda: get("/partials/home/sections")),
                "agent_page": measure(lambda: get("/agent")),
            }, indent=2))



if __name__ == "__main__":
    main()

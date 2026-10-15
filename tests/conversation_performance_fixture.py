"""Repeatable synthetic 301-card admission/read measurements, not provider SLAs.

Run: uv run python -m tests.conversation_performance_fixture
Uses temporary storage, ASGI transport and a 400ms synthetic dispatch only.
"""
import asyncio
import json
import math
import tempfile
from pathlib import Path
from time import perf_counter

import httpx
from fastapi import Response

from pa.modules.items import list_card_summaries_api
from tests.conversation_browser_fixture import build_conversation_fixture


async def measure(directory):
    app = build_conversation_fixture(directory)

    @app.get("/fixture/summaries")
    async def summaries(response: Response):
        from starlette.requests import Request
        request = Request({"type": "http", "app": app, "headers": []})
        return list_card_summaries_api(request, response, limit=30, offset=0, q="")

    reads, acks = [], []
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://fixture") as client:
        result = await client.post("/api/agent/sessions", json={"label": "performance"})
        sid = result.json()["session"]["id"]
        state = (await client.put(f"/api/agent/sessions/{sid}/issues", json={"revision": 0, "issues": [
            {"id": f"issue-{i}", "title": f"Selected work {i}"} for i in range(30)]})).json()
        for _ in range(5):
            await client.get("/fixture/summaries")
        for i in range(30):
            start = perf_counter()
            page = await client.get("/fixture/summaries")
            reads.append((perf_counter() - start) * 1000)
            assert len(page.json()["items"]) == 30
            state = (await client.get(f"/api/agent/sessions/{sid}/issues")).json()
            key = f"measurement-{i}"
            start = perf_counter()
            receipt = await client.post(f"/api/agent/sessions/{sid}/issues/start",
                headers={"Idempotency-Key": key}, json={"request_id": key, "revision": state["revision"], "issue_ids": [f"issue-{i}"]})
            acks.append((perf_counter() - start) * 1000)
            assert receipt.status_code == 202, receipt.text
    await app.state.ctx.services["async_runtime"].close(drain_timeout=30)
    p95 = lambda values: round(sorted(values)[math.ceil(.95 * len(values)) - 1], 2)
    return {"history_cards": 301, "samples": 30, "transport": "in-process ASGI", "provider": "synthetic dispatch,400ms",
            "warm_summary_read_p95_ms": p95(reads), "start_selected_ack_p95_ms": p95(acks),
            "provider_generation_measured": False, "browser_composer_measured": False,
            "dispatches": len(app.state.issue_dispatches)}


if __name__ == "__main__":
    with tempfile.TemporaryDirectory(prefix="pa-conversation-performance-") as directory:
        print(json.dumps(asyncio.run(measure(Path(directory))), indent=2))

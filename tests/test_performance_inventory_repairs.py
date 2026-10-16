"""Repairs measured after the complete retained-log performance inventory."""
import json
from types import SimpleNamespace
from unittest.mock import patch

import httpx
import pytest
from fastapi import FastAPI

from pa.core.async_runtime import AsyncRuntime
from pa.execution.dispatch import DispatchRecord, DispatchStore
from pa.modules.fleet import list_dispatches


def fixture_record(**kwargs):
    return DispatchRecord(mutation_id="fixture", authority_instance_id="local",
                          authority_url="http://fixture", target_instance_id="target", **kwargs)


def test_dispatch_list_preserves_protocol_gaps_controls_and_detached_rows(tmp_path):
    store = DispatchStore(tmp_path)
    proof = "fixture " * 25000
    item = store.put(fixture_record(
        state="running", session_id="session", progress_protocol_version=1,
        progress_highest_accepted_sequence=16,
        progress_accepted_ranges=[[i, i] for i in range(2, 17, 2)],
        progress_compacted_ranges=[[i, i] for i in range(2, 17, 2)],
        placement_decision={"proof": proof}, materialization_plan={"proof": proof},
        sync_evidence={"degraded_peers": [{"instance_id": str(i), "proof": proof} for i in range(8)]},
    ))
    try:
        expected = item.public_dict()
        with patch.object(store, "_snapshot", side_effect=AssertionError("copied stored proofs")):
            rows = store.public_statuses(target_instance_id="target")
            assert store.public_statuses(target_instance_id="other") == []
        row, = rows
        assert row["stale_session_recovery"]["eligible_candidate"]
        assert row["sync_evidence"]["degraded_peer_count"] == 8
        assert row["progress"]["sequence_gap"]
        for field in ("missing_ranges", "compacted_ranges"):
            assert len(row["progress"][field]) == 8
            assert row["progress"][field] == expected["progress"][field]
        assert len(json.dumps(rows)) < 5000
        row["progress"]["compacted_ranges"][0][0] = 999
        assert store.get(item.dispatch_id).progress_compacted_ranges[0][0] == 2
        assert store.get(item.dispatch_id).placement_decision["proof"] == proof
    finally:
        store.close()


@pytest.mark.asyncio
async def test_dispatch_list_http_defaults_compact_and_keeps_explicit_full_details(tmp_path):
    store = DispatchStore(tmp_path)
    runtime = AsyncRuntime()
    item = store.put(fixture_record(state="blocked", placement_decision={"proof": "fixture " * 25000}))
    services = {"dispatch_store": store, "async_runtime": runtime}
    app = FastAPI()
    app.state.ctx = SimpleNamespace(services=services, require_service=services.__getitem__)
    app.add_api_route("/jobs", list_dispatches)
    try:
        with patch("pa.modules.fleet.require_user"), patch.object(
            store, "list", side_effect=AssertionError("default list copied full records")
        ):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://fixture") as client:
                status = await client.get("/jobs")
                assert status.status_code == 200
                assert status.json()[0]["view"] == "status"
                assert (await client.get("/jobs?view=invalid")).status_code == 422
        with patch("pa.modules.fleet.require_user"):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://fixture") as client:
                full = await client.get("/jobs?view=full")
        assert full.json()[0]["placement_decision"] == item.placement_decision
        assert len(full.content) > 20 * len(status.content)
        assert full.json()[0]["dispatch_id"] == status.json()[0]["dispatch_id"]
        metrics = runtime.snapshot()["work"]["metrics"]
        assert metrics["http.dispatch_list.rows_returned"]["count"] == 2
        assert metrics["http.dispatch_list.response_bytes"]["count"] == 2
    finally:
        await runtime.close()
        store.close()

"""Automatic discovery stays bounded and never weakens explicit selection."""

import asyncio
from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest

from pa.config import Settings
from pa.execution.selection import SelectionError
from pa.execution.selection_service import SelectionService
from tests.test_execution_selection import candidate, resolve, NOW
from tests.test_execution_selection_api import selection_app
from tests.test_execution_selection_runtime import wire_app, required
from tests.selection_browser_app import FixtureProvider


def test_catalog_and_preview_refresh_expired_evidence(selection_app):
    client, app, service = selection_app
    fresh = candidate(observed_at=datetime.now(UTC))
    with (
        patch.object(
            service.store,
            "catalog",
            return_value=([], datetime.now(UTC) - timedelta(minutes=6)),
        ),
        patch(
            "pa.acp.providers.resolve.list_provider_summaries_bounded", return_value=[]
        ) as discovery,
    ):
        response = client.post("/api/execution/preview", json={})
        assert response.status_code == 409
        assert (
            response.json()["detail"]["execution_selection"]["recovery"]["action"]
            == "refresh_catalog"
        )
        assert discovery.call_count == 1
    service.store.save_catalog("local", [fresh.model_dump(mode="json")])
    assert client.post("/api/execution/preview", json={}).status_code == 200


def test_concurrent_cold_catalog_requests_coalesce(tmp_path):
    service = SelectionService(Settings(data_dir=tmp_path, instance_id="test"), None)

    async def run():
        with patch(
            "pa.acp.providers.resolve.list_provider_summaries_bounded", return_value=[]
        ) as discovery:
            results = await asyncio.gather(
                *(service.local_catalog(refresh=True) for _ in range(8))
            )
            assert results == [[]] * 8
            assert discovery.call_count == 1

    asyncio.run(run())


def test_composite_write_does_not_renew_old_candidate(tmp_path):
    service = SelectionService(Settings(data_dir=tmp_path, instance_id="test"), None)
    service.store.save_catalog(
        "test",
        [
            candidate(observed_at=datetime.now(UTC) - timedelta(minutes=6)).model_dump(
                mode="json"
            )
        ],
    )
    assert asyncio.run(service.local_catalog())[0].freshness == "stale"
    with pytest.raises(SelectionError) as exc:
        resolve([candidate(observed_at=NOW - timedelta(seconds=301))])
    assert "catalog_stale" in exc.value.receipt["alternatives"][0]["rejections"]


def test_cold_adapter_without_status_models_discovers_and_starts(wire_app):
    client, app = wire_app
    original = FixtureProvider.status

    def cold_status(self, data_dir):
        status = original(self, data_dir)
        return status.model_copy(update={"meta": {}})

    with patch.object(FixtureProvider, "status", cold_status):
        catalog = client.get("/api/execution/catalog")
        assert catalog.status_code == 200, catalog.text
        rows = catalog.json()["candidates"]
        assert any(
            c["model"] == "fixture-balanced"
            and c["catalog_source"] == "acp_discovery_session"
            for c in rows
        )
        manager = app.state.ctx.services["instance_agent"]
        assert not manager.list_runtimes()
        prefs = {
            "harness": required("codex"),
            "model": required("fixture-balanced"),
            "reasoning": required("xhigh"),
        }
        preview = client.post(
            "/api/execution/preview", json={"execution_preferences": prefs}
        )
        assert preview.status_code == 200, preview.text
        created = client.post(
            "/api/agent/sessions",
            json={
                "label": "cold-catalog-test",
                "mode_id": "agent-full-access",
                "execution_preferences": prefs,
            },
        )
        assert created.status_code == 200, created.text
        assert (
            created.json()["execution_selection"]["decision"]["selected"]["model"]
            == "fixture-balanced"
        )
        prefs["model"] = required("absent-explicit-model")
        rejected = client.post(
            "/api/execution/preview", json={"execution_preferences": prefs}
        )
        assert rejected.status_code == 409
        assert (
            "required_model_incompatible_or_unknown"
            in rejected.json()["detail"]["message"]
        )


def test_catalog_probe_timeout_is_bounded_and_redacted():
    from pa.acp.providers.probe import probe_acp_catalog
    from pa.acp.providers.base import AgentProviderSpec

    async def slow(*args, **kwargs):
        await asyncio.sleep(30)

    async def run():
        spec = AgentProviderSpec(id="fixture", display_name="fixture", command="unused")
        with patch("pa.acp.providers.probe._probe_async", side_effect=slow):
            result = await probe_acp_catalog(spec, timeout=0.01)
        assert result == {"ok": False, "provider_id": "fixture"}

    asyncio.run(run())


def test_failed_cold_discovery_does_not_invent_explicit_model(tmp_path):
    from pa.execution.selection_catalog import discover_missing_catalogs
    from pa.execution.selection_service import status_candidates
    from pa.execution.selection import ExecutionPreferences, Preference

    status = {"id": "codex", "available": True, "auth_state": "authenticated"}
    with patch("pa.acp.providers.probe.probe_acp_catalog", return_value={"ok": False}):
        statuses = asyncio.run(
            discover_missing_catalogs([status], Settings(data_dir=tmp_path))
        )
    rows = status_candidates("local", statuses)
    assert rows[0].model is None
    with pytest.raises(SelectionError) as exc:
        resolve(
            rows,
            prefs=ExecutionPreferences(
                model=Preference(intent="required", value="gpt-6-astra")
            ),
        )
    assert (
        "required_model_incompatible_or_unknown"
        in exc.value.receipt["recovery"]["rejections"]
    )


def test_explicit_refresh_retries_failed_auto_discovery_immediately(selection_app):
    client, app, service = selection_app
    with (
        patch.object(service.store, "catalog", return_value=([], None)),
        patch(
            "pa.acp.providers.resolve.list_provider_summaries_bounded", return_value=[]
        ) as discovery,
    ):
        assert client.get("/api/execution/catalog").json()["candidates"] == []
        assert discovery.call_count == 1
    with patch(
        "pa.acp.providers.resolve.list_provider_summaries_bounded",
        return_value=[
            {
                "id": "codex",
                "available": True,
                "auth_state": "authenticated",
                "models": ["gpt-6-astra"],
            }
        ],
    ) as discovery:
        response = client.post("/api/execution/catalog/refresh", json={})
        assert response.json()["refresh"]["state"] == "refreshed"
        assert response.json()["candidates"][0]["model"] == "gpt-6-astra"
        second = client.post("/api/execution/catalog/refresh", json={})
        assert second.json()["refresh"]["state"] == "rate_limited"
        assert discovery.call_count == 1


def test_discovery_uses_admission_custom_command_and_args(tmp_path):
    from pa.execution.selection_catalog import discover_missing_catalogs
    from pa.acp.providers.codex import CodexProvider
    from pa.acp.providers.base import AgentProviderSpec

    settings = Settings(
        data_dir=tmp_path, agent_command="custom-codex", agent_args=["custom-acp"]
    )
    status = {"id": "codex", "available": True, "auth_state": "authenticated"}
    spec = AgentProviderSpec(
        id="codex", display_name="custom", command="custom-codex", args=["custom-acp"]
    )
    with (
        patch.object(CodexProvider, "resolve_spawn", return_value=spec) as spawn,
        patch(
            "pa.acp.providers.probe.probe_acp_catalog",
            return_value={"ok": True, "models": ["custom-model"]},
        ) as probe,
    ):
        found = asyncio.run(discover_missing_catalogs([status], settings))
        assert spawn.call_args.kwargs["command_override"] == "custom-codex"
        assert spawn.call_args.kwargs["args_override"] == ["custom-acp"]
        assert probe.call_args.args[0] == spec
        assert found[0]["execution_catalogs"][0]["models"] == ["custom-model"]


@pytest.mark.parametrize("cancel_during_cleanup", [False, True])
def test_stalled_catalog_probe_reaps_child_ignoring_eof_and_sigterm(
    tmp_path, cancel_during_cleanup
):
    import os
    import sys
    import time
    from pa.acp.providers.base import AgentProviderSpec
    from pa.acp.providers.probe import probe_acp_catalog

    marker = tmp_path / "child.pid"
    program = r"""
import json, os, pathlib, signal, sys, time
signal.signal(signal.SIGTERM, signal.SIG_IGN)
pathlib.Path(sys.argv[1]).write_text(str(os.getpid()))
for line in sys.stdin:
    request = json.loads(line)
    if request.get('method') == 'initialize':
        print(json.dumps({'jsonrpc': '2.0', 'id': request['id'], 'result': {'protocolVersion': 1, 'agentCapabilities': {}, 'authMethods': []}}), flush=True)
    elif request.get('method') == 'session/new':
        while True: time.sleep(1)
while True: time.sleep(1)
"""
    spec = AgentProviderSpec(
        id="fixture",
        display_name="fixture",
        command=sys.executable,
        args=["-c", program, str(marker)],
    )

    async def run():
        task = asyncio.create_task(probe_acp_catalog(spec, timeout=0.3))
        if cancel_during_cleanup:
            await asyncio.sleep(0.4)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            assert (await task)["ok"] is False

    started = time.monotonic()
    asyncio.run(run())
    assert time.monotonic() - started < 4
    assert marker.exists(), "test must actually launch the stalled provider"
    with pytest.raises(ProcessLookupError):
        os.kill(int(marker.read_text()), 0)

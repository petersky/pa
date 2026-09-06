"""Real ACP wire fixtures exercise admission/confirmation, not an LLM's ability."""

from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient

from pa.acp.providers import registry
from pa.config import Settings, reset_settings
from pa.core.kernel import Kernel
from pa.domain.store import reset_store
from pa.instance.agent_session import reset_instance_agent
from tests.selection_browser_app import FixtureProvider


@pytest.fixture
def wire_app(tmp_path):
    reset_settings()
    reset_store()
    reset_instance_agent()
    with (
        patch.dict(
            registry._PROVIDERS,
            {h: FixtureProvider(h) for h in ("codex", "cursor", "openinterpreter")},
            clear=True,
        ),
        patch("pa.acp.client.pa_mcp_servers", return_value=[]),
    ):
        settings = Settings(
            data_dir=tmp_path / "data",
            workspace_root=tmp_path / "workspaces",
            instance_id="wire-fixture",
            peers=[],
            agent_provider="codex",
            agent_enabled=True,
            card_summary_api_key="",
            card_summary_auth_source="dedicated",
        )
        app = Kernel.boot(settings=settings).build_app()
        with TestClient(app) as client:
            client.get("/")
            client.headers["X-CSRF-Token"] = client.cookies.get("pa_csrf", "")
            yield client, app
    reset_instance_agent()
    reset_store()
    reset_settings()


def required(value):
    return {"intent": "required", "value": value}


def test_recovery_preserves_native_identity_and_receipt_after_defaults_change(wire_app):
    from pa.execution.selection import ExecutionPreferences, SelectionPolicy
    from pa.execution.selection_service import service_for

    client, app = wire_app
    created = client.post(
        "/api/agent/sessions",
        json={
            "label": "recover-selection",
            "execution_preferences": {
                "harness": required("codex"),
                "model": required("fixture-balanced"),
                "reasoning": required("xhigh"),
            },
        },
    )
    assert created.status_code == 200, created.text
    original = created.json()
    sid = original["session"]["id"]
    manager = app.state.ctx.services["instance_agent"]
    service = service_for(app.state.ctx)
    service.store.save_policy(
        "default",
        SelectionPolicy(
            defaults=ExecutionPreferences.model_validate(
                {
                    "harness": required("cursor"),
                    "model": required("fixture-grok"),
                }
            )
        ),
        expected_revision=1,
    )

    async def reconnect():
        await manager.get(sid).close(
            reason="selection-recovery-test", reconcile_workspace=False
        )
        return (await manager.recover_session(sid)).snapshot()

    recovered = client.portal.call(reconnect)
    assert recovered["session"]["id"] == sid
    assert (
        recovered["session"]["external_session_id"]
        == original["session"]["external_session_id"]
    )
    assert (
        recovered["execution_selection"]["decision"]["decision_id"]
        == original["execution_selection"]["decision"]["decision_id"]
    )
    assert (
        recovered["execution_selection"]["provider_confirmation"]["effective"][
            "reasoning"
        ]
        == "xhigh"
    )
    assert recovered["session"]["agent_name"] == "codex"


def test_unsolicited_native_drift_blocks_and_durably_retains_original_prompt(wire_app):
    client, app = wire_app
    created = client.post(
        "/api/agent/sessions",
        json={
            "label": "drift-fixture",
            "execution_preferences": {
                "harness": required("codex"),
                "model": required("fixture-balanced"),
                "reasoning": required("xhigh"),
            },
        },
    )
    assert created.status_code == 200, created.text
    manager = app.state.ctx.services["instance_agent"]
    runtime = manager.get(created.json()["session"]["id"])

    async def run():
        config = runtime.session.config_json
        next(o for o in config["options"] if o["id"] == "reasoning_effort")[
            "currentValue"
        ] = "low"
        assert (
            await runtime.prompt(
                "unchanged prompt payload", prompt_id="same-original-prompt", wait=True
            )
            == "blocked"
        )
        assert runtime._queue[0].id == "same-original-prompt"
        durable = manager.store.get_session(runtime.session.id)
        assert (
            durable.config_json["durable_runtime"]["queued_prompts"][0]["message"]
            == "unchanged prompt payload"
        )
        assert (
            durable.config_json["execution_selection_block"]["code"]
            == "selection_native_drift"
        )
        assert (
            runtime.snapshot()["execution_selection"]["blocked"]["prompt_id"]
            == "same-original-prompt"
        )

    client.portal.call(run)


def test_automatic_native_defaults_bind_after_confirmation_and_cannot_drift(wire_app):
    client, app = wire_app
    created = client.post(
        "/api/agent/sessions", json={"label": "native-default-binding"}
    )
    assert created.status_code == 200, created.text
    snap = created.json()
    assert snap["execution_selection"]["requested"]["reasoning"] is None
    binding = snap["execution_selection"]["native_default_binding"]
    assert binding["reasoning"] == "low"
    manager = app.state.ctx.services["instance_agent"]
    runtime = manager.get(snap["session"]["id"])

    async def run():
        next(
            o
            for o in runtime.session.config_json["options"]
            if o["id"] == "reasoning_effort"
        )["currentValue"] = "xhigh"
        assert (
            await runtime.prompt(
                "preserve automatic native default",
                prompt_id="auto-bound-prompt",
                wait=True,
            )
            == "blocked"
        )
        assert runtime._queue[0].id == "auto-bound-prompt"

    client.portal.call(run)


def test_real_native_confirmation_settings_lineage_and_linked_boundary(wire_app):
    client, app = wire_app
    prefs = {
        "harness": required("codex"),
        "model": required("fixture-balanced"),
        "reasoning": required("xhigh"),
    }
    created = client.post(
        "/api/agent/sessions",
        json={"label": "wire-source", "execution_preferences": prefs},
    )
    assert created.status_code == 200, created.text
    snap = created.json()
    sid = snap["session"]["id"]
    view = snap["execution_selection"]
    assert view["requested"]["model"] == "fixture-balanced"
    assert view["provider_confirmation"]["effective"]["reasoning"] == "xhigh"
    original_id = view["decision"]["decision_id"]
    changed = client.post(
        f"/api/execution/sessions/{sid}/settings",
        json={
            "execution_preferences": {"reasoning": required("low")},
            "idempotency_key": "lower-native",
            "expected_version": snap["session"]["updated_at"],
            "defer": False,
        },
    )
    assert changed.status_code == 200, changed.text
    assert changed.json()["state"] == "applied"
    current = client.get(f"/api/agent/sessions/{sid}").json()
    assert (
        current["execution_selection"]["provider_confirmation"]["effective"][
            "reasoning"
        ]
        == "low"
    )
    config = current["session"]["config_json"]
    assert config["execution_selection_origin"]["decision_id"] == original_id
    from pa.execution.selection import ExecutionPreferences, validate_attempt_request

    validate_attempt_request(config, ExecutionPreferences.model_validate(prefs))
    assert config["execution_selection"]["selected"]["reasoning"] == "low"
    manager = app.state.ctx.services["instance_agent"]

    async def recover_changed_settings():
        return (await manager.recover_session(sid)).snapshot()

    client.portal.call(manager.get(sid).close)
    historical = client.get(f"/api/agent/history/{sid}")
    assert historical.status_code == 200, historical.text
    assert (
        historical.json()["execution_selection"]["provider_confirmation"]["effective"][
            "reasoning"
        ]
        == "low"
    )
    resumed = client.portal.call(recover_changed_settings)
    assert (
        resumed["session"]["external_session_id"]
        == snap["session"]["external_session_id"]
    )
    assert (
        resumed["execution_selection"]["provider_confirmation"]["effective"][
            "reasoning"
        ]
        == "low"
    )
    assert (
        resumed["execution_selection"]["decision"]["decision_id"]
        == config["execution_selection"]["decision_id"]
    )
    body = {
        "execution_preferences": {
            "harness": required("cursor"),
            "model": required("fixture-grok"),
        },
        "idempotency_key": "new-native-context",
    }
    boundary = client.post(f"/api/execution/sessions/{sid}/boundary", json=body)
    assert boundary.status_code == 200, boundary.text
    target = boundary.json()["session"]
    assert target["id"] != sid
    assert target["agent_name"] == "cursor"
    assert target["cwd"] != snap["session"]["cwd"]
    assert target["initiating_workflow"]["context_boundary"]["source_session_id"] == sid
    source = app.state.ctx.store.get_session(sid)
    assert source.status == "closed"
    assert (
        source.config_json["execution_context_boundary"]["target_session_id"]
        == target["id"]
    )
    replay = client.post(f"/api/execution/sessions/{sid}/boundary", json=body)
    assert replay.status_code == 200, replay.text
    assert replay.json()["session"]["id"] == target["id"]
    competing = client.post(
        f"/api/execution/sessions/{sid}/boundary",
        json={**body, "idempotency_key": "competing"},
    )
    assert competing.status_code == 409
    manager = app.state.ctx.services["instance_agent"]
    assert manager.get(sid)._closed
    with pytest.raises(ValueError, match="linked attempt"):
        manager.get(sid).enqueue("must not run on superseded source")


def test_linked_attempt_rejects_pending_queue_without_closing_source(wire_app):
    client, app = wire_app
    created = client.post(
        "/api/agent/sessions",
        json={
            "label": "queued-source",
            "execution_preferences": {"harness": required("codex")},
        },
    )
    assert created.status_code == 200, created.text
    sid = created.json()["session"]["id"]
    runtime = app.state.ctx.services["instance_agent"].get(sid)
    runtime._queue_paused = True
    runtime.enqueue("retained original prompt", _defer_drain=True)
    response = client.post(
        f"/api/execution/sessions/{sid}/boundary",
        json={
            "execution_preferences": {"harness": required("cursor")},
            "idempotency_key": "busy-boundary",
        },
    )
    assert response.status_code == 409, response.text
    assert response.json()["detail"]["code"] == "context_boundary_busy"
    assert not runtime._closed
    assert runtime._queue[0].message == "retained original prompt"


def test_remote_authority_receipt_reaches_real_target_without_reselection(wire_app):
    from pa.execution.selection import (
        ExecutionPreferences,
        SelectionConstraints,
        SelectionError,
    )

    client, app = wire_app
    manager = app.state.ctx.services["instance_agent"]

    async def run():
        from pa.execution.selection_service import service_for

        service = service_for(app.state.ctx)
        candidates = await service.local_catalog(refresh=True)
        receipt = service.resolve(
            candidates=candidates,
            principal="user:local",
            realm="default",
            surface="execution",
            overrides=ExecutionPreferences.model_validate(
                {
                    "harness": required("codex"),
                    "model": required("fixture-balanced"),
                    "reasoning": required("xhigh"),
                }
            ),
        )
        receipt["context"]["policy_instance_id"] = "remote-authority-fixture"
        from pa.execution.selection import digest

        receipt["decision_id"] = digest(
            {k: v for k, v in receipt.items() if k != "decision_id"}
        )
        target = await manager.create_session(
            label="materialized-target",
            principal_id="user:local",
            realm_id="default",
            execution_selection=receipt,
        )
        assert target.session.config_json["execution_selection"] == receipt
        assert (
            target.snapshot()["execution_selection"]["provider_confirmation"][
                "effective"
            ]["reasoning"]
            == "xhigh"
        )
        policy = service.store.policy("default")
        policy.constraints = SelectionConstraints(harnesses=["cursor"])
        service.store.save_policy("default", policy, policy.revision)
        with pytest.raises(SelectionError):
            await manager.create_session(
                label="policy-changed-target",
                principal_id="user:local",
                realm_id="default",
                execution_selection=receipt,
            )
        assert not any(
            rt.session.label == "policy-changed-target"
            for rt in manager.list_runtimes()
        )

    client.portal.call(run)


def test_lost_context_continuation_uses_owned_fenced_idempotent_boundary(wire_app):
    client, app = wire_app
    result = client.post(
        "/api/agent/sessions",
        json={
            "label": "lost-context-source",
            "execution_preferences": {"harness": required("codex")},
        },
    )
    assert result.status_code == 200, result.text
    sid = result.json()["session"]["id"]
    manager = app.state.ctx.services["instance_agent"]
    source = manager.get(sid)
    source.session.recovery_json = {"context_lost": True}
    app.state.ctx.store.save_session(source.session)
    body = {
        "idempotency_key": "continue-once",
        "execution_preferences": {"harness": required("cursor")},
    }
    first = client.post(f"/api/agent/sessions/{sid}/continue", json=body)
    assert first.status_code == 200, first.text
    again = client.post(f"/api/agent/sessions/{sid}/continue", json=body)
    assert again.status_code == 200, again.text
    assert first.json()["session"]["id"] == again.json()["session"]["id"]
    target = manager.get(first.json()["session"]["id"])
    assert target.session.config_json["execution_context_continuation_queued"]
    assert source._closed

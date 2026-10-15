"""Actual HTTP admission and ACP confirmation agree on new-agent purpose."""
import pytest

from tests.test_execution_selection_runtime import wire_app, required


@pytest.mark.parametrize("purpose,legacy,expected", [
    ("chat", None, "agent-full-access"),
    ("chat", "None", "agent-full-access"),
    ("automated_run", "", "workspace-write"),
    ("one_shot_job", None, "workspace-write"),
])
def test_new_agent_defaults_by_purpose_and_confirms_native_mode(wire_app, purpose, legacy, expected):
    client, _ = wire_app
    # A saved chat default must not become the default of an automation agent
    # merely because it was started through the same HTTP/UI entry point.
    assert client.put("/api/agent/preferences", json={"agent_surfaces": {
        "chat.default": {"mode_id": "agent-full-access"},
    }}).status_code == 200
    body = {"label": "purpose-default", "purpose": purpose, "mode_id": legacy}
    created = client.post("/api/agent/sessions", json=body)
    assert created.status_code == 200, created.text
    snap = created.json()
    assert snap["session"]["mode_id"] == expected
    assert snap["modes"]["currentModeId"] == expected
    assert snap["execution_selection"]["provider_confirmation"]["effective"]["mode_id"] == expected


def test_explicit_worker_choices_and_existing_mode_survive_recovery(wire_app):
    client, app = wire_app
    created = client.post("/api/agent/sessions", json={
        "label": "intentional-worker", "purpose": "automated_run", "mode_id": "read-only",
        "execution_preferences": {"harness": required("codex"), "model": required("fixture-balanced"), "reasoning": required("xhigh")},
    })
    assert created.status_code == 200, created.text
    sid = created.json()["session"]["id"]
    assert client.put("/api/agent/preferences", json={"agent_surfaces": {"execution": {"mode_id": "agent-full-access"}}}).status_code == 200
    manager = app.state.ctx.services["instance_agent"]
    async def recover():
        await manager.get(sid).close(reason="permission-recovery-fixture", reconcile_workspace=False)
        return (await manager.recover_session(sid)).snapshot()
    restored = client.portal.call(recover)
    assert restored["session"]["mode_id"] == "read-only"
    assert restored["session"]["model_id"] == "fixture-balanced"
    assert restored["execution_selection"]["provider_confirmation"]["effective"]["reasoning"] == "xhigh"


def test_linked_new_worker_does_not_inherit_source_full_access(wire_app):
    client, _ = wire_app
    source = client.post("/api/agent/sessions", json={"label": "source-chat"}).json()["session"]
    created = client.post("/api/agent/sessions", json={
        "context_source_session_id": source["id"], "label": "linked-worker",
        "purpose": "automated_run",
    })
    assert created.status_code == 200, created.text
    assert source["mode_id"] == "agent-full-access"
    assert created.json()["session"]["mode_id"] == "workspace-write"


def test_linked_execution_api_keeps_worker_purpose(wire_app):
    client, _ = wire_app
    source = client.post("/api/agent/sessions", json={"label": "background-source", "purpose": "automated_run"}).json()["session"]
    created = client.post(f"/api/execution/sessions/{source['id']}/boundary", json={"idempotency_key": "linked-background", "execution_preferences": {}})
    assert created.status_code == 200, created.text
    target = created.json()["session"]
    assert target["purpose"] == "automated_run" and target["mode_id"] == "workspace-write"


def test_workspace_write_permission_reaches_pa_auto_approve_over_actual_acp(wire_app):
    client, app = wire_app
    assert client.put("/api/agent/preferences", json={"agent_auto_approve_permissions": True}).status_code == 200
    created = client.post("/api/agent/sessions", json={"label": "permission-noop", "purpose": "one_shot_job"})
    assert created.status_code == 200, created.text
    sid = created.json()["session"]["id"]
    manager = app.state.ctx.services["instance_agent"]
    runtime = manager.get(sid)
    assert runtime.session.mode_id == "workspace-write"
    async def prompt():
        result = await runtime.prompt("fixture-acp-permission-noop", prompt_id="noop-exact-id", wait=True)
        await runtime._drain_transcripts()
        return result
    client.portal.call(prompt)
    events = manager.store.list_transcript_events(sid)
    resolved = [e for e in events if e.event_type == "permission_resolved"]
    assert len(resolved) == 1
    assert resolved[0].payload["auto"] is True
    assert resolved[0].payload["response"]["outcome"]["optionId"] == "allow-noop"
    assert client.get("/api/agent/preferences").json()["agent_auto_approve_permissions"] is True

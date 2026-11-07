"""Resume admission retains authority custody without changing native settings."""

from copy import deepcopy
from unittest.mock import patch
from uuid import uuid4

import pytest

from pa.domain.models import CardCreate
from pa.execution.dispatch import DispatchRecord
from pa.execution.selection import SelectionError, digest, receipt_in_lineage
from pa.execution.selection_service import service_for
from tests.test_execution_selection_runtime import wire_app  # noqa: F401


def setup_resume(wire_app):
    client, app = wire_app
    card = app.state.ctx.store.create_card(CardCreate(title="private resume contract", auto_enrich=False))
    response = client.post("/api/agent/sessions", json={
        "label": "original-resume-owner", "card_id": card.id,
        "execution_preferences": {
            "harness": {"intent": "required", "value": "codex"},
            "model": {"intent": "required", "value": "fixture-balanced"},
            "reasoning": {"intent": "required", "value": "xhigh"},
        },
    })
    assert response.status_code == 200, response.text
    manager = app.state.ctx.services["instance_agent"]
    runtime = manager.get(response.json()["session"]["id"])
    receipt = deepcopy(runtime.session.config_json["execution_selection"])
    receipt["explanation"] += " Explicit resume admission of the same native tuple."
    receipt["decision_id"] = digest({k: v for k, v in receipt.items() if k != "decision_id"})
    record = DispatchRecord(
        dispatch_id=str(uuid4()), mutation_id=str(uuid4()),
        card_id=card.id, authority_instance_id=app.state.ctx.settings.instance_id,
        authority_url="http://private-fixture", target_instance_id=app.state.ctx.settings.instance_id,
        principal_id="user:local", session_id=runtime.session.id,
        resume_requested=True, resume_session_id=runtime.session.id,
        request_payload={"execution_selection": receipt}, state="provisioning",
    )
    app.state.ctx.services["dispatch_store"].put(record)
    return client, app, manager, runtime, receipt, record


@pytest.mark.parametrize("restore", [False, True])
def test_resume_retains_new_authority_receipt_and_original_native_identity(wire_app, restore):
    client, app, manager, runtime, receipt, record = setup_resume(wire_app)
    original = deepcopy(runtime.session.config_json)
    external = runtime.session.external_session_id
    if restore:
        # Simulate a process losing only its transport/runtime cache. Durable
        # live-session metadata and native identity survive, as after a crash.
        client.portal.call(runtime.connection.disconnect)
        manager._runtimes.pop(runtime.session.id)
    # This contract isolates receipt custody after transport restoration. The
    # existing workspace has already been admitted; workspace rebinding remains
    # covered by the separate live-cwd/lease rejection contracts.
    from contextlib import nullcontext
    from unittest.mock import AsyncMock
    workspace = patch.object(manager, "_prepare_workspace", AsyncMock(return_value={})) if restore else nullcontext()
    with workspace:
        response = client.post("/api/agent/sessions", json={
            "dispatch_id": record.dispatch_id, "card_id": record.card_id,
            "resume": True, "resume_session_id": record.session_id,
        })
    assert response.status_code == 200, response.text
    restored = manager.get(record.session_id)
    saved = manager.store.get_session(record.session_id)
    assert saved.id == runtime.session.id
    assert saved.external_session_id == external
    assert saved.config_json["execution_selection"] == original["execution_selection"]
    assert saved.config_json["execution_native_binding"] == original["execution_native_binding"]
    assert receipt_in_lineage(saved.config_json, receipt["decision_id"])
    assert len(saved.config_json["execution_selection_admissions"]) == 1
    assert restored.snapshot()["execution_selection"]["provider_confirmation"]["state"] == "ready"
    assert not restored._queue
    service = service_for(app.state.ctx)
    assert service.store.decision(receipt["decision_id"], saved.realm_id, "user:local") == receipt
    # Repeated admission does not create new decisions, aliases or settings.
    with patch.object(service.store, "save_decision", wraps=service.store.save_decision) as write:
        repeated = service.retain_resume_receipt(saved, receipt, authority_instance_id=record.authority_instance_id)
        write.assert_not_called()
    assert repeated == saved.config_json


@pytest.mark.parametrize("failure", ["model", "reasoning", "option", "pending", "scope", "principal", "realm", "instance", "harness", "authority", "connection", "integrity", "active-drift"])
def test_resume_receipt_rejects_unconfirmed_or_wrong_owner_without_mutation(wire_app, failure):
    _, app, _, runtime, receipt, record = setup_resume(wire_app)
    if failure == "model": receipt["selected"]["model"] = "fixture-deep"
    elif failure == "reasoning": receipt["selected"]["reasoning"] = "low"
    elif failure == "option": receipt["selected"]["options"] = {"unsupported-option": "on"}
    elif failure == "pending": runtime.session.config_json["configuration"]["state"] = "pending"
    elif failure == "scope": receipt["context"]["card_id"] = "other-card"
    elif failure == "principal": receipt["context"]["principal"] = "user:other"
    elif failure == "realm": receipt["context"]["realm"] = "other-realm"
    elif failure == "instance": receipt["selected"]["instance_id"] = "other-instance"
    elif failure == "harness": receipt["selected"]["harness"] = "cursor"
    elif failure == "authority": receipt["context"]["policy_instance_id"] = "other-authority"
    elif failure == "connection": receipt["selected"]["connection"] = "other-account"
    elif failure == "active-drift":
        receipt["selected"]["reasoning"] = "low"
        next(o for o in runtime.session.config_json["options"] if o["id"] == "reasoning_effort")["currentValue"] = "low"
    if failure != "integrity":
        receipt["decision_id"] = digest({k: v for k, v in receipt.items() if k != "decision_id"})
    else: receipt["explanation"] += " tampered"
    original = deepcopy(runtime.session.config_json)
    service = service_for(app.state.ctx)
    with patch.object(service.store, "save_decision", wraps=service.store.save_decision) as write:
        with pytest.raises(SelectionError):
            service.retain_resume_receipt(runtime.session, receipt, authority_instance_id=record.authority_instance_id)
        write.assert_not_called()
    assert runtime.session.config_json == original
    assert not runtime._queue


@pytest.mark.parametrize("named_connection", [False, True])
def test_legacy_resume_requires_provable_default_route_and_native_values(wire_app, named_connection):
    _, app, _, runtime, receipt, record = setup_resume(wire_app)
    runtime.session.config_json.pop("execution_selection")
    receipt["selected"].update(connection="named" if named_connection else "default",
                               connection_revision=None, model_provider=None,
                               native_model_provider=None)
    receipt["decision_id"] = digest({k: v for k, v in receipt.items() if k != "decision_id"})
    service = service_for(app.state.ctx)
    if named_connection:
        with pytest.raises(SelectionError, match="legacy session cannot prove"):
            service.retain_resume_receipt(runtime.session, receipt, authority_instance_id=record.authority_instance_id)
        assert "execution_selection" not in runtime.session.config_json
    else:
        config = service.retain_resume_receipt(runtime.session, receipt, authority_instance_id=record.authority_instance_id)
        assert config["execution_selection"] == receipt
        assert config["configuration"] == runtime.session.config_json["configuration"]


def test_concurrent_session_change_prevents_stale_receipt_binding(wire_app):
    from pa.modules.agent_chat import _retain_dispatch_resume_selection

    client, _, manager, runtime, _, record = setup_resume(wire_app)
    service = manager._selection_service
    original = service.retain_resume_receipt

    def changed(*args, **kwargs):
        config = original(*args, **kwargs)
        runtime.session.config_json = {**runtime.session.config_json, "concurrent_change": True}
        return config

    with patch.object(service, "retain_resume_receipt", side_effect=changed):
        with pytest.raises(SelectionError, match="session changed during receipt"):
            client.portal.call(_retain_dispatch_resume_selection, manager, runtime, record)
    assert runtime.session.config_json["concurrent_change"]
    assert "execution_selection_admissions" not in runtime.session.config_json

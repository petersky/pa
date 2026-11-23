from fastapi.testclient import TestClient

from pa.domain.models import AgentSession
from pa.domain.notifications import NotificationType
from tests.test_notifications import _create, _kernel, _reset_singletons


def test_recovery_actions_require_current_version_and_never_retry_source(tmp_path):
    kernel = _kernel(tmp_path)
    source = kernel.ctx.store.save_session(AgentSession(id="old", agent_name="codex"))
    notice = _create(kernel.ctx.require_service("notifications"),
        type=NotificationType.SERVICE_HEALTH, interaction=None,
        session_id=source.id, capability="session-attention:v1")
    with TestClient(kernel.build_app()) as client:
        data = client.get(f"/api/notifications/{notice.id}").json()
        assert data["actions"][0]["id"] == "open-current"
        endpoint = f"/api/notifications/{notice.id}/recovery-action"
        headers = {"X-CSRF-Token": client.cookies.get("pa_csrf")}
        stale = client.post(endpoint, headers=headers, json={"idempotency_key":"stale", "expected_version":notice.version + 1, "action_id":"open-current"})
        assert stale.status_code == 409
        opened = client.post(endpoint, headers=headers, json={"idempotency_key":"open", "expected_version":notice.version, "action_id":"open-current"})
        assert opened.status_code == 200
        assert opened.json()["destination"] == "/agent?session=old"
        unsafe = client.post(endpoint, headers=headers, json={"idempotency_key":"close", "expected_version":notice.version, "action_id":"close-obsolete"})
        assert unsafe.status_code == 409
        assert kernel.ctx.store.get_session(source.id).status != "closed"


def test_recovery_action_rejects_fleet_credential():
    import pytest
    from types import SimpleNamespace
    from fastapi import HTTPException
    from pa.modules.notifications import _require_operator_credentials
    request = SimpleNamespace(state=SimpleNamespace(instance_authenticated=True))
    with pytest.raises(HTTPException) as exc:
        _require_operator_credentials(request)
    assert exc.value.status_code == 403


def test_open_current_revalidates_replaced_dispatch_owner(tmp_path, request):
    from pa.execution.dispatch import DispatchRecord, DispatchStore
    kernel = _kernel(tmp_path)
    store = kernel.ctx.store
    source = store.save_session(AgentSession(id="source", agent_name="codex", dispatch_id="d"))
    successor = store.save_session(AgentSession(id="successor", agent_name="codex", dispatch_id="d"))
    notice = _create(kernel.ctx.require_service("notifications"),
        type=NotificationType.SERVICE_HEALTH, interaction=None,
        session_id=source.id, dispatch_id="d", capability="session-attention:v1")
    ledger = DispatchStore(tmp_path)
    request.addfinalizer(ledger.close)
    kernel.ctx.services["dispatch_store"] = ledger
    record = DispatchRecord(dispatch_id="d", mutation_id="m", card_id="card", session_id=source.id,
        authority_instance_id="local", authority_url="http://pa.test", target_instance_id="local",
        principal_id="user:local", state="running")
    ledger.put(record)
    with TestClient(kernel.build_app()) as client:
        client.get(f"/api/notifications/{notice.id}")
        record.session_id = successor.id
        ledger.put(record)
        response = client.post(f"/api/notifications/{notice.id}/recovery-action",
            headers={"X-CSRF-Token":client.cookies.get("pa_csrf")},
            json={"idempotency_key":"revalidate", "expected_version":notice.version, "action_id":"open-current"})
        assert response.status_code == 200
        assert response.json()["destination"] == "/agent?session=successor"

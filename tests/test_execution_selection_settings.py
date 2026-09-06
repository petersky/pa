import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from pa.config import Settings
from pa.domain.models import AgentSession
from pa.execution.selection import (
    ExecutionPreferences,
    Preference,
    SelectionConstraints,
    SelectionError,
)
from pa.execution.selection_service import SelectionService
from pa.execution.selection_settings import (
    apply_pending,
    cancel_settings,
    request_settings,
)
from tests.test_execution_selection import candidate


def test_advertised_immutable_native_setting_requires_a_context_boundary(tmp_path):
    async def run():
        rt = runtime(tmp_path)
        rt.connection.config_options.append(
            {
                "id": "native-fast",
                "type": "boolean",
                "currentValue": False,
                "_meta": {"pa.mutableBetweenTurns": False},
            }
        )
        with pytest.raises(SelectionError, match="immutable"):
            await request_settings(
                rt,
                ExecutionPreferences(
                    options={"native-fast": Preference(intent="required", value=True)}
                ),
                principal="user:local",
                key="immutable",
                expected_version=rt.session.updated_at,
                defer=True,
            )
        assert not rt.session.config_json.get("execution_pending_settings")
        rt.connection.configure.assert_not_awaited()

    asyncio.run(run())


def runtime(tmp_path):
    settings = Settings(data_dir=tmp_path, instance_id="local")
    store = SimpleNamespace(save_session=lambda session: None)
    service = SelectionService(settings, store)
    receipt = service.resolve(
        candidates=[candidate(connection="default")],
        principal="user:local",
        realm="default",
        surface="execution",
        persist=True,
    )
    session = AgentSession(
        id="session",
        agent_name="codex",
        realm_id="default",
        principal_id="user:local",
        model_id="model-a",
        config_json={"execution_selection": receipt},
    )
    options = [
        {
            "id": "model",
            "type": "select",
            "currentValue": "model-a",
            "options": [{"value": "model-a"}, {"value": "model-b"}],
        }
    ]
    connection = SimpleNamespace(
        session=session,
        models={
            "currentModelId": "model-a",
            "availableModels": [{"modelId": "model-a"}, {"modelId": "model-b"}],
        },
        config_options=options,
    )

    async def configure(request, **kwargs):
        options[0]["currentValue"] = request.model_id
        session.model_id = request.model_id
        session.config_json = {
            **session.config_json,
            "options": options,
            "configuration": {
                "state": "ready",
                "requested": request.as_dict(),
                "effective": {"model_id": request.model_id},
                "attempt": 1,
            },
        }
        return {"model_id": request.model_id}

    connection.configure = AsyncMock(side_effect=configure)

    async def offload(operation, call, *args, **kwargs):
        return call(*args, **kwargs)

    return SimpleNamespace(
        settings=settings,
        store=store,
        session=session,
        connection=connection,
        manager=SimpleNamespace(_selection_service=service),
        prompting=True,
        connected=True,
        _offload=offload,
        _prompt_lock=asyncio.Lock(),
    )


def test_deferred_change_is_durable_unapplied_and_replayed_once_at_boundary(tmp_path):
    async def run():
        rt = runtime(tmp_path)
        prefs = ExecutionPreferences(
            model=Preference(intent="required", value="model-b")
        )
        version = rt.session.updated_at
        pending = await request_settings(
            rt,
            prefs,
            principal="user:local",
            key="change",
            expected_version=version,
            defer=True,
        )
        assert pending["state"] == "pending"
        assert rt.session.model_id == "model-a"
        rt.connection.configure.assert_not_awaited()
        duplicate = await request_settings(
            rt,
            prefs,
            principal="user:local",
            key="change",
            expected_version=version,
            defer=True,
        )
        assert duplicate == pending
        rt.prompting = False
        async with rt._prompt_lock:
            await apply_pending(rt)
            await apply_pending(rt)
        assert rt.session.model_id == "model-b"
        assert (
            rt.session.config_json["execution_selection"]["selected"]["model"]
            == "model-b"
        )
        assert (
            rt.session.config_json["execution_selection_history"][0]["selected"][
                "model"
            ]
            == "model-a"
        )
        rt.connection.configure.assert_awaited_once()

    asyncio.run(run())


def test_conflicts_context_boundary_and_failed_confirmation_never_compete(tmp_path):
    async def run():
        rt = runtime(tmp_path)
        with pytest.raises(SelectionError, match="linked new attempt"):
            await request_settings(
                rt,
                ExecutionPreferences(
                    harness=Preference(intent="required", value="cursor")
                ),
                principal="user:local",
                key="harness",
                expected_version=rt.session.updated_at,
                defer=True,
            )
        prefs = ExecutionPreferences(
            model=Preference(intent="required", value="model-b")
        )
        version = rt.session.updated_at
        await request_settings(
            rt,
            prefs,
            principal="user:local",
            key="pending",
            expected_version=version,
            defer=True,
        )
        with pytest.raises(SelectionError):
            await request_settings(
                rt,
                prefs,
                principal="user:local",
                key="competing",
                expected_version=version,
                defer=True,
            )
        rt.connection.configure.side_effect = RuntimeError("not accepted")
        with pytest.raises(SelectionError, match="confirmed"):
            await apply_pending(rt)
        assert rt.session.model_id == "model-a"
        assert (
            rt.session.config_json["execution_selection"]["selected"]["model"]
            == "model-a"
        )
        assert rt.session.config_json["execution_pending_settings"]["state"] == "failed"
        with pytest.raises(SelectionError, match="deferred setting"):
            await apply_pending(rt)

    asyncio.run(run())


def test_policy_change_before_boundary_revalidates_fixed_target_not_alternative(
    tmp_path,
):
    async def run():
        rt = runtime(tmp_path)
        await request_settings(
            rt,
            ExecutionPreferences(model=Preference(intent="required", value="model-b")),
            principal="user:local",
            key="pending",
            expected_version=rt.session.updated_at,
            defer=True,
        )
        service = rt.manager._selection_service
        policy = service.store.policy("default")
        policy.constraints.models = ["model-a"]
        service.store.save_policy("default", policy, 1)
        with pytest.raises(SelectionError):
            await apply_pending(rt)
        rt.connection.configure.assert_not_awaited()
        assert rt.session.model_id == "model-a"

    asyncio.run(run())


def test_cancel_pending_is_correlated_and_does_not_invent_failed_rollback(tmp_path):
    async def run():
        rt = runtime(tmp_path)
        prefs = ExecutionPreferences(
            model=Preference(intent="required", value="model-b")
        )
        await request_settings(
            rt,
            prefs,
            principal="user:local",
            key="pending",
            expected_version=rt.session.updated_at,
            defer=True,
        )
        cancelled = await cancel_settings(
            rt,
            key="pending",
            expected_version=rt.session.updated_at,
            principal="user:local",
        )
        assert cancelled["state"] == "cancelled"
        await apply_pending(rt)
        assert rt.session.model_id == "model-a"
        rt.connection.configure.assert_not_awaited()
        await request_settings(
            rt,
            prefs,
            principal="user:local",
            key="new",
            expected_version=rt.session.updated_at,
            defer=True,
        )
        rt.connection.configure.side_effect = RuntimeError("partial native failure")
        with pytest.raises(SelectionError):
            await apply_pending(rt)
        with pytest.raises(SelectionError, match="cannot invent a rollback"):
            await cancel_settings(
                rt,
                key="new",
                expected_version=rt.session.updated_at,
                principal="user:local",
            )
        assert rt.session.config_json["execution_pending_settings"]["state"] == "failed"

    asyncio.run(run())


def test_admin_change_retains_card_owner_and_rechecks_card_routing_at_boundary(
    tmp_path,
):
    async def run():
        rt = runtime(tmp_path)
        card = SimpleNamespace(
            id="card", project_id=None, execution_preferences=ExecutionPreferences()
        )
        rt.store.get_card = lambda *args, **kwargs: card
        receipt = rt.session.config_json["execution_selection"]
        receipt["context"].update(
            card_id=card.id, surface="card", principal="user:local"
        )
        from pa.execution.selection import digest

        receipt["decision_id"] = digest(
            {k: v for k, v in receipt.items() if k != "decision_id"}
        )
        requested = await request_settings(
            rt,
            ExecutionPreferences(model=Preference(intent="required", value="model-b")),
            principal="user:administrator",
            key="admin-change",
            expected_version=rt.session.updated_at,
            defer=True,
        )
        assert requested["decision"]["context"] == receipt["context"]
        assert (
            requested["decision"]["settings_change"]["requested_by"]
            == "user:administrator"
        )
        card.execution_preferences.hard_constraints = SelectionConstraints(
            models=["model-a"]
        )
        with pytest.raises(SelectionError):
            await apply_pending(rt)
        rt.connection.configure.assert_not_awaited()
        assert rt.session.principal_id == "user:local"

    asyncio.run(run())

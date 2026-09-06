import asyncio
import json
from datetime import UTC, datetime
from unittest.mock import patch

import httpx
import pytest

from pa.acp.providers.base import AgentProviderSpec
from pa.acp.providers.metadata import save_credentials
from pa.execution.selection import (
    ExecutionPreferences,
    Preference,
    SelectionError,
    SelectionPolicy,
    TaskAssessment,
    resolve_selection,
)
from pa.execution.selection_catalog import candidates_from_advertisement
from pa.execution.selection_connections import (
    ConnectionProfile,
    apply_connection,
    apply_selected_connection,
    connection_revision,
    discover_connection,
)
from pa.execution.selection_store import SelectionStore


def profile(harness="codex", **kwargs):
    values = {
        "id": "research",
        "harness": harness,
        "backend": "openai",
        "account_label": "Research account",
        "credential_reference": "RESEARCH_KEY",
    }
    if harness != "cursor":
        values.update(base_url="https://configured.invalid/v1", wire_api="responses")
    return ConnectionProfile(**(values | kwargs))


@pytest.mark.parametrize("harness", ["codex", "cursor", "openinterpreter"])
def test_native_connection_overlays_are_process_local_and_revision_pinned(
    tmp_path, harness
):
    p = profile(
        harness,
        backend={
            "codex": "openai",
            "cursor": "cursor-account",
            "openinterpreter": "minimax",
        }[harness],
    )
    save_credentials(tmp_path, harness, {"RESEARCH_KEY": "fixture-secret"})
    store = SelectionStore(tmp_path)
    p = store.save_connection(p, 0)
    original = AgentProviderSpec(
        id=harness,
        display_name=harness,
        command="fixture-command",
        args=["acp"],
        env={"INITIAL_AGENT_MODE": "existing-permission"},
    )
    selected = {
        "harness": harness,
        "connection": p.id,
        "connection_revision": connection_revision(p, tmp_path),
        "model_provider": p.backend,
        "model": "native-model",
    }
    overlay = apply_selected_connection(original, selected, tmp_path)
    assert original.env == {"INITIAL_AGENT_MODE": "existing-permission"}
    assert original.args == ["acp"]
    assert overlay.env["INITIAL_AGENT_MODE"] == "existing-permission"
    if harness == "codex":
        config = json.loads(overlay.env["CODEX_CONFIG"])
        assert (
            config["model_providers"][p.native_id]["env_key"]
            == "PA_EXECUTION_CONNECTION_KEY"
        )
        assert "fixture-secret" not in overlay.env["CODEX_CONFIG"]
        assert overlay.env["MODEL_PROVIDER"] == p.native_id
    elif harness == "cursor":
        assert overlay.env["CURSOR_API_KEY"] == "fixture-secret"
        assert "MODEL_PROVIDER" not in overlay.env
    else:
        assert any(
            arg.startswith(f"model_providers.{p.native_id}.base_url=")
            for arg in overlay.args
        )
        assert "fixture-secret" not in " ".join(overlay.args)
        assert 'model="native-model"' in overlay.args
    store.save_connection(
        p.model_copy(update={"account_label": "Different account"}), p.revision
    )
    with pytest.raises(SelectionError, match="changed or is unavailable"):
        apply_selected_connection(original, selected, tmp_path)


def test_connections_do_not_invent_native_transport_combinations_or_expose_secrets(
    tmp_path,
):
    with pytest.raises(ValueError, match="arbitrary backend"):
        profile("cursor", base_url="https://minimax.invalid")
    with pytest.raises(ValueError, match="Responses"):
        profile("codex", wire_api="chat")
    with pytest.raises(ValueError, match="without credentials"):
        profile(base_url="https://user:secret@api.invalid")
    with pytest.raises(SelectionError, match="credential reference"):
        apply_connection(
            AgentProviderSpec(id="codex", display_name="Codex", command="fixture"),
            profile(),
            tmp_path,
        )


def test_credential_rotation_cannot_reuse_stale_account_health_or_catalog(tmp_path):
    from pa.config import Settings
    from pa.execution.selection_service import SelectionService

    p = SelectionStore(tmp_path).save_connection(profile(), 0)
    save_credentials(
        tmp_path,
        "codex",
        {
            "RESEARCH_KEY": "original-fixture-key",
            "OTHER_ACCOUNT_KEY": "must-not-inherit",
        },
    )
    revision = connection_revision(p, tmp_path)
    row = candidates_from_advertisement(
        instance_id="local",
        harness="codex",
        connection=p.id,
        model_provider=p.backend,
        advertisement={"models": ["model-a"], "connection_revision": revision},
        readiness="ready",
        observed_at=datetime.now(UTC),
        source="fixture",
    )[0]
    service = SelectionService(Settings(data_dir=tmp_path, instance_id="local"), None)
    service.store.save_catalog("local", [row.model_dump(mode="json")])
    assert len(asyncio.run(service.local_catalog())) == 1
    spec = AgentProviderSpec(
        id="codex",
        display_name="Codex",
        command="fixture",
        env={
            "OTHER_ACCOUNT_KEY": "must-not-inherit",
            "OPENAI_API_KEY": "wrong-default",
        },
    )
    overlay = apply_selected_connection(spec, row.model_dump(mode="json"), tmp_path)
    assert "OTHER_ACCOUNT_KEY" not in overlay.env
    assert "OPENAI_API_KEY" in overlay.excluded_env
    assert "must-not-inherit" not in json.dumps(overlay.model_dump())
    save_credentials(tmp_path, "codex", {"RESEARCH_KEY": "new-fixture-account-key"})
    assert connection_revision(p, tmp_path) != revision
    assert asyncio.run(service.local_catalog()) == []
    with pytest.raises(SelectionError, match="changed or is unavailable"):
        apply_selected_connection(spec, row.model_dump(mode="json"), tmp_path)


def test_disabling_a_profile_invalidates_composite_instance_catalog(tmp_path):
    store = SelectionStore(tmp_path)
    p = store.save_connection(profile(), 0)
    store.save_catalog("host", [{"connection": p.id}, {"connection": "default"}])
    store.save_connection(p.model_copy(update={"enabled": False}), p.revision)
    assert store.catalog("host")[0] == [{"connection": "default"}]


@pytest.mark.parametrize(
    "harness,backend,models,options,effort",
    [
        ("codex", "openai", ["gpt-6-astra[xhigh]"], [], "xhigh"),
        (
            "cursor",
            "cursor-account",
            ["grok-account-model"],
            [
                {
                    "id": "thinking_level",
                    "category": "thought_level",
                    "type": "select",
                    "currentValue": "native-deep",
                    "options": [{"value": "native-deep"}],
                }
            ],
            "native-deep",
        ),
        ("openinterpreter", "minimax", ["MiniMax-native"], [], None),
    ],
)
def test_adapter_contracts_keep_native_reasoning_and_account_scope(
    harness, backend, models, options, effort
):
    advertised = {
        "models": {
            "availableModels": [{"modelId": m} for m in models],
            "currentModelId": models[0],
        },
        "options": options,
    }
    rows = candidates_from_advertisement(
        instance_id="host",
        harness=harness,
        connection="account-one",
        model_provider=backend,
        advertisement=advertised,
        readiness="ready",
        observed_at=datetime.now(UTC),
        source="fixture_native_response",
    )
    assert rows[0].model_provider == backend
    if effort:
        assert rows[0].reasoning.values == [effort]
    else:
        assert rows[0].reasoning.support == "unknown"
    required = ExecutionPreferences(
        harness=Preference(intent="required", value=harness),
        connection=Preference(intent="required", value="account-two"),
    )
    with pytest.raises(SelectionError):
        resolve_selection(
            layers=[("dispatch", required)],
            candidates=rows,
            policy=SelectionPolicy(),
            assessment=TaskAssessment(),
        )
    with pytest.raises(SelectionError):
        resolve_selection(
            layers=[
                (
                    "dispatch",
                    ExecutionPreferences(
                        reasoning=Preference(
                            intent="required", value="not-a-native-level"
                        )
                    ),
                )
            ],
            candidates=rows,
            policy=SelectionPolicy(),
            assessment=TaskAssessment(),
        )


@pytest.mark.parametrize(
    "probe_ok,status,expected",
    [
        (True, 200, "ready"),
        (False, 200, "unavailable"),
        (True, 401, "unavailable"),
        (True, 404, "unknown"),
    ],
)
def test_backend_discovery_never_treats_credentials_or_initialize_as_health(
    tmp_path, probe_ok, status, expected
):
    save_credentials(tmp_path, "openinterpreter", {"RESEARCH_KEY": "fixture-secret"})
    p = profile("openinterpreter", backend="minimax", wire_api="chat")
    real_client = httpx.AsyncClient

    def client(**kwargs):
        return real_client(
            **kwargs,
            transport=httpx.MockTransport(
                lambda req: httpx.Response(
                    status, json={"data": [{"id": "MiniMax-native"}]}
                )
            ),
        )

    spec = AgentProviderSpec(id="openinterpreter", display_name="OI", command="fixture")
    with (
        patch(
            "pa.acp.providers.openinterpreter.OpenInterpreterProvider.resolve_spawn",
            return_value=spec,
        ),
        patch(
            "pa.acp.providers.probe.probe_acp_initialize", return_value={"ok": probe_ok}
        ),
        patch(
            "pa.execution.selection_connections.httpx.AsyncClient", side_effect=client
        ),
    ):
        rows = asyncio.run(discover_connection(p, tmp_path))
    assert rows[0].readiness == expected
    assert "fixture-secret" not in json.dumps([r.model_dump(mode="json") for r in rows])
    assert rows[0].reasoning.support == "unknown"

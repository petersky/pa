"""Migration and peer-interface contracts for execution intent."""

import asyncio
import sqlite3
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest
from fastapi import HTTPException
from mcp.server.mcpserver import MCPServer
from typer.testing import CliRunner

from pa.domain.models import CardCreate, CardUpdate
from pa.domain.projection import CardProjection
from pa.execution.selection import ExecutionPreferences
from tests.test_execution_selection import candidate, pref, resolve


def test_old_projection_and_card_events_migrate_to_inherit_without_reselection(
    tmp_path,
):
    path = tmp_path / "old-projection.db"
    projection = CardProjection(path)
    card = projection.create_card(
        CardCreate(title="legacy card", auto_enrich=False), via_log=False
    )
    # This is a private test projection, never the server's data directory.
    with sqlite3.connect(path) as conn:
        conn.execute("ALTER TABLE cards DROP COLUMN execution_preferences")
    migrated = CardProjection(path)
    restored = migrated.get_card(card.id)
    assert restored.title == "legacy card"
    assert restored.execution_preferences == ExecutionPreferences()
    assert (
        CardCreate.model_validate(
            {"title": "old API"}
        ).execution_preferences.model.intent
        == "inherit"
    )
    assert (
        "execution_preferences" not in CardUpdate(title="legacy edit").model_fields_set
    )


def test_cli_selection_validation_and_exact_start_payload():
    from pa.cli.execution import execution_app

    with patch("pa.cli.execution.send") as send:
        result = CliRunner().invoke(
            execution_app,
            [
                "start",
                "--idempotency-key",
                "stable",
                "--selection-json",
                '{"harness":{"intent":"required","value":"cursor"}}',
            ],
        )
        assert result.exit_code == 0, result.output
        body = send.call_args.args[2]
        assert body["label"] == "execution-cli:stable"
        assert body["execution_preferences"]["harness"]["value"] == "cursor"
        send.reset_mock()
        invalid = CliRunner().invoke(
            execution_app, ["start", "--idempotency-key", "", "--selection-json", "{}"]
        )
        assert invalid.exit_code != 0
        send.assert_not_called()
        invalid = CliRunner().invoke(
            execution_app,
            [
                "preview",
                "--selection-json",
                '{"options":{"sandbox":{"intent":"required","value":"full"}}}',
            ],
        )
        assert invalid.exit_code != 0
        send.assert_not_called()


def test_mcp_start_settings_and_boundary_forward_native_intent():
    from pa.modules.execution_selection import register_mcp

    async def run():
        server = MCPServer("execution-selection-contract")
        ctx = SimpleNamespace(settings=SimpleNamespace())
        with patch(
            "pa.mcp.local_api.request_local_pa", return_value={"accepted": True}
        ) as request:
            register_mcp(server, ctx)
            prefs = {
                "harness": {"intent": "required", "value": "openinterpreter"},
                "connection": {"intent": "preferred", "value": "minimax-account"},
            }
            await server.call_tool(
                "start_execution",
                {"execution_preferences": prefs, "idempotency_key": "stable"},
            )
            assert request.call_args.args[1:3] == ("POST", "/api/agent/sessions")
            assert (
                request.call_args.kwargs["json"]["execution_preferences"]["connection"]
                == prefs["connection"]
            )
            await server.call_tool(
                "request_execution_settings",
                {
                    "session_id": "owned",
                    "execution_preferences": {},
                    "expected_version": "2026-09-05T00:00:00Z",
                    "idempotency_key": "next",
                    "defer": True,
                },
            )
            assert request.call_args.kwargs["json"]["defer"] is True
            await server.call_tool(
                "start_linked_execution",
                {
                    "session_id": "owned",
                    "execution_preferences": prefs,
                    "idempotency_key": "boundary",
                },
            )
            assert request.call_args.args[2] == "/api/execution/sessions/owned/boundary"

    asyncio.run(run())


@pytest.mark.parametrize("peer_state", ["old-peer", "conflicting-native", "confirmed"])
def test_durable_remote_dispatch_requires_exact_receipt_and_normalized_confirmation(
    tmp_path, peer_state
):
    from pa.modules.fleet import _process_remote_dispatch
    from tests.test_dispatch_consistency import DurableDispatchJobTests

    async def run():
        fixture = DurableDispatchJobTests()
        app, ledger, _ = fixture._job_app(tmp_path)
        receipt = resolve(
            [candidate(instance_id="target")],
            ExecutionPreferences(model=pref("model-a"), reasoning=pref("xhigh")),
        )
        record = fixture._record(
            request_payload={
                "message": "same original prompt",
                "execution_selection": receipt,
                "model_id": "model-a",
                "effort": "xhigh",
            }
        )
        ledger.transition(record, "queued", "admitted")
        configuration = {
            "state": "ready",
            "effective": {"model_id": "model-a", "reasoning": "xhigh"},
        }
        config = {
            "configuration": configuration,
            "execution_selection": receipt,
            "options": [
                {
                    "id": "model",
                    "category": "model",
                    "type": "select",
                    "currentValue": "model-a",
                },
                {
                    "id": "reasoning_effort",
                    "category": "thought_level",
                    "type": "select",
                    "currentValue": "xhigh",
                },
            ],
        }
        if peer_state == "old-peer":
            config.pop("execution_selection")
        elif peer_state == "conflicting-native":
            config["options"][0]["currentValue"] = "another-model"
        snapshot = {
            "session": {"id": "peer-session", "config_json": config},
            "configuration": configuration,
        }
        ack = {
            "accepted": True,
            "accepted_event": "queue_enqueued",
            "session_id": "peer-session",
            "dispatch_id": record.dispatch_id,
            "prompt_id": "prompt-1",
        }
        peer = AsyncMock(side_effect=[snapshot, ack])
        with (
            patch(
                "pa.modules.fleet._peer_dispatch_json",
                AsyncMock(return_value={"resolvable": True}),
            ),
            patch("pa.modules.fleet._peer_agent_json", peer),
        ):
            if peer_state == "confirmed":
                await _process_remote_dispatch(app, record)
                assert record.state == "running"
                assert peer.await_count == 2
            else:
                with pytest.raises(HTTPException) as error:
                    await _process_remote_dispatch(app, record)
                assert error.value.detail["code"] == (
                    "selection_peer_contract_unavailable"
                    if peer_state == "old-peer"
                    else "remote_configuration_unconfirmed"
                )
                assert peer.await_count == 1  # No prompt before native confirmation.
                assert record.session_id == "peer-session"

    asyncio.run(run())

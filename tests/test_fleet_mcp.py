from __future__ import annotations

import inspect
import unittest
from unittest.mock import MagicMock, patch

from pa.modules.fleet import FleetModule
from pa.modules.items import ItemsModule


class FakeMcp:
    def __init__(self) -> None:
        self.functions: dict[str, object] = {}

    def tool(self):
        def register(fn):
            self.functions[fn.__name__] = fn
            return fn

        return register


class FleetMcpTests(unittest.TestCase):
    def setUp(self) -> None:
        self.mcp = FakeMcp()
        self.ctx = MagicMock()
        self.local_api = MagicMock()
        self.patch = patch("pa.mcp.local_api.request_local_pa", self.local_api)
        self.patch.start()
        self.addCleanup(self.patch.stop)
        FleetModule().register_mcp(self.mcp, self.ctx)
        ItemsModule().register_mcp(self.mcp, self.ctx)

    def test_registers_first_class_dispatch_lifecycle_schema(self) -> None:
        expected = {
            "dispatch_card_to_instance",
            "get_dispatch",
            "retry_dispatch",
            "cancel_dispatch",
            "prompt_dispatch_session",
            "update_card_preferred_instance",
        }
        self.assertTrue(expected.issubset(self.mcp.functions))

        dispatch = inspect.signature(self.mcp.functions["dispatch_card_to_instance"])
        self.assertEqual(list(dispatch.parameters)[:2], ["card_id", "instance_id"])
        self.assertIn("idempotency_key", dispatch.parameters)
        self.assertEqual(
            dispatch.parameters["idempotency_key"].default, inspect.Parameter.empty
        )
        self.assertIn("authority_instance_id", dispatch.parameters)
        self.assertIn("config", dispatch.parameters)

        for name in ("retry_dispatch", "cancel_dispatch"):
            signature = inspect.signature(self.mcp.functions[name])
            self.assertEqual(
                list(signature.parameters)[:2], ["dispatch_id", "idempotency_key"]
            )

    def test_dispatch_uses_authenticated_local_durable_control_plane(self) -> None:
        authority_result = {
            "accepted": True,
            "duplicate": False,
            "dispatch_id": "dispatch-1",
            "dispatch": {
                "dispatch_id": "dispatch-1",
                "card_id": "card-1",
                "card_version": "2026-07-24T00:00:00Z",
                "authority_instance_id": "authority",
                "target_instance_id": "target",
                "session_id": None,
                "state": "queued",
            },
        }
        self.local_api.return_value = authority_result

        result = self.mcp.functions["dispatch_card_to_instance"](
            "card-1",
            "target",
            message="Implement it",
            idempotency_key="dispatch-key-1",
            provider="codex",
        )

        self.assertEqual(result, authority_result)
        self.local_api.assert_called_once_with(
            self.ctx.settings,
            "POST",
            "/api/fleet/instances/target/agent/start",
            json={
                "authority_instance_id": None,
                "card_id": "card-1",
                "message": "Implement it",
                "provider": "codex",
                "model_id": None,
                "mode_id": None,
                "effort": None,
                "cwd": None,
                "config": {},
                "idempotency_key": "dispatch-key-1",
            },
        )

    def test_lifecycle_tools_preserve_normalized_dispatch_state(self) -> None:
        normalized = {
            "dispatch_id": "dispatch-1",
            "card_id": "card-1",
            "card_version": "v1",
            "authority_instance_id": "authority",
            "target_instance_id": "target",
            "session_id": "session-1",
            "state": "running",
        }
        self.local_api.return_value = normalized

        self.assertEqual(self.mcp.functions["get_dispatch"]("dispatch-1"), normalized)
        self.local_api.assert_called_with(
            self.ctx.settings,
            "GET",
            "/api/fleet/dispatch-jobs/dispatch-1",
            allow_not_found=True,
        )

        self.mcp.functions["retry_dispatch"]("dispatch-1", idempotency_key="retry-1")
        self.local_api.assert_called_with(
            self.ctx.settings,
            "POST",
            "/api/fleet/dispatch-jobs/dispatch-1/retry",
            json={"idempotency_key": "retry-1"},
        )
        self.mcp.functions["cancel_dispatch"]("dispatch-1", idempotency_key="cancel-1")
        self.local_api.assert_called_with(
            self.ctx.settings,
            "POST",
            "/api/fleet/dispatch-jobs/dispatch-1/cancel",
            json={"idempotency_key": "cancel-1"},
        )

        self.mcp.functions["prompt_dispatch_session"](
            "dispatch-1", "Continue", "prompt-1", authority_instance_id="peer"
        )
        self.local_api.assert_called_with(
            self.ctx.settings,
            "POST",
            "/api/fleet/instances/peer/dispatch-jobs/dispatch-1/prompt",
            json={
                "message": "Continue",
                "action": "append",
                "idempotency_key": "prompt-1",
            },
        )

    def test_preferred_instance_update_returns_new_card_version(self) -> None:
        card = {
            "id": "card-1",
            "preferred_instance": "target",
            "updated_at": "2026-07-24T00:00:01Z",
        }
        self.local_api.return_value = card
        result = self.mcp.functions["update_card_preferred_instance"](
            "card-1", "target", realm="fleet"
        )
        self.assertEqual(result, card)
        self.local_api.assert_called_with(
            self.ctx.settings,
            "PATCH",
            "/api/cards/card-1",
            params={"realm": "fleet"},
            json={"preferred_instance": "target"},
            allow_not_found=True,
        )


if __name__ == "__main__":
    unittest.main()

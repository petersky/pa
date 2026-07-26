from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import HTTPException

from pa.domain.models import AgentSession, FleetInstance, TranscriptEvent
from pa.domain.projection import CardProjection
from pa.execution.dispatch import DispatchRecord
from pa.modules.agent_chat import recover_session
from pa.modules.fleet import resolve_session_route


class SessionDurabilityPersistenceTests(unittest.TestCase):
    def test_restart_rehydrates_origin_provider_identity_and_event_history(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "pa.db"
            writer = CardProjection(db_path)
            session = AgentSession(
                id="session-restart",
                agent_name="codex",
                external_session_id="provider-thread-7",
                origin_instance_id="monica-id",
                origin_instance_name="monica",
                authority_instance_id="0c7d8ecb-7e45-4579-8fa0-35159492d3f1",
                dispatch_id="33333333-3333-4333-8333-333333333333",
                realm_id="engineering",
                card_id="44444444-4444-4444-8444-444444444444",
                project_id="55555555-5555-4555-8555-555555555555",
                status="idle",
            )
            writer.save_session(session)
            writer.append_transcript_events(
                [
                    TranscriptEvent(
                        session_id=session.id,
                        seq=1,
                        event_type="user_message",
                        payload={"message": "continue"},
                    )
                ]
            )

            restarted = CardProjection(db_path)
            restored = restarted.get_session(session.id)
            events = restarted.list_transcript_events(session.id)

            self.assertEqual(restored.origin_instance_id, "monica-id")
            self.assertEqual(restored.origin_instance_name, "monica")
            self.assertEqual(
                restored.authority_instance_id,
                "0c7d8ecb-7e45-4579-8fa0-35159492d3f1",
            )
            self.assertEqual(
                restored.dispatch_id, "33333333-3333-4333-8333-333333333333"
            )
            self.assertEqual(restored.realm_id, "engineering")
            self.assertEqual(restored.card_id, "44444444-4444-4444-8444-444444444444")
            self.assertEqual(
                restored.project_id, "55555555-5555-4555-8555-555555555555"
            )
            self.assertEqual(restored.external_session_id, "provider-thread-7")
            self.assertEqual(events[0].payload["message"], "continue")


class SessionRecoveryTests(unittest.IsolatedAsyncioTestCase):
    async def test_recovery_reuses_exact_pa_and_provider_session_identity(self) -> None:
        session = AgentSession(
            id="session-recover",
            agent_name="codex",
            external_session_id="provider-thread-9",
            status="recoverable_interrupted",
            label="card:1",
        )
        runtime = MagicMock()
        runtime._closed = False
        runtime.snapshot.return_value = {"session": {"id": session.id}}

        manager = MagicMock()
        manager.startup_state.return_value = {"phase": "ready", "complete": True}
        manager.recover_session = AsyncMock(return_value=runtime)
        request = MagicMock()
        with patch("pa.modules.agent_chat._manager", return_value=manager):
            result = await recover_session(request, session.id)

        self.assertEqual(result["session"]["id"], session.id)
        manager.recover_session.assert_awaited_once_with(
            session.id, provider_override=None
        )


class RemoteOwnerReconnectTests(unittest.IsolatedAsyncioTestCase):
    async def test_remote_owner_unreachable_then_reconnects_to_same_session(
        self,
    ) -> None:
        dispatch = DispatchRecord(
            mutation_id="mutation-1",
            authority_instance_id="local-id",
            authority_url="http://local",
            target_instance_id="monica-id",
            target_instance_name="monica",
            session_id="remote-session",
        )
        fleet = MagicMock()
        fleet.get_instance.return_value = FleetInstance(
            instance_id="monica-id",
            name="monica",
            url="http://monica:8080",
        )
        dispatch_store = MagicMock()
        dispatch_store.by_session.return_value = dispatch
        ctx = MagicMock()
        ctx.store.get_session.return_value = None
        ctx.settings.instance_id = "local-id"
        ctx.settings.instance_name = "local"
        ctx.services = {"dispatch_store": dispatch_store}
        ctx.require_service.return_value = fleet
        request = MagicMock()
        request.app.state.ctx = ctx
        history = {
            "session": {
                "id": "remote-session",
                "agent_name": "codex",
                "external_session_id": "provider-remote",
                "status": "idle",
            },
            "live": True,
        }
        peer = AsyncMock(
            side_effect=[
                HTTPException(status_code=502, detail="Peer unreachable"),
                history,
            ]
        )
        with (
            patch("pa.modules.fleet.require_user", return_value=object()),
            patch("pa.modules.fleet._peer_agent_json", peer),
        ):
            unavailable = await resolve_session_route(request, "remote-session")
            reconnected = await resolve_session_route(request, "remote-session")

        self.assertEqual(unavailable["state"], "owner_unreachable")
        self.assertTrue(unavailable["recoverable"])
        self.assertEqual(reconnected["state"], "live")
        self.assertEqual(reconnected["owner"]["instance_id"], "monica-id")
        self.assertEqual(reconnected["provider"]["session_id"], "provider-remote")
        self.assertIn("/instances/monica-id/agent", reconnected["api_base"])

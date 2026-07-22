from __future__ import annotations

import asyncio
import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
from fastapi import HTTPException

from pa.config import Settings
from pa.domain.models import Card, CardEvent, CardLane, EventType
from pa.execution.dispatch import (
    CompletionOutbox,
    DispatchRecord,
    DispatchStore,
    DispatchWorker,
)
from pa.modules.fleet import (
    DispatchCompletionBody,
    DispatchMaterializeBody,
    _assert_dispatch_sync_health,
    _process_remote_dispatch,
    cancel_dispatch,
    complete_dispatch,
    materialize_dispatch,
    retry_dispatch,
)
from pa.sync.event_log import EventLog
from pa.sync.object_store import ObjectStore


def request_for(settings: Settings, store: MagicMock, services: dict | None = None):
    ctx = MagicMock(settings=settings, store=store)
    ctx.services = services or {}
    ctx.require_service.side_effect = lambda name: ctx.services[name]
    ctx.register_service.side_effect = lambda name, value: ctx.services.__setitem__(
        name, value
    )
    request = MagicMock()
    request.app.state.ctx = ctx
    request.headers = {}
    return request


class MaterializationTests(unittest.TestCase):
    def test_missing_target_card_is_durably_materialized_at_exact_version(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="target")
            card = Card(id="card-1", title="Fleet convergence")
            store = MagicMock()
            store.get_card.return_value = None
            log = MagicMock()
            request = request_for(settings, store, {"event_log": log})
            body = DispatchMaterializeBody(
                dispatch_id="dispatch-1",
                mutation_id="mutation-1",
                card=card.model_dump(mode="json"),
                card_version=card.updated_at.isoformat(),
                realm_id="default",
                authority_instance_id="authority",
                authority_url="http://authority:8080",
                target_instance_id="target",
            )

            result = materialize_dispatch(request, body)

            self.assertTrue(result["resolvable"])
            log.append_event.assert_called_once()
            store.apply_event.assert_called_once()
            self.assertEqual(
                DispatchStore(settings.data_dir).get("dispatch-1").card_id, "card-1"
            )

    def test_stale_target_returns_actionable_409(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="target")
            target = Card(id="card-1", title="stale")
            authority = target.model_copy(
                update={"title": "new", "updated_at": datetime.now(UTC)}
            )
            store = MagicMock()
            store.get_card.return_value = target
            request = request_for(settings, store, {"event_log": MagicMock()})
            with self.assertRaises(HTTPException) as raised:
                materialize_dispatch(
                    request,
                    DispatchMaterializeBody(
                        dispatch_id="dispatch-1",
                        mutation_id="mutation-1",
                        card=authority.model_dump(mode="json"),
                        card_version=authority.updated_at.isoformat(),
                        realm_id="default",
                        authority_instance_id="authority",
                        authority_url="http://authority",
                        target_instance_id="target",
                    ),
                )
            self.assertEqual(raised.exception.status_code, 409)
            self.assertEqual(raised.exception.detail["code"], "stale_target_card")


class CompletionTests(unittest.TestCase):
    def test_duplicate_completion_updates_card_exactly_once(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="authority")
            card = Card(id="card-1", title="done remotely")
            store = MagicMock()
            store.get_card.return_value = card
            ledger = DispatchStore(settings.data_dir)
            ledger.put(
                DispatchRecord(
                    dispatch_id="dispatch-1",
                    mutation_id="mutation-1",
                    card_id=card.id,
                    realm_id="default",
                    card_version=card.updated_at.isoformat(),
                    authority_instance_id="authority",
                    authority_url="http://authority",
                    target_instance_id="target",
                    session_id="session-1",
                    state="dispatched",
                )
            )
            request = request_for(settings, store, {"dispatch_store": ledger})
            request.headers = {"idempotency-key": "mutation-1"}
            body = DispatchCompletionBody(
                mutation_id="mutation-1",
                card_id=card.id,
                realm_id="default",
                card_version=card.updated_at.isoformat(),
                source_instance_id="target",
                session_id="session-1",
            )

            first = complete_dispatch(request, "dispatch-1", body)
            second = complete_dispatch(request, "dispatch-1", body)

            self.assertFalse(first["duplicate"])
            self.assertTrue(second["duplicate"])
            store.update_card.assert_called_once()

    def test_end_to_end_remote_completion_accepts_recorded_dispatch_transition(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), instance_id="authority")
            original = Card(id="card-1", title="remote")
            active = original.model_copy(
                update={"lane": CardLane.ACTIVE, "preferred_instance": "target"}
            )
            store = MagicMock()
            store.get_card.return_value = active
            ledger = DispatchStore(settings.data_dir)
            ledger.put(
                DispatchRecord(
                    dispatch_id="dispatch-1",
                    mutation_id="mutation-1",
                    card_id=original.id,
                    realm_id="default",
                    card_version=original.updated_at.isoformat(),
                    authority_instance_id="authority",
                    authority_url="http://authority",
                    target_instance_id="target",
                    session_id="session-1",
                    state="dispatched",
                )
            )
            request = request_for(settings, store, {"dispatch_store": ledger})
            request.headers = {"idempotency-key": "mutation-1"}
            result = complete_dispatch(
                request,
                "dispatch-1",
                DispatchCompletionBody(
                    mutation_id="mutation-1",
                    card_id=original.id,
                    realm_id="default",
                    card_version=original.updated_at.isoformat(),
                    source_instance_id="target",
                    session_id="session-1",
                ),
            )
            self.assertTrue(result["acknowledged"])
            self.assertEqual(store.update_card.call_args.args[1].lane, CardLane.DONE)


class RetryAndConflictTests(unittest.IsolatedAsyncioTestCase):
    async def test_authority_unavailable_keeps_completion_pending_for_restart(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ledger = DispatchStore(Path(tmp))
            ledger.put(
                DispatchRecord(
                    dispatch_id="dispatch-1",
                    mutation_id="mutation-1",
                    card_id="card-1",
                    realm_id="default",
                    card_version="v1",
                    authority_instance_id="authority",
                    authority_url="http://authority",
                    target_instance_id="target",
                    session_id="session-1",
                    state="running",
                )
            )
            outbox = CompletionOutbox(ledger, "secret", retry_seconds=0.01)
            outbox.queue("session-1", {"stop_reason": "end_turn"})
            with patch("pa.execution.dispatch.httpx.AsyncClient") as client:
                client.return_value.__aenter__.return_value.post = AsyncMock(
                    side_effect=httpx.ConnectError("offline")
                )
                await outbox._send(ledger.get("dispatch-1"))

            reloaded = DispatchStore(Path(tmp)).get("dispatch-1")
            self.assertEqual(reloaded.state, "completion_pending")
            self.assertEqual(reloaded.attempts, 1)
            self.assertIn("offline", reloaded.last_error)

    async def test_completion_uses_latest_running_dispatch_for_resumed_session(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ledger = DispatchStore(Path(tmp))
            old = DispatchRecord(
                dispatch_id="old",
                mutation_id="old-mutation",
                card_id="old-card",
                authority_instance_id="authority",
                authority_url="http://authority",
                target_instance_id="target",
                session_id="session-1",
                state="completed",
            )
            ledger.put(old)
            current = DispatchRecord(
                dispatch_id="current",
                mutation_id="current-mutation",
                card_id="current-card",
                authority_instance_id="authority",
                authority_url="http://authority",
                target_instance_id="target",
                session_id="session-1",
                state="running",
            )
            ledger.put(current)
            outbox = CompletionOutbox(ledger, "", retry_seconds=60)

            self.assertTrue(outbox.queue("session-1", {"stop_reason": "end_turn"}))
            self.assertEqual(current.state, "completion_pending")
            self.assertEqual(old.state, "completed")

    async def test_unacknowledged_session_cannot_enqueue_completion(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            ledger = DispatchStore(Path(tmp))
            record = DispatchRecord(
                dispatch_id="dispatch-1",
                mutation_id="mutation-1",
                authority_instance_id="authority",
                authority_url="http://authority",
                target_instance_id="target",
                session_id="session-1",
                state="starting_session",
            )
            ledger.put(record)
            outbox = CompletionOutbox(ledger, "", retry_seconds=60)
            self.assertFalse(outbox.queue("session-1", {}))
            self.assertEqual(record.state, "starting_session")

    async def test_dispatch_is_blocked_when_two_peer_heads_diverge(self) -> None:
        settings = Settings(
            instance_id="authority",
            peers=["http://peer-a", "http://peer-b"],
            sync_token="secret",
        )
        log = MagicMock()
        log.get_head.return_value = "head-local"
        request = request_for(settings, MagicMock(), {"event_log": log})
        responses = []
        for head in ("head-a", "head-b"):
            response = MagicMock()
            response.json.return_value = [{"realm_id": "default", "head_hash": head}]
            responses.append(response)
        with patch("pa.modules.fleet.httpx.AsyncClient") as client:
            client.return_value.__aenter__.return_value.get = AsyncMock(
                side_effect=responses
            )
            with self.assertRaises(HTTPException) as raised:
                await _assert_dispatch_sync_health(request, "default")
        self.assertEqual(raised.exception.status_code, 409)
        self.assertEqual(raised.exception.detail["code"], "sync_conflict")

    async def test_unreachable_peer_blocks_dispatch_instead_of_hiding_divergence(
        self,
    ) -> None:
        settings = Settings(
            instance_id="authority",
            peers=["http://peer-a"],
            sync_token="secret",
        )
        log = MagicMock()
        log.get_head.return_value = "head-local"
        request = request_for(settings, MagicMock(), {"event_log": log})
        with patch("pa.modules.fleet.httpx.AsyncClient") as client:
            client.return_value.__aenter__.return_value.get = AsyncMock(
                side_effect=httpx.ConnectError("offline")
            )
            with self.assertRaises(HTTPException) as raised:
                await _assert_dispatch_sync_health(request, "default")
        self.assertEqual(raised.exception.status_code, 409)
        self.assertEqual(raised.exception.detail["code"], "sync_unavailable")


class DurableDispatchJobTests(unittest.IsolatedAsyncioTestCase):
    def _job_app(self, root: Path):
        settings = Settings(
            data_dir=root,
            instance_id="authority",
            instance_name="authority",
            instance_url="http://authority:8080",
            sync_token="secret",
        )
        ledger = DispatchStore(root)
        fleet = MagicMock()
        fleet.list_instances.return_value = [
            MagicMock(instance_id="target", name="target", url="http://target:8080")
        ]
        domain = MagicMock()
        ctx = MagicMock(settings=settings, store=domain)
        ctx.services = {"dispatch_store": ledger, "fleet_registry": fleet}
        ctx.require_service.side_effect = lambda name: ctx.services[name]
        app = MagicMock()
        app.state.ctx = ctx
        return app, ledger, domain

    def _record(self, **updates) -> DispatchRecord:
        values = {
            "dispatch_id": "dispatch-1",
            "mutation_id": "mutation-1",
            "idempotency_key": "browser-1",
            "request_fingerprint": "fingerprint-1",
            "request_payload": {"message": "Do the work"},
            "authority_instance_id": "authority",
            "authority_url": "http://authority:8080",
            "target_instance_id": "target",
        }
        values.update(updates)
        return DispatchRecord(**values)

    async def test_worker_records_every_stage_and_requires_delivery_ack(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            app, ledger, domain = self._job_app(Path(tmp))
            record = self._record()
            ledger.transition(record, "queued", "admitted")
            ack = {
                "accepted": True,
                "accepted_event": "queue_enqueued",
                "session_id": "session-new",
                "dispatch_id": record.dispatch_id,
                "prompt_id": "prompt-1",
            }
            peer_agent = AsyncMock(
                side_effect=[{"session": {"id": "session-new"}}, ack]
            )
            with (
                patch(
                    "pa.modules.fleet._peer_dispatch_json",
                    AsyncMock(return_value={"resolvable": True}),
                ),
                patch("pa.modules.fleet._peer_agent_json", peer_agent),
            ):
                await _process_remote_dispatch(app, record)

            self.assertEqual(record.state, "running")
            self.assertEqual(record.session_id, "session-new")
            self.assertIsNotNone(record.prompt_acknowledged_at)
            states = [event.state for event in record.events]
            self.assertEqual(
                states,
                [
                    "queued",
                    "checking_sync",
                    "materializing",
                    "starting_session",
                    "delivering_prompt",
                    "running",
                ],
            )
            create_body = peer_agent.await_args_list[0].kwargs["body"]
            self.assertEqual(create_body["label"], f"dispatch:{record.dispatch_id}")
            self.assertNotEqual(create_body["label"], "card:card-1")
            domain.add_knowledge.assert_called_once()

    async def test_missing_prompt_ack_is_retryable_and_keeps_exact_session(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            app, ledger, _domain = self._job_app(Path(tmp))
            record = self._record()
            ledger.transition(record, "queued", "admitted")
            peer_agent = AsyncMock(
                side_effect=[
                    {"session": {"id": "session-new"}},
                    {"started": True, "session_id": "session-new"},
                ]
            )
            worker = DispatchWorker(
                ledger, lambda item: _process_remote_dispatch(app, item)
            )
            with (
                patch(
                    "pa.modules.fleet._peer_dispatch_json",
                    AsyncMock(return_value={"resolvable": True}),
                ),
                patch("pa.modules.fleet._peer_agent_json", peer_agent),
            ):
                await worker._execute(record)

            self.assertEqual(record.state, "failed")
            self.assertEqual(record.error_code, "prompt_ack_missing")
            self.assertEqual(record.session_id, "session-new")
            self.assertTrue(record.recoverable)

    async def test_materialization_409_is_audited_without_starting_session(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            app, ledger, _domain = self._job_app(Path(tmp))
            record = self._record(card_id="card-1", card_version="v1")
            ledger.transition(record, "queued", "admitted")
            app.state.ctx.store.get_card.return_value = MagicMock(
                id="card-1",
                title="Card",
                project_id=None,
                updated_at=MagicMock(isoformat=MagicMock(return_value="v1")),
                model_dump=MagicMock(return_value={"id": "card-1"}),
            )
            conflict = HTTPException(
                status_code=409,
                detail={
                    "code": "stale_target_card",
                    "message": "Target has a different card version.",
                    "recoverable": True,
                },
            )
            worker = DispatchWorker(
                ledger, lambda item: _process_remote_dispatch(app, item)
            )
            with (
                patch("pa.modules.fleet._assert_dispatch_sync_health", AsyncMock()),
                patch(
                    "pa.modules.fleet._peer_dispatch_json",
                    AsyncMock(side_effect=conflict),
                ),
                patch("pa.modules.fleet._peer_agent_json", AsyncMock()) as peer,
            ):
                await worker._execute(record)

            self.assertEqual(record.state, "failed")
            self.assertEqual(record.error_code, "stale_target_card")
            peer.assert_not_awaited()

    async def test_provider_timeout_is_background_failure_not_admission_failure(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            app, ledger, _domain = self._job_app(Path(tmp))
            record = self._record()
            ledger.transition(record, "queued", "admitted")
            worker = DispatchWorker(
                ledger, lambda item: _process_remote_dispatch(app, item)
            )
            timeout = HTTPException(
                status_code=502,
                detail={
                    "code": "provider_timeout",
                    "message": "Provider startup timed out.",
                    "recoverable": True,
                },
            )
            with (
                patch(
                    "pa.modules.fleet._peer_dispatch_json",
                    AsyncMock(return_value={"resolvable": True}),
                ),
                patch(
                    "pa.modules.fleet._peer_agent_json", AsyncMock(side_effect=timeout)
                ),
            ):
                await worker._execute(record)
            self.assertEqual(record.state, "failed")
            self.assertEqual(record.error_code, "provider_timeout")

    async def test_retry_and_cancel_preserve_dispatch_identity(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            app, ledger, _domain = self._job_app(Path(tmp))
            record = self._record(state="failed", last_error="offline")
            ledger.put(record)
            request = MagicMock()
            request.app = app
            with patch("pa.modules.fleet.require_user"):
                retried = retry_dispatch(request, record.dispatch_id)
                self.assertEqual(retried["state"], "queued")
                cancelled = cancel_dispatch(request, record.dispatch_id)
            self.assertEqual(cancelled["state"], "cancelled")
            self.assertEqual(cancelled["dispatch_id"], "dispatch-1")


class DispatchRestartTests(unittest.TestCase):
    def test_restart_requeues_interrupted_job_with_same_session_and_identity(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ledger = DispatchStore(root)
            record = DispatchRecord(
                dispatch_id="dispatch-1",
                mutation_id="mutation-1",
                idempotency_key="browser-1",
                request_payload={"message": "work"},
                authority_instance_id="authority",
                authority_url="http://authority",
                target_instance_id="target",
                session_id="session-1",
                state="delivering_prompt",
            )
            ledger.put(record)

            reloaded = DispatchStore(root)
            reloaded.reconcile_interrupted()
            recovered = reloaded.get("dispatch-1")

            self.assertEqual(recovered.state, "queued")
            self.assertEqual(recovered.session_id, "session-1")
            self.assertEqual(recovered.mutation_id, "mutation-1")
            self.assertEqual(
                recovered.events[-1].detail["previous_state"], "delivering_prompt"
            )

    def test_legacy_orphan_expires_with_actionable_retry_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ledger = DispatchStore(root)
            ledger.put(
                DispatchRecord(
                    dispatch_id="orphan-1",
                    mutation_id="mutation-1",
                    card_id="card-1",
                    authority_instance_id="authority",
                    authority_url="http://authority",
                    target_instance_id="target",
                    state="dispatching",
                )
            )
            ledger.reconcile_interrupted()
            orphan = ledger.get("orphan-1")
            self.assertEqual(orphan.state, "failed")
            self.assertEqual(orphan.error_code, "orphaned_legacy_dispatch")
            self.assertIn("retry", orphan.last_error.lower())


class EventLogMergeTests(unittest.TestCase):
    def test_compatible_heads_produce_same_deterministic_merge_hash(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            objects = ObjectStore(Path(tmp) / "objects")
            log = EventLog(objects, Path(tmp), "node-a")
            other = EventLog(objects, Path(tmp) / "other", "node-b")
            # Parent hashes are sufficient for proving deterministic merge encoding.
            first = log.merge_heads("default", "b" * 64, "a" * 64, "ignored")
            second = other.merge_heads("default", "a" * 64, "b" * 64, "other")
            self.assertEqual(first.hash, second.hash)

    def test_three_instance_disjoint_histories_converge_without_operator_conflict(
        self,
    ) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            objects = ObjectStore(root / "objects")
            for name in ("left", "right", "observer"):
                (root / name).mkdir()
            left = EventLog(objects, root / "left", "left")
            right = EventLog(objects, root / "right", "right")
            observer = EventLog(objects, root / "observer", "observer")
            base_event = CardEvent(
                type=EventType.CARD_CREATED,
                realm_id="default",
                card_id="base",
                author_principal="test",
                author_instance="left",
                payload=Card(id="base", title="base").model_dump(mode="json"),
            )
            _, base = left.append_event(base_event)
            right.advance_ref("default", base.hash)
            _, left_head = left.append_event(
                CardEvent(
                    type=EventType.CARD_UPDATED,
                    realm_id="default",
                    card_id="left-card",
                    author_principal="test",
                    author_instance="left",
                    payload={"title": "left"},
                )
            )
            _, right_head = right.append_event(
                CardEvent(
                    type=EventType.CARD_UPDATED,
                    realm_id="default",
                    card_id="right-card",
                    author_principal="test",
                    author_instance="right",
                    payload={"title": "right"},
                )
            )
            compatible, health = observer.compatible_histories(
                left_head.hash, right_head.hash
            )
            self.assertTrue(compatible, health)
            merged = observer.merge_heads(
                "default", left_head.hash, right_head.hash, "sync:auto"
            )
            seen: list[str] = []
            observer.apply_commit_chain(
                merged.hash, lambda event: seen.append(event.card_id or "merge")
            )
            self.assertIn("left-card", seen)
            self.assertIn("right-card", seen)

    def test_delete_and_concurrent_edit_require_operator_resolution(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            objects = ObjectStore(root / "objects")
            for name in ("left", "right"):
                (root / name).mkdir()
            left = EventLog(objects, root / "left", "left")
            right = EventLog(objects, root / "right", "right")
            _, base = left.append_event(
                CardEvent(
                    type=EventType.CARD_CREATED,
                    realm_id="default",
                    card_id="card-1",
                    author_principal="test",
                    author_instance="left",
                    payload=Card(id="card-1", title="base").model_dump(mode="json"),
                )
            )
            right.advance_ref("default", base.hash)
            _, deleted = left.append_event(
                CardEvent(
                    type=EventType.CARD_DELETED,
                    realm_id="default",
                    card_id="card-1",
                    author_principal="test",
                    author_instance="left",
                )
            )
            _, edited = right.append_event(
                CardEvent(
                    type=EventType.CARD_UPDATED,
                    realm_id="default",
                    card_id="card-1",
                    author_principal="test",
                    author_instance="right",
                    payload={"title": "edited"},
                )
            )
            compatible, health = left.compatible_histories(deleted.hash, edited.hash)
            self.assertFalse(compatible)
            self.assertEqual(health["conflicts"][0]["field"], "__terminal__")


class BoundedDrainTests(unittest.IsolatedAsyncioTestCase):
    async def test_outbox_shutdown_is_bounded_with_pending_stream_work(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            outbox = CompletionOutbox(DispatchStore(Path(tmp)), "", retry_seconds=60)
            outbox.start()
            await asyncio.wait_for(outbox.close(timeout=0.01), timeout=1.5)

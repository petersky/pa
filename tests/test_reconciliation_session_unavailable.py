"""A prompted card reconciliation never waits forever for an unavailable session."""
from __future__ import annotations

import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from pa.execution.dispatch import ReconciliationCandidate
from tests.test_completion_reconciliation import CompletionReconciliationTests


def prompted(tmp, *, status="quiesced", waited=timedelta(hours=2), limit=3600):
    ledger, runtime, _supervisor, outbox, reconciler = (
        CompletionReconciliationTests().make_fixture(Path(tmp), session_status=status)
    )
    runtime.connected = False
    record = ledger.get("dispatch-1")
    record.completion_payload = {"queued_prompt_id": "initial-prompt"}
    record.reconciliation_state = "prompted"
    record.reconciliation_prompt_id = "reconcile-prompt"
    record.reconciliation_updated_at = datetime.now(UTC) - waited
    ledger.put(record)
    reconciler._session_unavailable_source = limit
    reconciler.stall_notifier = MagicMock()
    return ledger, outbox, reconciler


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["quiesced", "idle", "disconnected"])
async def test_unavailable_session_past_the_bound_delivers_without_reconciliation_and_notifies(status):
    with tempfile.TemporaryDirectory() as tmp:
        ledger, outbox, reconciler = prompted(tmp, status=status)
        await reconciler._recover_prompted(ledger.get("dispatch-1"))
        current = ledger.get("dispatch-1")
        assert current.reconciliation_state == "failed"
        assert current.reconciliation_recoverable is False
        assert len(outbox.payloads) == 1
        reconciler.stall_notifier.assert_called_once()


@pytest.mark.asyncio
async def test_unavailable_session_inside_the_bound_keeps_waiting():
    with tempfile.TemporaryDirectory() as tmp:
        ledger, outbox, reconciler = prompted(tmp, waited=timedelta(minutes=5))
        await reconciler._recover_prompted(ledger.get("dispatch-1"))
        assert ledger.get("dispatch-1").reconciliation_state == "prompted"
        assert outbox.payloads == []
        reconciler.stall_notifier.assert_not_called()


@pytest.mark.asyncio
async def test_zero_bound_keeps_the_old_wait_forever_behaviour():
    with tempfile.TemporaryDirectory() as tmp:
        ledger, outbox, reconciler = prompted(tmp, waited=timedelta(days=30), limit=0)
        await reconciler._recover_prompted(ledger.get("dispatch-1"))
        assert ledger.get("dispatch-1").reconciliation_state == "prompted"
        assert outbox.payloads == []


@pytest.mark.asyncio
async def test_unchanged_candidate_is_not_deep_copied_every_tick():
    with tempfile.TemporaryDirectory() as tmp:
        ledger, _outbox, reconciler = prompted(tmp, waited=timedelta(minutes=5))
        candidate = ledger.reconciliation_candidates(due_at=datetime.now(UTC))[0]
        assert isinstance(candidate, ReconciliationCandidate)
        with patch.object(ledger, "get", wraps=ledger.get) as get:
            for _ in range(5):
                await reconciler._recover_prompted(candidate)
        assert get.call_count == 1

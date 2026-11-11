"""Background follow-through for post-turn evaluations.

The evaluator is read-only and only *recommends* catalog actions.  This worker is
the missing executor: it runs the recommendations that need no operator
approval, inside the automatic follow-up budget, through PA's own idempotent
action endpoint (claims, fencing tokens and receipts), and tells the owner when
it cannot continue on its own.  Nothing here widens what an action may do.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from pa.core.background import BackgroundTaskSupervisor
from pa.execution.dispatch import DispatchRecord, DispatchStore
from pa.execution.post_turn import (
    ActionReceiptStatus,
    ActionReceiptV1,
    FollowupActionName,
    FollowupActionStatus,
    FollowupActionV1,
    PostTurnDecision,
    PostTurnEvaluationV1,
    TurnEndSnapshotV1,
    automatic_followup_budget_exhausted,
    automatic_followups_used,
    canonical_digest,
    is_authorized_same_session_continuation,
)

logger = logging.getLogger(__name__)

# Evaluations older than this are history, not work: a restart after a long
# outage must not wake agents with prompts that were meant for a turn long gone.
MAX_EVALUATION_AGE = timedelta(hours=6)
CANDIDATE_WINDOW = timedelta(hours=24)

# Conditions, not executable effects: another owner (the PR supervisor, a
# refresh) acts on these, so the worker neither runs nor escalates them.
INERT_ACTIONS = frozenset(
    {
        FollowupActionName.NO_ACTION,
        FollowupActionName.RECORD_TURN_OUTCOME,
        FollowupActionName.WAIT_FOR_PR_SUPERVISOR,
        FollowupActionName.REFRESH_READ_ONLY_EVIDENCE,
    }
)

# Reasons a Done claim was downgraded while a PR (or its checks) is still in
# flight.  That is the PR supervisor's normal churn, not an agent mistake.
_EXPECTED_DOWNGRADE_MARKERS = (
    "merged state is unknown",
    "stability window",
    "stable observations",
    "stability timestamp",
    "merge-on-green",
    "stable-green evidence",
    "does not confirm the exact watched head",
)
_PR_IN_FLIGHT_CLOSED_STATES = {"closed", "retired"}


def downgrade_is_expected_churn(reason: str) -> bool:
    """True when a Done downgrade just means the linked PR is not merged yet."""
    text = (reason or "").casefold()
    if not text.startswith("done was downgraded to waiting"):
        # Acceptance gates and stale requirements have their own owners.
        return True
    if any(marker in text for marker in _EXPECTED_DOWNGRADE_MARKERS):
        return True
    if "linked pr watch" in text and text.rstrip(".").endswith(", not merged"):
        state = text.rsplit(" is ", 1)[-1].split(",", 1)[0].strip()
        return state not in _PR_IN_FLIGHT_CLOSED_STATES
    return False


def downgrade_feedback_message(reason: str) -> str:
    detail = reason.split(":", 1)[-1].strip() if ":" in reason else reason
    return (
        "PA downgraded your Done claim to Waiting: "
        f"{detail}\n\n"
        "To finish: if the work needs a pull request, open it and link it so PA "
        "can track and complete the card when it merges. If no integration is "
        "needed, send the card disposition again with "
        '"integration_required": false in its evidence and say why in the '
        "outcome. Do not repeat the Done claim without addressing this."
    )


@dataclass
class PostTurnHooks:
    """The few effects the worker needs, injected by the fleet module."""

    # Execute one non-approval action; raises on failure.
    execute: Callable[
        [DispatchRecord, PostTurnEvaluationV1, FollowupActionV1, str],
        Awaitable[Any],
    ]
    # Send one same-session prompt; raises on failure.
    prompt: Callable[[DispatchRecord, str, str], Awaitable[Any]]
    # Raise a manual-review notification; returns a truthy value when created.
    escalate: Callable[..., Any]


class PostTurnWorker:
    """Executes eligible recommendations and escalates when it cannot."""

    def __init__(
        self,
        store: DispatchStore,
        hooks: PostTurnHooks,
        settings: Callable[[], Any],
        *,
        async_runtime: Any | None = None,
    ) -> None:
        self.store = store
        self.hooks = hooks
        self._settings = settings
        self.async_runtime = async_runtime
        self._wake = asyncio.Event()
        self._closing = False
        self._failures: dict[str, tuple[int, datetime]] = {}
        self._supervisor = BackgroundTaskSupervisor(
            "post-turn-worker",
            self._run,
            self.store.db_path.parent / "post_turn_worker.json",
        )

    # -- lifecycle ----------------------------------------------------------

    def start(self) -> None:
        self._closing = False
        self._supervisor.start()

    def wake(self) -> None:
        self._wake.set()

    async def close(self) -> None:
        self._closing = True
        self._wake.set()
        await self._supervisor.close()

    def worker_health(self) -> dict[str, Any]:
        return self._supervisor.health()

    async def _offload(self, operation: str, call, *args, **kwargs):
        if self.async_runtime:
            return await self.async_runtime.run_blocking(
                operation, call, *args, **kwargs
            )
        return await asyncio.to_thread(call, *args, **kwargs)

    async def _run(self, heartbeat: Callable[[], None] | None = None) -> None:
        while not self._closing:
            if heartbeat:
                heartbeat()
            try:
                await self.run_once(heartbeat)
            except Exception:
                logger.exception("Post-turn follow-through pass failed")
            self._wake.clear()
            try:
                await asyncio.wait_for(
                    self._wake.wait(),
                    float(self._settings().post_turn_retry_seconds),
                )
            except TimeoutError:
                pass

    # -- one pass -----------------------------------------------------------

    async def run_once(self, heartbeat: Callable[[], None] | None = None) -> int:
        """Process every candidate once; returns how many dispatches had work."""
        if not self._settings().post_turn_worker_enabled:
            return 0
        now = datetime.now(UTC)
        candidates = await self._offload(
            "post_turn.candidates",
            self.store.post_turn_candidates,
            since=now - CANDIDATE_WINDOW,
        )
        handled = 0
        for record in candidates:
            if heartbeat:
                heartbeat()
            try:
                handled += int(await self._process(record, now))
            except Exception:
                logger.exception(
                    "Post-turn follow-through failed for dispatch %s",
                    record.dispatch_id,
                )
        return handled

    async def _process(self, record: DispatchRecord, now: datetime) -> bool:
        if record.state in {"failed", "cancelled"}:
            return False
        evaluation = record.post_turn_evaluations[-1]
        snapshot = next(
            (
                item
                for item in record.turn_end_snapshots
                if item.snapshot_id == evaluation.snapshot_id
            ),
            None,
        )
        if now - evaluation.created_at > MAX_EVALUATION_AGE:
            return False
        did_work = False
        if snapshot is not None:
            did_work |= await self._downgrade_feedback(record, snapshot)
        did_work |= await self._unable_to_determine(record, snapshot)
        for action in evaluation.recommended_actions:
            did_work |= await self._action(record, evaluation, action, now)
        return did_work

    # -- markers: durable once-only facts, stored as fenced receipts ---------

    async def _claim(self, record: DispatchRecord, marker: str) -> ActionReceiptV1 | None:
        receipt = ActionReceiptV1(
            action_id=marker,
            action_digest=canonical_digest({"marker": marker}),
            idempotency_key=marker[:500],
            lease_owner="pa.post-turn-worker",
            lease_expires_at=datetime.now(UTC) + timedelta(seconds=60),
        )
        _record, claimed_receipt, claimed = await self._offload(
            "post_turn.marker_claim",
            self.store.claim_post_turn_action,
            record.dispatch_id,
            receipt,
        )
        return claimed_receipt if claimed else None

    async def _finish(
        self,
        record: DispatchRecord,
        receipt: ActionReceiptV1,
        *,
        ok: bool,
        error: str | None = None,
    ) -> None:
        receipt.status = (
            ActionReceiptStatus.SUCCEEDED if ok else ActionReceiptStatus.FAILED
        )
        receipt.finished_at = datetime.now(UTC)
        receipt.error = (error or "")[:1000] or None
        receipt.history.append(
            {"status": receipt.status.value, "at": receipt.finished_at.isoformat()}
        )
        await self._offload(
            "post_turn.marker_finish",
            self.store.finish_post_turn_action,
            record.dispatch_id,
            receipt,
        )

    async def _release(self, record: DispatchRecord, marker: str) -> None:
        def drop(current: DispatchRecord) -> bool:
            return current.post_turn_action_receipts.pop(marker, None) is not None

        await self._offload(
            "post_turn.marker_release",
            self.store.mutate_current,
            record.dispatch_id,
            mutate=drop,
        )

    async def _escalate_once(
        self,
        record: DispatchRecord,
        marker: str,
        *,
        title: str,
        body: str,
    ) -> bool:
        receipt = await self._claim(record, f"escalated:{marker}")
        if receipt is None:
            return False
        created = await self._offload(
            "post_turn.escalate",
            self.hooks.escalate,
            record,
            title=title,
            body=body,
            dedupe_key=marker,
        )
        if not created:
            # The notification service was unavailable; try again next pass.
            await self._release(record, f"escalated:{marker}")
            return False
        await self._finish(record, receipt, ok=True)
        return True

    # -- the three jobs -------------------------------------------------------

    async def _downgrade_feedback(
        self, record: DispatchRecord, snapshot: TurnEndSnapshotV1
    ) -> bool:
        limit = int(self._settings().post_turn_downgrade_feedback_max)
        if (
            limit <= 0
            or record.card_disposition_status != "downgraded"
            or not record.session_id
            or not record.card_id
        ):
            return False
        reason = record.card_disposition_reason or ""
        if downgrade_is_expected_churn(reason):
            return False
        marker = f"downgrade-feedback:{record.dispatch_id}"
        if marker in record.post_turn_action_receipts:
            return False
        siblings = await self._offload(
            "post_turn.card_dispatches",
            self.store.list,
            card_id=record.card_id,
            limit=200,
            deep=False,
        )
        used = sum(
            1
            for sibling in siblings
            for key in sibling.post_turn_action_receipts
            if key.startswith("downgrade-feedback:")
        )
        if used >= limit:
            return False
        receipt = await self._claim(record, marker)
        if receipt is None:
            return False
        try:
            await self.hooks.prompt(record, downgrade_feedback_message(reason), marker)
        except Exception as exc:
            logger.warning(
                "Downgrade feedback for dispatch %s was not delivered: %s",
                record.dispatch_id,
                exc,
            )
            await self._finish(record, receipt, ok=False, error=str(exc))
            return True
        await self._finish(record, receipt, ok=True)
        return True

    async def _unable_to_determine(
        self, record: DispatchRecord, snapshot: TurnEndSnapshotV1 | None
    ) -> bool:
        threshold = int(self._settings().post_turn_escalation_threshold)
        streak = 0
        for evaluation in reversed(record.post_turn_evaluations):
            if evaluation.decision != PostTurnDecision.UNABLE_TO_DETERMINE:
                break
            streak += 1
        if streak < threshold:
            return False
        if snapshot is not None and snapshot.card_lane_after == "done":
            return False
        return await self._escalate_once(
            record,
            f"{record.dispatch_id}:unable_to_determine",
            title="Work needs a look",
            body=(
                f"The last {streak} turns of this work ended without enough "
                "evidence for PA to tell whether it is finished: no card "
                "disposition, deliverable, validation or integration record. "
                "Open the card to continue or close it out."
            ),
        )

    async def _action(
        self,
        record: DispatchRecord,
        evaluation: PostTurnEvaluationV1,
        action: FollowupActionV1,
        now: datetime,
    ) -> bool:
        if action.name in INERT_ACTIONS or action.status in {
            FollowupActionStatus.EXECUTED,
            FollowupActionStatus.REJECTED,
            FollowupActionStatus.SUPERSEDED,
        }:
            return False
        receipt = record.post_turn_action_receipts.get(action.action_id)
        if receipt is not None:
            if receipt.status == ActionReceiptStatus.SUCCEEDED:
                return False
            if (
                receipt.status == ActionReceiptStatus.STARTED
                and receipt.lease_expires_at
                and receipt.lease_expires_at > now
            ):
                return False
            return await self._escalate_once(
                record,
                f"action-failed:{action.action_id}",
                title="Automatic follow-up did not complete",
                body=(
                    f"PA tried to run '{action.name.value}' for this work but it "
                    f"ended as {receipt.status.value}"
                    f"{': ' + receipt.error if receipt.error else ''}. "
                    "Open the dispatch's follow-up actions to review it."
                ),
            )

        settings = self._settings()
        needs_approval = action.human_approval_required
        if (
            not needs_approval
            and action.name == FollowupActionName.PROMPT_SAME_SESSION
        ):
            if not is_authorized_same_session_continuation(
                action, decision=evaluation.decision, session_id=record.session_id
            ):
                needs_approval = True
            elif automatic_followup_budget_exhausted(
                record.post_turn_evaluations,
                settings.post_turn_max_automatic_followups,
            ):
                return await self._escalate_once(
                    record,
                    f"budget:{record.dispatch_id}",
                    title="Automatic follow-ups used up",
                    body=(
                        f"PA already continued this work "
                        f"{automatic_followups_used(record.post_turn_evaluations)} "
                        "times on its own and stopped (setting "
                        "post_turn_max_automatic_followups). Review it and "
                        "approve the next step or take over."
                    ),
                )
        if needs_approval:
            return await self._escalate_once(
                record,
                f"approval:{action.action_id}",
                title=f"Approval needed: {action.name.value.replace('_', ' ')}",
                body=(
                    f"The post-turn evaluation recommends '{action.name.value}' "
                    f"for this work and it needs your approval. "
                    f"Reason: {evaluation.rationale} "
                    f"Action {action.action_id[:12]}, evaluation "
                    f"{evaluation.evaluation_id[:12]}."
                ),
            )

        failures, retry_at = self._failures.get(action.action_id, (0, now))
        if retry_at > now:
            return False
        try:
            await self.hooks.execute(
                record, evaluation, action, f"post-turn:{action.action_id}"
            )
        except Exception as exc:
            failures += 1
            delay = float(settings.post_turn_retry_seconds) * 2 ** (failures - 1)
            self._failures[action.action_id] = (
                failures,
                now + timedelta(seconds=min(delay, 900.0)),
            )
            logger.warning(
                "Post-turn action %s for dispatch %s failed (%d): %s",
                action.name.value,
                record.dispatch_id,
                failures,
                exc,
            )
            if failures >= int(settings.post_turn_escalation_threshold):
                await self._escalate_once(
                    record,
                    f"exec-failed:{action.action_id}",
                    title="Automatic follow-up is failing",
                    body=(
                        f"PA could not run '{action.name.value}' for this work "
                        f"after {failures} attempts: {str(exc)[:400]}"
                    ),
                )
            return True
        self._failures.pop(action.action_id, None)
        return True

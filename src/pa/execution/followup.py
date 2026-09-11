"""Identity and receipt contract for dispatch follow-up admission."""

import json
from uuid import NAMESPACE_URL, uuid5

from fastapi import HTTPException

from pa.execution.dispatch import DispatchRecord, DispatchStore


def bind_followup_prompt(
    ledger: DispatchStore,
    record: DispatchRecord,
    key: str,
    fingerprint: str,
    *,
    claim_admission: bool = False,
) -> tuple[DispatchRecord, bool]:
    """Bind before delivery; claim target admission once, even across restarts.

    Both authority and target derive the same identity for a *new* operation.
    An old operation without an identity cannot be safely assigned one: its
    original admission may already have executed with an unrelated UUID.
    """
    claimed = False

    def mutate(current: DispatchRecord) -> bool:
        nonlocal claimed
        if current.session_id != record.session_id:
            raise HTTPException(409, detail={"code": "dispatch_session_mismatch"})
        changed = False
        operation = current.followup_operations.get(key)
        if operation is not None:
            if operation.get("fingerprint") != fingerprint:
                raise HTTPException(409, detail={"code": "idempotency_conflict"})
            if not operation.get("prompt_id"):
                raise HTTPException(503, detail={
                    "code": "legacy_followup_identity_unknown",
                    "message": "Existing follow-up has no validated prompt identity; automatic replay is unsafe.",
                    "recoverable": True,
                })
        else:
            identity = json.dumps([
                "pa.dispatch-followup.v1", current.authority_instance_id,
                current.dispatch_id, current.session_id, key,
            ], separators=(",", ":"))
            operation = {
                "fingerprint": fingerprint,
                "prompt_id": str(uuid5(NAMESPACE_URL, identity)),
                "state": "pending",
            }
            current.followup_operations[key] = operation
            changed = True
        if claim_admission and not operation.get("target_admission_started"):
            operation["target_admission_started"] = True
            claimed = True
            changed = True
        return changed

    return ledger.mutate_current(record.dispatch_id, mutate=mutate), claimed


def followup_receipt(
    record: DispatchRecord, prompt_id: str, event_type: str,
    action: str | None, *, duplicate: bool,
) -> dict:
    """Describe the original admission, independently of subsequent completion."""
    queued = event_type == "queue_enqueued" and action != "run"
    return {
        "stop_reason": "queued" if queued else "started",
        "queued": queued,
        "started": not queued,
        "accepted": True,
        "accepted_event": event_type,
        "prompt_id": prompt_id,
        "dispatch_id": record.dispatch_id,
        "session_id": record.session_id,
        "duplicate": duplicate,
    }

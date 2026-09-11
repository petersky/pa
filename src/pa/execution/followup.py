"""Identity and receipt contract for dispatch prompt admission."""

import json
from uuid import NAMESPACE_URL, uuid5

from fastapi import HTTPException

from pa.execution.dispatch import DispatchRecord, DispatchStore

PROMPT_IDENTITY_PROTOCOL = "pa.dispatch-prompt.v1"


def prompt_fingerprint(message: str, action: str) -> str:
    import hashlib

    return hashlib.sha256(json.dumps(
        {"message": message, "action": action}, sort_keys=True, separators=(",", ":"),
    ).encode()).hexdigest()


def prompt_operation(record: DispatchRecord, key: str | None) -> dict:
    return record.followup_operations[key] if key else record.initial_prompt_operation


def dispatch_prompt_identity(record: DispatchRecord, key: str | None) -> str:
    identity = json.dumps([
        PROMPT_IDENTITY_PROTOCOL, record.authority_instance_id,
        record.dispatch_id, record.session_id, key,
    ], separators=(",", ":"))
    return str(uuid5(NAMESPACE_URL, identity))


def bind_followup_prompt(
    ledger: DispatchStore, record: DispatchRecord, key: str | None, fingerprint: str,
) -> DispatchRecord:
    """Bind identity before admission; the durable queue owns recovery thereafter.

    An old ambiguous operation cannot safely acquire a new ID. It may already
    have executed with an unrelated UUID and is not automatically recoverable.
    """
    def mutate(current: DispatchRecord) -> bool:
        if current.session_id != record.session_id:
            raise HTTPException(409, detail={"code": "dispatch_session_mismatch"})
        operation = (current.followup_operations.get(key) if key
                     else current.initial_prompt_operation)
        if not key and not operation and current.error_code in {
            "prompt_not_persisted", "prompt_ack_missing", "delivery_ambiguous",
        }:
            raise HTTPException(409, detail={
                "code": "legacy_initial_prompt_identity_unknown", "recoverable": False,
                "message": "The old initial delivery has no validated prompt identity; automatic replay is unsafe.",
            })
        if operation:
            if operation.get("fingerprint") != fingerprint:
                raise HTTPException(409, detail={"code": "idempotency_conflict"})
            if (not operation.get("prompt_id")
                    or operation.get("admission_protocol") != PROMPT_IDENTITY_PROTOCOL):
                raise HTTPException(409, detail={
                    "code": "legacy_followup_identity_unknown",
                    "message": "Existing operation has no validated recoverable admission contract; automatic replay is unsafe.",
                    "recoverable": False,
                })
            return False
        operation = {
            "fingerprint": fingerprint,
            "prompt_id": dispatch_prompt_identity(current, key),
            "admission_protocol": PROMPT_IDENTITY_PROTOCOL,
            "state": "pending",
        }
        if key:
            current.followup_operations[key] = operation
        else:
            current.initial_prompt_operation = operation
        return True

    return ledger.mutate_current(record.dispatch_id, mutate=mutate)


def followup_receipt(
    record: DispatchRecord, prompt_id: str, event_type: str,
    action: str | None, *, duplicate: bool,
) -> dict:
    """Describe original admission, independently of subsequent completion."""
    queued = event_type == "queue_enqueued" and action != "run"
    return {
        "stop_reason": "queued" if queued else "started",
        "queued": queued, "started": not queued, "accepted": True,
        "accepted_event": event_type, "prompt_id": prompt_id,
        "dispatch_id": record.dispatch_id, "session_id": record.session_id,
        "duplicate": duplicate,
    }

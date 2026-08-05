"""Canonical operation identities for durable Goal mutations."""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime
from enum import Enum
from typing import Any
from uuid import UUID


def _json_value(value: Any, *, exclude_unset: bool = False) -> Any:
    if hasattr(value, "model_dump"):
        return value.model_dump(mode="json", exclude_unset=exclude_unset)
    if isinstance(value, dict):
        return {
            str(key): _json_value(item, exclude_unset=exclude_unset)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_json_value(item, exclude_unset=exclude_unset) for item in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    if isinstance(value, UUID):
        return str(value)
    return value


def operation_fingerprint(
    *,
    realm_id: str,
    entity_type: str,
    entity_id: str,
    event_type: str,
    operation: Any,
    context: Any,
) -> str:
    """Hash the complete canonical identity of one mutation attempt."""

    envelope = {
        "contract": "pa.goal-operation.v1",
        "realm_id": realm_id,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "event_type": event_type,
        "operation": _json_value(operation, exclude_unset=True),
        "context": _json_value(context),
    }
    encoded = json.dumps(
        envelope,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode()
    return hashlib.sha256(encoded).hexdigest()

"""Small shared observation contract over existing owner receipts.

This module performs no admission, I/O, retries, or lifecycle transitions.
Unknown facts stay unknown. Owner-specific lifecycle adapters retain their
original result and status without treating durable acceptance as an effect.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from pydantic import BaseModel, ConfigDict


class OperationIdentity(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    version: Literal[1] = 1
    owner: Literal["canonical", "restart", "dispatch"] | None
    realm_id: str
    idempotency_key: str
    operation: str | None = None
    request_fingerprint: str | None = None


@dataclass(frozen=True)
class OperationObservation:
    identity: OperationIdentity
    status: str
    accepted: bool | None
    committed: bool | None
    projected: bool | None
    effect: str
    reconciliation: dict | None
    receipt: dict

    @classmethod
    def from_receipt(cls, receipt: dict) -> OperationObservation:
        """Adapt normalized owner evidence without inferring terminal success."""
        return cls(OperationIdentity.model_validate(receipt["identity"]),
                   receipt["status"], receipt.get("accepted"),
                   receipt.get("committed"), receipt.get("projected"),
                   receipt.get("effect", "unknown"),
                   receipt.get("reconciliation"), dict(receipt))

    def as_outcome(self) -> dict:
        return {**self.receipt, "identity": self.identity.model_dump(),
                "accepted": self.accepted, "committed": self.committed,
                "projected": self.projected, "effect": self.effect}

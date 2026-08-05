"""Immutable canonical bindings for governed goal materialization."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

# Kept local until the shared GoalReferenceId alias lands on this branch.  This
# is intentionally the same public-input contract: non-empty, bounded, and
# containing at least one non-whitespace character.
MaterializationReferenceId = Annotated[
    str,
    Field(min_length=1, max_length=200, pattern=r"\S"),
]


def canonical_materialization_digest(payload: Any) -> str:
    """Return one stable digest for JSON-compatible materialization evidence."""

    return hashlib.sha256(
        json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode()
    ).hexdigest()


class GoalMaterializationResourceClaimV1(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    key: MaterializationReferenceId
    access: Literal["shared", "exclusive"] = "shared"
    quantity: float = Field(default=1, gt=0)
    preemptible: bool = True
    expires_at: datetime | None = None

    @model_validator(mode="after")
    def expiry_is_absolute(self) -> GoalMaterializationResourceClaimV1:
        if self.expires_at is not None and self.expires_at.tzinfo is None:
            raise ValueError("resource-claim expiry must include a timezone")
        return self


class GoalMaterializationEnvelopeV1(BaseModel):
    """Exact pre-reservation resources; canonical and immutable after creation."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    version: Literal[1] = 1
    repository_ids: tuple[MaterializationReferenceId, ...] = ()
    data_scopes: tuple[MaterializationReferenceId, ...] = ()
    attachment_ids: tuple[MaterializationReferenceId, ...] = ()
    attachment_classes: tuple[MaterializationReferenceId, ...] = ()
    resource_claims: tuple[GoalMaterializationResourceClaimV1, ...] = ()
    execution_contract_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    digest: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    def canonical_payload(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "repository_ids": list(self.repository_ids),
            "data_scopes": list(self.data_scopes),
            "attachment_ids": list(self.attachment_ids),
            "attachment_classes": list(self.attachment_classes),
            "resource_claims": [
                item.model_dump(mode="json") for item in self.resource_claims
            ],
            "execution_contract_digest": self.execution_contract_digest,
        }

    @model_validator(mode="after")
    def canonicalize_and_verify(self) -> GoalMaterializationEnvelopeV1:
        for field in (
            "repository_ids",
            "data_scopes",
            "attachment_ids",
            "attachment_classes",
        ):
            values = tuple(sorted({str(item).strip() for item in getattr(self, field)}))
            if any(not item for item in values):
                raise ValueError(f"{field} cannot contain empty identifiers")
            object.__setattr__(self, field, values)
        claims = tuple(
            sorted(
                self.resource_claims,
                key=lambda item: (
                    item.key,
                    item.access,
                    item.quantity,
                    item.preemptible,
                    item.expires_at.isoformat() if item.expires_at else "",
                ),
            )
        )
        claim_keys = [item.key for item in claims]
        if len(claim_keys) != len(set(claim_keys)):
            raise ValueError("materialization resource-claim keys must be unique")
        object.__setattr__(self, "resource_claims", claims)
        computed = canonical_materialization_digest(self.canonical_payload())
        if self.digest is not None and self.digest != computed:
            raise ValueError(
                "materialization envelope digest does not match its payload"
            )
        object.__setattr__(self, "digest", computed)
        return self


class GoalMaterializationReceiptV1(BaseModel):
    """Exact target-dependent result bound to one immutable envelope."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    version: Literal[1] = 1
    envelope_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    target_instance_id: MaterializationReferenceId
    provider_id: MaterializationReferenceId
    model_id: MaterializationReferenceId | None = None
    mode_id: MaterializationReferenceId | None = None
    materialization_plan_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    digest: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    def canonical_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"digest"})

    @model_validator(mode="after")
    def verify_digest(self) -> GoalMaterializationReceiptV1:
        computed = canonical_materialization_digest(self.canonical_payload())
        if self.digest is not None and self.digest != computed:
            raise ValueError(
                "materialization receipt digest does not match its payload"
            )
        object.__setattr__(self, "digest", computed)
        return self


class GoalExecutionIdentityV1(BaseModel):
    """Persisted binding for an assigned service; never contains credential material."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    version: Literal[1] = 1
    assigned_service_principal: MaterializationReferenceId
    provider_id: MaterializationReferenceId
    target_instance_id: MaterializationReferenceId
    session_id: MaterializationReferenceId
    fencing_token: int = Field(ge=1)
    materialization_receipt_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    credential_digest: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    credential_expires_at: datetime | None = None
    digest: str | None = Field(default=None, pattern=r"^[0-9a-f]{64}$")

    def canonical_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude={"digest"})

    @model_validator(mode="after")
    def verify_digest(self) -> GoalExecutionIdentityV1:
        if (self.credential_digest is None) != (self.credential_expires_at is None):
            raise ValueError(
                "credential digest and expiry must be populated atomically"
            )
        if (
            self.credential_expires_at is not None
            and self.credential_expires_at.tzinfo is None
        ):
            raise ValueError("credential expiry must include a timezone")
        computed = canonical_materialization_digest(self.canonical_payload())
        if self.digest is not None and self.digest != computed:
            raise ValueError("execution identity digest does not match its payload")
        object.__setattr__(self, "digest", computed)
        return self

    def credential_authenticated(self, now: datetime | None = None) -> bool:
        """Return true only for a populated, unexpired credential binding."""

        now = now or datetime.now(UTC)
        expires_at = self.credential_expires_at
        if self.credential_digest is None or expires_at is None:
            return False
        if expires_at.tzinfo is None:
            return False
        return expires_at > now

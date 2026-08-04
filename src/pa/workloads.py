"""Canonical fleet workload profiles and compatibility normalization."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Annotated, Any

from pydantic import WithJsonSchema

LEGACY_CODE_PROFILE_REASON = "legacy_code_profile_normalized_to_repository"


class WorkloadProfile(StrEnum):
    """The only workload-profile enum used across PA's public surfaces."""

    AUTOMATIC = "automatic"
    REPOSITORY = "repository"
    RESEARCH = "research"
    OPERATIONS = "operations"

    @classmethod
    def _missing_(cls, value: object) -> WorkloadProfile | None:
        # Pydantic, FastMCP, and direct enum construction all use this path, so
        # mixed-version callers can continue to send the former public value.
        if isinstance(value, str) and value.strip() == "code":
            return cls.REPOSITORY
        return None


CANONICAL_WORKLOAD_PROFILES = tuple(profile.value for profile in WorkloadProfile)
PLACEMENT_WORKLOAD_PROFILES = tuple(
    profile.value
    for profile in WorkloadProfile
    if profile is not WorkloadProfile.AUTOMATIC
)
LEGACY_WORKLOAD_PROFILE_ALIASES = {"code": WorkloadProfile.REPOSITORY.value}
WorkloadProfileInput = Annotated[
    str,
    WithJsonSchema(
        {
            "type": "string",
            "enum": list(CANONICAL_WORKLOAD_PROFILES),
            "description": (
                "Canonical workload profile. The legacy value 'code' remains "
                "accepted and is normalized to 'repository'."
            ),
        }
    ),
]


@dataclass(frozen=True)
class WorkloadProfileResolution:
    profile: WorkloadProfile
    requested_profile: str
    migration_reason: str | None = None


class WorkloadProfileError(ValueError):
    code = "invalid_workload_profile"

    def __init__(self, value: Any, *, allow_automatic: bool = True) -> None:
        supported = (
            CANONICAL_WORKLOAD_PROFILES
            if allow_automatic
            else PLACEMENT_WORKLOAD_PROFILES
        )
        rendered = repr(value)
        message = (
            f"Unknown workload profile {rendered}. Use one of: "
            f"{', '.join(supported)}. Legacy 'code' is accepted and normalized "
            "to 'repository'."
        )
        super().__init__(message)
        self.value = value
        self.message = message
        self.supported_profiles = list(supported)

    def detail(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "field": "workload_profile",
            "value": self.value,
            "supported_profiles": self.supported_profiles,
            "legacy_aliases": dict(LEGACY_WORKLOAD_PROFILE_ALIASES),
            "recoverable": True,
        }


def normalize_workload_profile(
    value: Any, *, allow_automatic: bool = True
) -> WorkloadProfileResolution:
    """Normalize a canonical profile or the deliberate legacy ``code`` alias."""

    requested = value.value if isinstance(value, WorkloadProfile) else value
    if not isinstance(requested, str):
        raise WorkloadProfileError(value, allow_automatic=allow_automatic)
    requested = requested.strip()
    if requested == "code":
        profile = WorkloadProfile.REPOSITORY
        reason = LEGACY_CODE_PROFILE_REASON
    else:
        try:
            profile = WorkloadProfile(requested)
        except ValueError as exc:
            raise WorkloadProfileError(value, allow_automatic=allow_automatic) from exc
        reason = None
    if not allow_automatic and profile is WorkloadProfile.AUTOMATIC:
        raise WorkloadProfileError(value, allow_automatic=False)
    return WorkloadProfileResolution(
        profile=profile,
        requested_profile=requested,
        migration_reason=reason,
    )

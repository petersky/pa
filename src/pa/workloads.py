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


def normalize_profile_list(values: Any, *, allow_unknown: bool = False) -> list[str]:
    """Canonicalize a profile set; projection decoding may preserve future values."""
    result: set[str] = set()
    for value in values or []:
        try:
            result.add(normalize_workload_profile(value, allow_automatic=False).profile.value)
        except WorkloadProfileError:
            if not allow_unknown:
                raise
            result.add(str(value))
    return sorted(result)


def normalize_profile_limits(
    values: Any, *, allow_unknown: bool = False
) -> dict[str, int]:
    """Merge alias collisions deterministically using the strictest capacity."""
    result: dict[str, int] = {}
    for raw_key, raw_limit in dict(values or {}).items():
        try:
            key = normalize_workload_profile(
                raw_key, allow_automatic=False
            ).profile.value
        except WorkloadProfileError:
            if not allow_unknown:
                raise
            key = str(raw_key)
        limit = int(raw_limit)
        result[key] = min(result.get(key, limit), limit)
    return dict(sorted(result.items()))


def canonical_default_scope_key(
    project_id: str | None, workload_profile: Any, raw_scope_key: str | None = None
) -> str:
    """Return one semantic placement-default identity, including old wire events."""
    if raw_scope_key:
        parts = str(raw_scope_key).split(":", 3)
        if len(parts) == 4 and parts[0] == "project" and parts[2] == "profile":
            if project_id is None and parts[1] != "*":
                project_id = parts[1]
            if workload_profile is None and parts[3] != "*":
                workload_profile = parts[3]
    profile = "*"
    if workload_profile not in {None, "*"}:
        try:
            profile = normalize_workload_profile(
                workload_profile, allow_automatic=False
            ).profile.value
        except WorkloadProfileError:
            # Future wire values retain a stable identity but are not executable.
            profile = str(workload_profile)
    return f"project:{project_id or '*'}:profile:{profile}"

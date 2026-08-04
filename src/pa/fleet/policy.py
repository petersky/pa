"""Realm-synchronized fleet groups and instance participation policy."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from pydantic import BaseModel, Field, model_validator

from pa.workloads import (
    PLACEMENT_WORKLOAD_PROFILES,
    WorkloadProfile,
    WorkloadProfileError,
    normalize_workload_profile,
)

if TYPE_CHECKING:
    from pa.domain.models import FleetInstance
    from pa.domain.store import Store


POLICY_SCHEMA_VERSION = 1
MACBOOK_INSTANCE_ID = "0c7d8ecb-7e45-4579-8fa0-35159492d3f1"
# Compatibility export for modules/extensions; values derive from the one enum.
WORKLOAD_PROFILES = PLACEMENT_WORKLOAD_PROFILES


class GroupLifecycle(StrEnum):
    ACTIVE = "active"
    ARCHIVED = "archived"


class ParticipationMode(StrEnum):
    DISABLED = "disabled"
    MANUAL_ONLY = "manual_only"
    AUTOMATIC = "automatic"


class DispatchIntent(StrEnum):
    MANUAL = "manual"
    AUTOMATIC = "automatic"
    PRIVILEGED_OVERRIDE = "privileged_override"


class GroupSelector(BaseModel):
    """Non-security-sensitive, declared selector inputs for a custom group."""

    zones: list[str] = Field(default_factory=list)
    required_capabilities: list[str] = Field(default_factory=list)
    provider_ids: list[str] = Field(default_factory=list)
    labels: list[str] = Field(default_factory=list)
    lifecycle_states: list[str] = Field(default_factory=lambda: ["active"])

    @property
    def configured(self) -> bool:
        return bool(
            self.zones
            or self.required_capabilities
            or self.provider_ids
            or self.labels
            or self.lifecycle_states != ["active"]
        )


class InstanceGroup(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid4()))
    realm_id: str = "default"
    name: str = Field(min_length=1, max_length=120)
    description: str = Field(default="", max_length=1000)
    lifecycle_state: GroupLifecycle = GroupLifecycle.ACTIVE
    included_instance_ids: list[str] = Field(default_factory=list)
    excluded_instance_ids: list[str] = Field(default_factory=list)
    selector: GroupSelector = Field(default_factory=GroupSelector)
    nested_group_ids: list[str] = Field(default_factory=list)
    permitted_placement_policies: list[str] = Field(
        default_factory=lambda: [
            "best_match",
            "least_busy",
            "round_robin",
            "random_eligible",
        ]
    )
    visible_project_ids: list[str] = Field(default_factory=list)
    system: bool = False
    membership_generation: int = Field(default=1, ge=1)
    version: int = Field(default=1, ge=1)
    created_by: str = ""
    updated_by: str = ""
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @model_validator(mode="after")
    def normalize_membership(self) -> InstanceGroup:
        self.included_instance_ids = sorted(set(self.included_instance_ids))
        self.excluded_instance_ids = sorted(set(self.excluded_instance_ids))
        self.permitted_placement_policies = sorted(
            set(self.permitted_placement_policies)
        )
        self.visible_project_ids = sorted(set(self.visible_project_ids))
        if self.nested_group_ids:
            raise ValueError("nested instance groups are deferred in policy version 1")
        return self


class InstanceGroupCreate(BaseModel):
    realm_id: str = "default"
    name: str = Field(min_length=1, max_length=120)
    description: str = Field(default="", max_length=1000)
    included_instance_ids: list[str] = Field(default_factory=list)
    excluded_instance_ids: list[str] = Field(default_factory=list)
    selector: GroupSelector = Field(default_factory=GroupSelector)
    nested_group_ids: list[str] = Field(default_factory=list)
    permitted_placement_policies: list[str] = Field(
        default_factory=lambda: [
            "best_match",
            "least_busy",
            "round_robin",
            "random_eligible",
        ]
    )
    visible_project_ids: list[str] = Field(default_factory=list)


class InstanceGroupUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=120)
    description: str | None = Field(default=None, max_length=1000)
    lifecycle_state: GroupLifecycle | None = None
    included_instance_ids: list[str] | None = None
    excluded_instance_ids: list[str] | None = None
    selector: GroupSelector | None = None
    nested_group_ids: list[str] | None = None
    permitted_placement_policies: list[str] | None = None
    visible_project_ids: list[str] | None = None
    expected_version: int | None = Field(default=None, ge=1)


class InstanceParticipationPolicy(BaseModel):
    schema_version: int = POLICY_SCHEMA_VERSION
    realm_id: str = "default"
    instance_id: str
    participation_mode: ParticipationMode = ParticipationMode.AUTOMATIC
    automatic_dispatch: bool | None = None
    manual_dispatch: bool | None = None
    allowed_profiles: list[WorkloadProfile] = Field(default_factory=list)
    denied_profiles: list[WorkloadProfile] = Field(default_factory=list)
    allowed_project_ids: list[str] = Field(default_factory=list)
    denied_project_ids: list[str] = Field(default_factory=list)
    allowed_repository_ids: list[str] = Field(default_factory=list)
    denied_repository_ids: list[str] = Field(default_factory=list)
    allowed_provider_ids: list[str] = Field(default_factory=list)
    denied_provider_ids: list[str] = Field(default_factory=list)
    allowed_model_families: list[str] = Field(default_factory=list)
    denied_model_families: list[str] = Field(default_factory=list)
    max_concurrent_by_profile: dict[WorkloadProfile, int] = Field(default_factory=dict)
    max_queued_by_profile: dict[WorkloadProfile, int] = Field(default_factory=dict)
    hard_denied_profiles: list[WorkloadProfile] = Field(default_factory=list)
    hard_max_concurrent_by_profile: dict[WorkloadProfile, int] = Field(
        default_factory=dict
    )
    maintenance: bool = False
    quiescing: bool = False
    authority_enabled: bool = True
    pr_supervision_enabled: bool = True
    browser_enabled: bool = True
    sync_enabled: bool = True
    reason: str = Field(default="", max_length=1000)
    enablement_confirmation_reason: str = Field(default="", max_length=1000)
    source: str = "operator"
    version: int = Field(default=1, ge=1)
    actor: str = ""
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @model_validator(mode="after")
    def normalize_policy(self) -> InstanceParticipationPolicy:
        if self.automatic_dispatch is None:
            self.automatic_dispatch = (
                self.participation_mode == ParticipationMode.AUTOMATIC
            )
        if self.manual_dispatch is None:
            self.manual_dispatch = self.participation_mode != ParticipationMode.DISABLED
        if not self.automatic_dispatch and not self.manual_dispatch:
            self.participation_mode = ParticipationMode.DISABLED
        elif not self.automatic_dispatch and self.manual_dispatch:
            self.participation_mode = ParticipationMode.MANUAL_ONLY
        else:
            self.participation_mode = ParticipationMode.AUTOMATIC
        for name in (
            "allowed_profiles",
            "denied_profiles",
            "allowed_project_ids",
            "denied_project_ids",
            "allowed_repository_ids",
            "denied_repository_ids",
            "allowed_provider_ids",
            "denied_provider_ids",
            "allowed_model_families",
            "denied_model_families",
            "hard_denied_profiles",
        ):
            setattr(self, name, sorted(set(getattr(self, name))))
        return self

    def summary(self) -> str:
        if self.participation_mode == ParticipationMode.DISABLED:
            return "No dispatched work"
        if self.participation_mode == ParticipationMode.MANUAL_ONLY:
            return "Manual dispatch only"
        allowed = set(self.allowed_profiles)
        denied = set(self.denied_profiles) | set(self.hard_denied_profiles)
        effective = allowed or set(WORKLOAD_PROFILES)
        effective -= denied
        if effective == {"research"}:
            return "Research only"
        if effective == {"operations"}:
            return "Operations only"
        if "repository" in effective and not (
            self.allowed_project_ids or self.allowed_repository_ids
        ):
            return "Automatic code worker"
        if self.allowed_project_ids or self.allowed_repository_ids:
            return "Project-restricted"
        return "Custom participation"


class InstanceParticipationPolicyUpdate(BaseModel):
    participation_mode: ParticipationMode | None = None
    automatic_dispatch: bool | None = None
    manual_dispatch: bool | None = None
    allowed_profiles: list[WorkloadProfile] | None = None
    denied_profiles: list[WorkloadProfile] | None = None
    allowed_project_ids: list[str] | None = None
    denied_project_ids: list[str] | None = None
    allowed_repository_ids: list[str] | None = None
    denied_repository_ids: list[str] | None = None
    allowed_provider_ids: list[str] | None = None
    denied_provider_ids: list[str] | None = None
    allowed_model_families: list[str] | None = None
    denied_model_families: list[str] | None = None
    max_concurrent_by_profile: dict[WorkloadProfile, int] | None = None
    max_queued_by_profile: dict[WorkloadProfile, int] | None = None
    hard_denied_profiles: list[WorkloadProfile] | None = None
    hard_max_concurrent_by_profile: dict[WorkloadProfile, int] | None = None
    maintenance: bool | None = None
    quiescing: bool | None = None
    authority_enabled: bool | None = None
    pr_supervision_enabled: bool | None = None
    browser_enabled: bool | None = None
    sync_enabled: bool | None = None
    reason: str | None = Field(default=None, max_length=1000)
    expected_version: int | None = Field(default=None, ge=1)
    confirm_enable: bool = False
    confirmation_reason: str | None = Field(default=None, max_length=1000)


class PlacementDefault(BaseModel):
    realm_id: str = "default"
    project_id: str | None = None
    workload_profile: WorkloadProfile | None = None
    group_id: str
    version: int = Field(default=1, ge=1)
    actor: str = ""
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    @property
    def scope_key(self) -> str:
        return default_scope_key(self.project_id, self.workload_profile)


class FleetPolicyAuditEvent(BaseModel):
    id: str
    realm_id: str
    entity_type: str
    entity_id: str
    action: str
    actor: str
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: datetime


class GroupResolution(BaseModel):
    requested_group_id: str | None = None
    resolved_group_id: str
    resolved_group_name: str
    group_version: int
    default_source: str
    included_instance_ids: list[str] = Field(default_factory=list)
    excluded_instance_ids: list[str] = Field(default_factory=list)
    membership: dict[str, str] = Field(default_factory=dict)
    permitted_placement_policies: list[str] = Field(default_factory=list)


BUILTIN_GROUPS: dict[str, dict[str, Any]] = {
    "all-active": {
        "name": "All eligible",
        "description": "Every canonical active fleet member, subject to instance policy.",
    },
    "automatic-workers": {
        "name": "Automatic workers",
        "description": "Canonical members opted into automatic placement.",
    },
    "code-workers": {
        "name": "Code workers",
        "description": "Members whose own policies allow repository work.",
    },
    "research-workers": {
        "name": "Research workers",
        "description": "Members whose own policies allow research and artifact work.",
    },
    "operations-workers": {
        "name": "Operations workers",
        "description": "Members whose own policies allow operations and external-tool work.",
    },
    "local-only": {
        "name": "Local only",
        "description": "The authority-local instance, still subject to its participation policy.",
    },
}


def builtin_group(group_id: str, realm_id: str = "default") -> InstanceGroup | None:
    spec = BUILTIN_GROUPS.get(group_id)
    if not spec:
        return None
    return InstanceGroup(
        id=group_id,
        realm_id=realm_id,
        system=True,
        version=1,
        membership_generation=1,
        created_by="system",
        updated_by="system",
        **spec,
    )


def default_scope_key(
    project_id: str | None, workload_profile: WorkloadProfile | str | None
) -> str:
    profile: WorkloadProfile | str | None = workload_profile
    if isinstance(workload_profile, str):
        try:
            profile = normalize_workload_profile(workload_profile).profile
        except WorkloadProfileError:
            # Projection replay must preserve unknown mixed-version scope keys;
            # new writes are rejected by typed API boundaries.
            profile = workload_profile
    return f"project:{project_id or '*'}:profile:{profile or '*'}"


def compatibility_policy(
    instance_id: str, realm_id: str = "default"
) -> InstanceParticipationPolicy:
    return InstanceParticipationPolicy(
        realm_id=realm_id,
        instance_id=instance_id,
        source="compatibility_derived",
        version=1,
        reason=(
            "Derived from the pre-policy fleet configuration; migrate this "
            "instance to make participation intent explicit."
        ),
    )


class FleetPolicyService:
    """Resolve stable groups/defaults and orchestrate deliberate migration."""

    def __init__(self, store: Store) -> None:
        self.store = store

    def list_groups(
        self, realm_id: str, *, include_archived: bool = False
    ) -> list[InstanceGroup]:
        builtins = [builtin_group(group_id, realm_id) for group_id in BUILTIN_GROUPS]
        custom = self.store.list_instance_groups(
            realm_id, include_archived=include_archived
        )
        return [group for group in builtins if group is not None] + custom

    def get_group(self, realm_id: str, group_id: str) -> InstanceGroup | None:
        return builtin_group(group_id, realm_id) or self.store.get_instance_group(
            group_id, realm_id
        )

    def effective_policy(
        self, realm_id: str, instance_id: str
    ) -> tuple[InstanceParticipationPolicy, bool]:
        explicit = self.store.get_instance_participation_policy(instance_id, realm_id)
        return (
            (explicit, True)
            if explicit
            else (compatibility_policy(instance_id, realm_id), False)
        )

    def resolve_default_group(
        self,
        *,
        realm_id: str,
        project_id: str | None,
        workload_profile: WorkloadProfile,
        requested_group_id: str | None,
    ) -> tuple[str, str]:
        if requested_group_id:
            return requested_group_id, "dispatch_override"
        defaults = {
            item.scope_key: item
            for item in self.store.list_placement_defaults(realm_id)
        }
        scopes = []
        if project_id:
            scopes.extend(
                [
                    (
                        default_scope_key(project_id, workload_profile),
                        "project_profile_default",
                    ),
                    (default_scope_key(project_id, None), "project_default"),
                ]
            )
        scopes.extend(
            [
                (
                    default_scope_key(None, workload_profile),
                    "realm_profile_default",
                ),
                (default_scope_key(None, None), "realm_default"),
            ]
        )
        for key, source in scopes:
            if key in defaults:
                return defaults[key].group_id, source
        return "automatic-workers", "realm_automatic_workers"

    @staticmethod
    def _provider_ids(candidate: Any) -> set[str]:
        envelope = candidate.providers if isinstance(candidate.providers, dict) else {}
        value = envelope.get("value")
        if not isinstance(value, list):
            return set()
        return {
            str(item.get("id"))
            for item in value
            if isinstance(item, dict) and item.get("available") and item.get("id")
        }

    def resolve_group(
        self,
        *,
        realm_id: str,
        project_id: str | None,
        workload_profile: WorkloadProfile,
        requested_group_id: str | None,
        candidates: list[Any],
        local_instance_id: str,
    ) -> GroupResolution:
        group_id, source = self.resolve_default_group(
            realm_id=realm_id,
            project_id=project_id,
            workload_profile=workload_profile,
            requested_group_id=requested_group_id,
        )
        group = self.get_group(realm_id, group_id)
        if not group or group.lifecycle_state != GroupLifecycle.ACTIVE:
            raise ValueError(
                f"configured_group_unavailable:{group_id}:"
                "The requested or configured worker group is missing or archived."
            )
        if group.visible_project_ids and project_id not in group.visible_project_ids:
            raise ValueError(
                f"group_not_visible:{group_id}:"
                "The worker group is not visible to the requested project."
            )

        ids = {candidate.instance_id for candidate in candidates}
        selected: set[str]
        if group.system and group.id != "local-only":
            selected = set(ids)
        elif group.id == "local-only":
            selected = {local_instance_id} & ids
        else:
            selected = set(group.included_instance_ids) & ids
            selector = group.selector
            if selector.configured:
                for candidate in candidates:
                    labels = {
                        value[6:]
                        for value in candidate.capabilities
                        if value.startswith("label:")
                    }
                    if selector.zones and candidate.zone not in selector.zones:
                        continue
                    if selector.lifecycle_states and (
                        getattr(candidate, "lifecycle_state", "active")
                        not in selector.lifecycle_states
                    ):
                        continue
                    if not set(selector.required_capabilities).issubset(
                        candidate.capabilities
                    ):
                        continue
                    if selector.labels and not set(selector.labels).issubset(labels):
                        continue
                    if selector.provider_ids and not set(
                        selector.provider_ids
                    ).issubset(self._provider_ids(candidate)):
                        continue
                    selected.add(candidate.instance_id)

        excluded = set(group.excluded_instance_ids) & ids
        selected -= excluded
        membership = {
            instance_id: (
                "explicitly_excluded_from_group"
                if instance_id in excluded
                else (
                    "included" if instance_id in selected else "not_in_requested_group"
                )
            )
            for instance_id in sorted(ids)
        }
        return GroupResolution(
            requested_group_id=requested_group_id,
            resolved_group_id=group.id,
            resolved_group_name=group.name,
            group_version=group.version,
            default_source=source,
            included_instance_ids=sorted(selected),
            excluded_instance_ids=sorted(excluded),
            membership=membership,
            permitted_placement_policies=list(group.permitted_placement_policies),
        )

    def migrate(
        self,
        *,
        realm_id: str,
        instances: list[FleetInstance],
        actor: str,
        author_instance: str,
        apply: bool,
    ) -> dict[str, Any]:
        planned = []
        for instance in instances:
            if self.store.get_instance_participation_policy(
                instance.instance_id, realm_id
            ):
                continue
            policy = compatibility_policy(instance.instance_id, realm_id)
            policy.source = "migration_derived"
            if instance.instance_id == MACBOOK_INSTANCE_ID:
                policy.denied_profiles = [WorkloadProfile.REPOSITORY]
                policy.allowed_profiles = [
                    WorkloadProfile.RESEARCH,
                    WorkloadProfile.OPERATIONS,
                ]
                policy.reason = (
                    "MacBook is an interactive authority/UI host, not an "
                    "automatic code worker"
                )
            planned.append(policy)
        existing_defaults = self.store.list_placement_defaults(realm_id)
        create_default = not any(
            item.project_id is None and item.workload_profile is None
            for item in existing_defaults
        )
        if apply:
            for policy in planned:
                self.store.set_instance_participation_policy(
                    policy,
                    principal_id=actor,
                    instance_id=author_instance,
                )
            if create_default:
                self.store.set_placement_default(
                    PlacementDefault(
                        realm_id=realm_id,
                        group_id="automatic-workers",
                        actor=actor,
                    ),
                    principal_id=actor,
                    instance_id=author_instance,
                )
        return {
            "applied": apply,
            "realm_id": realm_id,
            "policies": [
                {
                    "instance_id": policy.instance_id,
                    "summary": policy.summary(),
                    "reason": policy.reason,
                }
                for policy in planned
            ],
            "default_group": "automatic-workers" if create_default else None,
            "warnings": (
                []
                if planned
                else ["All current canonical instances already have explicit policies."]
            ),
        }

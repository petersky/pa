"""Versioned execution contracts and authoritative dispatch preflight."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Literal

from pydantic import BaseModel, Field, ValidationError, model_validator

from pa.workloads import (
    LEGACY_CODE_PROFILE_REASON,
    WorkloadProfile,
    WorkloadProfileError,
    normalize_workload_profile,
)

# Compatibility import for extensions; WorkloadProfile is the canonical enum.
ExecutionProfile = WorkloadProfile


class RepositoryRequirement(BaseModel):
    repository_id: str
    branch: str | None = None
    base_ref: str | None = None
    worktree_required: bool = True


class ExecutionRequirements(BaseModel):
    repository_required: bool = False
    repositories: list[RepositoryRequirement] = Field(default_factory=list)
    attachments: bool = False
    browser: bool = False
    external_tools: list[str] = Field(default_factory=list)
    required_capabilities: list[str] = Field(default_factory=list)
    writable_artifact_workspace: bool = True
    network_policy: Literal["provider-default", "restricted", "disabled"] = (
        "provider-default"
    )
    expected_deliverables: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def integration_requires_repository(self) -> ExecutionRequirements:
        if {"commit", "pull_request", "code"} & set(self.expected_deliverables):
            self.repository_required = True
        return self


class ExecutionContract(BaseModel):
    version: Literal[1] = 1
    profile: WorkloadProfile = WorkloadProfile.AUTOMATIC
    profile_normalization_reason: (
        Literal["legacy_code_profile_normalized_to_repository"] | None
    ) = None
    requirements: ExecutionRequirements = Field(default_factory=ExecutionRequirements)
    confirmed: bool = False

    @model_validator(mode="before")
    @classmethod
    def normalize_legacy_profile(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        payload = dict(value)
        resolution = normalize_workload_profile(
            payload.get("profile", WorkloadProfile.AUTOMATIC)
        )
        payload["profile"] = resolution.profile
        supplied_reason = payload.get("profile_normalization_reason")
        payload["profile_normalization_reason"] = resolution.migration_reason or (
            LEGACY_CODE_PROFILE_REASON
            if supplied_reason == LEGACY_CODE_PROFILE_REASON
            else None
        )
        return payload


class ExecutionContractError(ValueError):
    code = "invalid_execution_contract"

    def __init__(self, message: str, *, errors: list[dict[str, Any]]) -> None:
        super().__init__(message)
        self.message = message
        self.errors = errors

    def detail(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "message": self.message,
            "field": "execution_contract",
            "field_errors": self.errors,
            "recoverable": True,
        }


def validate_execution_contract(value: Any) -> ExecutionContract:
    """Validate untrusted or persisted contracts without leaking ValidationError."""

    raw_profile = (
        value.get("profile", WorkloadProfile.AUTOMATIC)
        if isinstance(value, Mapping)
        else WorkloadProfile.AUTOMATIC
    )
    # Preserve the stable, actionable profile error rather than Pydantic's
    # internal enum diagnostics at every API/CLI/MCP boundary.
    normalize_workload_profile(raw_profile)
    try:
        return ExecutionContract.model_validate(value)
    except WorkloadProfileError:
        raise
    except ValidationError as exc:
        errors = [
            {
                "field": ".".join(str(part) for part in item.get("loc", ())),
                "message": str(item.get("msg") or "Invalid value"),
                "type": str(item.get("type") or "value_error"),
            }
            for item in exc.errors(include_url=False)
        ]
        raise ExecutionContractError(
            "The execution contract is malformed. Correct the reported fields and retry.",
            errors=errors,
        ) from exc


class MaterializationPlan(BaseModel):
    contract_version: Literal[1] = 1
    profile: ExecutionProfile
    profile_source: str
    profile_normalization_reason: str | None = None
    requirements: ExecutionRequirements
    target_instance_id: str
    repositories: list[dict[str, Any]] = Field(default_factory=list)
    workspace: dict[str, Any]
    missing_dependencies: list[dict[str, str]] = Field(default_factory=list)
    stale_dependencies: list[dict[str, str]] = Field(default_factory=list)
    confirmation_required: bool = False
    summary: str

    @property
    def admissible(self) -> bool:
        return not (
            self.confirmation_required
            or self.missing_dependencies
            or self.stale_dependencies
        )


def resolve_materialization_plan(
    *,
    requested: ExecutionContract | None,
    card: Any,
    project: Any,
    project_repositories: list[tuple[Any, Any]],
    explicit_repositories: list[Any],
    target_instance_id: str,
) -> MaterializationPlan:
    project_default = dict(getattr(project, "tool_config", None) or {}).get(
        "execution_contract"
    )
    if requested and requested.profile != ExecutionProfile.AUTOMATIC:
        contract, source = requested, "dispatch_override"
    elif project_default:
        contract, source = validate_execution_contract(project_default), "project"
    else:
        contract, source = requested or ExecutionContract(), "automatic"
    req = contract.requirements.model_copy(deep=True)
    if not req.repositories:
        req.repositories = [
            RepositoryRequirement(
                repository_id=r.id, branch=getattr(link, "branch", None)
            )
            for r, link in project_repositories
        ]
        if explicit_repositories:
            req.repositories = [
                RepositoryRequirement(repository_id=r.id) for r in explicit_repositories
            ]
    profile, confirmation = contract.profile, False
    if profile == ExecutionProfile.AUTOMATIC:
        if req.repository_required or req.repositories:
            profile, source = ExecutionProfile.REPOSITORY, "declared_resources"
        elif req.browser or req.external_tools:
            profile, source = ExecutionProfile.OPERATIONS, "declared_resources"
        elif card is None:
            profile = ExecutionProfile.RESEARCH
        else:
            profile, confirmation = ExecutionProfile.RESEARCH, not contract.confirmed
    missing: list[dict[str, str]] = []
    if profile == ExecutionProfile.REPOSITORY:
        req.repository_required = True
        if not req.repositories:
            missing.append(
                {
                    "resource": "repository",
                    "reason": "Repository work requires at least one repository.",
                }
            )
    elif req.repository_required:
        missing.append(
            {
                "resource": "execution_profile",
                "reason": "Non-Git profile contradicts repository-required deliverables.",
            }
        )
    selected = {
        r.id: r
        for r in (explicit_repositories or [row[0] for row in project_repositories])
    }
    repos = []
    for requirement in req.repositories:
        repository = selected.get(requirement.repository_id)
        if not repository:
            missing.append(
                {
                    "resource": f"repository:{requirement.repository_id}",
                    "reason": "Repository is unavailable or unauthorized.",
                }
            )
            continue
        repos.append(
            {
                "repository_id": repository.id,
                "name": repository.name or repository.url,
                "url": repository.url,
                "branch": requirement.branch or repository.default_branch,
                "base_ref": requirement.base_ref,
                "worktree_required": requirement.worktree_required,
            }
        )
    if profile == ExecutionProfile.REPOSITORY:
        summary = f"Code workspace: {', '.join(r['name'] for r in repos) or 'missing repository'}, isolated worktree on target instance"
        workspace = {"kind": "repository", "leased": True, "durable_artifacts": False}
    elif profile == ExecutionProfile.OPERATIONS:
        resources = (["browser"] if req.browser else []) + req.external_tools
        summary, workspace = (
            f"Operations task: {' + '.join(resources) or 'external tools'}; no source checkout",
            {"kind": "operational", "leased": True, "durable_artifacts": True},
        )
    else:
        summary, workspace = (
            "Research workspace: no Git repository required",
            {"kind": "artifact", "leased": True, "durable_artifacts": True},
        )
    return MaterializationPlan(
        profile=profile,
        profile_source=source,
        profile_normalization_reason=contract.profile_normalization_reason,
        requirements=req,
        target_instance_id=target_instance_id,
        repositories=repos,
        workspace=workspace,
        missing_dependencies=missing,
        confirmation_required=confirmation,
        summary=summary,
    )

"""Safe diagnostic values shared by supervision and its read-only presentation."""
from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field

from pa.pr_supervisor.models import GitHubCapability, PR_WATCH_PROTOCOL_VERSION, utcnow

Reason = Literal["scope_denied", "scope_config_invalid", "scope_config_unavailable",
                 "credentials_unavailable", "credentials_rejected", "verification_unavailable", "repository_access_denied", "capability_stale",
                 "capability_incompatible", "authority_unreachable", "authority_response_invalid",
                 "no_candidates"]
ACTIONS = {
    "scope_denied": "Review the exact repository scope in Settings on the named instance.",
    "scope_config_invalid": "Repair the invalid local GitHub scope configuration, then review its exact scope in Settings.",
    "scope_config_unavailable": "Restore the missing or unreadable local GitHub scope configuration; environment credentials do not establish scope.",
    "verification_unavailable": "GitHub credential verification is unavailable. Check provider connectivity; automatic retry is scheduled.",
    "credentials_rejected": "GitHub rejected the named instance's credential. Check its authentication.",
    "credentials_unavailable": "Check GitHub authentication on the named instance.",
    "repository_access_denied": "Check the existing credential's access to this repository on the named instance.",
    "capability_stale": "Check the named instance's capability publisher and its connection to the authority.",
    "capability_incompatible": "Upgrade the named instance to a compatible supervision protocol.",
    "authority_unreachable": "Check the supervision authority connection. Automatic retry is scheduled.",
    "authority_response_invalid": "Check the supervision authority's version and capability response. Automatic retry is scheduled.",
    "no_candidates": "No capability advertisements are known in the bounded authority inventory. Check instance capability publication.",
}


class EligibilityCandidate(BaseModel):
    instance_id: str
    instance_name: str | None = None
    scope_mode: str | None
    repositories: list[str] | None
    policy_source: str
    policy_revision: str | None
    configuration_status: str
    authenticated: bool
    observed_at: datetime
    authority_received_at: datetime | None = None
    freshness: Literal["fresh", "stale", "invalid"]
    reason_code: Reason | None = None
    action: str | None = None


class EligibilityReport(BaseModel):
    dependency: Literal["capability_inventory", "github_repository_observation"] = "capability_inventory"
    eligible: list[str] = Field(default_factory=list)
    evaluation_state: Literal["complete", "unavailable"] = "complete"
    authority_instance_id: str | None = None
    authority_reference: str = "configured supervision authority"
    history_seconds: int | None = None
    observed_at: datetime = Field(default_factory=utcnow)
    candidates: list[EligibilityCandidate] = Field(default_factory=list)
    reason_code: Reason | None = None

    def summary(self) -> str:
        if self.reason_code:
            return f"{self.authority_instance_id or self.authority_reference}: {self.reason_code}. {ACTIONS[self.reason_code]}"
        failures = [f"{c.instance_name or c.instance_id} ({c.instance_id}): {c.reason_code}. {c.action}" for c in self.candidates if c.reason_code]
        return " ".join(failures)[:1900] or "An eligible instance is available."


def validate_advertisement(payload: object) -> GitHubCapability:
    # Older advertisements explicitly carried this list. Missing must not be
    # reinterpreted via the model's in-process construction defaults.
    if not isinstance(payload, dict) or not isinstance(payload.get("allowed_repositories"), list) or "checked_at" not in payload:
        raise ValueError("invalid capability advertisement")
    if type(payload.get("authenticated", False)) is not bool:
        raise ValueError("invalid capability authentication state")
    return GitHubCapability.model_validate(payload)


def evaluate(capabilities: list[GitHubCapability], repository: str | None, *,
             authority_instance_id: str | None, ttl: int = 120,
             now: datetime | None = None) -> EligibilityReport:
    now = now or utcnow()
    report = EligibilityReport(authority_instance_id=authority_instance_id, observed_at=now)
    for item in capabilities:
        age = (now - item.checked_at).total_seconds()
        freshness = "invalid" if age < -5 else "stale" if age > ttl else "fresh"
        mode = item.scope_mode or ("allowlist" if item.allowed_repositories else "unrestricted")
        reason = None
        if freshness != "fresh":
            reason = "capability_stale"
        elif item.pr_watch_protocol_version < PR_WATCH_PROTOCOL_VERSION:
            reason = "capability_incompatible"
        elif item.configuration_status != "valid":
            reason = "scope_config_invalid" if item.configuration_status == "invalid" else "scope_config_unavailable"
        elif item.state in ("verification_unavailable", "credentials_rejected"):
            reason = item.state
        elif not item.authenticated:
            reason = "credentials_unavailable"
        elif item.state == "repository_access_denied":
            reason = "repository_access_denied"
        elif repository and not item.supports(repository):
            reason = "scope_denied"
        report.candidates.append(EligibilityCandidate(
            instance_id=item.instance_id, instance_name=item.instance_name, scope_mode=mode if item.configuration_status == "valid" else None,
            repositories=item.allowed_repositories if item.configuration_status == "valid" else None,
            policy_source=item.policy_source, policy_revision=item.policy_revision,
            configuration_status=item.configuration_status, authenticated=item.authenticated,
            observed_at=item.checked_at, authority_received_at=item.authority_received_at,
            freshness=freshness, reason_code=reason,
            action=ACTIONS.get(reason)))
        if reason is None:
            report.eligible.append(item.instance_id)
    if not capabilities:
        report.reason_code = "no_candidates"
    return report

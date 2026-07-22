from __future__ import annotations

import hashlib
import json
import re
from typing import Any

from pa.pr_supervisor.models import GateResult, PRCheck, PRPolicy, PRSnapshot, PRWatch
from pa.prompts import PROMPTS, RenderedPrompt

_FAILURES = {
    "failure",
    "cancelled",
    "timed_out",
    "action_required",
    "startup_failure",
    "stale",
}
_SUCCESS = {"success"}
_SECRET_PATTERNS = (
    re.compile(r"\b(?:gh[pousr]_[A-Za-z0-9_]{20,}|github_pat_[A-Za-z0-9_]{20,})\b"),
    re.compile(r"(?i)\bBearer\s+[A-Za-z0-9._~+/-]{12,}"),
    re.compile(r"(?i)\b(token|secret|password|api[_-]?key)\s*[:=]\s*[^\s,;]+"),
)
_CONTROL = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f\x7f]")


def condition_fingerprint(data: dict[str, Any]) -> str:
    encoded = json.dumps(data, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode()).hexdigest()[:24]


def evaluate_gate(
    snapshot: PRSnapshot, policy: PRPolicy, *, stable_head: bool
) -> GateResult:
    reasons: list[str] = []
    actionable_reasons: list[str] = []
    pending_reasons: list[str] = []
    failing: list[PRCheck] = []
    pending: list[PRCheck] = []
    allowed = _SUCCESS | {value.lower() for value in policy.allowed_neutral_conclusions}

    if snapshot.draft:
        actionable_reasons.append("pull request is draft")
    if snapshot.state.lower() != "open":
        pending_reasons.append(f"pull request state is {snapshot.state}")
    if not snapshot.branch_protection_known:
        pending_reasons.append("branch protection could not be verified")
    if not snapshot.required_checks_known:
        pending_reasons.append("required-check policy could not be verified")
    if not snapshot.review_threads_known:
        pending_reasons.append("review threads could not be verified")

    for check in snapshot.checks:
        conclusion = (check.conclusion or "").lower()
        if check.required:
            if conclusion in _FAILURES:
                failing.append(check)
                actionable_reasons.append(
                    f"required check {check.name} concluded {conclusion}"
                )
            elif not check.terminal or conclusion not in allowed:
                pending.append(check)
                pending_reasons.append(f"required check {check.name} is pending")

    unresolved = [thread for thread in snapshot.review_threads if thread.actionable]
    if unresolved:
        actionable_reasons.append(
            f"{len(unresolved)} unresolved actionable review thread(s)"
        )

    decision = (snapshot.review_decision or "").upper()
    if decision == "CHANGES_REQUESTED":
        actionable_reasons.append("review decision is changes requested")
    if snapshot.approvals < snapshot.required_approvals:
        pending_reasons.append(
            f"approvals {snapshot.approvals}/{snapshot.required_approvals}"
        )

    merge_state = (snapshot.mergeable_state or "").lower()
    if snapshot.mergeable is False or merge_state in {"dirty", "conflicting"}:
        actionable_reasons.append("pull request has merge conflicts")
    elif snapshot.mergeable is not True or merge_state != "clean":
        pending_reasons.append(
            f"merge state is {snapshot.mergeable_state or 'unknown'}"
        )

    if not stable_head:
        pending_reasons.append("head SHA is not yet stable")

    reasons.extend(actionable_reasons)
    reasons.extend(pending_reasons)
    fingerprint_data = {
        "state": snapshot.state,
        "draft": snapshot.draft,
        "head_sha": snapshot.head_sha,
        "stable_head": stable_head,
        "checks": [
            {
                "name": check.name,
                "required": check.required,
                "status": check.status,
                "conclusion": check.conclusion,
            }
            for check in sorted(snapshot.checks, key=lambda item: item.name)
        ],
        "review_decision": snapshot.review_decision,
        "approvals": [snapshot.approvals, snapshot.required_approvals],
        "threads": [
            {
                "id": thread.id,
                "resolved": thread.resolved,
                "outdated": thread.outdated,
                "body_hash": hashlib.sha256(thread.body.encode()).hexdigest()[:12],
            }
            for thread in sorted(snapshot.review_threads, key=lambda item: item.id)
        ],
        "mergeable": snapshot.mergeable,
        "mergeable_state": snapshot.mergeable_state,
        "known": [
            snapshot.branch_protection_known,
            snapshot.required_checks_known,
            snapshot.review_threads_known,
        ],
    }
    actionable = bool(actionable_reasons)
    is_pending = bool(pending_reasons)
    return GateResult(
        green=not actionable and not is_pending,
        actionable=actionable,
        pending=is_pending,
        reasons=reasons,
        failing_checks=failing,
        pending_checks=pending,
        unresolved_threads=unresolved,
        fingerprint=condition_fingerprint(fingerprint_data),
    )


def redact_external_text(value: str | None, *, limit: int = 12000) -> str:
    text = _CONTROL.sub("", str(value or ""))[:limit]
    for pattern in _SECRET_PATTERNS:
        text = pattern.sub("[REDACTED]", text)
    return text


def redact_external_value(value: Any) -> Any:
    if isinstance(value, str):
        return redact_external_text(value)
    if isinstance(value, list):
        return [redact_external_value(item) for item in value]
    if isinstance(value, dict):
        return {str(key): redact_external_value(item) for key, item in value.items()}
    return value


def _external_payload(
    snapshot: PRSnapshot, gate: GateResult, *, limit: int = 48_000
) -> str:
    data = {
        "pull_request": {
            "title": redact_external_text(snapshot.title),
            "url": snapshot.url,
            "head_sha": snapshot.head_sha,
            "base_branch": snapshot.base_branch,
            "mergeable_state": snapshot.mergeable_state,
        },
        "failing_checks": [
            {
                "name": redact_external_text(check.name, limit=500),
                "conclusion": check.conclusion,
                "details_url": check.details_url,
                "title": redact_external_text(check.title),
                "summary": redact_external_text(check.summary),
                "log_excerpt": redact_external_text(check.text),
            }
            for check in gate.failing_checks
        ],
        "review_threads": [
            {
                "id": thread.id,
                "path": redact_external_text(thread.path, limit=1000),
                "line": thread.line,
                "url": thread.url,
                "author": redact_external_text(thread.author, limit=500),
                "comment": redact_external_text(thread.body),
            }
            for thread in gate.unresolved_threads
        ],
    }
    # Escaping angle brackets prevents external text from closing our delimiter.
    encoded = json.dumps(data, indent=2, ensure_ascii=False).replace("<", "\\u003c")
    if len(encoded) <= limit:
        return encoded

    # Keep the delimiter payload valid JSON while bounding arbitrarily large check
    # and review collections. Binary search accounts for JSON escaping overhead.
    low, high = 0, len(encoded)
    while low < high:
        midpoint = (low + high + 1) // 2
        bounded = json.dumps(
            {
                "truncated": True,
                "notice": "External GitHub data exceeded the supervisor prompt budget.",
                "excerpt": encoded[:midpoint],
            },
            indent=2,
            ensure_ascii=False,
        )
        if len(bounded) <= limit:
            low = midpoint
        else:
            high = midpoint - 1
    return json.dumps(
        {
            "truncated": True,
            "notice": "External GitHub data exceeded the supervisor prompt budget.",
            "excerpt": encoded[:low],
        },
        indent=2,
        ensure_ascii=False,
    )


def build_executor_prompt_rendered(
    watch: PRWatch,
    snapshot: PRSnapshot,
    gate: GateResult,
    *,
    green: bool,
    merged: bool = False,
    provider: str = "default",
) -> RenderedPrompt:
    action_key = (
        "pr_supervisor.action.merged"
        if merged
        else "pr_supervisor.action.green"
        if green
        else "pr_supervisor.action.required"
    )
    action = PROMPTS.render(action_key, provider=provider)
    reasons = "\n".join(f"- {reason}" for reason in gate.reasons)
    if not reasons:
        reasons = "- all gate conditions satisfied" if green else "- merged"
    return PROMPTS.render(
        "pr_supervisor.executor",
        {
            "action": action.text,
            "repository": {"url": watch.repository},
            "pull_request": {
                "number": watch.pr_number,
                "url": watch.pr_url,
            },
            "base_sha": snapshot.head_sha,
            "branch": watch.policy.integration_branch or snapshot.base_branch,
            "card": {"id": watch.card_id or "unlinked"},
            "project": {"id": watch.project_id or "unlinked"},
            "worktree": {
                "path": watch.executor_cwd or "resolve from the card/session context"
            },
            "supervisor": {"conditions": reasons},
            "github": {"external_content": _external_payload(snapshot, gate)},
        },
        provider=provider,
        reserve_context=True,
    )


def build_executor_prompt(
    watch: PRWatch,
    snapshot: PRSnapshot,
    gate: GateResult,
    *,
    green: bool,
    merged: bool = False,
) -> str:
    return build_executor_prompt_rendered(
        watch, snapshot, gate, green=green, merged=merged
    ).text

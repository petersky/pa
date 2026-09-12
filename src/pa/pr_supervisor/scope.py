"""Instance-local repository scope; only the running server calls the writer.

The credential document is updated in place, never exported or duplicated into
the domain store. Scope, audit and replay receipts share one atomic commit.
"""
from __future__ import annotations

import hashlib
import json
import threading
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

from pa.core.io import atomic_write_json
from pa.pr_supervisor.github import GitHubCredentials
from pa.pr_supervisor.models import canonical_repository_name
from pa.pr_supervisor.policy import PolicyLoadError, parse_policy, read_document

_LOCK = threading.RLock()
_META = "pa_supervision_scope"


class ScopeError(ValueError):
    def __init__(self, code: str, message: str, status: int = 409):
        super().__init__(message)
        self.code = code
        self.status = status


def normalize_repositories(values: list[str]) -> list[str]:
    try:
        return sorted({canonical_repository_name(value) for value in values})
    except (ValueError, TypeError, AttributeError):
        raise ScopeError("invalid_repository", "Use owner/name or a supported GitHub HTTPS/SSH URL.", 422) from None


def _read(data_dir: Path) -> dict:
    try:
        payload = read_document(data_dir)
        parse_policy(payload)
        return payload
    except PolicyLoadError as exc:
        if exc.status == "missing":
            return {}  # A first exact-list update still requires access proof and consent.
        raise ScopeError(exc.code, str(exc), 503) from None


def _snapshot(payload: dict) -> dict:
    if not payload:
        return {"allowed_repositories": [], "revision": "missing", "scope_mode": "none",
                "policy_source": "unconfigured", "configuration_status": "missing"}
    return parse_policy(payload).public()


def snapshot(data_dir: Path) -> dict:
    with _LOCK:
        return _snapshot(_read(data_dir))


def _receipt_result(receipt: dict) -> dict:
    # Receipts share a credential document; return only the public scope fields.
    result = receipt["result"]
    return {key: result[key] for key in ("allowed_repositories", "revision", "scope_mode")}


def prepare(data_dir: Path, *, repositories: list[str], expected_revision: str,
            actor: str, idempotency_key: str, confirmed_additions: list[str],
            confirmation_id: str) -> tuple[dict, dict, GitHubCredentials, str]:
    """Capture validation inputs, or replay a committed receipt before CAS."""
    repositories = normalize_repositories(repositories)
    if not repositories:
        raise ScopeError("empty_scope_forbidden", "Specify at least one repository; clearing the list would enable unrestricted supervision.", 422)
    fingerprint = hashlib.sha256(json.dumps({
        "repositories": repositories, "expected_revision": expected_revision,
        "actor": actor, "confirmed_additions": normalize_repositories(confirmed_additions),
        "confirmation_id": confirmation_id,
    }, sort_keys=True).encode()).hexdigest()
    with _LOCK:
        payload = _read(data_dir)
        receipt = payload.get(_META, {}).get("receipts", {}).get(idempotency_key)
        if receipt:
            if receipt["fingerprint"] != fingerprint:
                raise ScopeError("idempotency_conflict", "This idempotency key was used for a different scope request.")
            return {**_receipt_result(receipt), "duplicate": True}, {}, GitHubCredentials(), fingerprint
        current = _snapshot(payload)
        if current["revision"] != expected_revision:
            raise ScopeError("scope_revision_conflict", "GitHub scope changed; refresh and review the change again.")
        additions = sorted(set(repositories) - set(current["allowed_repositories"]))
        if additions and (normalize_repositories(confirmed_additions) != additions or not confirmation_id.strip()):
            raise ScopeError("scope_confirmation_required", "Confirm the exact additions from the scope preview before updating.")
        credentials = GitHubCredentials.load(data_dir)
        if not credentials.token:
            raise ScopeError("github_unauthenticated", "Configure GitHub credentials on this instance first.", 401)
        return {**current, "additions": additions, "candidate": repositories}, payload, credentials, fingerprint


def commit(data_dir: Path, *, original: dict, credentials: GitHubCredentials,
           repositories: list[str], actor: str, instance_id: str,
           idempotency_key: str, fingerprint: str, confirmation_id: str) -> dict:
    with _LOCK:
        current = _read(data_dir)
        receipt = current.get(_META, {}).get("receipts", {}).get(idempotency_key)
        if receipt:
            if receipt["fingerprint"] != fingerprint:
                raise ScopeError("idempotency_conflict", "This idempotency key was used for a different scope request.")
            return {**_receipt_result(receipt), "duplicate": True}
        # Recheck the whole document and effective credentials after the network
        # probe. Preserve unrelated metadata and reject concurrent token rotation.
        if current != original or GitHubCredentials.load(data_dir) != credentials:
            raise ScopeError("scope_revision_conflict", "GitHub configuration changed during validation; refresh and retry.")
        previous = _snapshot(current)
        revision = str(uuid4())
        result = {"allowed_repositories": repositories, "revision": revision,
                  "scope_mode": "allowlist", "duplicate": False}
        metadata = dict(current.get(_META, {}))
        audit = list(metadata.get("audit", []))
        audit.append({"event_id": str(uuid4()), "timestamp": datetime.now(UTC).isoformat(),
                      "action": "github.supervision_scope_updated", "actor": actor,
                      "instance_id": instance_id, "previous_revision": previous["revision"],
                      "revision": revision, "before": previous["allowed_repositories"],
                      "after": repositories, "confirmation_id": confirmation_id,
                      "idempotency_key": idempotency_key})
        receipts = dict(metadata.get("receipts", {}))
        receipts[idempotency_key] = {"fingerprint": fingerprint, "result": result}
        metadata.update(revision=revision, audit=audit, receipts=receipts, schema_version=1, mode="allowlist")
        current["allowed_repositories"] = repositories
        current[_META] = metadata
        atomic_write_json(data_dir / "integrations" / "github.json", current, mode=0o600)
        return result


def audit(data_dir: Path) -> list[dict]:
    with _LOCK:
        return list(reversed(_read(data_dir).get(_META, {}).get("audit", [])))

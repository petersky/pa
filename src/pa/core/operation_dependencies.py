"""Explicit recovery dependencies, independent of authentication policy.

Unknown operations remain global-history dependent. Modules may mark narrowly
authenticated local control/reporting endpoints with ``local_operational``;
this is the extension seam for the health journal. It grants no credentials or
realm permissions and must never annotate sync push, ref changes, workspace
creation, user/role changes, or permission grants.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
import re


class OperationDependency(StrEnum):
    PASSIVE = "passive"
    LOCAL_OPERATIONAL = "local_operational"
    RECOVERY = "recovery"
    REALM_HISTORY = "realm_history"
    GLOBAL_HISTORY = "global_history"


def local_operational(endpoint):
    """Declare a local operation with no canonical-history prerequisite.

    Apply below the route decorator. Existing middleware and endpoint-specific
    authentication/authorization remain mandatory, including fleet credentials.
    """
    endpoint.pa_operation_dependency = OperationDependency.LOCAL_OPERATIONAL
    return endpoint


@dataclass(frozen=True)
class Dependency:
    kind: OperationDependency
    realm_source: str | None = None
    body_constraint: str | None = None


def classify_operation(method: str, path: str, *, endpoint=None) -> Dependency:
    if method == "GET" and path == "/api/sync/check":
        # Legacy GET actually converges refs; it is not a passive status route.
        return Dependency(OperationDependency.REALM_HISTORY, "query")
    if method in {"GET", "HEAD", "OPTIONS"}:
        return Dependency(OperationDependency.PASSIVE)
    if getattr(endpoint, "pa_operation_dependency", None) == OperationDependency.LOCAL_OPERATIONAL:
        return Dependency(OperationDependency.LOCAL_OPERATIONAL)
    if method == "POST" and path.startswith("/api/operation-recovery/"):
        return Dependency(OperationDependency.RECOVERY)
    if method == "POST" and path in {"/api/auth/login", "/api/auth/logout", "/login"}:
        # Credential verification / cookie control only; ordinary auth policy
        # and the login form CSRF check still run. No user or grant mutation.
        return Dependency(OperationDependency.LOCAL_OPERATIONAL)
    if method == "POST" and path in {
        "/api/sync/get", "/api/sync/have", "/api/sync/need",
        "/api/sync/recovery", "/api/sync/reconcile",
    }:
        return Dependency(OperationDependency.RECOVERY)
    if method == "POST" and (path in {"/api/agent/quiesce", "/api/agent/unquiesce"}
            or re.fullmatch(r"/api/fleet/dispatch/[A-Za-z0-9-]{1,80}/progress", path)):
        return Dependency(OperationDependency.LOCAL_OPERATIONAL)
    if method == "POST" and re.fullmatch(r"/api/fleet/dispatch-jobs/[A-Za-z0-9-]{1,80}/checkpoint", path):
        # Operator input creates a canonical notification; a plain checkpoint
        # writes only the owned progress ledger/outbox.
        return Dependency(OperationDependency.LOCAL_OPERATIONAL, body_constraint="no_operator_input")
    if method == "POST" and path in {
        "/api/cards", "/api/sync/push", "/api/sync/converge", "/api/sync/conflicts/resolve",
    }:
        return Dependency(OperationDependency.REALM_HISTORY, "body")
    if method == "PATCH" and re.fullmatch(r"/api/cards/[^/]+", path):
        return Dependency(OperationDependency.REALM_HISTORY, "query")
    if (method == "POST" and path in {"/cards", "/items", "/partials/cards/new"}
            or method in {"POST", "DELETE"} and re.fullmatch(r"/partials/cards/[^/]+(?:/move|/project-change)?", path)
            or method == "POST" and re.fullmatch(r"/api/cards/[^/]+/project-change", path)):
        return Dependency(OperationDependency.REALM_HISTORY, "query")
    return Dependency(OperationDependency.GLOBAL_HISTORY)

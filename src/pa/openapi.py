"""OpenAPI contract for PA's middleware-enforced HTTP requirements."""

from __future__ import annotations

import re
from typing import Any

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi

from pa.auth.csrf import COOKIE_NAME as CSRF_COOKIE_NAME
from pa.auth.csrf import HEADER_NAME as CSRF_HEADER_NAME
from pa.auth.sessions import SessionManager

PUBLIC_API_PATHS = {
    "/api/health",
    "/api/auth/login",
    "/api/fleet/join",
    "/api/pr-supervisor/webhook/github",
}
MUTATING_METHODS = {"post", "put", "patch", "delete"}


def _error_response(description: str) -> dict[str, Any]:
    return {
        "description": description,
        "content": {
            "application/json": {
                "schema": {"$ref": "#/components/schemas/HTTPError"},
            }
        },
    }


def _is_instance_api(path: str) -> bool:
    return path.startswith(("/api/sync/", "/api/fleet/dispatch/"))


def _is_hybrid_instance_api(path: str) -> bool:
    return bool(
        re.fullmatch(
            r"/api/fleet/(?:instances/\{[^}]+\}/)?dispatch-jobs/\{[^}]+\}"
            r"(?:/(?:retry|cancel|prompt))?",
            path,
        )
        or path == "/api/fleet/dispatch"
        or re.fullmatch(r"/api/fleet/instances/\{[^}]+\}/agent/start", path)
        or re.fullmatch(r"/api/agent/sessions/\{[^}]+\}/prompt", path)
    )


def _document_operation(path: str, method: str, operation: dict[str, Any]) -> None:
    if path in PUBLIC_API_PATHS:
        operation["security"] = []
        return

    mutating = method in MUTATING_METHODS
    if _is_instance_api(path):
        operation["security"] = [{"instanceBearer": []}]
    elif mutating:
        operation["security"] = [
            {
                "paSession": [],
                "paCsrfCookie": [],
                "paCsrfHeader": [],
            },
            {"userBearer": []},
        ]
        if _is_hybrid_instance_api(path):
            operation["security"].append({"instanceBearer": []})
    else:
        operation["security"] = [{"paSession": []}, {"userBearer": []}]
        if _is_hybrid_instance_api(path):
            operation["security"].append({"instanceBearer": []})

    operation.setdefault("responses", {}).setdefault(
        "401",
        _error_response(
            "Authentication is missing or invalid. PA returns 401 before CSRF "
            "validation when authentication is required."
        ),
    )
    if mutating and not _is_instance_api(path):
        operation["responses"].setdefault(
            "403",
            _error_response(
                f"Cookie-authenticated mutation is missing a valid "
                f"{CSRF_HEADER_NAME} value matching the {CSRF_COOKIE_NAME} cookie."
            ),
        )


def _document_remote_dispatch(schema: dict[str, Any]) -> None:
    placement = schema["paths"].get("/api/fleet/dispatch", {}).get("post")
    if placement:
        placement["summary"] = "Resolve placement and durably dispatch work"
        placement["description"] = (
            "Accepts exactly one concrete `target_instance_id` or a centralized "
            "`placement_policy` (`best_match`, `least_busy`, `round_robin`, or "
            "`random_eligible`) plus an optional stable `group_id` and explicit "
            "execution/workload profile. PA expands project/realm defaults and "
            "considers only instances admitted by the shared group, participation, "
            "workload, project, repository, readiness, workspace, and capacity pipeline; "
            "uses working turns plus queued prompts plus durable pre-start "
            "reservations against the typed global/provider capacity, persists "
            "the explainable resolved target and reservation before admission, "
            "and returns that same target for idempotent retries. Idle/deferred "
            "sessions do not consume capacity. There is no authority/local fallback. "
            "Administrator capacity and named participation overrides require "
            "an explicit audited reason; self-protective hard limits remain enforced."
        )
        placement.setdefault("parameters", []).append(
            {
                "name": "Idempotency-Key",
                "in": "header",
                "required": False,
                "description": (
                    "Stable logical request key. A retry returns the original "
                    "resolved target instead of rerunning placement."
                ),
                "schema": {"type": "string", "minLength": 1},
            }
        )
        placement["responses"]["202"] = {
            "description": "Placement resolved and dispatch durably admitted.",
            "content": {
                "application/json": {
                    "schema": {"$ref": "#/components/schemas/DispatchAdmission"}
                }
            },
        }
        placement["responses"].setdefault(
            "409",
            _error_response(
                "No eligible instance, exhausted capacity, concurrent card "
                "dispatch, stale readiness, unauthorized override, or idempotency "
                "conflict. Capacity errors include limit/source, working, queued, "
                "reserved, freshness, and consumer links."
            ),
        )

    operation = (
        schema["paths"]
        .get("/api/fleet/instances/{instance_id}/agent/start", {})
        .get("post")
    )
    if not operation:
        return

    operation["summary"] = "Admit remote agent work"
    operation["description"] = (
        "Durably admits work on the target fleet instance identified by "
        "`instance_id`; the response does not wait for the provider to start. "
        "Set `card_id` to link the dispatch to a card. When `project_id` is omitted "
        "for card-linked work, PA inherits the card's project; an explicit "
        "`project_id` must exist. Standalone work may omit both values. "
        "Use `Idempotency-Key` for safe retries. Repeating a key for the same target "
        "and request returns the original dispatch with `duplicate: true`; reusing "
        "it for different work returns 409. If omitted, PA generates a key, so a "
        "client retry cannot be correlated."
    )
    operation.setdefault("parameters", []).append(
        {
            "name": "Idempotency-Key",
            "in": "header",
            "required": False,
            "description": (
                "Client-generated retry key, scoped to the target instance. "
                "Prefer this header over the legacy body field."
            ),
            "schema": {"type": "string", "minLength": 1},
            "example": "dispatch-2026-07-24-001",
        }
    )
    content = operation["requestBody"]["content"]["application/json"]
    content["examples"] = {
        "cardLinked": {
            "summary": "Card-linked work (project inherited from the card)",
            "value": {
                "card_id": "9a5e8b7c-2d41-4f84-a32c-9128f97dbe20",
                "message": "Implement the linked task and run focused tests.",
                "provider": "codex",
                "model_id": "gpt-5.2-codex",
            },
        },
        "projectLinked": {
            "summary": "Standalone work linked to a project",
            "value": {
                "project_id": "1f743692-86af-41a8-9b97-9db98c9db115",
                "title": "Investigate CI",
                "message": "Find the failing check and report the cause.",
                "provider": "codex",
            },
        },
    }
    operation["responses"]["202"] = {
        "description": (
            "Dispatch durably admitted. A repeated identical request has "
            "`duplicate: true` and returns the original identifiers."
        ),
        "content": {
            "application/json": {
                "schema": {"$ref": "#/components/schemas/DispatchAdmission"},
                "examples": {
                    "accepted": {
                        "value": {
                            "accepted": True,
                            "duplicate": False,
                            "dispatch_id": "33b67df4-5381-4d02-a23a-55e829f730f8",
                            "job_id": "33b67df4-5381-4d02-a23a-55e829f730f8",
                            "dispatch": {
                                "dispatch_id": "33b67df4-5381-4d02-a23a-55e829f730f8",
                                "card_id": "9a5e8b7c-2d41-4f84-a32c-9128f97dbe20",
                                "project_id": "1f743692-86af-41a8-9b97-9db98c9db115",
                                "target_instance_id": "02dbcd47-8f40-44eb-8403-5eb57545afc8",
                                "state": "queued",
                            },
                        }
                    }
                },
            }
        },
    }
    operation["responses"].setdefault(
        "409",
        _error_response(
            "The idempotency key was already used for different work, or dispatch "
            "cannot proceed until the instance configuration is corrected."
        ),
    )

    body_schema = schema["components"]["schemas"].get("RemoteAgentStartBody", {})
    properties = body_schema.get("properties", {})
    properties.get("card_id", {}).update(
        description="Existing card to link; its project is inherited when project_id is omitted."
    )
    properties.get("project_id", {}).update(
        description="Existing project to link, or an override for card-linked work."
    )
    properties.get("idempotency_key", {}).update(
        description="Legacy body fallback. Prefer the Idempotency-Key header.",
        deprecated=True,
    )


def install_openapi_contract(app: FastAPI) -> None:
    """Install PA's generated-client and interactive-documentation contract."""

    def pa_openapi() -> dict[str, Any]:
        if app.openapi_schema:
            return app.openapi_schema
        schema = get_openapi(
            title=app.title,
            version=app.version,
            description=(
                f"{app.description}\n\n"
                "When authentication is enabled, interactive browser clients use "
                f"the `{SessionManager.COOKIE_NAME}` session cookie. For POST, PUT, "
                f"PATCH, and DELETE, echo the readable `{CSRF_COOKIE_NAME}` cookie "
                f"as `{CSRF_HEADER_NAME}`. Bearer-authenticated clients do not send "
                "CSRF tokens. Missing or invalid authentication returns 401; a "
                "cookie-authenticated mutation with an absent or mismatched CSRF "
                "token returns 403."
            ),
            routes=app.routes,
        )
        components = schema.setdefault("components", {})
        security_schemes = components.setdefault("securitySchemes", {})
        security_schemes.update(
            {
                "paSession": {
                    "type": "apiKey",
                    "in": "cookie",
                    "name": SessionManager.COOKIE_NAME,
                    "description": "HttpOnly user session cookie returned by /api/auth/login.",
                },
                "paCsrfCookie": {
                    "type": "apiKey",
                    "in": "cookie",
                    "name": CSRF_COOKIE_NAME,
                    "description": "Readable double-submit token cookie.",
                },
                "paCsrfHeader": {
                    "type": "apiKey",
                    "in": "header",
                    "name": CSRF_HEADER_NAME,
                    "description": f"Must exactly match the {CSRF_COOKIE_NAME} cookie.",
                },
                "userBearer": {
                    "type": "http",
                    "scheme": "bearer",
                    "description": "User CLI token; bearer requests are CSRF-exempt.",
                },
                "instanceBearer": {
                    "type": "http",
                    "scheme": "bearer",
                    "description": "Shared instance token for fleet and sync operations.",
                },
            }
        )
        components.setdefault("schemas", {}).update(
            {
                "HTTPError": {
                    "type": "object",
                    "required": ["detail"],
                    "properties": {
                        "detail": {"oneOf": [{"type": "string"}, {"type": "object"}]}
                    },
                },
                "DispatchAdmission": {
                    "type": "object",
                    "required": [
                        "accepted",
                        "duplicate",
                        "dispatch_id",
                        "job_id",
                        "dispatch",
                    ],
                    "properties": {
                        "accepted": {"type": "boolean", "const": True},
                        "duplicate": {"type": "boolean"},
                        "dispatch_id": {"type": "string", "format": "uuid"},
                        "job_id": {"type": "string", "format": "uuid"},
                        "dispatch": {
                            "type": "object",
                            "required": [
                                "dispatch_id",
                                "target_instance_id",
                                "state",
                            ],
                            "properties": {
                                "dispatch_id": {"type": "string", "format": "uuid"},
                                "card_id": {
                                    "type": ["string", "null"],
                                    "format": "uuid",
                                },
                                "project_id": {
                                    "type": ["string", "null"],
                                    "format": "uuid",
                                },
                                "target_instance_id": {
                                    "type": "string",
                                    "format": "uuid",
                                },
                                "state": {"type": "string", "example": "queued"},
                            },
                            "additionalProperties": True,
                        },
                    },
                },
            }
        )
        for path, path_item in schema.get("paths", {}).items():
            for method, operation in path_item.items():
                if method in {"get", "post", "put", "patch", "delete"}:
                    _document_operation(path, method, operation)
        _document_remote_dispatch(schema)
        app.openapi_schema = schema
        return schema

    app.openapi = pa_openapi

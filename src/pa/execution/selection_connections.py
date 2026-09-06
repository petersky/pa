"""Configured connection profiles; secret references stay on the owning instance.

Codex: documented MODEL_PROVIDER + CODEX_CONFIG session overrides.
OpenInterpreter: documented invocation-scoped -c provider configuration.
Cursor: account API-key environment only; arbitrary endpoints are unsupported.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from urllib.parse import urlsplit

import httpx
from pydantic import Field, model_validator

from pa.acp.providers.metadata import load_credentials
from pa.execution.selection import (
    SelectionError,
    StrictModel,
    digest,
)
from pa.execution.selection_catalog import candidates_from_advertisement


class ConnectionProfile(StrictModel):
    version: int = 1
    id: str = Field(pattern=r"^[A-Za-z][A-Za-z0-9_-]{0,63}$")
    revision: int = Field(default=1, ge=1)
    harness: str
    backend: str = Field(min_length=1, max_length=100)
    account_label: str = Field(min_length=1, max_length=100)
    base_url: str | None = None
    wire_api: str | None = None
    credential_reference: str = Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]{0,99}$")
    enabled: bool = True

    @model_validator(mode="after")
    def supported_transport(self):
        if self.version != 1 or self.id == "default":
            raise ValueError(
                "Use a v1 named profile; default is reserved for existing adapter configuration"
            )
        if self.harness not in {"codex", "cursor", "openinterpreter"}:
            raise ValueError(
                "This adapter does not implement named connection profiles"
            )
        if self.harness == "cursor":
            if self.base_url or self.wire_api:
                raise ValueError(
                    "Cursor exposes account models; arbitrary backend endpoints are unsupported"
                )
        else:
            if not self.base_url:
                raise ValueError("A configured native provider endpoint is required")
            url = urlsplit(self.base_url)
            if (
                url.scheme not in {"http", "https"}
                or not url.hostname
                or url.username
                or url.password
                or url.query
                or url.fragment
            ):
                raise ValueError(
                    "Endpoint must be an HTTP(S) URL without credentials, query, or fragment"
                )
            if self.harness == "codex" and self.wire_api != "responses":
                raise ValueError(
                    "The Codex connection adapter supports Responses transport only"
                )
            if self.harness == "openinterpreter" and self.wire_api not in {
                "responses",
                "chat",
                "messages",
            }:
                raise ValueError(
                    "OpenInterpreter requires a supported native wire_api: responses, chat, or messages"
                )
        return self

    @property
    def native_id(self):
        return "pa_connection_" + digest([self.harness, self.id])[:16]

    @property
    def fingerprint(self):
        return digest(self.model_dump(mode="json"))


def connection_secret(data_dir, profile):
    secret = load_credentials(data_dir, profile.harness).get(
        profile.credential_reference
    )
    if not secret:
        raise SelectionError(
            "connection_credential_missing",
            "The selected connection credential reference is unavailable on this instance. Configure it through the adapter credential API.",
        )
    return secret


def connection_revision(profile, data_dir, *, secret=None):
    # A credential rotation may change accounts even when its reference stays
    # constant. Only a one-way opaque revision leaves this owning instance.
    return digest(
        [
            profile.fingerprint,
            secret if secret is not None else connection_secret(data_dir, profile),
        ]
    )


def apply_connection(
    spec, profile: ConnectionProfile, data_dir, *, model=None, _secret=None
):
    """Create a per-process overlay. No changes to shared homes, workers, or defaults."""
    if spec.id != profile.harness or not profile.enabled:
        raise SelectionError(
            "connection_incompatible",
            "Connection is disabled or belongs to another harness",
        )
    spec = spec.model_copy(deep=True)
    secret = _secret if _secret is not None else connection_secret(data_dir, profile)
    selected_key = (
        "CURSOR_API_KEY"
        if profile.harness == "cursor"
        else "PA_EXECUTION_CONNECTION_KEY"
    )
    excluded = set(load_credentials(data_dir, profile.harness)) | {
        "OPENAI_API_KEY",
        "ANTHROPIC_API_KEY",
        "MINIMAX_API_KEY",
        "CURSOR_API_KEY",
        "XAI_API_KEY",
        "GROK_API_KEY",
        "CODEX_API_KEY",
    }
    excluded.discard(selected_key)
    spec.excluded_env = sorted(set(spec.excluded_env) | excluded)
    spec.env = {k: v for k, v in spec.env.items() if k not in excluded}
    if profile.harness == "cursor":
        spec.env["CURSOR_API_KEY"] = secret
        return spec
    env_key = "PA_EXECUTION_CONNECTION_KEY"
    native = {
        "name": profile.account_label,
        "base_url": profile.base_url,
        "wire_api": profile.wire_api,
        "env_key": env_key,
    }
    spec.env[env_key] = secret
    if profile.harness == "codex":
        native["requires_openai_auth"] = False
        try:
            config = json.loads(spec.env.get("CODEX_CONFIG") or "{}")
        except ValueError as exc:
            raise SelectionError(
                "invalid_native_connection_configuration",
                "Existing CODEX_CONFIG is invalid; repair it before selecting a profile",
            ) from exc
        if not isinstance(config, dict):
            raise SelectionError(
                "invalid_native_connection_configuration",
                "CODEX_CONFIG must be an object",
            )
        config["model_provider"] = profile.native_id
        config["model_providers"] = {
            **config.get("model_providers", {}),
            profile.native_id: native,
        }
        spec.env["CODEX_CONFIG"] = json.dumps(config)
        spec.env["MODEL_PROVIDER"] = profile.native_id
    else:
        from pa.acp.providers.openinterpreter import _spawn_args

        args = []
        for key, value in native.items():
            args += [
                "-c",
                f"model_providers.{profile.native_id}.{key}={json.dumps(value)}",
            ]
        spec.args = args + _spawn_args(model_provider=profile.native_id, model=model)
    return spec


def apply_selected_connection(spec, selected, data_dir):
    from pa.execution.selection_store import SelectionStore

    profile = next(
        (
            p
            for p in SelectionStore(data_dir).connections()
            if p.id == selected["connection"]
        ),
        None,
    )
    secret = (
        connection_secret(data_dir, profile) if profile and profile.enabled else None
    )
    if (
        not profile
        or not profile.enabled
        or connection_revision(profile, data_dir, secret=secret)
        != selected.get("connection_revision")
        or profile.backend != selected.get("model_provider")
        or profile.harness != selected.get("harness")
    ):
        raise SelectionError(
            "connection_revision_changed",
            "The selected connection changed or is unavailable. Refresh and explicitly start a linked attempt; the original account/endpoint will not be silently replaced.",
        )
    return apply_connection(
        spec, profile, data_dir, model=selected.get("model"), _secret=secret
    )


async def discover_connection(profile, data_dir, *, timeout=8.0):
    """Bounded, explicit discovery. A successful initialize is not backend health."""
    from pa.acp.providers.probe import probe_acp_initialize
    from pa.acp.providers.registry import get_provider

    now = datetime.now(UTC)
    evidence, models = [], []
    readiness = "unknown"
    revision = profile.fingerprint  # Failed discovery never makes this usable.
    try:
        secret = await asyncio.to_thread(connection_secret, data_dir, profile)
        revision = connection_revision(profile, data_dir, secret=secret)
        spec = await asyncio.to_thread(
            get_provider(profile.harness).resolve_spawn, data_dir=data_dir
        )
        spec = apply_connection(spec, profile, data_dir, _secret=secret)

        async def backend_models():
            if not profile.base_url:
                return None
            headers = (
                {"x-api-key": secret, "anthropic-version": "2023-06-01"}
                if profile.wire_api == "messages"
                else {"Authorization": f"Bearer {secret}"}
            )
            async with (
                httpx.AsyncClient(timeout=timeout, follow_redirects=False) as client,
                client.stream(
                    "GET", profile.base_url.rstrip("/") + "/models", headers=headers
                ) as response,
            ):
                response.raise_for_status()
                raw = bytearray()
                async for chunk in response.aiter_bytes():
                    raw.extend(chunk)
                    if len(raw) > 256 * 1024:
                        raise ValueError("catalog_too_large")
                payload = json.loads(raw)
                return [
                    str(m["id"])
                    for m in payload.get("data", [])[:500]
                    if isinstance(m, dict)
                    and isinstance(m.get("id"), str)
                    and len(m["id"]) <= 300
                ]

        async with asyncio.timeout(timeout):
            probe, backend = await asyncio.gather(
                asyncio.to_thread(probe_acp_initialize, spec, timeout=timeout / 2),
                backend_models(),
                return_exceptions=True,
            )
        if isinstance(probe, dict) and probe.get("ok"):
            evidence.append("acp_initialize_succeeded")
        else:
            evidence.append("acp_initialize_failed_or_unknown")
            readiness = "unavailable"
        if isinstance(backend, list):
            models = backend
            evidence.append("backend_model_discovery_authenticated")
            if readiness != "unavailable":
                readiness = "ready"
        else:
            evidence.append("backend_models_unsupported_or_unknown")
            if isinstance(
                backend, httpx.HTTPStatusError
            ) and backend.response.status_code in {401, 403}:
                readiness = "unavailable"
                evidence.append("backend_authentication_failed")
    except Exception:  # noqa: BLE001 - optional discovery fails closed; never expose raw secret-bearing errors
        # Raw provider/network exceptions can contain credentials and endpoints.
        readiness = "unavailable"
        evidence.append("connection_discovery_failed_or_timed_out")
    candidates = candidates_from_advertisement(
        instance_id="",
        harness=profile.harness,
        connection=profile.id,
        model_provider=profile.backend,
        advertisement={"models": models},
        readiness=readiness,
        observed_at=now,
        source="native_initialize_and_backend_discovery",
        health_evidence=evidence,
    )
    return [
        c.model_copy(
            update={
                "connection_revision": revision,
                "native_model_provider": profile.native_id
                if profile.harness != "cursor"
                else None,
                "account_label": profile.account_label,
                "endpoint_label": profile.base_url,
                "can_attempt_unverified": readiness == "unknown",
            }
        )
        for c in candidates
    ]

"""Client used by PA's stdio MCP process to reach the sole local writer."""

from __future__ import annotations

import os
import time

import httpx

from pa.auth.users import UserDirectory
from pa.config import Settings


class LocalPAServerUnavailable(RuntimeError):
    pass


def local_pa_url(settings: Settings) -> str:
    explicit = os.environ.get("PA_LOCAL_API_URL", "").strip()
    if explicit:
        return explicit.rstrip("/")
    host = settings.host if settings.host not in {"0.0.0.0", "::"} else "127.0.0.1"
    return f"http://{host}:{settings.port}"


def request_local_pa(
    settings: Settings,
    method: str,
    path: str,
    *,
    params: dict | None = None,
    json: dict | None = None,
    allow_not_found: bool = False,
):
    token = os.environ.get("PA_LOCAL_API_TOKEN", "").strip()
    if not token:
        token = UserDirectory(settings.data_dir).ensure_default_user().cli_token
    headers = {"Authorization": f"Bearer {token}"}
    deadline = time.monotonic() + 10.0
    while True:
        try:
            response = httpx.request(
                method,
                f"{local_pa_url(settings)}{path}",
                params=params,
                json=json,
                headers=headers,
                timeout=min(2.0, max(0.1, deadline - time.monotonic())),
            )
            if allow_not_found and response.status_code == 404:
                return None
            response.raise_for_status()
            if response.status_code == 204:
                return None
            return response.json()
        except httpx.ConnectError as exc:
            if time.monotonic() >= deadline:
                raise LocalPAServerUnavailable(
                    "The owning PA server did not become reachable after 10 seconds. "
                    "Do not write PA_DATA_DIR from the MCP process."
                ) from exc
            time.sleep(0.1)
        except httpx.HTTPError as exc:
            status = getattr(exc.response, "status_code", None)
            detail = f" (HTTP {status})" if status is not None else ""
            raise LocalPAServerUnavailable(
                "The owning PA server rejected the MCP request"
                f"{detail}. Verify that the server and MCP bridge belong to the same "
                "PA instance; do not write PA_DATA_DIR from the MCP process."
            ) from exc

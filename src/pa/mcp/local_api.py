"""Client used by PA's stdio MCP process to reach the sole local writer."""

from __future__ import annotations

import httpx

from pa.auth.users import UserDirectory
from pa.config import Settings


class LocalPAServerUnavailable(RuntimeError):
    pass


def request_local_pa(
    settings: Settings,
    method: str,
    path: str,
    *,
    params: dict | None = None,
    json: dict | None = None,
    allow_not_found: bool = False,
):
    host = settings.host if settings.host not in {"0.0.0.0", "::"} else "127.0.0.1"
    user = UserDirectory(settings.data_dir).ensure_default_user()
    headers = {"Authorization": f"Bearer {user.cli_token}"}
    try:
        response = httpx.request(
            method,
            f"http://{host}:{settings.port}{path}",
            params=params,
            json=json,
            headers=headers,
            timeout=10.0,
        )
        if allow_not_found and response.status_code == 404:
            return None
        response.raise_for_status()
        if response.status_code == 204:
            return None
        return response.json()
    except httpx.HTTPError as exc:
        raise LocalPAServerUnavailable(
            "The local PA server is unavailable or rejected the request. Start the "
            "server that owns PA_DATA_DIR; do not write pa.db, sync_refs.json, or "
            "the object store from an agent process."
        ) from exc

"""ACP stdio transport helpers."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

from acp import spawn_agent_process

try:
    # Prefer the library default (50 MiB) when available.
    from acp.core import DEFAULT_STDIO_BUFFER_LIMIT_BYTES as STDIO_BUFFER_LIMIT_BYTES
except ImportError:  # pragma: no cover - older agent-client-protocol
    STDIO_BUFFER_LIMIT_BYTES = 50 * 1024 * 1024


def spawn_agent(
    to_client: Any,
    command: str,
    *args: str,
    env: Mapping[str, str] | None = None,
    cwd: str | Path | None = None,
    transport_kwargs: Mapping[str, Any] | None = None,
    **connection_kwargs: Any,
) -> Any:
    """Spawn an ACP agent with a large enough asyncio StreamReader limit.

    asyncio's default readline limit is 64 KiB. ACP frames are newline-delimited
    JSON and routinely exceed that (tool results, file contents, multimodal
    payloads), which kills the receive loop with::

        ValueError: Separator is not found, and chunk exceed the limit

    Pass the same elevated limit the ACP library uses for ``run_agent``.
    """
    merged = dict(transport_kwargs or {})
    merged.setdefault("limit", STDIO_BUFFER_LIMIT_BYTES)
    return spawn_agent_process(
        to_client,
        command,
        *args,
        env=env,
        cwd=cwd,
        transport_kwargs=merged,
        **connection_kwargs,
    )

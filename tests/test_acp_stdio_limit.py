"""ACP stdio StreamReader limit must be raised above asyncio's 64 KiB default."""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest

from pa.acp.transport import STDIO_BUFFER_LIMIT_BYTES, spawn_agent


def test_stdio_buffer_limit_is_well_above_asyncio_default() -> None:
    # asyncio.StreamReader default limit is 2**16 (64 KiB).
    assert STDIO_BUFFER_LIMIT_BYTES > 64 * 1024


def test_spawn_agent_passes_elevated_limit() -> None:
    captured: dict = {}

    def fake_spawn(to_client, command, *args, env=None, cwd=None, transport_kwargs=None, **kwargs):
        captured["transport_kwargs"] = dict(transport_kwargs or {})
        return "ctx"

    with patch("pa.acp.transport.spawn_agent_process", side_effect=fake_spawn):
        result = spawn_agent(object(), "agent", "--print")

    assert result == "ctx"
    assert captured["transport_kwargs"]["limit"] == STDIO_BUFFER_LIMIT_BYTES


def test_spawn_agent_preserves_caller_limit_override() -> None:
    captured: dict = {}

    def fake_spawn(to_client, command, *args, env=None, cwd=None, transport_kwargs=None, **kwargs):
        captured["transport_kwargs"] = dict(transport_kwargs or {})
        return "ctx"

    with patch("pa.acp.transport.spawn_agent_process", side_effect=fake_spawn):
        spawn_agent(object(), "agent", transport_kwargs={"limit": 12345})

    assert captured["transport_kwargs"]["limit"] == 12345


def test_asyncio_readline_fails_at_default_limit_but_passes_with_elevated() -> None:
    """Reproduce the production failure mode and show the elevated limit avoids it."""
    # Over asyncio's default 64 KiB limit; newline arrives only at the end of the frame
    # (same shape as a large ACP JSON-RPC line on stdio).
    body = b"x" * (64 * 1024 + 1)
    payload = body + b"\n"

    async def _run() -> None:
        default_reader = asyncio.StreamReader()  # default limit=2**16
        default_reader.feed_data(body)  # no separator yet → LimitOverrun
        with pytest.raises(ValueError, match="Separator is not found, and chunk exceed the limit"):
            await default_reader.readline()

        elevated = asyncio.StreamReader(limit=STDIO_BUFFER_LIMIT_BYTES)
        elevated.feed_data(payload)
        elevated.feed_eof()
        line = await elevated.readline()
        assert line == payload

    asyncio.run(_run())

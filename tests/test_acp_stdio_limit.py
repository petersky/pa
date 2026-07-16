"""ACP transport robustness: stdio buffer limit and dead-connection detection."""

from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

from pa.acp.client import (
    ACP_STDIO_BUFFER_LIMIT_BYTES,
    AgentConnection,
)
from pa.config import Settings
from pa.domain.models import AgentSession


class AcpStdioLimitTests(unittest.TestCase):
    def test_buffer_limit_exceeds_asyncio_default(self) -> None:
        # asyncio.StreamReader default is 64 KiB; ACP frames need far more.
        self.assertGreater(ACP_STDIO_BUFFER_LIMIT_BYTES, 64 * 1024)
        self.assertGreaterEqual(ACP_STDIO_BUFFER_LIMIT_BYTES, 10 * 1024 * 1024)

    def test_connect_passes_transport_limit(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp), agent_enabled=True)
            store = MagicMock()
            conn = AgentConnection(settings, store, agent_name="cursor")

            fake_spec = MagicMock()
            fake_spec.id = "cursor"
            fake_spec.command = "true"
            fake_spec.args = []
            fake_spec.env = {}

            entered = AsyncMock(return_value=(MagicMock(), MagicMock()))
            ctx = MagicMock()
            ctx.__aenter__ = entered
            ctx.__aexit__ = AsyncMock(return_value=None)

            init_resp = MagicMock()
            init_resp.agent_capabilities = None
            fake_acp = MagicMock()
            fake_acp.initialize = AsyncMock(return_value=init_resp)
            fake_session = MagicMock()
            fake_session.session_id = "ext-1"
            fake_acp.new_session = AsyncMock(return_value=fake_session)
            entered.return_value = (fake_acp, MagicMock())

            with (
                patch.object(conn, "_resolved_spec", return_value=fake_spec),
                patch("pa.acp.client.resolve_executable", return_value=None),
                patch("pa.acp.client.spawn_agent_process", return_value=ctx) as spawn,
                patch("pa.acp.client.pa_mcp_servers", return_value=[]),
                patch("pa.acp.client.extract_models_modes_config", return_value={}),
            ):
                asyncio.run(conn.connect())

            kwargs = spawn.call_args.kwargs
            self.assertEqual(
                kwargs.get("transport_kwargs"),
                {"limit": ACP_STDIO_BUFFER_LIMIT_BYTES},
            )


class AcpConnectedTests(unittest.TestCase):
    def test_connected_false_when_inner_transport_disconnected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp))
            store = MagicMock()
            conn = AgentConnection(settings, store)
            conn.session = AgentSession(agent_name="cursor", status="prompting")
            inner = MagicMock()
            inner._closed = False
            inner._disconnected = True
            wrapper = MagicMock()
            wrapper._conn = inner
            conn._conn = wrapper
            self.assertFalse(conn.connected)

    def test_mark_transport_dead_clears_connection(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            settings = Settings(data_dir=Path(tmp))
            store = MagicMock()
            conn = AgentConnection(settings, store)
            conn.session = AgentSession(agent_name="cursor", status="prompting")
            conn._conn = MagicMock()
            conn._ctx = None
            asyncio.run(conn._mark_transport_dead())
            self.assertIsNone(conn._conn)
            self.assertEqual(conn.session.status, "disconnected")
            self.assertFalse(conn.connected)


if __name__ == "__main__":
    unittest.main()

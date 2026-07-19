from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from pa.acp.client import AgentConnection, PAClient, _agent_supports_load
from pa.acp.providers.base import AgentProviderSpec
from pa.config import Settings
from pa.domain.models import AgentSession


class PAClientFileSystemTests(unittest.TestCase):
    def test_read_and_write_text_file_requests_are_supported(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "notes.txt"
            client = PAClient(MagicMock())

            async def run() -> None:
                await client.write_text_file("one\ntwo\nthree\n", str(target), "session-1")
                response = await client.read_text_file(
                    str(target), "session-1", line=2, limit=1
                )
                self.assertEqual(response.content, "two\n")

            asyncio.run(run())

    def test_file_requests_require_absolute_paths(self) -> None:
        client = PAClient(MagicMock())

        async def run() -> None:
            with self.assertRaisesRegex(ValueError, "absolute"):
                await client.read_text_file("relative.txt", "session-1")

        asyncio.run(run())

    def test_optional_extension_requests_are_acknowledged(self) -> None:
        wire = MagicMock()
        client = PAClient(MagicMock(), wire_logger=wire)

        async def run() -> None:
            self.assertEqual(await client.ext_method("cursor/todos", {"items": []}), {})
            await client.ext_notification("cursor/status", {"ready": True})

        asyncio.run(run())
        self.assertEqual(wire.call_count, 2)


class AgentSessionRestoreTests(unittest.TestCase):
    def test_load_capability_is_detected_in_dict_and_object_responses(self) -> None:
        self.assertTrue(
            _agent_supports_load({"agentCapabilities": {"loadSession": True}})
        )
        self.assertTrue(
            _agent_supports_load(
                SimpleNamespace(
                    agent_capabilities=SimpleNamespace(load_session=True)
                )
            )
        )

    def test_loads_existing_session_when_resume_is_not_supported(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MagicMock()
            acp = MagicMock()
            acp.initialize = AsyncMock(
                return_value={
                    "agentCapabilities": {
                        "loadSession": True,
                        "sessionCapabilities": {"resume": None},
                    }
                }
            )
            acp.load_session = AsyncMock(return_value=SimpleNamespace())
            acp.resume_session = AsyncMock()
            acp.new_session = AsyncMock()
            context = MagicMock()
            context.__aenter__ = AsyncMock(return_value=(acp, MagicMock()))
            context.__aexit__ = AsyncMock()
            existing = AgentSession(
                id="pa-session",
                agent_name="cursor",
                external_session_id="cursor-session",
                status="disconnected",
            )
            connection = AgentConnection(
                Settings(data_dir=Path(tmp)),
                store,
                provider_spec=AgentProviderSpec(
                    id="cursor",
                    display_name="Cursor",
                    command="agent",
                ),
            )

            async def run() -> None:
                with (
                    patch("pa.acp.client.spawn_agent", return_value=context),
                    patch("pa.acp.client.pa_mcp_servers", return_value=[]),
                ):
                    restored = await connection.connect(
                        resume_external_id="cursor-session",
                        existing_session=existing,
                    )
                self.assertIs(restored, existing)

            asyncio.run(run())

            acp.load_session.assert_awaited_once_with(
                cwd=str(Path(tmp)),
                session_id="cursor-session",
                mcp_servers=[],
            )
            acp.resume_session.assert_not_awaited()
            acp.new_session.assert_not_awaited()
            capabilities = acp.initialize.await_args.kwargs["client_capabilities"]
            self.assertTrue(capabilities.fs.read_text_file)
            self.assertTrue(capabilities.fs.write_text_file)
            self.assertEqual(existing.status, "idle")


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from acp.exceptions import RequestError

from pa.acp.client import (
    AgentConnection,
    PAClient,
    _agent_supports_load,
    _agent_supports_session_list,
    _resolve_session_load_target,
    _tolerated_client_method,
)
from pa.acp.configuration import (
    ACPConfigurationError,
    SessionConfigurationRequest,
    parse_model_selector,
)
from pa.acp.providers.base import AgentProviderSpec
from pa.config import Settings
from pa.domain.models import AgentSession
from pa.instance.agent_session import AgentSessionRuntime


class PAClientFileSystemTests(unittest.TestCase):
    def test_read_and_write_text_file_requests_are_supported(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "notes.txt"
            client = PAClient(MagicMock())

            async def run() -> None:
                await client.write_text_file(
                    "one\ntwo\nthree\n", str(target), "session-1"
                )
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

    def test_tolerated_client_methods_include_cursor_and_elicitation(self) -> None:
        self.assertTrue(_tolerated_client_method("cursor/update_todos"))
        self.assertTrue(_tolerated_client_method("_cursor/update_todos"))
        self.assertTrue(_tolerated_client_method("elicitation/create"))
        self.assertFalse(_tolerated_client_method("session/update"))
        self.assertFalse(_tolerated_client_method("fs/read_text_file"))


class AgentConfigurationCompatibilityTests(unittest.TestCase):
    def _connection(
        self,
        tmp: str,
        client: object,
        *,
        models: dict | None = None,
        modes: dict | None = None,
        options: list[dict] | None = None,
    ) -> tuple[AgentConnection, MagicMock]:
        store = MagicMock()
        connection = AgentConnection(Settings(data_dir=Path(tmp)), store)
        connection._conn = client
        connection.session = AgentSession(
            agent_name="test",
            external_session_id="external-1",
            status="connected",
        )
        connection.models = models
        connection.modes = modes
        connection.config_options = options
        return connection, store

    def test_combined_selector_is_provider_neutral(self) -> None:
        self.assertEqual(
            parse_model_selector("gpt-5.6-sol[high]"),
            ("gpt-5.6-sol", "high"),
        )
        self.assertEqual(parse_model_selector("vendor/model"), ("vendor/model", None))

    def test_config_only_connection_sets_and_verifies_model_and_reasoning(self) -> None:
        options = [
            {
                "id": "model",
                "name": "Model",
                "type": "select",
                "currentValue": "default",
                "options": [
                    {"value": "default", "name": "Default"},
                    {"value": "gpt-5.6-sol", "name": "GPT"},
                ],
            },
            {
                "id": "thoughtLevel",
                "name": "Thought level",
                "type": "select",
                "currentValue": "medium",
                "options": [
                    {"value": "medium", "name": "Medium"},
                    {"value": "high", "name": "High"},
                ],
            },
        ]

        class ConfigOnlyClient:
            def __init__(self) -> None:
                self.calls: list[tuple[str, str | bool]] = []

            async def set_config_option(self, **kwargs):
                self.calls.append((kwargs["config_id"], kwargs["value"]))
                for option in options:
                    if option["id"] == kwargs["config_id"]:
                        option["currentValue"] = kwargs["value"]
                return {"configOptions": options}

        with tempfile.TemporaryDirectory() as tmp:
            client = ConfigOnlyClient()
            connection, _store = self._connection(tmp, client, options=options)
            effective = asyncio.run(
                connection.configure(
                    SessionConfigurationRequest.from_values(
                        model_id="gpt-5.6-sol[high]"
                    )
                )
            )

        self.assertEqual(
            client.calls, [("model", "gpt-5.6-sol"), ("thoughtLevel", "high")]
        )
        self.assertEqual(effective["model_id"], "gpt-5.6-sol")
        self.assertEqual(effective["reasoning"], "high")
        self.assertEqual(connection.session.model_id, "gpt-5.6-sol")
        self.assertEqual(
            connection.session.config_json["configuration"]["state"], "ready"
        )

    def test_dedicated_setters_are_preferred_when_advertised(self) -> None:
        class DedicatedClient:
            def __init__(self) -> None:
                self.calls: list[tuple[str, str]] = []

            async def set_session_model(self, **kwargs):
                self.calls.append(("model", kwargs["model_id"]))

            async def set_session_mode(self, **kwargs):
                self.calls.append(("mode", kwargs["mode_id"]))

        with tempfile.TemporaryDirectory() as tmp:
            client = DedicatedClient()
            connection, _store = self._connection(
                tmp,
                client,
                models={
                    "currentModelId": "default",
                    "availableModels": [{"modelId": "gpt-next"}],
                },
                modes={
                    "currentModeId": "ask",
                    "availableModes": [{"id": "code"}],
                },
                options=[
                    {
                        "id": "model",
                        "name": "Model",
                        "type": "select",
                        "currentValue": "default",
                    }
                ],
            )
            effective = asyncio.run(
                connection.configure(
                    SessionConfigurationRequest.from_values(
                        model_id="gpt-next", mode_id="code"
                    )
                )
            )

        self.assertEqual(client.calls, [("model", "gpt-next"), ("mode", "code")])
        self.assertEqual(effective["model_id"], "gpt-next")
        self.assertEqual(effective["mode_id"], "code")

    def test_absent_support_fails_with_actionable_compatibility_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            connection, _store = self._connection(tmp, object(), options=[])
            with self.assertRaisesRegex(
                ACPConfigurationError, "Upgrade the ACP client/provider"
            ):
                asyncio.run(
                    connection.configure(
                        SessionConfigurationRequest.from_values(model_id="gpt-next")
                    )
                )

    def test_partial_failure_is_not_persisted_as_effective_and_retry_is_stable(
        self,
    ) -> None:
        options = [
            {
                "id": "reasoning",
                "name": "Reasoning",
                "type": "select",
                "currentValue": "medium",
                "options": [{"value": "high", "name": "High"}],
            }
        ]

        class FlakyClient:
            fail = True

            async def set_session_model(self, **_kwargs):
                return None

            async def set_config_option(self, **kwargs):
                if self.fail:
                    self.fail = False
                    raise RuntimeError("provider rejected reasoning")
                options[0]["currentValue"] = kwargs["value"]
                return {"configOptions": options}

        requested = SessionConfigurationRequest.from_values(
            model_id="gpt-next", reasoning="high"
        )
        with tempfile.TemporaryDirectory() as tmp:
            client = FlakyClient()
            connection, _store = self._connection(
                tmp,
                client,
                models={
                    "currentModelId": "default",
                    "availableModels": [{"modelId": "gpt-next"}],
                },
                options=options,
            )
            with self.assertRaisesRegex(
                ACPConfigurationError, "provider rejected reasoning"
            ):
                asyncio.run(connection.configure(requested))
            failed = connection.session.config_json["configuration"]
            self.assertEqual(failed["state"], "failed")
            self.assertNotIn("effective", failed)
            self.assertIsNone(connection.session.model_id)

            effective = asyncio.run(connection.configure(requested, force=True))

        ready = connection.session.config_json["configuration"]
        self.assertEqual(ready["state"], "ready")
        self.assertEqual(ready["attempt"], 2)
        self.assertEqual(effective["model_id"], "gpt-next")
        self.assertTrue(ready["history"])

    def test_prompt_is_not_sent_while_configuration_is_unconfirmed(self) -> None:
        prompt = AsyncMock()
        with tempfile.TemporaryDirectory() as tmp:
            connection, _store = self._connection(
                tmp, SimpleNamespace(prompt=prompt), options=[]
            )
            connection.session.config_json = {"configuration": {"state": "applying"}}
            with self.assertRaisesRegex(
                ACPConfigurationError, "prompt was not delivered"
            ):
                asyncio.run(connection.prompt("must stay local"))
        prompt.assert_not_awaited()

    def test_on_connect_acknowledges_cursor_vendor_methods(self) -> None:
        wire = MagicMock()
        client = PAClient(MagicMock(), wire_logger=wire)
        original = AsyncMock(
            side_effect=RequestError.method_not_found("cursor/update_todos")
        )
        inner = SimpleNamespace(_handler=original)
        conn = SimpleNamespace(_conn=inner)

        client.on_connect(conn)

        async def run() -> None:
            result = await inner._handler(
                "cursor/update_todos",
                {"todos": [{"id": "1", "content": "x", "status": "pending"}]},
                False,
            )
            self.assertEqual(result, {})

        asyncio.run(run())
        original.assert_awaited_once()
        self.assertEqual(wire.call_args.args[0], "in")
        self.assertEqual(wire.call_args.args[1]["method"], "_cursor/update_todos")

    def test_on_connect_still_raises_unknown_methods(self) -> None:
        client = PAClient(MagicMock())
        original = AsyncMock(side_effect=RequestError.method_not_found("mystery/call"))
        inner = SimpleNamespace(_handler=original)
        client.on_connect(SimpleNamespace(_conn=inner))

        async def run() -> None:
            with self.assertRaises(RequestError):
                await inner._handler("mystery/call", {}, False)

        asyncio.run(run())


class AgentConfigurationAdmissionTests(unittest.IsolatedAsyncioTestCase):
    def _runtime(self, tmp: str, session: AgentSession):
        store = MagicMock()
        store.next_transcript_seq.return_value = 1
        store.get_session.return_value = session
        manager = MagicMock()
        manager.settings = Settings(data_dir=Path(tmp))
        manager.store = store
        manager.browser = MagicMock()
        runtime = AgentSessionRuntime(manager, session)
        return runtime, store

    async def test_restart_reapplies_persisted_configuration_before_admission(
        self,
    ) -> None:
        requested = SessionConfigurationRequest.from_values(
            model_id="gpt-next", reasoning="high"
        )
        with tempfile.TemporaryDirectory() as tmp:
            session = AgentSession(
                agent_name="codex",
                external_session_id="external-1",
                status="disconnected",
                config_json={
                    "configuration": {
                        "state": "ready",
                        "requested": requested.as_dict(),
                    }
                },
            )
            runtime, _store = self._runtime(tmp, session)
            connection = MagicMock()
            connection.connect = AsyncMock(return_value=session)
            connection.configure = AsyncMock(
                return_value={"model_id": "gpt-next", "reasoning": "high"}
            )
            connection.disconnect = AsyncMock()
            connection.session = session
            connection.agent_name = "codex"
            with patch(
                "pa.instance.agent_session.AgentConnection", return_value=connection
            ):
                await runtime.start(resume_external_id="external-1")

        configured = connection.configure.await_args.args[0]
        self.assertEqual(configured.as_dict(), requested.as_dict())
        self.assertTrue(connection.configure.await_args.kwargs["force"])
        connection.disconnect.assert_not_awaited()

    async def test_startup_configuration_failure_terminates_provider(self) -> None:
        requested = SessionConfigurationRequest.from_values(model_id="gpt-next")
        with tempfile.TemporaryDirectory() as tmp:
            session = AgentSession(agent_name="codex", status="connecting")
            runtime, _store = self._runtime(tmp, session)
            connection = MagicMock()
            connection.connect = AsyncMock(return_value=session)
            connection.configure = AsyncMock(
                side_effect=ACPConfigurationError("unsupported model")
            )
            connection.disconnect = AsyncMock()
            connection.session = session
            connection.agent_name = "codex"
            with patch(
                "pa.instance.agent_session.AgentConnection", return_value=connection
            ):
                with self.assertRaisesRegex(ACPConfigurationError, "unsupported"):
                    await runtime.start(initial_configuration=requested)

        connection.disconnect.assert_awaited_once()
        self.assertIsNone(runtime.connection)


class AgentSessionRestoreTests(unittest.TestCase):
    def test_load_capability_is_detected_in_dict_and_object_responses(self) -> None:
        self.assertTrue(
            _agent_supports_load({"agentCapabilities": {"loadSession": True}})
        )
        self.assertTrue(
            _agent_supports_load(
                SimpleNamespace(agent_capabilities=SimpleNamespace(load_session=True))
            )
        )
        self.assertTrue(
            _agent_supports_session_list(
                {
                    "agentCapabilities": {
                        "sessionCapabilities": {"list": {}},
                    }
                }
            )
        )
        self.assertFalse(
            _agent_supports_session_list(
                {
                    "agentCapabilities": {
                        "sessionCapabilities": {"resume": None},
                    }
                }
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
                agent_name="generic",
                external_session_id="agent-session",
                status="disconnected",
            )
            connection = AgentConnection(
                Settings(data_dir=Path(tmp)),
                store,
                provider_spec=AgentProviderSpec(
                    id="generic",
                    display_name="Generic",
                    command="agent",
                ),
            )

            async def run() -> None:
                with (
                    patch("pa.acp.client.spawn_agent", return_value=context),
                    patch("pa.acp.client.pa_mcp_servers", return_value=[]),
                ):
                    restored = await connection.connect(
                        resume_external_id="agent-session",
                        existing_session=existing,
                    )
                self.assertIs(restored, existing)

            asyncio.run(run())

            acp.load_session.assert_awaited_once_with(
                cwd=str(Path(tmp)),
                session_id="agent-session",
                mcp_servers=[],
            )
            acp.resume_session.assert_not_awaited()
            acp.new_session.assert_not_awaited()
            capabilities = acp.initialize.await_args.kwargs["client_capabilities"]
            self.assertTrue(capabilities.fs.read_text_file)
            self.assertTrue(capabilities.fs.write_text_file)
            self.assertEqual(existing.status, "idle")

    def test_loads_with_cwd_from_session_list(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MagicMock()
            listed_cwd = str(Path(tmp) / "project")
            acp = MagicMock()
            acp.initialize = AsyncMock(
                return_value={
                    "agentCapabilities": {
                        "loadSession": True,
                        "sessionCapabilities": {
                            "resume": None,
                            "list": {},
                        },
                    }
                }
            )
            acp.list_sessions = AsyncMock(
                return_value=SimpleNamespace(
                    sessions=[
                        SimpleNamespace(
                            session_id="cursor-session",
                            cwd=listed_cwd,
                        )
                    ]
                )
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
                cwd=str(Path(tmp)),
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
                        cwd=str(Path(tmp)),
                        existing_session=existing,
                    )
                self.assertIs(restored, existing)

            asyncio.run(run())

            acp.list_sessions.assert_awaited_once()
            acp.load_session.assert_awaited_once_with(
                cwd=listed_cwd,
                session_id="cursor-session",
                mcp_servers=[],
            )
            acp.new_session.assert_not_awaited()
            self.assertEqual(existing.status, "idle")
            self.assertEqual(connection.session_cwd, listed_cwd)
            self.assertEqual(existing.cwd, listed_cwd)

    def test_skips_load_when_session_missing_from_list(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = MagicMock()
            acp = MagicMock()
            acp.initialize = AsyncMock(
                return_value={
                    "agentCapabilities": {
                        "loadSession": True,
                        "sessionCapabilities": {
                            "resume": None,
                            "list": {},
                        },
                    }
                }
            )
            acp.list_sessions = AsyncMock(
                return_value=SimpleNamespace(
                    sessions=[
                        SimpleNamespace(
                            session_id="other-session",
                            cwd=str(Path(tmp)),
                        )
                    ]
                )
            )
            acp.load_session = AsyncMock()
            acp.resume_session = AsyncMock()
            acp.new_session = AsyncMock(
                return_value=SimpleNamespace(session_id="new-cursor-session")
            )
            context = MagicMock()
            context.__aenter__ = AsyncMock(return_value=(acp, MagicMock()))
            context.__aexit__ = AsyncMock()
            existing = AgentSession(
                id="pa-session",
                agent_name="cursor",
                external_session_id="stale-session",
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
                        resume_external_id="stale-session",
                        existing_session=existing,
                    )
                self.assertIs(restored, existing)

            asyncio.run(run())

            acp.list_sessions.assert_awaited_once()
            acp.load_session.assert_not_awaited()
            acp.new_session.assert_awaited_once()
            self.assertEqual(existing.external_session_id, "new-cursor-session")
            self.assertEqual(existing.status, "connected")

    def test_resolve_session_load_target_helpers(self) -> None:
        async def run() -> None:
            listed = SimpleNamespace(
                sessions=[
                    {"sessionId": "abc", "cwd": "/work"},
                    SimpleNamespace(session_id="xyz", cwd="/other"),
                ]
            )
            conn = SimpleNamespace(list_sessions=AsyncMock(return_value=listed))
            self.assertEqual(
                await _resolve_session_load_target(
                    conn, session_id="abc", cwd="/fallback"
                ),
                ("abc", "/work"),
            )
            self.assertIsNone(
                await _resolve_session_load_target(
                    conn, session_id="missing", cwd="/fallback"
                )
            )
            # Attribute missing entirely → try load with the provided cwd.
            self.assertEqual(
                await _resolve_session_load_target(
                    SimpleNamespace(), session_id="abc", cwd="/fallback"
                ),
                ("abc", "/fallback"),
            )

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()

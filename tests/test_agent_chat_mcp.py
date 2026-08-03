from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from pa.modules.agent_chat import AgentChatModule


class FakeMcp:
    def __init__(self) -> None:
        self.functions: dict[str, object] = {}

    def tool(self):
        def register(fn):
            self.functions[fn.__name__] = fn
            return fn

        return register


def test_session_observability_tools_allow_bounded_server_collection_time() -> None:
    mcp = FakeMcp()
    settings = SimpleNamespace()
    local_api = MagicMock(return_value={"sessions": []})

    with patch("pa.mcp.local_api.request_local_pa", local_api):
        AgentChatModule().register_mcp(mcp, SimpleNamespace(settings=settings))
        mcp.functions["list_agent_session_liveness"](limit=5)
        mcp.functions["get_agent_session_liveness"]("session-1")
        mcp.functions["list_agent_session_turns"]("session-1")
        mcp.functions["request_agent_session_diagnostics"]("session-1", limit=10)

    assert local_api.call_args_list[0].kwargs == {
        "params": {"limit": 5},
        "timeout_seconds": 15.0,
    }
    for call in local_api.call_args_list[1:3]:
        assert call.kwargs == {
            "allow_not_found": True,
            "timeout_seconds": 15.0,
        }
    assert local_api.call_args_list[3].kwargs == {
        "params": {"limit": 10},
        "allow_not_found": True,
        "timeout_seconds": 15.0,
    }

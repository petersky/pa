from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock

from pa.mcp.local_api import request_local_pa
from pa.modules.instance import InstanceModule


class FakeMcp:
    def __init__(self) -> None:
        self.functions: dict[str, object] = {}

    def tool(self):
        def register(fn):
            self.functions[fn.__name__] = fn
            return fn

        return register


def test_workspace_reconcile_allows_bounded_collection_time() -> None:
    mcp = FakeMcp()
    settings = SimpleNamespace()
    runtime = SimpleNamespace(run_blocking=AsyncMock(return_value={"leases": []}))
    ctx = SimpleNamespace(
        settings=settings,
        require_service=lambda name: runtime if name == "async_runtime" else None,
    )
    InstanceModule().register_mcp(mcp, ctx)

    result = asyncio.run(mcp.functions["workspace_reconcile"](collect=True))

    assert result == {"leases": []}
    runtime.run_blocking.assert_awaited_once_with(
        "mcp.workspace_reconcile_http",
        request_local_pa,
        settings,
        "POST",
        "/api/workspaces/reconcile",
        json={"collect": True},
        timeout_seconds=120.0,
        timeout=300.0,
    )

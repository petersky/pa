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


def test_workspace_collect_selected_uses_distinct_route_and_exact_targets() -> None:
    mcp = FakeMcp()
    runtime = SimpleNamespace(run_blocking=AsyncMock(return_value={"targets": []}))
    settings = SimpleNamespace()
    ctx = SimpleNamespace(settings=settings, require_service=lambda _:runtime)
    InstanceModule().register_mcp(mcp,ctx)
    targets=[{"lease_id":"a"*64,"expected_fence":12,"expected_head":"b"*40,
              "expected_updated_at":"2026-11-08T00:00:00Z","merged_pr_number":359}]
    asyncio.run(mcp.functions['workspace_collect_selected'](targets=targets))
    assert runtime.run_blocking.call_args.args[3:5] == ('POST', '/api/workspaces/collect-selected')
    assert runtime.run_blocking.call_args.kwargs['json'] == {'targets':targets}


def test_preservation_status_uses_only_its_exact_distinct_route() -> None:
    mcp=FakeMcp()
    runtime=SimpleNamespace(run_blocking=AsyncMock(return_value={'phase':'not_started'}))
    ctx=SimpleNamespace(settings=SimpleNamespace(),require_service=lambda _:runtime)
    InstanceModule().register_mcp(mcp,ctx)
    fields=dict(target={'lease_id':'a'*64},relative_path='audit',attachment_card_id='original',
                attachment_id='b'*32,attachment_sha256='c'*64,action='status')
    result=asyncio.run(mcp.functions['workspace_preserve_selected_untracked'](**fields))
    assert result=={'phase':'not_started'}
    assert runtime.run_blocking.call_args.args[3:5]==('POST','/api/workspaces/preserve-selected-untracked')
    assert runtime.run_blocking.call_args.kwargs['json']==fields

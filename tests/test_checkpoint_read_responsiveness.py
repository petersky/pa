"""Checkpoint authorization reads must not block the serving event loop."""
import asyncio
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException

from pa.config import Settings
from pa.core.async_runtime import AsyncRuntime
from pa.execution.dispatch import DispatchStore
from pa.execution.progress import ExplicitProgressCheckpointV1
from pa.modules import fleet
from tests.test_dispatch_progress import DISPATCH, record


@pytest.mark.asyncio
async def test_checkpoint_read_yields_loop_and_preserves_authorization(tmp_path, monkeypatch):
    store = DispatchStore(tmp_path)
    original = record()
    store.put(original)
    runtime = AsyncRuntime()
    service = SimpleNamespace(explicit=AsyncMock(return_value=SimpleNamespace(
        model_dump=lambda **kwargs: {"sequence": 7, "status": "accepted"})))
    request = SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(ctx=SimpleNamespace(
            settings=Settings(data_dir=tmp_path), services={
                "dispatch_store": store, "async_runtime": runtime, "progress_service": service}))),
        state=SimpleNamespace(instance_authenticated=True))
    entered, release = threading.Event(), threading.Event()
    reader_threads = []
    serving_thread = threading.get_ident()
    get = store.get

    def blocked_read(dispatch_id):
        reader_threads.append(threading.get_ident())
        entered.set()
        # A loop callback must run before the read can finish; this assertion
        # checks actual scheduling, without a small host-dependent deadline.
        assert release.wait(10)
        return get(dispatch_id)

    monkeypatch.setattr(store, "get", blocked_read)
    body = ExplicitProgressCheckpointV1(phase="testing", summary="Fixture checkpoint")
    task = asyncio.create_task(fleet.report_dispatch_checkpoint(request, DISPATCH, body))
    try:
        assert await asyncio.to_thread(entered.wait, 10)
        assert len(reader_threads) == 1
        assert reader_threads[0] != serving_thread
        assert not task.done()
        assert service.explicit.await_count == 0
        release.set()
        assert await task == {"sequence": 7, "status": "accepted"}
        service.explicit.assert_awaited_once_with(DISPATCH, body)
        assert runtime.snapshot()["operations"]["progress.dispatch_read"]["completed"] == 1

        # Keep missing/foreign/protocol checks before any durable checkpoint.
        service.explicit.reset_mock()
        monkeypatch.setattr(store, "get", lambda _: None)
        with pytest.raises(HTTPException) as missing:
            await fleet.report_dispatch_checkpoint(request, DISPATCH, body)
        assert missing.value.status_code == 404
        monkeypatch.setattr(store, "get", lambda _: original)
        request.state.instance_authenticated = False
        request.app.state.ctx.settings.auth_required = True
        monkeypatch.setattr(fleet, "require_user", lambda _: SimpleNamespace(role="member"))
        monkeypatch.setattr(fleet, "get_principal_id", lambda _: "foreign")
        with pytest.raises(HTTPException) as foreign:
            await fleet.report_dispatch_checkpoint(request, DISPATCH, body)
        assert foreign.value.status_code == 403
        request.state.instance_authenticated = True
        monkeypatch.setattr(store, "get", lambda _: original.model_copy(update={"progress_protocol_version": 0}))
        with pytest.raises(HTTPException) as protocol:
            await fleet.report_dispatch_checkpoint(request, DISPATCH, body)
        assert protocol.value.status_code == 409
        service.explicit.assert_not_awaited()
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)
        await runtime.close()

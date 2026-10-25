"""Short failures remain diagnosable without changing completion ownership."""
import asyncio
import json
import sqlite3
import threading
from unittest.mock import patch

import pytest
from fastapi import HTTPException

from pa.core.async_runtime import AsyncRuntime, BlockingOperationTimeout


@pytest.mark.asyncio
async def test_failed_only_window_excludes_success_and_preserves_original_error():
    runtime = AsyncRuntime()
    error = HTTPException(503, detail={"secret": "private-body"}, headers={"private": "header"})
    try:
        await runtime.run_blocking("fixture.short", lambda: "private-success")
        def fail():
            raise error
        with pytest.raises(HTTPException) as caught:
            await runtime.run_blocking("fixture.short", fail)
        assert caught.value is error
        await runtime.close()  # A completed worker result can precede its metrics callback.
        snapshot = runtime.snapshot()
        operation = snapshot["operations"]["fixture.short"]
        assert (operation["completed"], operation["failed"]) == (1, 1)
        assert operation["runtime_ms"]["samples"] == 2
        assert operation["failed_runtime_ms"]["samples"] == 1
        item = snapshot["failed_operations"]["observations"][0]
        assert item["category"] == "http_error" and item["http_status"] == 503
        assert item["phase"] == "worker_completion"
        assert item["worker_service_ms"] >= 0 and item["thread_cpu_ms"] >= 0
        assert "private" not in json.dumps(snapshot)
        item["category"] = "modified-snapshot"
        assert runtime.snapshot()["failed_operations"]["observations"][0]["category"] == "http_error"
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_late_worker_failure_is_recorded_once_after_wait_timeout():
    runtime = AsyncRuntime(max_workers=1)
    release = threading.Event()
    error = OSError("private-path")
    def fail_later():
        release.wait(2)
        raise error
    try:
        with pytest.raises(BlockingOperationTimeout):
            await runtime.run_blocking("fixture.late", fail_later, timeout=.01)
        before = runtime.snapshot()
        assert before["executor"]["active"] == 1
        assert before["failed_operations"]["observations"] == []
        release.set()
        await runtime.close()
        snapshot = runtime.snapshot()
        operation = snapshot["operations"]["fixture.late"]
        assert (operation["failed"], operation["timed_out"], operation["active"]) == (1, 1, 0)
        assert len(snapshot["failed_operations"]["observations"]) == 1
        assert snapshot["failed_operations"]["observations"][0]["category"] == "os_error"
        assert "private-path" not in json.dumps(snapshot)
    finally:
        release.set()
        await runtime.close()


@pytest.mark.asyncio
async def test_native_failure_ring_and_percentile_windows_have_distinct_bounds():
    runtime = AsyncRuntime()
    class PrivateErrorName(Exception):
        def __str__(self):
            raise AssertionError("must not stringify")
    async def fail():
        raise PrivateErrorName("private-message")
    try:
        for _ in range(260):
            with pytest.raises(PrivateErrorName):
                await runtime.observe("fixture.native", fail())
        snapshot = runtime.snapshot()
        assert snapshot["operations"]["fixture.native"]["failed"] == 260
        assert snapshot["operations"]["fixture.native"]["failed_runtime_ms"]["samples"] == 256
        items = snapshot["failed_operations"]["observations"]
        assert len(items) == 64
        assert all(item["category"] == "exception" and item["phase"] == "native_async" for item in items)
        assert "private-message" not in json.dumps(snapshot) and "PrivateErrorName" not in json.dumps(snapshot)
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_native_cancellation_is_not_a_completed_failure():
    runtime = AsyncRuntime()
    started = asyncio.Event()
    async def blocked():
        started.set()
        await asyncio.Event().wait()
    try:
        task = asyncio.create_task(runtime.observe("fixture.cancel", blocked()))
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert runtime.snapshot()["operations"]["fixture.cancel"]["cancelled"] == 1
        assert runtime.snapshot()["failed_operations"]["observations"] == []
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_submission_error_preserves_exception_and_releases_slot():
    runtime = AsyncRuntime(max_workers=1)
    error = RuntimeError("private-submission")
    try:
        loop = asyncio.get_running_loop()
        with patch.object(loop, "run_in_executor", side_effect=error):
            with pytest.raises(RuntimeError) as caught:
                await runtime.run_blocking("fixture.submit", lambda: None)
        assert caught.value is error
        snapshot = runtime.snapshot()
        assert snapshot["executor"]["active"] == 0
        assert snapshot["failed_operations"]["observations"][0]["phase"] == "executor_submission"
        assert await runtime.run_blocking("fixture.submit", lambda: 42) == 42
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_fixed_sqlite_and_timeout_categories_do_not_retain_sql_or_names():
    runtime = AsyncRuntime()
    async def fail(error):
        raise error
    try:
        for code, category in [(sqlite3.SQLITE_BUSY | (2 << 8), "sqlite_busy"),
                               (sqlite3.SQLITE_LOCKED, "sqlite_locked"), (sqlite3.SQLITE_ERROR, "sqlite_error")]:
            error = sqlite3.OperationalError("private-sql")
            error.sqlite_errorcode = code
            error.sqlite_errorname = "private-name"
            with pytest.raises(sqlite3.OperationalError):
                await runtime.observe("fixture.sql", fail(error))
            assert runtime.snapshot()["failed_operations"]["observations"][-1]["category"] == category
        with pytest.raises(TimeoutError):
            await runtime.observe("fixture.timeout", fail(TimeoutError("private-timeout")))
        assert runtime.snapshot()["failed_operations"]["observations"][-1]["category"] == "timeout_error"
        assert runtime.snapshot()["operations"]["fixture.timeout"]["timed_out"] == 1
        assert "private-" not in json.dumps(runtime.snapshot())
    finally:
        await runtime.close()


def test_short_http_and_failed_sse_are_visible_without_pooling_stream_lifetimes():
    runtime = AsyncRuntime()
    for _ in range(70):
        runtime.record_request("/api/agent/session-events", 503, 2, response_bytes=128, streaming=True)
    runtime.record_request("/api/cards/{card_id}", 404, 3, response_bytes=64)
    runtime.record_request("/api/cards/{card_id}", 200, 5, response_bytes=256)
    runtime.record_request("/api/agent/session-events", 200, 50000, streaming=True)
    snapshot = runtime.snapshot()
    assert snapshot["failed_requests"]["count"] == 71
    assert len(snapshot["failed_requests"]["observations"]) == 64
    assert snapshot["failed_requests"]["observations"][-1]["status"] == 404
    assert snapshot["requests"]["count"] == 2
    assert snapshot["requests"]["max_ms"] == 5
    assert snapshot["requests"]["excluded_sse_requests"] == 71
    assert snapshot["requests"]["routes"]["/api/cards/{card_id}"]["count"] == 2
    runtime._executor.shutdown()


@pytest.mark.asyncio
async def test_http_middleware_retains_template_and_status_without_private_body_or_id():
    import httpx
    from fastapi import FastAPI
    from pa.core.kernel import _ResponsivenessMiddleware

    runtime = AsyncRuntime()
    app = FastAPI()
    @app.get("/fixture/{session_id}")
    async def unavailable(session_id: str):
        raise HTTPException(503, detail="private-error-body")
    app.add_middleware(_ResponsivenessMiddleware, runtime_getter=lambda: runtime)
    try:
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://fixture") as client:
            response = await client.get("/fixture/private-id?token=private-query")
        assert response.status_code == 503
        assert response.json() == {"detail": "private-error-body"}
        snapshot = runtime.snapshot()
        item = snapshot["failed_requests"]["observations"][0]
        assert item["path"] == "/fixture/{session_id}" and item["status"] == 503
        assert item["response_bytes"] == len(response.content)
        assert "private-" not in json.dumps(snapshot)
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_diagnostic_classification_failure_cannot_replace_original_exception():
    class UninspectableHTTPError(HTTPException):
        def __getattribute__(self, name):
            if name == "__dict__":
                raise RuntimeError("private-diagnostic-failure")
            return super().__getattribute__(name)
    error = UninspectableHTTPError(503, "private-detail")
    async def fail():
        raise error
    runtime = AsyncRuntime()
    try:
        with pytest.raises(UninspectableHTTPError) as caught:
            await runtime.observe("fixture.uninspectable", fail())
        assert caught.value is error
        snapshot = runtime.snapshot()
        assert snapshot["failed_operations"]["observations"][0]["category"] == "unclassified"
        assert "private-" not in json.dumps(snapshot)
    finally:
        await runtime.close()

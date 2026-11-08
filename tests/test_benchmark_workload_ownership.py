"""A failed private workload must settle its own sibling tasks before teardown."""
import asyncio
import json
from unittest.mock import patch

import httpx
import pytest

from scripts.benchmark_systemic_latency import _run_workload, run


@pytest.mark.asyncio
@pytest.mark.parametrize('cancel_parent', [False, True])
async def test_failure_and_cancellation_wait_for_sibling_finalizers(cancel_parent):
    started, finalizing, release, finished = (asyncio.Event() for _ in range(4))
    failure = ValueError('private fixture failure')
    outcomes = {}

    async def sibling():
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            finalizing.set()
            await release.wait()
            finished.set()

    async def fail():
        await started.wait()
        if cancel_parent:
            await asyncio.Event().wait()
        raise failure

    task = asyncio.create_task(_run_workload(sibling(), fail(), outcomes=outcomes))
    try:
        await started.wait()
        if cancel_parent:
            task.cancel()
        await asyncio.wait_for(finalizing.wait(), 1)
        assert not task.done() and not finished.is_set()
        release.set()
        if cancel_parent:
            with pytest.raises(asyncio.CancelledError):
                await task
        else:
            with pytest.raises(ValueError) as caught:
                await task
            assert caught.value is failure
        assert finished.is_set()
        assert outcomes['0'] == 'cancelled'
        assert outcomes['1'] == ('cancelled' if cancel_parent else 'ValueError')
    finally:
        release.set()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_success_preserves_result_order():
    async def value(x):
        await asyncio.sleep(0)
        return x
    assert await _run_workload(value(2), value(1)) == [2, 1]


@pytest.mark.asyncio
async def test_private_http_failure_retains_partial_phases_without_response_payload(tmp_path):
    from pa.config import reset_settings
    from pa.core.kernel import reset_kernel
    from pa.domain.store import reset_store
    from pa.instance.agent_session import reset_instance_agent

    captured = []
    original = httpx.AsyncClient.get

    async def get(client, path, *args, **kwargs):
        if path == '/api/sync/status':
            return httpx.Response(504, content=b'private-response-marker',
                                  request=httpx.Request('GET', 'http://fixture'+path))
        return await original(client, path, *args, **kwargs)

    try:
        with patch.object(httpx.AsyncClient, 'get', get):
            with pytest.raises(httpx.HTTPStatusError) as caught:
                await run(tmp_path/'fixture', history=1, iterations=1, inventory_reads=True,
                          failure_observer=captured.append)
        assert caught.value.response.status_code == 504
        assert len(captured) == 1
        snapshot = captured[0]
        assert snapshot['http_status'] == 504 and snapshot['route'] == '/api/sync/status'
        assert snapshot['error_type'] == 'HTTPStatusError'
        assert 'HTTPStatusError' in snapshot['workload_outcomes'].values()
        assert snapshot['runtime']['operations']
        assert 'private-response-marker' not in json.dumps(snapshot)
    finally:
        reset_instance_agent()
        reset_store()
        reset_settings()
        reset_kernel()

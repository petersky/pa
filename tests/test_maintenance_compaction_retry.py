"""Deferred rewrite retries coalesce without repeating durable retention work."""
import asyncio
from types import SimpleNamespace
import threading
import sqlite3
from contextlib import closing
from unittest.mock import patch

import pytest

from pa.config import Settings
from pa.instance.maintenance import InstanceMaintenanceService
from pa.domain.models import AgentSession, TranscriptEvent
from pa.domain.projection import CardProjection


def report(*, deferred=True):
    return {"ran_at": "original full sweep", "transcript_events_deleted": 7,
            "transcript_storage": {"measured_at": "original health"},
            "sqlite": {"status": "deferred" if deferred else "ok",
                       "reason": "foreground_write" if deferred else None,
                       "compacted": not deferred},
            "transcript_sqlite": {"status": "ok", "compacted": True}}


@pytest.mark.asyncio
async def test_bursts_retry_only_pending_database_with_bounded_backoff_then_quiet_success(tmp_path):
    service = InstanceMaintenanceService(Settings(data_dir=tmp_path), None, {})
    full, retries, waits = [], [], []

    async def work(function, *args, **kwargs):
        if function.__name__ == "run_maintenance":
            full.append(True)
            return report()
        retries.append(args[1])
        assert service.snapshot()["pending_database_compactions"] == ["sqlite"]
        return {"sqlite": report(deferred=len(retries) < 10)["sqlite"]}

    async def tick(awaitable, *, timeout):
        awaitable.close()
        waits.append(timeout)
        if len(waits) == 11:
            service._closing = True
        else:
            raise TimeoutError

    with patch.object(service, "_call", side_effect=work), patch("pa.instance.maintenance.asyncio.wait_for", side_effect=tick):
        service.start()
        await service._task
    assert len(full) == 1 and retries == [("sqlite",)] * 10
    assert waits == [1, 2, 4, 8, 16, 30, 30, 30, 30, 30, 86400]
    assert service.snapshot()["pending_database_compactions"] == []
    assert service.last_result["sqlite"]["status"] == "ok"
    assert service.last_result["ran_at"] == "original full sweep"
    assert service.last_result["transcript_storage"]["measured_at"] == "original health"
    assert service.last_result["transcript_events_deleted"] == 7
    assert service.last_result["database_retry"]["attempt"] == 10
    await service.close()


@pytest.mark.asyncio
async def test_continued_writes_do_not_starve_periodic_full_retention(tmp_path):
    service = InstanceMaintenanceService(Settings(data_dir=tmp_path, maintenance_interval_seconds=60), None, {})
    clock, calls, waits = [0.0], [], []

    async def work(function, *args, **kwargs):
        calls.append(function.__name__)
        if function.__name__ == "run_maintenance":
            return report()
        return {"sqlite": report()["sqlite"]}

    async def tick(awaitable, *, timeout):
        awaitable.close()
        waits.append(timeout)
        clock[0] += timeout
        if calls.count("run_maintenance") == 2:
            service._closing = True
        else:
            raise TimeoutError

    with (patch.object(service, "_call", side_effect=work),
          patch("pa.instance.maintenance.asyncio.wait_for", side_effect=tick),
          patch("pa.instance.maintenance.asyncio.get_running_loop", return_value=SimpleNamespace(time=lambda: clock[0]))):
        await service._run()
    assert calls == ["run_maintenance"] + ["_retry_compactions"] * 5 + ["run_maintenance"]
    assert waits[:6] == [1, 2, 4, 8, 16, 29]
    assert service.snapshot()["pending_database_compactions"] == ["sqlite"]
    assert service.last_result["sqlite"]["status"] == "deferred"


@pytest.mark.asyncio
async def test_explicit_wake_requests_full_sweep_instead_of_retry(tmp_path):
    service = InstanceMaintenanceService(Settings(data_dir=tmp_path), None, {})
    calls = []
    async def work(function, *args, **kwargs):
        calls.append(function.__name__)
        return report()
    async def tick(awaitable, *, timeout):
        awaitable.close()
        if len(calls) == 1:
            service.wake()
        else:
            service._closing = True
    with patch.object(service, "_call", side_effect=work), patch("pa.instance.maintenance.asyncio.wait_for", side_effect=tick):
        await service._run()
    assert calls == ["run_maintenance", "run_maintenance"]


@pytest.mark.asyncio
async def test_cancelled_manual_waiter_cannot_release_retry_and_other_manual_request_gets_full_sweep(tmp_path):
    service = InstanceMaintenanceService(Settings(data_dir=tmp_path), None, {})
    entered, release = threading.Event(), threading.Event()
    calls = []
    def work(function, *args, **kwargs):
        calls.append(function.__name__)
        if function.__name__ == "_retry_compactions":
            entered.set()
            assert release.wait(5)
            return {"sqlite": report(deferred=False)["sqlite"]}
        return report(deferred=len(calls) == 1)
    async def invoke(function, *args, **kwargs):
        return await asyncio.to_thread(work, function, *args, **kwargs)
    with patch.object(service, "_call", side_effect=invoke):
        try:
            await service.run_once()
            retry = asyncio.create_task(service._run_once(database_only=True))
            assert await asyncio.to_thread(entered.wait, 5)
            cancelled = asyncio.create_task(service.run_once())
            manual = asyncio.create_task(service.run_once())
            await asyncio.sleep(0)
            cancelled.cancel()
            with pytest.raises(asyncio.CancelledError):
                await cancelled
            assert calls == ["run_maintenance", "_retry_compactions"]
            release.set()
            await retry
            assert (await manual)["sqlite"]["status"] == "ok"
            assert calls == ["run_maintenance", "_retry_compactions", "run_maintenance"]
        finally:
            release.set()
            await service.close()


@pytest.mark.asyncio
async def test_restart_runs_full_sweep_and_clears_pending_compaction_after_shutdown(tmp_path):
    service = InstanceMaintenanceService(Settings(data_dir=tmp_path), None, {})
    calls = []
    async def work(function, *args, **kwargs):
        calls.append(function.__name__)
        return report(deferred=len(calls) == 1)
    with patch.object(service, "_call", side_effect=work):
        await service.run_once()
        assert service.snapshot()["pending_database_compactions"] == ["sqlite"]
        await service.close()
        service.start()
        await asyncio.sleep(0)
        await service._active_run
        await service.close()
    assert calls == ["run_maintenance", "run_maintenance"]
    assert not service.snapshot()["pending_database_compactions"]


@pytest.mark.asyncio
async def test_full_sweep_busy_or_error_does_not_forget_pending_reclamation(tmp_path):
    service = InstanceMaintenanceService(Settings(data_dir=tmp_path), None, {})
    calls = []
    async def work(function, *args, **kwargs):
        calls.append(function.__name__)
        if len(calls) == 1:
            return report()
        if len(calls) == 2:
            value = report()
            value["sqlite"]["reason"] = "busy"
            return value
        if len(calls) == 3:
            raise RuntimeError("fixture retry failure")
        return {"sqlite": report(deferred=False)["sqlite"]}
    with patch.object(service, "_call", side_effect=work):
        await service.run_once()
        await service.run_once()
        assert service.snapshot()["pending_database_compactions"] == ["sqlite"]
        with pytest.raises(RuntimeError, match="fixture retry failure"):
            await service._run_once(database_only=True)
        assert service.snapshot()["pending_database_compactions"] == ["sqlite"]
        assert service.last_error == "fixture retry failure"
        await service._run_once(database_only=True)
        assert not service.snapshot()["pending_database_compactions"]
        assert service.last_error is None
        await service.close()


@pytest.mark.asyncio
async def test_real_retry_reclaims_only_pending_database_and_retains_complete_sweep_evidence(tmp_path):
    settings = Settings(data_dir=tmp_path)
    store = CardProjection(settings.db_path)
    store.save_session(AgentSession(id="kept", agent_name="codex"))
    event = TranscriptEvent(session_id="kept", seq=1, event_type="output", payload={"text": "retained"})
    store.append_transcript_events([event])
    with closing(sqlite3.connect(store.db_path)) as seed, seed:
        seed.execute("CREATE TABLE retained_fixture(id INTEGER PRIMARY KEY, body BLOB)")
        seed.executemany("INSERT INTO retained_fixture VALUES (?,zeroblob(10000))", ((i,) for i in range(100)))
        seed.execute("DELETE FROM retained_fixture WHERE id!=1")
    service = InstanceMaintenanceService(settings, store, {})
    try:
        with (patch.object(store, "prune_closed_session_transcripts", wraps=store.prune_closed_session_transcripts) as prune,
              patch.object(store, "migrate_legacy_transcripts", wraps=store.migrate_legacy_transcripts) as migrate):
            with store.maintenance.writer():
                full = await service.run_once()
            assert full["sqlite"]["reason"] == "foreground_write"
            assert service.snapshot()["pending_database_compactions"] == ["sqlite"]
            retried = await service._run_once(database_only=True)
            assert prune.call_count == migrate.call_count == 1
        assert retried["sqlite"]["status"] == "ok" and retried["sqlite"]["compacted"]
        assert retried["sqlite"]["after"]["free_bytes"] == 0
        assert retried["sqlite"]["after"]["bytes"] < retried["sqlite"]["before"]["bytes"]
        assert retried["transcript_sqlite"] == full["transcript_sqlite"]
        assert retried["transcript_storage"] == full["transcript_storage"]
        assert retried["ran_at"] == full["ran_at"]
        assert "original measurement timestamps" in retried["database_retry"]["scope"]
        assert service.snapshot()["pending_database_compactions"] == []
        assert store.get_session("kept").id == "kept"
        assert store.list_transcript_events("kept")[0].payload == event.payload
        with closing(sqlite3.connect(store.db_path)) as verify:
            assert verify.execute("SELECT id,length(body) FROM retained_fixture").fetchall() == [(1, 10000)]
    finally:
        await service.close()
        store.close_thread_connection()

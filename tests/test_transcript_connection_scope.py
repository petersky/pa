"""Transcript read handles live only within the actual worker invocation."""
import asyncio
import sqlite3
import threading

import pytest

from pa.core.async_runtime import AsyncRuntime, BlockingOperationTimeout
from pa.core.sqlite_metrics import ObservedConnection, sqlite_connection_scope
from pa.domain.transcript_storage import TranscriptStorage


def test_direct_calls_close_and_worker_scope_reuses_with_same_durability(tmp_path):
    store = TranscriptStorage(tmp_path / 'fixture.db')
    with store._conn() as direct:
        assert direct.execute('PRAGMA busy_timeout').fetchone()[0] == 30000
        assert direct.execute('PRAGMA synchronous').fetchone()[0] == 1
    with pytest.raises(sqlite3.ProgrammingError):
        direct.execute('SELECT 1')
    with sqlite_connection_scope():
        with store._conn() as first:
            first.execute('CREATE TABLE fixture(value TEXT)')
        with store._conn() as reused:
            assert reused is first
            assert reused.execute('PRAGMA busy_timeout').fetchone()[0] == 30000
            assert reused.execute('PRAGMA synchronous').fetchone()[0] == 1
    with pytest.raises(sqlite3.ProgrammingError):
        first.execute('SELECT 1')


def test_failed_borrow_rolls_back_and_cannot_be_reused(tmp_path):
    store = TranscriptStorage(tmp_path / 'fixture.db')
    with sqlite_connection_scope():
        with store._conn() as first:
            first.execute('CREATE TABLE fixture(value TEXT)')
        with pytest.raises(ValueError), store._conn() as failed:
            assert failed is first
            failed.execute("INSERT INTO fixture VALUES('rollback')")
            raise ValueError('disposable fixture')
        with store._conn() as replacement:
            assert replacement is not failed
            assert replacement.execute('SELECT COUNT(*) FROM fixture').fetchone()[0] == 0


def test_nested_borrow_never_aliases_an_active_connection(tmp_path):
    store = TranscriptStorage(tmp_path / 'fixture.db')
    with sqlite_connection_scope():
        with store._conn() as outer:
            with store._conn() as inner:
                assert inner is not outer
                assert inner.execute('SELECT 1').fetchone()[0] == 1
            assert outer.execute('SELECT 1').fetchone()[0] == 1


@pytest.mark.asyncio
async def test_timed_out_worker_keeps_connection_until_actual_completion(tmp_path, monkeypatch):
    store = TranscriptStorage(tmp_path / 'fixture.db')
    created = []
    connect = sqlite3.connect
    class Tracked(ObservedConnection):
        closed = False
        def close(self):
            self.closed = True
            return super().close()
    def tracked(*args, **kwargs):
        if kwargs.get('factory') is ObservedConnection:
            kwargs['factory'] = Tracked
            conn = connect(*args, **kwargs)
            created.append(conn)
            return conn
        return connect(*args, **kwargs)
    monkeypatch.setattr('pa.domain.transcript_storage.sqlite3.connect', tracked)
    runtime = AsyncRuntime(max_workers=1, max_queue=1)
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    def worker():
        try:
            with store._conn() as conn:
                entered.set()
                assert release.wait(5)
                assert conn.execute('SELECT 1').fetchone()[0] == 1
        finally:
            finished.set()
    task = asyncio.create_task(runtime.run_blocking('fixture.transcript_read', worker, timeout=0.02))
    try:
        assert await asyncio.to_thread(entered.wait, 5)
        with pytest.raises(BlockingOperationTimeout):
            await task
        assert len(created) == 1 and not created[0].closed
        assert runtime.snapshot()['executor']['active'] == 1
        release.set()
        assert await asyncio.to_thread(finished.wait, 5)
        await runtime.run_blocking('fixture.barrier', lambda: None)
        assert created[0].closed
    finally:
        release.set()
        await runtime.close()


def test_reused_handle_reports_work_only_to_current_budget(tmp_path, monkeypatch):
    import time
    from pa.core.operation_budget import OperationBudget, OperationDeadline, current_operation, report_work_progress
    store = TranscriptStorage(tmp_path / 'fixture.db')
    observed = []
    def progress():
        observed.append(current_operation.get())
        return report_work_progress()
    monkeypatch.setattr('pa.domain.transcript_storage.report_work_progress', progress)
    query = 'WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x<5000) SELECT SUM(x) FROM n'
    first = OperationDeadline(OperationBudget(), time.monotonic())
    second = OperationDeadline(OperationBudget(), time.monotonic())
    with sqlite_connection_scope():
        token = current_operation.set(first)
        try:
            with store._conn() as original:
                assert original.execute(query).fetchone()[0] == 12502500
            assert observed and all(value is first for value in observed)
        finally:
            current_operation.reset(token)
        observed.clear()
        with store._conn() as unbudgeted:
            assert unbudgeted is original
            unbudgeted.execute(query).fetchone()
        assert not observed
        token = current_operation.set(second)
        try:
            with store._conn() as new_budget:
                assert new_budget is original
                new_budget.execute(query).fetchone()
            assert observed and all(value is second for value in observed)
        finally:
            current_operation.reset(token)

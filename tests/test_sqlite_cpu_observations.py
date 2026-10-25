"""Real SQLite calls retain timing evidence without changing results or errors."""
import json
import sqlite3
import asyncio
import threading
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from unittest.mock import patch

import pytest

from pa.core.async_runtime import AsyncRuntime
from pa.core.performance import OperationObservation, current_observation, capture_retention_phases, retention_phase
from pa.core.sqlite_metrics import ObservedConnection


def test_lazy_rows_and_commit_have_separate_cpu_evidence():
    observation = OperationObservation()
    token = current_observation.set(observation)
    try:
        with closing(sqlite3.connect(':memory:', factory=ObservedConnection)) as conn:
            conn.execute('CREATE TABLE fixture(value INTEGER)')
            conn.executemany('INSERT INTO fixture VALUES(?)', [(i,) for i in range(400)])
            conn.commit()
            assert observation.sqlite_commit_thread_cpu_ms > 0
            commit_cpu = observation.sqlite_commit_thread_cpu_ms
            cursor = conn.execute('SELECT value FROM fixture ORDER BY value')
            before_fetch = observation.sqlite_statement_thread_cpu_ms
            assert cursor.fetchall() == [(i,) for i in range(400)]
            assert observation.sqlite_statement_thread_cpu_ms > before_fetch
            assert observation.sqlite_rows == 400
            assert observation.sqlite_commit_thread_cpu_ms == commit_cpu
    finally:
        current_observation.reset(token)


def test_failed_commit_keeps_transaction_and_measures_cpu():
    with closing(sqlite3.connect(':memory:', factory=ObservedConnection)) as conn:
        conn.execute('PRAGMA foreign_keys=ON')
        conn.execute('CREATE TABLE parent(id INTEGER PRIMARY KEY)')
        conn.execute('CREATE TABLE child(id REFERENCES parent(id) DEFERRABLE INITIALLY DEFERRED)')
        conn.execute('INSERT INTO child VALUES(1)')
        observation = OperationObservation()
        token = current_observation.set(observation)
        try:
            with pytest.raises(sqlite3.IntegrityError):
                conn.commit()
        finally:
            current_observation.reset(token)
        assert conn.in_transaction
        assert observation.sqlite_commit_ms > 0
        assert observation.sqlite_commit_thread_cpu_ms > 0
        assert observation.sqlite_statement_thread_cpu_ms == 0
        conn.rollback()
        assert conn.execute('SELECT * FROM child').fetchall() == []


def test_inactive_observer_reads_no_additional_cpu_clock():
    with closing(sqlite3.connect(':memory:', factory=ObservedConnection)) as conn:
        with patch('pa.core.sqlite_metrics.time.thread_time', side_effect=AssertionError('inactive clock')):
            assert conn.execute('SELECT 41+1').fetchone() == (42,)


@pytest.mark.asyncio
@pytest.mark.parametrize("completed_before_wait", [False, True])
async def test_cpu_reaches_bounded_worker_diagnostics_and_retention_phases(completed_before_wait):
    private = 'private-sql-result-must-not-appear'
    phases = {}
    loop_thread = threading.get_ident()

    def query():
        assert threading.get_ident() != loop_thread
        with capture_retention_phases(phases), retention_phase('retention_selection'):
            with closing(sqlite3.connect(':memory:', factory=ObservedConnection)) as conn:
                assert conn.execute('SELECT ?', (private,)).fetchone() == (private,)
                with pytest.raises(sqlite3.OperationalError):
                    conn.execute('SELECT * FROM missing_private_table')
        return 42

    runtime = AsyncRuntime(max_workers=1, slow_call_seconds=0)
    try:
        if completed_before_wait:
            # Exercise an executor result already done before shield is awaited.
            # The real SQLite work still runs on a separate private thread.
            loop = asyncio.get_running_loop()
            with ThreadPoolExecutor(max_workers=1) as executor:
                def already_complete(_executor, bound):
                    result = executor.submit(bound).result(timeout=5)
                    future = loop.create_future()
                    future.set_result(result)
                    return future
                with patch.object(loop, 'run_in_executor', side_effect=already_complete):
                    assert await runtime.run_blocking('fixture.sqlite_cpu', query) == 42
        else:
            assert await runtime.run_blocking('fixture.sqlite_cpu', query) == 42
        # Awaiting a completed future need not yield to its metrics callback.
        # Drain only this private runtime before asserting completed evidence;
        # do not change production completion semantics or any request deadline.
        await runtime.close()
        snapshot = runtime.snapshot()
        operation = snapshot['operations']['fixture.sqlite_cpu']
        assert operation['sqlite_statement_thread_cpu_ms']['max'] > 0
        assert operation['sqlite_commit_thread_cpu_ms']['max'] == 0
        assert operation['sqlite_calls']['max'] == 3
        assert operation['sqlite_rows']['max'] == 1
        assert phases['retention_selection']['sqlite_statement_thread_cpu_ms'] > 0
        assert phases['retention_selection']['sqlite_commit_thread_cpu_ms'] == 0
        slow = snapshot['slow_operations']['observations'][-1]
        assert slow['sqlite_statement_thread_cpu_ms'] > 0
        assert slow['sqlite_commit_thread_cpu_ms'] == 0
        assert private not in json.dumps(snapshot)
        assert 'missing_private_table' not in json.dumps(snapshot)
    finally:
        await runtime.close()

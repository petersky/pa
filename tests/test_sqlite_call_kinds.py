"""Bounded maintenance call timings preserve SQLite and scope ownership."""
import json
import sqlite3
import threading
import time
from contextlib import closing
from contextvars import copy_context

import pytest

from pa.core.performance import (
    OperationObservation, capture_retention_phases, capture_sqlite_calls,
    current_observation, retention_phase, sqlite_call_bucket,
)
from pa.core.sqlite_metrics import ObservedConnection
from pa.instance.maintenance import _maintain_database


def connection():
    return sqlite3.connect(':memory:', factory=ObservedConnection)


def test_static_kinds_preserve_rows_exceptions_and_iterator_exhaustion():
    bucket = {}
    observation = OperationObservation()
    token = current_observation.set(observation)
    try:
        with closing(connection()) as conn, capture_sqlite_calls(bucket):
            conn.execute('CREATE TABLE fixture(value)')
            conn.executemany('INSERT INTO fixture VALUES(?)', [(1,), (2,), (3,)])
            conn.commit()
            assert conn.execute('SELECT value FROM fixture ORDER BY value').fetchone() == (1,)
            assert conn.execute('SELECT value FROM fixture ORDER BY value').fetchmany(2) == [(1,), (2,)]
            assert conn.execute('SELECT value FROM fixture ORDER BY value').fetchall() == [(1,), (2,), (3,)]
            assert list(conn.execute('SELECT value FROM fixture ORDER BY value')) == [(1,), (2,), (3,)]
            with pytest.raises(sqlite3.OperationalError):
                conn.execute('SELECT private_sql_literal FROM absent_private_table')
            def private_callable_label():
                return 'private_result'
            assert conn._observe(private_callable_label) == 'private_result'
    finally:
        current_observation.reset(token)
    assert set(bucket) == {'execute', 'executemany', 'fetchone', 'fetchmany', 'fetchall', '__next__', 'commit', 'other'}
    assert bucket['__next__']['calls'] == 4 and bucket['__next__']['errors'] == 0
    assert bucket['execute']['errors'] == 1
    assert observation.sqlite_rows == 9
    serialized = json.dumps(bucket)
    assert 'private' not in serialized and 'fixture' not in serialized
    for item in bucket.values():
        assert item['wall_ms'] >= item['max_wall_ms'] >= 0
        assert item['thread_cpu_ms'] >= item['max_call_thread_cpu_ms'] >= 0
        assert item['max_finished_at_monotonic'] >= item['max_started_at_monotonic']
        assert item['max_wall_ms'] == pytest.approx(1000 * (item['max_finished_at_monotonic'] - item['max_started_at_monotonic']))


def test_nested_repeated_scope_restores_exclusive_owner_even_on_exception():
    outer, inner = {}, {}
    with closing(connection()) as conn, capture_sqlite_calls(outer):
        conn.execute('SELECT 1').fetchone()
        with pytest.raises(ValueError), capture_sqlite_calls(inner):
            conn.execute('SELECT 2').fetchone()
            raise ValueError('fixture')
        conn.execute('SELECT 3').fetchone()
    assert sqlite_call_bucket() is None
    with closing(connection()) as conn, capture_sqlite_calls(inner):
        conn.execute('SELECT 4').fetchone()
    assert outer['execute']['calls'] == inner['execute']['calls'] == 2
    assert outer['fetchone']['calls'] == inner['fetchone']['calls'] == 2


def test_copied_context_cannot_record_on_another_thread_or_after_scope_exit():
    bucket = {}
    def read_in_copied_context():
        with closing(connection()) as conn:
            assert conn.execute('SELECT 1').fetchone() == (1,)
        return sqlite_call_bucket()
    with capture_sqlite_calls(bucket):
        copied = copy_context()
        observed = []
        thread = threading.Thread(target=lambda: copied.run(lambda: observed.append(read_in_copied_context())))
        thread.start()
        thread.join(5)
        assert not thread.is_alive() and observed == [None]
        escaped = copy_context()
    assert escaped.run(read_in_copied_context) is None
    assert bucket == {}


def test_failed_transaction_keeps_exception_identity_and_rolls_back():
    bucket = {}
    with closing(connection()) as conn:
        conn.execute('CREATE TABLE fixture(value UNIQUE)')
        failure = sqlite3.IntegrityError('private failure')
        def raise_original():
            raise failure
        with pytest.raises(sqlite3.IntegrityError) as caught, capture_sqlite_calls(bucket), conn:
            conn.execute('INSERT INTO fixture VALUES(1)')
            conn._observe(raise_original)
        assert caught.value is failure
        assert conn.execute('SELECT COUNT(*) FROM fixture').fetchone() == (0,)
    assert bucket['other']['errors'] == 1
    assert 'private' not in json.dumps(bucket)


def test_maintenance_and_retention_spans_attribute_only_innermost_calls(tmp_path):
    path = tmp_path / 'fixture.db'
    with sqlite3.connect(path) as conn:
        conn.execute('CREATE TABLE fixture(value)')
        conn.execute('INSERT INTO fixture VALUES(1)')
    result = _maintain_database(path)
    assert result['phases']['size_initial']['sqlite_call_kinds']['execute']['calls'] == 3
    assert result['phases']['size_initial']['sqlite_call_kinds']['fetchone']['calls'] == 3
    assert result['phases']['total']['sqlite_call_kinds'] == {}
    phases = {}
    with closing(connection()) as conn, capture_retention_phases(phases):
        with retention_phase('retention_canonical'):
            conn.execute('SELECT 1').fetchall()
            with retention_phase('retention_discovery'):
                conn.execute('SELECT 2').fetchone()
    assert phases['retention_canonical']['sqlite_call_kinds']['fetchall']['calls'] == 1
    assert 'fetchone' not in phases['retention_canonical']['sqlite_call_kinds']
    assert phases['retention_discovery']['sqlite_call_kinds']['fetchone']['calls'] == 1
    assert sqlite_call_bucket() is None


def test_successful_private_busy_wait_keeps_the_native_result_and_call_interval(tmp_path):
    path = tmp_path / 'busy.db'
    with sqlite3.connect(path) as conn:
        conn.execute('CREATE TABLE fixture(value)')
    locked, release = threading.Event(), threading.Event()
    anchors, errors = [], []
    def writer():
        try:
            with closing(sqlite3.connect(path)) as conn:
                conn.execute('BEGIN IMMEDIATE')
                locked.set()
                assert release.wait(5)
                anchors.append(time.perf_counter())
                conn.commit()
        except BaseException as exc:
            errors.append(exc)
    thread = threading.Thread(target=writer)
    thread.start()
    bucket = {}
    timer = None
    try:
        assert locked.wait(5)
        with closing(sqlite3.connect(path, factory=ObservedConnection)) as conn:
            # Private synchronization only: known successful native contention,
            # without changing production busy/warning/deadline configuration.
            def traced(sql):
                nonlocal timer
                if sql.startswith('INSERT') and timer is None:
                    timer = threading.Timer(.02, release.set)
                    timer.start()
            conn.set_trace_callback(traced)
            with capture_sqlite_calls(bucket):
                conn.execute('INSERT INTO fixture VALUES(1)')
                conn.commit()
            assert conn.execute('SELECT value FROM fixture').fetchall() == [(1,)]
    finally:
        release.set()
        if timer is not None:
            timer.join(5)
        thread.join(5)
    assert not thread.is_alive() and not errors and anchors
    call = bucket['execute']
    assert call['calls'] == 1 and call['errors'] == 0
    assert call['max_started_at_monotonic'] <= anchors[0] <= call['max_finished_at_monotonic']

"""Durable write/lock observations retain ordering, errors and unknown outcomes."""
import json
import os
from contextlib import contextmanager

import pytest

from pa.core.async_runtime import AsyncRuntime
from pa.core.io import atomic_write_text
from pa.core.performance import OperationObservation, current_observation
from pa.sync.event_log import EventLog
from pa.sync.object_store import ObjectStore


@contextmanager
def observation():
    value = OperationObservation()
    token = current_observation.set(value)
    try:
        yield value
    finally:
        current_observation.reset(token)


def test_atomic_utf8_write_observes_durability_without_changing_order(tmp_path, monkeypatch):
    destination = tmp_path / 'private-destination'
    calls = []
    fsync, replace = os.fsync, os.replace
    def synced(fd):
        calls.append('directory' if __import__('stat').S_ISDIR(os.fstat(fd).st_mode) else 'file')
        return fsync(fd)
    def replaced(a, b):
        calls.append('replace')
        return replace(a, b)
    monkeypatch.setattr('pa.core.io.os.fsync', synced)
    monkeypatch.setattr('pa.core.io.os.replace', replaced)
    text = 'fixture π🙂'
    with observation() as obs:
        atomic_write_text(destination, text)
    assert destination.read_bytes() == text.encode('utf-8')
    assert calls == ['file', 'replace', 'directory']
    assert obs.atomic_write_calls == obs.file_fsync_calls == obs.directory_fsync_calls == 1
    assert obs.atomic_write_bytes == len(text.encode('utf-8'))
    assert obs.fsync_failures == 0
    assert 'private-destination' not in str(obs)


def test_failed_file_fsync_preserves_old_content_and_cleans_temporary(tmp_path, monkeypatch):
    destination = tmp_path / 'value'
    destination.write_text('old')
    def fail(fd):
        raise OSError('disposable fixture failure')
    monkeypatch.setattr('pa.core.io.os.fsync', fail)
    with observation() as obs, pytest.raises(OSError):
        atomic_write_text(destination, 'new')
    assert destination.read_text() == 'old'
    assert not list(tmp_path.glob('.value.*.tmp'))
    assert obs.file_fsync_calls == obs.fsync_failures == obs.atomic_write_calls == 1
    assert obs.directory_fsync_calls == 0


def test_file_lock_events_are_distinct_and_fd_released_on_exception(tmp_path):
    log = EventLog(ObjectStore(tmp_path / 'objects'), tmp_path / 'data', 'fixture')
    with observation() as obs, pytest.raises(ValueError):
        with log._refs_file_lock():
            raise ValueError('fixture')
    with log._refs_file_lock():
        pass
    assert obs.file_lock_wait_ms >= 0 and obs.file_lock_hold_ms > 0
    assert obs.python_lock_wait_ms == obs.python_lock_hold_ms == 0
    assert obs.lock_depth == 0 and obs.lock_events[0]['name'] == 'sync.refs_file'


@pytest.mark.asyncio
async def test_filesystem_phases_are_bounded_and_serializable_in_runtime(tmp_path):
    runtime = AsyncRuntime(slow_call_seconds=0)
    try:
        await runtime.run_blocking('fixture.durable_write', atomic_write_text, tmp_path / 'private', 'data')
        snapshot = runtime.snapshot()
        json.dumps(snapshot)
        metric = snapshot['operations']['fixture.durable_write']
        assert metric['file_fsync_calls']['max'] == metric['directory_fsync_calls']['max'] == 1
        assert metric['atomic_write_bytes']['max'] == 4
        assert snapshot['slow_operations']['observations'][-1]['file_fsync_calls'] == 1
    finally:
        await runtime.close()


def test_object_io_counts_logical_bytes_and_avoids_counting_duplicate_put(tmp_path):
    store = ObjectStore(tmp_path / 'objects')
    with observation() as obs:
        key = store.put(b'fixture bytes')
        assert store.put(b'fixture bytes') == key
        assert store.get(key) == b'fixture bytes'
        assert store.get('f' * 64) is None
    assert obs.object_put_files == 1 and obs.object_put_bytes == 13
    assert obs.object_get_calls == 2 and obs.object_get_bytes == 13
    assert obs.object_get_ms > 0


@pytest.mark.asyncio
async def test_lifetime_write_totals_do_not_turn_into_last256_window(tmp_path, monkeypatch):
    monkeypatch.setattr('pa.core.io.os.fsync', lambda fd: None)
    runtime = AsyncRuntime()
    total = 0
    try:
        for index in range(270):
            text = str(index)
            total += len(text.encode('utf-8'))
            await runtime.run_blocking('fixture.write_amplification', atomic_write_text, tmp_path / 'fixture', text)
        snapshot = runtime.snapshot()['operations']['fixture.write_amplification']
        assert snapshot['atomic_write_bytes']['samples'] == 256
        assert snapshot['filesystem_totals']['atomic_write_bytes'] == total
        assert snapshot['filesystem_totals']['atomic_write_calls'] == 270
        snapshot['filesystem_totals']['atomic_write_bytes'] = -1
        assert runtime.snapshot()['operations']['fixture.write_amplification']['filesystem_totals']['atomic_write_bytes'] == total
    finally:
        await runtime.close()

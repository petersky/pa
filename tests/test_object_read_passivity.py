"""Read immutable bytes once; retain absence, failure and observation semantics."""
from pathlib import Path
from unittest.mock import patch

import pytest

from pa.core.performance import OperationObservation, current_observation
from pa.sync.object_store import ObjectStore


def test_existing_and_missing_reads_do_not_probe_path_metadata(tmp_path):
    store = ObjectStore(tmp_path)
    data = b'canonical immutable bytes'
    digest = store.put(data)
    observation = OperationObservation()
    token = current_observation.set(observation)
    try:
        with patch.object(Path, 'exists', side_effect=AssertionError('extra stat')):
            assert store.get(digest) == data
            assert store.get('f' * 64) is None
        assert observation.object_get_calls == 2
        assert observation.object_get_bytes == len(data)
        assert observation.object_get_ms >= 0
    finally:
        current_observation.reset(token)


@pytest.mark.parametrize('error', [PermissionError, IsADirectoryError, OSError])
def test_read_failures_preserve_original_exists_fallback(tmp_path, error):
    store = ObjectStore(tmp_path)
    digest = store.put(b'original')
    with patch.object(Path, 'read_bytes', side_effect=error('read failed')):
        with pytest.raises(error, match='read failed'):
            store.get(digest)
        # A path whose metadata is no longer visible retains missing semantics.
        with patch.object(Path, 'exists', return_value=False):
            assert store.get(digest) is None


def test_repair_is_visible_to_the_next_read_without_cached_bytes(tmp_path):
    store = ObjectStore(tmp_path)
    raw = b'original'
    digest = store.put(raw)
    path = store._path_for(digest)
    path.write_bytes(b'corrupt')
    assert store.get(digest) == b'corrupt'
    store.repair(digest, raw)
    assert store.get(digest) == raw

"""An indexed sole parent proves one edge, but never a new fork's closure."""
import pytest

from pa.core.performance import OperationObservation, current_observation
from pa.domain.models import SyncCommit
from pa.sync.event_log import EventHistoryObjectError
from tests.test_realm_convergence import _Node


def graph(tmp_path):
    node = _Node(tmp_path, 'proof', 'Proof')
    def commit(parents):
        return node.objects.put_json(SyncCommit(hash='', realm_id='default',
            instance_id='proof', parent_hashes=parents, event_hashes=[]).model_dump(mode='json'))
    root = commit([])
    parent = commit([root])
    node.log.ensure_indexed('default', parent)
    return node, commit, parent


def test_ready_parent_proof_avoids_full_hash_materialization(tmp_path, monkeypatch):
    node, commit, parent = graph(tmp_path)
    child = commit([parent])
    monkeypatch.setattr(node.log.index, 'ancestors', lambda *a: pytest.fail('whole history copied'))
    obs = OperationObservation()
    token = current_observation.set(obs)
    try:
        assert node.log.is_ancestor(parent, child)
    finally:
        current_observation.reset(token)
    assert obs.sqlite_aggregate_items == obs.sqlite_aggregate_bytes == 0


def test_unready_index_requires_canonical_ancestry_walk(tmp_path, monkeypatch):
    node, commit, parent = graph(tmp_path)
    child = commit([parent])
    node.log.index.mark_failed('default', 'fixture derived index unavailable')
    get = node.log.get_commit
    visited = []
    def counted(h):
        visited.append(h)
        return get(h)
    monkeypatch.setattr(node.log, 'get_commit', counted)
    assert node.log.is_ancestor(parent, child)
    assert parent in visited  # No ready proof means the canonical parent is read.


def test_additional_parent_is_not_skipped_by_direct_edge(tmp_path):
    node, commit, parent = graph(tmp_path)
    child = commit([parent, 'f' * 64])
    with pytest.raises(EventHistoryObjectError):
        node.log.is_ancestor(parent, child)


def test_corrupt_direct_child_never_uses_parent_proof(tmp_path):
    node, commit, parent = graph(tmp_path)
    child = commit([parent])
    path = node.objects._path_for(child)
    path.write_bytes(b'corrupt fixture')
    with pytest.raises(EventHistoryObjectError):
        node.log.is_ancestor(parent, child)

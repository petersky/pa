from pathlib import Path
from types import SimpleNamespace

import pytest

from pa.repository.residual_cleanup import snapshot
from tests.test_selected_workspace_cleanup import setup, target


def prepared(tmp_path, monkeypatch):
    manager, (lease, other) = setup(tmp_path)
    manager.collect_garbage(targets=[target(lease)])
    lease = manager.get_by_id(lease.id)
    root = Path(lease.worktree_path).parent.parent
    Path(lease.worktree_path).mkdir(parents=True)
    manager.store.list_card_lanes.return_value = {lease.card_id: 'done'}
    manager.store.get_session.return_value = SimpleNamespace(status='closed', config_json={
        'durable_runtime': {'in_flight': None, 'queued_prompts': []}})
    monkeypatch.setattr('pa.repository.residual_cleanup._no_open_files', lambda p: None)
    return manager, lease, other, root


def test_explicit_fixture_disposal_and_empty_container_removal(tmp_path, monkeypatch):
    manager, lease, other, root = prepared(tmp_path, monkeypatch)
    fixture = Path(lease.worktree_path) / '.pytest_cache'
    fixture.mkdir()
    (fixture / 'nodeids').write_text('["fixture"]')
    inventory = snapshot(root)[1]
    result = manager.collect_garbage(targets=[target(lease, residual_inventory_sha256=inventory)])
    assert result['targets'][0]['state'] == 'residual_cleaned'
    assert not root.exists()
    assert Path(other.worktree_path).exists()
    assert manager.get_by_id(lease.id).cleanup_evidence['residual_cleanup']['inventory_sha256'] == inventory


@pytest.mark.parametrize('guard', ['queue', 'active', 'unfinished', 'useful', 'changed', 'symlink', 'open'])
def test_residual_guards_retain_every_file(tmp_path, monkeypatch, guard):
    manager, lease, other, root = prepared(tmp_path, monkeypatch)
    fixture = Path(lease.worktree_path) / '.pytest_cache'
    fixture.mkdir()
    valuable = fixture / 'nodeids'
    valuable.write_text('retain')
    digest = snapshot(root)[1]
    active = set()
    if guard == 'queue':
        manager.store.get_session.return_value.config_json['durable_runtime']['queued_prompts'] = ['pending']
    if guard == 'active': active.add(lease.session_id)
    if guard == 'unfinished': manager.store.list_card_lanes.return_value[lease.card_id] = 'active'
    if guard == 'useful':
        (Path(lease.worktree_path) / 'notes.txt').write_text('unique')
        digest = snapshot(root)[1]
    if guard == 'changed': valuable.write_text('new bytes')
    if guard == 'symlink': (fixture / 'link').symlink_to(valuable)
    if guard == 'open':
        from pa.repository.workspace import WorkspaceProvisioningError
        def busy(p): raise WorkspaceProvisioningError('busy')
        monkeypatch.setattr('pa.repository.residual_cleanup._no_open_files', busy)
    result = manager.collect_garbage(targets=[target(lease, residual_inventory_sha256=digest)],
                                     active_session_ids=active)
    assert result['targets'][0]['state'] == 'blocked'
    assert valuable.exists() and Path(other.worktree_path).exists()

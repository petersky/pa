"""Disposable GC hydration contracts; existing Git/fencing tests run separately."""
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import Mock

import pytest

from pa.config import Settings
from pa.repository.workspace import WorkspaceLease, WorkspaceManager


def private_manager(tmp_path: Path) -> WorkspaceManager:
    settings = Settings(data_dir=tmp_path / 'data', workspace_root=tmp_path / 'workspaces', instance_id='private-test', instance_name='private')
    return WorkspaceManager(settings, SimpleNamespace(card_completion_eligible=lambda _card: True))


def lease(manager, identity, **overrides):
    values = dict(id=identity, repository_id='private-repository', repository_url='https://example.invalid/private', card_id='card-a', session_id=identity, cache_path=str(manager.root / 'cache'), worktree_path=str(manager.root / identity), branch='private-' + identity, base_sha='0' * 40, fencing_token=1, state='ready', completed=True, merged=True, expires_at=datetime.now(UTC) + timedelta(hours=1))
    values.update(overrides)
    return manager._save(WorkspaceLease(**values))


def test_filtered_list_preserves_default_full_history_and_card_filter(tmp_path):
    manager = private_manager(tmp_path)
    cleaned = lease(manager, 'cleaned', state='cleaned', cleanup_evidence={'proof': 'é' * 65536})
    ready = lease(manager, 'ready')
    other = lease(manager, 'other', card_id='card-b')
    assert manager.list() == [cleaned, ready, other]
    assert manager.list(card_id='card-a') == [cleaned, ready]
    assert manager.list(include_cleaned=False) == [ready, other]
    assert manager.list(card_id='card-a', include_cleaned=False) == [ready]
    assert manager.get(cleaned.repository_id, cleaned.session_id) == cleaned


def test_gc_does_not_hydrate_cleaned_proofs_but_retains_full_durable_history(tmp_path, monkeypatch):
    manager = private_manager(tmp_path)
    cleaned = [lease(manager, f'cleaned-{i}', state='cleaned', cleanup_evidence={'proof': 'a' * 65536}) for i in range(10)]
    retained = [lease(manager, f'retained-{i}') for i in range(3)]
    original = manager._row
    hydrated = []
    def check(row):
        assert row['state'] != 'cleaned', 'GC must not decode already ignored proof bodies'
        hydrated.append(row['id'])
        return original(row)
    monkeypatch.setattr(manager, '_row', check)
    assert manager.collect_garbage() == dict(cleaned=0, blocked=0, missing_evidence=0, transient_retry=0, retained=3)
    assert hydrated == [row.id for row in retained]
    monkeypatch.setattr(manager, '_row', original)
    assert manager.list() == [*cleaned, *retained]


@pytest.mark.parametrize('reason', ['active', 'incomplete', 'unmerged', 'unexpired'])
def test_already_retained_leases_do_not_read_card_completion(tmp_path, reason):
    manager = private_manager(tmp_path)
    kwargs = dict(expires_at=datetime.now(UTC) - timedelta(seconds=1))
    if reason == 'incomplete': kwargs['completed'] = False
    if reason == 'unmerged': kwargs['merged'] = False
    if reason == 'unexpired': kwargs['expires_at'] = datetime.now(UTC) + timedelta(hours=1)
    row = lease(manager, 'retain', **kwargs)
    manager.store.card_completion_eligible = Mock(side_effect=AssertionError('no completion lookup for a retained lease'))
    result = manager.collect_garbage(active_session_ids={row.session_id} if reason == 'active' else set())
    assert result == dict(cleaned=0, blocked=0, missing_evidence=0, transient_retry=0, retained=1)
    manager.store.card_completion_eligible.assert_not_called()
    assert manager.get(row.repository_id, row.session_id) == row


def test_expired_eligible_shape_still_requires_current_card_acceptance(tmp_path):
    manager = private_manager(tmp_path)
    row = lease(manager, 'unaccepted', expires_at=datetime.now(UTC) - timedelta(seconds=1))
    manager.store.card_completion_eligible = Mock(return_value=False)
    assert manager.collect_garbage() == dict(cleaned=0, blocked=0, missing_evidence=0, transient_retry=0, retained=1)
    manager.store.card_completion_eligible.assert_called_once_with('card-a')
    assert manager.get(row.repository_id, row.session_id) == row

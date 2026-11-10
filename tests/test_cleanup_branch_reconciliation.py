"""Cleanup-only observed branches never rebind an owner or weaken admission."""
import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from pa.repository.untracked_preservation import UntrackedPreservation
from pa.repository.workspace import WorkspaceProvisioningError
from tests.test_repository_workspaces import git
from tests.test_selected_workspace_cleanup import target
from tests.test_untracked_preservation import fixture


@pytest.fixture(autouse=True)
def observer(monkeypatch):
    monkeypatch.setattr('pa.repository.untracked_preservation._no_open_files', lambda _: None)


def scenario(tmp_path):
    manager, lease, other, source, request = fixture(tmp_path, single_file=True)
    tree = Path(lease.worktree_path)
    expected, actual = 'pa/selected-session-cache', 'pa/selected-session-final-review'
    git(tree, 'branch', '-m', expected)
    lease.branch = expected
    manager._save(lease)
    git(tree, 'branch', '-m', actual)
    owner = SimpleNamespace(id=lease.session_id, status='closed', card_id=lease.card_id,
        execution_binding={'lease_ids': [lease.id]}, external_session_id='native-fixture',
        config_json={'durable_runtime': {'in_flight': None, 'queued_prompts': []}},
        model_id='saved-sol', mode_id='saved-mode')
    manager.store.get_session.return_value = owner
    manager.store.native_history_session_ids.return_value = [lease.session_id]
    request['target'] = target(lease, expected_actual_branch=actual, expected_native_id='native-fixture')
    return manager, lease, other, source, request, owner


def index_bytes(tree):
    return Path(git(tree, 'rev-parse', '--path-format=absolute', '--git-path', 'index').stdout.strip()).read_bytes()


def test_preserve_and_collect_without_rebinding_or_rewriting_any_ref(tmp_path):
    manager, lease, other, source, request, owner = scenario(tmp_path)
    tree = Path(lease.worktree_path)
    before_index, before_settings = index_bytes(tree), dict(owner.__dict__)
    before_other = other.model_dump()
    with pytest.raises(WorkspaceProvisioningError, match='wrong branch'):
        manager._verify_worktree(Path(lease.cache_path), tree, lease)
    original_git = manager._git
    writes = []
    def observed(*args, **kwargs):
        if any(a in args for a in ('update-ref', 'symbolic-ref', 'checkout', 'switch', 'reset')):
            # symbolic-ref without a new value is a read.
            if 'symbolic-ref' not in args or '--quiet' not in args:
                writes.append(args)
        assert 'fetch' not in args and 'clone' not in args
        return original_git(*args, **kwargs)
    manager._git = observed
    result = UntrackedPreservation(manager).operate(**request, action='preserve')
    assert result['effect'] == 'complete' and not writes
    assert index_bytes(tree) == before_index
    assert git(tree, 'symbolic-ref', '--short', 'HEAD').stdout.strip() == request['target'].expected_actual_branch
    assert manager.get_by_id(lease.id).branch == lease.branch
    # Ordinary admission/automatic collection still cannot adopt this branch.
    with pytest.raises(WorkspaceProvisioningError, match='wrong branch'):
        manager._verify_worktree(Path(lease.cache_path), tree, lease)
    assert manager.collect_garbage(active_session_ids={other.session_id})['cleaned'] == 0
    lease = manager.get_by_id(lease.id)
    request['target'] = target(lease, expected_actual_branch=request['target'].expected_actual_branch,
                               expected_native_id='native-fixture')
    outcome = manager.collect_garbage(targets=[request['target']])
    assert outcome['cleaned'] == 1 and not tree.exists()
    assert outcome['targets'][0]['cleanup_evidence']['branch_reconciliation']['lease_branch_unchanged']
    assert manager.get_by_id(lease.id).branch == lease.branch
    assert owner.__dict__ == before_settings
    assert manager.get_by_id(other.id).model_dump() == before_other
    assert Path(other.worktree_path).exists()
    assert len(writes) == 1 and 'update-ref' in writes[0]
    assert writes[0][-1] == request['target'].expected_head


@pytest.mark.parametrize('fault', ['native', 'native_missing', 'alias', 'queued', 'inflight',
    'owner_live', 'binding', 'card', 'expected_exists', 'foreign_branch', 'other_lease', 'head'])
def test_exact_owner_namespace_and_ref_fences_reject_without_mutation(tmp_path, fault):
    manager, lease, other, source, request, owner = scenario(tmp_path)
    tree = Path(lease.worktree_path)
    if fault == 'native': owner.external_session_id = 'different'
    if fault == 'native_missing': request['target'].expected_native_id = None
    if fault == 'alias': manager.store.native_history_session_ids.return_value.append('another-owner')
    if fault == 'queued': owner.config_json['durable_runtime']['queued_prompts'] = [{'id':'retained'}]
    if fault == 'inflight': owner.config_json['durable_runtime']['in_flight'] = {'id':'retained'}
    if fault == 'owner_live': owner.status = 'idle'
    if fault == 'binding': owner.execution_binding = {}
    if fault == 'card': owner.card_id = 'different'
    if fault == 'expected_exists': git(tree, 'branch', lease.branch)
    if fault == 'foreign_branch': request['target'].expected_actual_branch = 'main'
    if fault == 'other_lease':
        other.branch = request['target'].expected_actual_branch; manager._save(other)
    if fault == 'head': request['target'].expected_head = 'a' * 40
    before = source.read_bytes(), source.stat(), index_bytes(tree)
    with pytest.raises(WorkspaceProvisioningError):
        UntrackedPreservation(manager).operate(**request, action='preserve')
    assert (source.read_bytes(), source.stat().st_ino, index_bytes(tree)) == (before[0], before[1].st_ino, before[2])
    assert UntrackedPreservation(manager).operate(**request, action='status')['effect'] == 'none'


@pytest.mark.parametrize('when', ['observation', 'fetch'])
def test_queue_race_or_new_expected_ref_before_effect_retains_all_bytes(tmp_path, monkeypatch, when):
    manager, lease, _, source, request, owner = scenario(tmp_path)
    tree = Path(lease.worktree_path)
    if when == 'observation':
        def change(_):owner.config_json['durable_runtime']['queued_prompts'] = [{'id':'new'}]
        monkeypatch.setattr('pa.repository.untracked_preservation._no_open_files', change)
        with pytest.raises(WorkspaceProvisioningError):
            UntrackedPreservation(manager).operate(**request, action='preserve')
        assert source.exists()
    else:
        UntrackedPreservation(manager).operate(**request, action='preserve')
        def refresh(_):
            git(tree, 'branch', lease.branch)
            return None
        manager._refresh_origin_for_cleanup = refresh
        result = manager.collect_garbage(targets=[request['target']])
        assert not result['cleaned'] and tree.exists()
        assert 'missing' in result['targets'][0]['reason']


def test_existing_preservation_receipt_identity_is_unchanged(tmp_path):
    manager, _, _, _, request = fixture(tmp_path, single_file=True)
    old = request['target'].model_dump(mode='json')
    old.pop('expected_actual_branch'); old.pop('expected_native_id')
    old.pop('expected_owner_updated_at')
    fingerprint = dict(target=old, relative_path=request['relative_path'],
        attachment_sha256=request['attachment_sha256'], principal=request['principal'])
    expected = hashlib.sha256(json.dumps(fingerprint, sort_keys=True).encode()).hexdigest()
    result = UntrackedPreservation(manager).operate(**request, action='status')
    assert result == {'operation_id':expected, 'phase':'not_started', 'effect':'none'}


@pytest.mark.parametrize('fault', [None, 'version', 'cwd', 'instance', 'project', 'ambiguous', 'partial_binding'])
def test_legacy_owner_requires_explicit_version_and_exact_unique_lease_without_repairing_binding(tmp_path, fault):
    from datetime import UTC, datetime, timedelta
    manager, lease, other, source, request, owner = scenario(tmp_path)
    owner.execution_binding = {}
    owner.updated_at = datetime.now(UTC)
    owner.cwd, owner.project_id = lease.worktree_path, lease.project_id
    owner.origin_instance_id = manager.settings.instance_id
    request['target'].expected_owner_updated_at = owner.updated_at
    if fault == 'version': request['target'].expected_owner_updated_at -= timedelta(seconds=1)
    if fault == 'cwd': owner.cwd = other.worktree_path
    if fault == 'instance': owner.origin_instance_id = 'other-instance'
    if fault == 'project': owner.project_id = 'other-project'
    if fault == 'ambiguous':
        other.session_id = lease.session_id; manager._save(other)
    if fault == 'partial_binding': owner.execution_binding = {'lease_ids':[]}
    if fault:
        with pytest.raises(WorkspaceProvisioningError):
            UntrackedPreservation(manager).operate(**request, action='preserve')
        assert source.exists()
    else:
        assert UntrackedPreservation(manager).operate(**request, action='preserve')['effect'] == 'complete'
        assert owner.execution_binding == {}


def test_observed_branch_unknown_preservation_is_not_replayed_after_restart(tmp_path, monkeypatch):
    import os
    manager, _, _, source, request, _ = scenario(tmp_path)
    rename = os.rename
    def lost_ack(src, dest):
        rename(src, dest)
        raise OSError('fixture: lost acknowledgement after retained rename')
    monkeypatch.setattr('pa.repository.untracked_preservation.os.rename', lost_ack)
    with pytest.raises(OSError):UntrackedPreservation(manager).operate(**request, action='preserve')
    # A new service instance observes the original durable operation identity.
    service = UntrackedPreservation(manager)
    status = service.operate(**request, action='status')
    assert status['effect'] == 'unknown' and status['phase'] == 'relocating'
    assert not source.exists()
    retained = manager.root/'preserved-untracked'/status['operation_id']/'files'/source.name
    assert retained.is_file()
    with pytest.raises(WorkspaceProvisioningError, match='receipt'):
        service.operate(**request, action='preserve')


@pytest.mark.asyncio
@pytest.mark.parametrize('fault', ['live', 'principal', 'queued', 'native_alias'])
async def test_route_denies_before_origin_fetch_and_releases_reservation(tmp_path, monkeypatch, fault):
    from fastapi import HTTPException
    from pa.core.async_runtime import AsyncRuntime
    from pa.modules import instance
    manager, lease, _, source, request, owner = scenario(tmp_path)
    owner.principal_id = 'user:other' if fault == 'principal' else 'user:local'
    if fault == 'queued': owner.config_json['durable_runtime']['queued_prompts'] = [{'id':'retained'}]
    if fault == 'native_alias': manager.store.native_history_session_ids.return_value.append('other-owner')
    def forbidden(_):raise AssertionError('No origin fetch before validated admission')
    manager._refresh_origin_for_cleanup = forbidden
    agent = SimpleNamespace(workspace_manager=manager, store=manager.store, _admitting_sessions=set(),
        get=lambda _:SimpleNamespace(_closed=False) if fault == 'live' else None, list_runtimes=lambda:[])
    runtime = AsyncRuntime(max_workers=2, max_queue=2); await runtime.start()
    services = {'instance_agent':agent, 'async_runtime':runtime}
    req = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(ctx=SimpleNamespace(require_service=services.__getitem__))))
    monkeypatch.setattr(instance, 'require_user', lambda _:SimpleNamespace(role='user'))
    monkeypatch.setattr(instance, 'get_principal_id', lambda _:'user:local')
    try:
        with pytest.raises(HTTPException):
            await instance.collect_selected_workspaces(req, instance.WorkspaceSelectedCleanupRequest(targets=[request['target']]))
        assert source.exists() and not agent._admitting_sessions
    finally:await runtime.close()


@pytest.mark.asyncio
async def test_preservation_route_accepts_version_fenced_legacy_owner_without_writing_binding(tmp_path, monkeypatch):
    from datetime import UTC, datetime
    from pa.core.async_runtime import AsyncRuntime
    from pa.modules import instance, items
    manager, lease, _, source, params, owner = scenario(tmp_path)
    owner.execution_binding = {}; owner.updated_at = datetime.now(UTC)
    owner.cwd, owner.project_id = lease.worktree_path, lease.project_id
    owner.origin_instance_id = manager.settings.instance_id; owner.principal_id = 'user:local'
    params['target'].expected_owner_updated_at = owner.updated_at
    agent = SimpleNamespace(workspace_manager=manager, store=manager.store,
        _admitting_sessions=set(), get=lambda _:None)
    runtime = AsyncRuntime(max_workers=2, max_queue=2); await runtime.start()
    services = {'instance_agent':agent, 'async_runtime':runtime}
    req = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(ctx=SimpleNamespace(require_service=services.__getitem__))))
    monkeypatch.setattr(instance, 'require_user', lambda _:SimpleNamespace(role='user'))
    monkeypatch.setattr(instance, 'get_principal_id', lambda _:'user:local')
    monkeypatch.setattr(items, '_card_attachment_or_404', lambda *_:(None, SimpleNamespace(sha256=params['attachment_sha256'])))
    monkeypatch.setattr(items, '_attachment_store', lambda _:SimpleNamespace(blob_path=lambda _:params['attachment_path']))
    body = instance.WorkspaceUntrackedPreservationRequest(target=params['target'],
        relative_path=params['relative_path'], attachment_card_id='custody-card', attachment_id='a'*32,
        attachment_sha256=params['attachment_sha256'], action='preserve')
    try:
        result = await instance.preserve_selected_untracked(req, body)
        assert result['effect'] == 'complete' and not source.exists()
        assert owner.execution_binding == {} and not agent._admitting_sessions
        body.action = 'status'
        assert await instance.preserve_selected_untracked(req, body) == result
    finally:await runtime.close()

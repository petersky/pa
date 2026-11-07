"""Targeted recovery preserves the old state and never infers discard authority."""
import asyncio
import json
import os
import tarfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from pa.core.async_runtime import AsyncRuntime
from pa.repository.materialization import MaterializationRepair, digest
from pa.repository.workspace import WorkspaceProvisioningError
from tests.test_repository_workspaces import manager_for, git


def empty_checkout(tmp_path):
    manager, _, linked = manager_for(tmp_path)
    lease = manager.provision_repository(linked, project_id='project-1', session_id='same', card_id='card')
    tree = Path(lease.worktree_path)
    git(tree, 'config', 'user.name', 'Fixture');git(tree, 'config', 'user.email', 'fixture@pa.invalid')
    # Disposable interrupted checkout state, no production fault injection.
    git(tree, 'read-tree', '--empty')
    (tree / 'README.md').unlink()
    (tree / '.private').mkdir()
    (tree / '.private' / 'evidence').write_bytes(b'untracked\x00retained')
    (tree / 'link').symlink_to('outside-must-not-be-read')
    index = Path(git(tree, 'rev-parse', '--path-format=absolute', '--git-path', 'index').stdout.strip())
    args = dict(session_id=lease.session_id, lease_id=lease.id, card_id=lease.card_id,
                expected_fence=lease.fencing_token, expected_head=lease.base_sha,
                idempotency_key='one-recovery', principal='user:local')
    return manager, lease, tree, index, args


def test_failed_checkout_retry_cannot_publish_empty_index_ready(tmp_path, monkeypatch):
    manager, lease, tree, index, _ = empty_checkout(tmp_path)
    before = index.read_bytes()
    lease.state = 'failed';manager._save(lease)
    linked = manager.linked_repositories('project-1', realm_id='default')[0]
    with pytest.raises(WorkspaceProvisioningError, match='Empty-index'):
        manager.provision_repository(linked, project_id='project-1', session_id=lease.session_id, card_id='card')
    assert index.read_bytes() == before
    assert (tree / '.private' / 'evidence').read_bytes() == b'untracked\x00retained'
    assert not (tree / 'README.md').exists()


def test_normal_resume_guard_reads_header_without_extra_git_process(tmp_path, monkeypatch):
    manager, _, linked = manager_for(tmp_path)
    lease = manager.provision_repository(linked, project_id='project-1', session_id='same', card_id='card')
    calls=[];original=manager._git
    def tracked(*args,**kwargs):
        calls.append(args);return original(*args,**kwargs)
    monkeypatch.setattr(manager,'_git',tracked)
    manager._ensure_worktree(Path(lease.cache_path),Path(lease.worktree_path),lease.branch,lease.base_sha,lease)
    assert len(calls)==3  # Existing root/common-directory/branch identity reads.
    assert not any('ls-files' in call for call in calls)


def test_prepare_preserves_index_all_files_and_apply_fills_only_missing_paths(tmp_path):
    manager, lease, tree, index, args = empty_checkout(tmp_path)
    service = MaterializationRepair(manager)
    before = index.read_bytes()
    prepared = service.operate(**args, action='prepare')
    assert prepared['phase'] == 'prepared' and prepared['effect'] == 'none'
    assert not (tree / 'README.md').exists() and index.read_bytes() == before
    root = manager.root / 'materialization-repairs' / prepared['operation_id']
    assert (root / 'index.before').read_bytes() == before
    with tarfile.open(root / 'worktree.before.tar') as tar:
        assert tar.extractfile('.private/evidence').read() == b'untracked\x00retained'
        assert tar.getmember('link').linkname == 'outside-must-not-be-read'
    result = service.operate(**args, action='apply', expected_revision=prepared['revision'])
    assert result['phase'] == 'completed' and result['restored_paths'] == 1
    assert (tree / 'README.md').read_text() == 'base\n'
    assert git(tree, 'diff', '--cached', '--name-only').stdout == ''
    assert (tree / '.private' / 'evidence').read_bytes() == b'untracked\x00retained'
    assert (root / 'index.before').read_bytes() == before
    assert service.operate(**args, action='apply', expected_revision=prepared['revision']) == result
    assert service.operate(**args, action='status') == result


@pytest.mark.parametrize('change', ['file', 'index', 'index_mode', 'head', 'fence', 'preservation'])
def test_stale_or_changed_state_refuses_apply_without_overwrite(tmp_path, change):
    manager, lease, tree, index, args = empty_checkout(tmp_path)
    service = MaterializationRepair(manager); prepared = service.operate(**args, action='prepare')
    if change == 'file':(tree / 'README.md').write_bytes(b'new user work')
    if change == 'index':git(tree, 'read-tree', 'HEAD')
    if change == 'index_mode':index.chmod(0o600 if index.stat().st_mode & 0o777 != 0o600 else 0o644)
    if change == 'head':git(tree, 'commit', '--allow-empty', '-m', 'unique')
    if change == 'fence':lease.fencing_token += 1;manager._save(lease)
    if change == 'preservation':
        (manager.root / 'materialization-repairs' / prepared['operation_id'] / 'worktree.before.tar').write_bytes(b'changed')
    original = index.read_bytes()
    with pytest.raises(WorkspaceProvisioningError):
        service.operate(**args, action='apply', expected_revision=prepared['revision'])
    assert index.read_bytes() == original
    if change == 'file':assert (tree / 'README.md').read_bytes() == b'new user work'


def test_idempotency_is_bound_to_exact_lease_arguments(tmp_path):
    manager, _, _, _, args = empty_checkout(tmp_path)
    service = MaterializationRepair(manager);service.operate(**args, action='prepare')
    with pytest.raises(WorkspaceProvisioningError, match='Idempotency'):
        service.operate(**{**args, 'expected_head': 'a' * 40}, action='prepare')


def test_unique_commits_preserved_in_bundle_with_original_base(tmp_path):
    manager, _, tree, _, args = empty_checkout(tmp_path)
    git(tree, 'read-tree', 'HEAD');git(tree, 'checkout-index', '--all')
    git(tree, 'config', 'user.name', 'Fixture');git(tree, 'config', 'user.email', 'fixture@pa.invalid')
    (tree / 'README.md').write_text('unique version\n');git(tree, 'add', 'README.md');git(tree, 'commit', '-m', 'unique')
    head = git(tree, 'rev-parse', 'HEAD').stdout.strip();args['expected_head'] = head
    git(tree, 'read-tree', '--empty');(tree / 'README.md').unlink()
    prepared = MaterializationRepair(manager).operate(**args, action='prepare')
    root = manager.root / 'materialization-repairs' / prepared['operation_id']
    assert head in git(tree, 'bundle', 'list-heads', str(root / 'head.before.bundle')).stdout
    assert prepared['preservation']['history_scope'] == 'bundle_with_base_prerequisite'


def test_explicit_quarantine_relocates_every_entry_intact_and_returns_clean_checkout(tmp_path):
    manager, _, tree, index, args = empty_checkout(tmp_path)
    service = MaterializationRepair(manager)
    before = index.read_bytes()
    args['quarantine_untracked'] = True
    prepared = service.operate(**args, action='prepare')
    assert (tree / '.private' / 'evidence').exists()
    result = service.operate(**args, action='apply', expected_revision=prepared['revision'])
    assert not result['dirty'] and result['untracked'] == 0
    root = manager.root / 'materialization-repairs' / prepared['operation_id']
    assert (root / 'retained-worktree' / '.private' / 'evidence').read_bytes() == b'untracked\x00retained'
    assert os.readlink(root / 'retained-worktree' / 'link') == 'outside-must-not-be-read'
    assert (root / 'index.before').read_bytes() == before
    assert (tree / '.git').is_file() and (tree / 'README.md').read_text() == 'base\n'
    assert git(tree, 'status', '--porcelain').stdout == ''


def test_empty_index_existing_head_files_are_verified_and_left_in_place(tmp_path):
    manager, _, tree, index, args = empty_checkout(tmp_path)
    (tree/'README.md').write_text('base\n')
    inode=(tree/'README.md').stat().st_ino
    before=index.read_bytes()
    service=MaterializationRepair(manager)
    prepared=service.operate(**args,action='prepare')
    assert prepared['matching_existing_tracked_paths']==1 and prepared['missing_tracked_paths']==0
    result=service.operate(**args,action='apply',expected_revision=prepared['revision'])
    assert result['restored_paths']==0 and result['matching_existing_tracked_paths']==1
    assert (tree/'README.md').stat().st_ino==inode and (tree/'README.md').read_text()=='base\n'
    root=manager.root/'materialization-repairs'/prepared['operation_id']
    assert (root/'index.before').read_bytes()==before


def test_mixed_matching_missing_files_and_ignored_cache_are_preserved(tmp_path):
    manager, lease, tree, index, args = empty_checkout(tmp_path)
    git(tree,'read-tree','HEAD');git(tree,'checkout-index','--all')
    (tree/'.gitignore').write_text('cache/\n');(tree/'second.txt').write_text('second\n')
    git(tree,'add','.gitignore','second.txt');git(tree,'commit','-m','tracked ignore and second')
    args['expected_head']=git(tree,'rev-parse','HEAD').stdout.strip()
    git(tree,'read-tree','--empty');(tree/'second.txt').unlink()
    (tree/'cache').mkdir();(tree/'cache'/'active').write_text('keep active cache in place')
    before=(tree/'README.md').stat().st_ino
    service=MaterializationRepair(manager);args['quarantine_untracked']=True
    prepared=service.operate(**args,action='prepare')
    assert prepared['matching_existing_tracked_paths']==2 and prepared['missing_tracked_paths']==1
    result=service.operate(**args,action='apply',expected_revision=prepared['revision'])
    assert result['restored_paths']==1 and not result['dirty']
    assert (tree/'README.md').stat().st_ino==before
    assert (tree/'cache'/'active').read_text()=='keep active cache in place'
    assert (tree/'second.txt').read_text()=='second\n'


def test_status_survives_runtime_and_lease_changes(tmp_path):
    manager, lease, _, _, args = empty_checkout(tmp_path)
    service = MaterializationRepair(manager);prepared = service.operate(**args, action='prepare')
    lease.fencing_token += 1;lease.completed = True;manager._save(lease)
    assert service.operate(**args, action='status') == prepared


def test_preservation_does_not_hold_shared_cache_lock_or_block_another_card(tmp_path, monkeypatch):
    import threading
    from concurrent.futures import ThreadPoolExecutor
    manager, lease, tree, _, args = empty_checkout(tmp_path)
    linked = manager.linked_repositories('project-1',realm_id='default')[0]
    entered = threading.Event();release = threading.Event()
    original = tarfile.TarFile.addfile
    def paused(tar, *a, **k):
        entered.set()
        assert release.wait(10)
        return original(tar,*a,**k)
    monkeypatch.setattr(tarfile.TarFile,'addfile',paused)
    with ThreadPoolExecutor(max_workers=2) as pool:
        preparation=pool.submit(MaterializationRepair(manager).operate,**args,action='prepare')
        assert entered.wait(5)
        try:
            other=pool.submit(manager.provision_repository,linked,project_id='project-1',session_id='other',card_id='other-card')
            second=other.result(timeout=5)
            assert second.cache_path==lease.cache_path and second.state=='ready'
            assert not preparation.done()
        finally:release.set()
        assert preparation.result(timeout=10)['phase']=='prepared'
    assert not (tree/'README.md').exists()


@pytest.mark.parametrize('kind', ['tracked-file', 'symlink-parent', 'index-lock'])
def test_existing_content_and_writers_refuse_prepare(tmp_path, kind):
    manager, _, tree, index, args = empty_checkout(tmp_path)
    if kind == 'tracked-file':(tree / 'README.md').write_bytes(b'precious data')
    elif kind == 'index-lock':index.with_name('index.lock').write_bytes(b'other writer')
    else:(tree / 'README.md').symlink_to(tmp_path / 'outside')
    before = index.read_bytes()
    with pytest.raises(WorkspaceProvisioningError):MaterializationRepair(manager).operate(**args, action='prepare')
    assert index.read_bytes() == before


def test_interrupted_apply_retains_everything_and_is_never_blindly_replayed(tmp_path, monkeypatch):
    manager, _, tree, index, args = empty_checkout(tmp_path)
    service = MaterializationRepair(manager);prepared = service.operate(**args, action='prepare')
    before = index.read_bytes()
    monkeypatch.setattr(os, 'link', lambda *a, **k: (_ for _ in ()).throw(OSError('disposable fault')))
    with pytest.raises(OSError):service.operate(**args, action='apply', expected_revision=prepared['revision'])
    assert index.read_bytes() == before
    assert service.operate(**args, action='status')['effect'] == 'unknown'
    with pytest.raises(WorkspaceProvisioningError, match='unknown'):
        service.operate(**args, action='apply', expected_revision=prepared['revision'])


@pytest.mark.asyncio
async def test_http_owner_guard_and_cancellation_keep_fence_until_effect_finishes(tmp_path, monkeypatch):
    from pa.modules.instance import repair_workspace_materialization, WorkspaceMaterializationRequest
    manager, lease, _, _, args = empty_checkout(tmp_path)
    runtime = SimpleNamespace(session=SimpleNamespace(card_id='card', principal_id='user:local', execution_binding={'lease_ids':[lease.id]}), prompting=False, _queue=[],
                              _prompt_lock=asyncio.Lock(), _prompt_admission_lock=asyncio.Lock())
    agent = SimpleNamespace(get=lambda _:runtime, _admitting_sessions=set(), workspace_manager=manager)
    executor = AsyncRuntime(max_workers=1)
    services = {'instance_agent':agent,'async_runtime':executor}
    request = SimpleNamespace(state=SimpleNamespace(user=object(),principal_id='user:local'),
                              app=SimpleNamespace(state=SimpleNamespace(ctx=SimpleNamespace(require_service=services.__getitem__))))
    body = WorkspaceMaterializationRequest(**{k:v for k,v in args.items() if k not in {'lease_id','principal'}}, action='prepare')
    runtime._queue.append(object())
    with pytest.raises(HTTPException) as denied:await repair_workspace_materialization(request,lease.id,body)
    assert denied.value.detail['code']=='workspace_materialization_owner_busy'
    runtime._queue.clear()
    entered=asyncio.Event();release=asyncio.Event()
    async def worker(*a,**k):
        entered.set();await release.wait();return {'phase':'prepared'}
    monkeypatch.setattr(executor,'run_blocking',worker)
    try:
        task=asyncio.create_task(repair_workspace_materialization(request,lease.id,body))
        await entered.wait();task.cancel()
        with pytest.raises(asyncio.CancelledError):await task
        assert lease.session_id in agent._admitting_sessions and runtime._prompt_lock.locked()
        release.set()
        for _ in range(10):
            await asyncio.sleep(0)
            if not agent._admitting_sessions:break
        assert not agent._admitting_sessions and not runtime._prompt_lock.locked()
    finally:await executor.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('change', ['idle', 'missing', 'queued', 'in_flight', 'other_principal', 'other_lease'])
async def test_disconnected_owner_requires_positive_durable_idle_and_exact_binding(tmp_path, change):
    from pa.modules.instance import repair_workspace_materialization, WorkspaceMaterializationRequest
    manager, lease, _, _, args = empty_checkout(tmp_path)
    durable = {'in_flight':None,'queued_prompts':[]}
    session = SimpleNamespace(card_id='card', principal_id='user:local', status='disconnected',
                              execution_binding={'lease_ids':[lease.id]}, config_json={'durable_runtime':durable})
    if change == 'missing':session.config_json = {}
    if change == 'queued':durable['queued_prompts'] = [{'id':'retained-prompt'}]
    if change == 'in_flight':durable['in_flight'] = {'id':'active-prompt'}
    if change == 'other_principal':session.principal_id = 'user:other'
    if change == 'other_lease':session.execution_binding = {'lease_ids':['other']}
    executor = AsyncRuntime(max_workers=1)
    agent = SimpleNamespace(get=lambda _:None, _admitting_sessions=set(), workspace_manager=manager,
                            store=SimpleNamespace(get_session=lambda _:session))
    services = {'instance_agent':agent,'async_runtime':executor}
    request = SimpleNamespace(state=SimpleNamespace(user=object(),principal_id='user:local'),
                              app=SimpleNamespace(state=SimpleNamespace(ctx=SimpleNamespace(require_service=services.__getitem__))))
    body = WorkspaceMaterializationRequest(**{k:v for k,v in args.items() if k not in {'lease_id','principal'}}, action='prepare')
    try:
        if change == 'idle':
            result = await repair_workspace_materialization(request,lease.id,body)
            assert result['phase'] == 'prepared'
        else:
            with pytest.raises(HTTPException) as denied:await repair_workspace_materialization(request,lease.id,body)
            expected = ('owner_principal_mismatch' if change=='other_principal' else
                        'owner_lease_mismatch' if change=='other_lease' else 'owner_idle_unverified')
            assert denied.value.detail['code']=='workspace_materialization_'+expected
            assert MaterializationRepair(manager).operate(**args,action='status')['phase']=='not_started'
        assert not agent._admitting_sessions
    finally:await executor.close()


@pytest.mark.asyncio
async def test_mcp_unknown_response_recovers_workspace_receipt_without_canonical_absence(monkeypatch):
    from tests.test_instance_mcp import FakeMcp
    from pa.mcp import local_api
    from pa.mcp.tools.instance import register_mcp
    error = local_api.LocalPAUnknownOutcome('private transport details', operation_id='same-key',
        correlation_id='same-correlation', endpoint='/api/workspaces/lease/materialization')
    async def offload(*a, **kwargs):raise error
    mcp = FakeMcp()
    register_mcp(mcp, SimpleNamespace(settings=SimpleNamespace(),require_service=lambda _:SimpleNamespace(run_blocking=offload)))
    with pytest.raises(local_api.LocalPAUnknownOutcome) as caught:
        await mcp.functions['workspace_materialization_repair'](
            lease_id='lease',session_id='same',card_id='card',expected_fence=1,
            expected_head='a'*40,idempotency_key='same-key',action='prepare')
    assert caught.value.operation_id == 'same-key' and caught.value.correlation_id == 'same-correlation'
    assert 'action=status' in caught.value.recovery_action
    assert 'get_operation_outcome' not in str(caught.value) and 'private transport details' not in str(caught.value)


@pytest.mark.asyncio
async def test_owner_fences_remain_until_physical_worker_completion(tmp_path, monkeypatch):
    import threading
    from pa.modules.instance import repair_workspace_materialization, WorkspaceMaterializationRequest
    manager, lease, _, _, args = empty_checkout(tmp_path)
    runtime=SimpleNamespace(session=SimpleNamespace(card_id='card',principal_id='user:local',execution_binding={'lease_ids':[lease.id]}),prompting=False,_queue=[],_prompt_lock=asyncio.Lock(),_prompt_admission_lock=asyncio.Lock())
    agent=SimpleNamespace(get=lambda _:runtime,_admitting_sessions=set(),workspace_manager=manager)
    executor=AsyncRuntime(max_workers=1)
    entered=threading.Event();release=threading.Event()
    def work(*a,**k):entered.set();assert release.wait(5);return {'phase':'prepared'}
    monkeypatch.setattr(MaterializationRepair,'operate',work)
    original=executor.run_blocking
    async def bounded(operation,call,*a,**k):
        assert k['wait_for_completion'] is True
        k['timeout']=0.01
        return await original(operation,call,*a,**k)
    monkeypatch.setattr(executor,'run_blocking',bounded)
    services={'instance_agent':agent,'async_runtime':executor}
    request=SimpleNamespace(state=SimpleNamespace(user=object(),principal_id='user:local'),app=SimpleNamespace(state=SimpleNamespace(ctx=SimpleNamespace(require_service=services.__getitem__))))
    body=WorkspaceMaterializationRequest(**{k:v for k,v in args.items() if k not in {'lease_id','principal'}},action='prepare')
    task=asyncio.create_task(repair_workspace_materialization(request,lease.id,body))
    try:
        for _ in range(100):
            if entered.is_set():break
            await asyncio.sleep(0.001)
        assert entered.is_set()
        await asyncio.sleep(0.03)
        assert not task.done() and runtime._prompt_lock.locked() and lease.session_id in agent._admitting_sessions
        release.set();assert (await task)['phase']=='prepared'
        assert not runtime._prompt_lock.locked() and not agent._admitting_sessions
    finally:release.set();await executor.close()

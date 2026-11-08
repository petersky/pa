"""A durable copy never authorizes discard; relocation preserves exact originals."""
import hashlib
import json
import os
import subprocess
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from pa.repository.untracked_preservation import UntrackedPreservation, _no_open_files as real_open_check
from pa.config import Settings
from pa.repository.workspace import WorkspaceLease, WorkspaceManager, WorkspaceProvisioningError
from tests.test_selected_workspace_cleanup import target
from tests.test_repository_workspaces import git


def setup(tmp_path):
    """Real disposable Git/index/leases, initialized without clone or fetch."""
    store = MagicMock()
    store.pending_restart_continuation.return_value = False
    store.card_completion_eligible.return_value = True
    manager = WorkspaceManager(Settings(data_dir=tmp_path / 'data',
        workspace_root=tmp_path / 'workspace'), store)
    cache = manager.cache_root / 'fixture'
    cache.mkdir(parents=True)
    git(cache, 'init', '-b', 'main')
    git(cache, 'config', 'user.email', 'test@pa.invalid')
    git(cache, 'config', 'user.name', 'PA Test')
    (cache / 'README.md').write_text('base\n')
    git(cache, 'add', 'README.md'); git(cache, 'commit', '-m', 'base')
    head = git(cache, 'rev-parse', 'HEAD').stdout.strip()
    git(cache, 'remote', 'add', 'origin', str(cache))
    git(cache, 'update-ref', 'refs/remotes/origin/main', head)
    # These contracts test preservation/retention, not remote transport. Production
    # origin refresh stays unchanged; all fixture objects and refs already exist.
    manager._refresh_origin_for_cleanup = lambda _: None
    leases = []
    for i in range(2):
        tree = manager.worktree_root / f'card-{i}' / f'session-{i}'
        tree.parent.mkdir(parents=True)
        branch = f'fixture-{i}'
        git(cache, 'worktree', 'add', '-b', branch, str(tree), head)
        lease = WorkspaceLease(id=hashlib.sha256(str(i).encode()).hexdigest(),
            repository_id='repo-1', repository_url=cache.as_uri(), card_id=f'card-{i}',
            session_id=f'session-{i}', cache_path=str(cache), worktree_path=str(tree),
            branch=branch, base_sha=head, fencing_token=i+1, state='completed',
            completed=True, merged=True, expires_at=datetime.now(UTC)-timedelta(hours=1))
        manager._save(lease); leases.append(lease)
    return manager, leases


def fixture(tmp_path, single_file=False):
    manager, (selected, other) = setup(tmp_path)
    tree = Path(selected.worktree_path)
    directory = tree / ('audit.log' if single_file else 'audit')
    if single_file:
        directory.write_bytes((b'valuable\x00log\n' * 80)[:1038])
        os.chmod(directory, 0o600)
    else:
        directory.mkdir()
        (directory / 'report.md').write_bytes(b'valuable audit\n')
        (directory / 'evidence.json').write_bytes(b'{"result":"retained"}\n')
        os.chmod(directory / 'report.md', 0o600)
    children = [directory] if single_file else sorted(directory.iterdir())
    files = [{'name': p.name, 'bytes': p.stat().st_size,
              'sha256': hashlib.sha256(p.read_bytes()).hexdigest()} for p in children]
    archive = tmp_path / 'durable.zip'
    manifest = {'source_card_id': selected.card_id, 'source_lease_id': selected.id, 'files': files}
    with zipfile.ZipFile(archive, 'w') as z:
        for p in children:
            z.write(p, p.name)
        z.writestr('preservation-manifest.json', json.dumps(manifest))
    request = dict(target=target(selected), relative_path=directory.name, attachment_path=archive,
                   attachment_sha256=hashlib.sha256(archive.read_bytes()).hexdigest(), principal='user:local')
    return manager, selected, other, directory, request


@pytest.fixture(autouse=True)
def no_external_observer(monkeypatch):
    # OS observer failures are tested separately; Git/index/lease checks are real.
    monkeypatch.setattr('pa.repository.untracked_preservation._no_open_files', lambda _: None)


@pytest.mark.parametrize('single_file', [False, True])
def test_preserve_then_selected_cleanup_retains_original_files_and_unselected_lease(tmp_path, single_file):
    manager, selected, other, directory, request = fixture(tmp_path, single_file)
    children = [directory] if single_file else directory.iterdir()
    original = {p.name: (p.read_bytes(), p.stat()) for p in children}
    before_other = other.model_dump()
    assert manager.collect_garbage(targets=[target(selected)])['cleaned'] == 0
    # The earlier deliberate dirty guard updates the lease timestamp.
    selected = manager.get_by_id(selected.id)
    request['target'] = target(selected)
    service = UntrackedPreservation(manager)
    index = Path(git(Path(selected.worktree_path), 'rev-parse', '--path-format=absolute', '--git-path','index').stdout.strip())
    original_index = index.read_bytes()
    result = service.operate(**request, action='preserve')
    assert result['phase'] == 'preserved' and result['effect'] == 'complete'
    assert result['reclaimed_bytes'] == 0 and result['worktree_removed'] is False
    if single_file: assert result['source_kind'] == 'file'
    if single_file: assert result['preserved_bytes'] == 1038
    assert not directory.exists()
    assert index.read_bytes() == original_index
    retained = manager.root / 'preserved-untracked' / result['operation_id'] / 'files'
    for name, (data, info) in original.items():
        p = retained / name
        assert (p.read_bytes(), p.stat().st_mode, p.stat().st_ino, p.stat().st_mtime_ns) == (
            data, info.st_mode, info.st_ino, info.st_mtime_ns)
    assert service.operate(**request, action='status') == result
    with pytest.raises(WorkspaceProvisioningError, match='receipt'):
        service.operate(**request, action='preserve')
    cleaned = manager.collect_garbage(targets=[target(selected)])
    assert cleaned['cleaned'] == 1
    assert all((retained / name).read_bytes() == data for name, (data, _) in original.items())
    assert manager.get_by_id(other.id).model_dump() == before_other
    assert Path(other.worktree_path).is_dir()


@pytest.mark.parametrize('fault', ['fence','timestamp','head','unfinished','unmerged','unexpired','acceptance',
    'restart','tracked','index_lock','changed_bytes','extra_file','symlink','hardlink','wrong_attachment',
    'traversal','open_files'])
@pytest.mark.parametrize('single_file', [False, True])
def test_guards_preserve_every_target_byte_and_unselected_state(tmp_path, monkeypatch, fault, single_file):
    manager, selected, other, directory, request = fixture(tmp_path, single_file)
    tree = Path(selected.worktree_path)
    primary = directory if single_file else directory / 'report.md'
    if fault == 'fence': request['target'].expected_fence += 1
    if fault == 'timestamp': request['target'].expected_updated_at = datetime.now(UTC)
    if fault == 'head': request['target'].expected_head = 'a' * 40
    if fault == 'unfinished': selected.completed = False; manager._save(selected)
    if fault == 'unmerged': selected.merged = False; manager._save(selected)
    if fault == 'unexpired': selected.expires_at = datetime.now(UTC) + timedelta(days=1); manager._save(selected)
    if fault == 'acceptance': manager.store.card_completion_eligible.return_value = False
    if fault == 'restart': manager.store.pending_restart_continuation.return_value = True
    if fault == 'tracked': git(tree, 'add', directory.name)
    if fault == 'index_lock':
        index = Path(git(tree,'rev-parse','--path-format=absolute','--git-path','index').stdout.strip())
        index.with_name('index.lock').write_text('another writer')
    if fault == 'changed_bytes': primary.write_text('new valuable work')
    if fault == 'extra_file':
        if single_file:
            with zipfile.ZipFile(request['attachment_path'], 'a') as z: z.writestr('other', b'extra')
            request['attachment_sha256'] = hashlib.sha256(request['attachment_path'].read_bytes()).hexdigest()
        else: (directory / 'other').write_text('extra valuable work')
    if fault == 'symlink': primary.unlink(); primary.symlink_to('README.md' if single_file else 'evidence.json')
    if fault == 'hardlink': os.link(primary, tmp_path / 'other-hardlink')
    if fault == 'wrong_attachment': request['attachment_sha256'] = 'a' * 64
    if fault == 'traversal': request['relative_path'] = '../audit'
    if fault == 'open_files':
        def busy(_): raise WorkspaceProvisioningError('open writer')
        monkeypatch.setattr('pa.repository.untracked_preservation._no_open_files', busy)
    def retained():
        children = [directory] if single_file else directory.iterdir()
        return {p.name: (os.readlink(p) if p.is_symlink() else p.read_bytes()) for p in children}
    before = retained()
    other_before = other.model_dump()
    with pytest.raises(WorkspaceProvisioningError): UntrackedPreservation(manager).operate(**request, action='preserve')
    assert directory.exists()
    assert retained() == before
    assert manager.get_by_id(other.id).model_dump() == other_before


def test_interrupted_rename_is_status_unknown_and_never_replayed(tmp_path, monkeypatch):
    manager, selected, other, directory, request = fixture(tmp_path)
    def interrupted(*_): raise OSError('injected before rename')
    monkeypatch.setattr('pa.repository.untracked_preservation.os.rename', interrupted)
    service = UntrackedPreservation(manager)
    with pytest.raises(OSError): service.operate(**request, action='preserve')
    status = service.operate(**request, action='status')
    assert status['phase'] == 'relocating' and status['effect'] == 'unknown'
    with pytest.raises(WorkspaceProvisioningError, match='receipt'):
        service.operate(**request, action='preserve')
    assert directory.is_dir() and Path(other.worktree_path).is_dir()


def test_interruption_after_rename_retains_all_bytes_with_unknown_receipt(tmp_path, monkeypatch):
    manager, _, other, directory, request = fixture(tmp_path)
    original={p.name:p.read_bytes() for p in directory.iterdir()}
    def interrupted(_): raise OSError('injected after rename')
    # The first two directory syncs happen before rename; interrupt the third.
    calls=0
    def sync(path):
        nonlocal calls
        calls+=1
        if calls==3: interrupted(path)
    monkeypatch.setattr('pa.repository.untracked_preservation._sync_dir',sync)
    service=UntrackedPreservation(manager)
    with pytest.raises(OSError): service.operate(**request,action='preserve')
    status=service.operate(**request,action='status')
    assert status['effect']=='unknown' and not directory.exists()
    preserved=manager.root/'preserved-untracked'/status['operation_id']/'files'
    assert {p.name:p.read_bytes() for p in preserved.iterdir()}==original
    with pytest.raises(WorkspaceProvisioningError,match='receipt'):
        service.operate(**request,action='preserve')
    assert Path(other.worktree_path).is_dir()


def test_attachment_does_not_preserve_new_tracked_work(tmp_path):
    manager, selected, _, directory, request = fixture(tmp_path)
    Path(selected.worktree_path, 'README.md').write_text('valuable tracked modification')
    with pytest.raises(WorkspaceProvisioningError, match='Tracked changes'):
        UntrackedPreservation(manager).operate(**request, action='preserve')
    assert directory.is_dir()


@pytest.mark.parametrize('observation', ['busy','denied','missing','timeout','empty'])
@pytest.mark.parametrize('single_file', [False, True])
def test_open_file_observer_is_fail_closed(monkeypatch, tmp_path, observation, single_file):
    path = tmp_path / 'log' if single_file else tmp_path
    if single_file: path.write_bytes(b'log')
    def run(*a, **kw):
        assert a[0] == ['lsof', '-nP', '-Fpcfn', *([str(path)] if single_file else ['+d', str(path)])]
        if observation == 'missing': raise FileNotFoundError('lsof')
        if observation == 'timeout': raise subprocess.TimeoutExpired('lsof',5)
        return SimpleNamespace(returncode=0 if observation=='busy' else 1,
            stdout=b'p123\nf1\n' if observation=='busy' else b'',
            stderr=b'denied' if observation=='denied' else b'')
    monkeypatch.setattr('pa.repository.untracked_preservation.subprocess.run',run)
    if observation=='empty': real_open_check(path)
    else:
        with pytest.raises(WorkspaceProvisioningError): real_open_check(path)


@pytest.mark.asyncio
@pytest.mark.parametrize('fault', [None,'live','queued','inflight','missing_durable','binding','principal',
    'missing_blob','attachment_hash'])
@pytest.mark.parametrize('single_file', [False, True])
async def test_api_owner_admission_and_durable_attachment_guards(tmp_path, monkeypatch, fault, single_file):
    from fastapi import HTTPException
    from pa.core.async_runtime import AsyncRuntime
    from pa.modules import instance, items
    manager, selected, _, directory, params = fixture(tmp_path, single_file)
    durable={'in_flight':None,'queued_prompts':[]}
    owner=SimpleNamespace(config_json={'durable_runtime':durable},card_id=selected.card_id,
        execution_binding={'lease_ids':[selected.id]},principal_id='user:local')
    if fault=='queued': durable['queued_prompts']=[{'id':'retain queued prompt'}]
    if fault=='inflight': durable['in_flight']={'id':'retain in-flight prompt'}
    if fault=='missing_durable': owner.config_json={}
    if fault=='binding': owner.execution_binding={}
    if fault=='principal': owner.principal_id='user:someone-else'
    manager.store.get_session.return_value=owner
    live=SimpleNamespace(_closed=False) if fault=='live' else None
    agent=SimpleNamespace(workspace_manager=manager,store=manager.store,
        _admitting_sessions=set(),get=lambda _:live)
    runtime=AsyncRuntime(max_workers=2,max_queue=2); await runtime.start()
    services={'instance_agent':agent,'async_runtime':runtime}
    req=SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(ctx=SimpleNamespace(require_service=services.__getitem__))))
    monkeypatch.setattr(instance,'require_user',lambda _:SimpleNamespace(role='user'))
    monkeypatch.setattr(instance,'get_principal_id',lambda _:'user:local')
    attachment=SimpleNamespace(sha256='a'*64 if fault=='attachment_hash' else params['attachment_sha256'])
    monkeypatch.setattr(items,'_card_attachment_or_404',lambda *_:(None,attachment))
    monkeypatch.setattr(items,'_attachment_store',lambda _:SimpleNamespace(
        blob_path=lambda _:tmp_path/'missing.zip' if fault=='missing_blob' else params['attachment_path']))
    body=instance.WorkspaceUntrackedPreservationRequest(target=params['target'],relative_path=params['relative_path'],
        attachment_card_id='cleanup-card',attachment_id='a'*32,
        attachment_sha256=params['attachment_sha256'],action='preserve')
    try:
        if fault:
            with pytest.raises(HTTPException): await instance.preserve_selected_untracked(req,body)
            assert directory.exists()
        else:
            result=await instance.preserve_selected_untracked(req,body)
            assert result['phase']=='preserved' and not directory.exists()
            body.action='status'
            assert await instance.preserve_selected_untracked(req,body)==result
        assert not agent._admitting_sessions
    finally: await runtime.close()


@pytest.mark.asyncio
async def test_cancelled_caller_keeps_owner_reservation_until_preservation_finishes(tmp_path,monkeypatch):
    import asyncio
    import threading
    from pa.core.async_runtime import AsyncRuntime
    from pa.modules import instance,items
    manager,selected,_,directory,params=fixture(tmp_path)
    manager.store.get_session.return_value=SimpleNamespace(card_id=selected.card_id,principal_id='user:local',
        execution_binding={'lease_ids':[selected.id]},config_json={'durable_runtime':{'in_flight':None,'queued_prompts':[]}})
    agent=SimpleNamespace(workspace_manager=manager,store=manager.store,_admitting_sessions=set(),get=lambda _:None)
    runtime=AsyncRuntime(max_workers=2,max_queue=2);await runtime.start()
    services={'instance_agent':agent,'async_runtime':runtime}
    req=SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(ctx=SimpleNamespace(require_service=services.__getitem__))))
    monkeypatch.setattr(instance,'require_user',lambda _:SimpleNamespace(role='user'))
    monkeypatch.setattr(instance,'get_principal_id',lambda _:'user:local')
    monkeypatch.setattr(items,'_card_attachment_or_404',lambda *_:(None,SimpleNamespace(sha256=params['attachment_sha256'])))
    monkeypatch.setattr(items,'_attachment_store',lambda _:SimpleNamespace(blob_path=lambda _:params['attachment_path']))
    started,release=threading.Event(),threading.Event()
    def held(_): started.set(); assert release.wait(5)
    monkeypatch.setattr('pa.repository.untracked_preservation._no_open_files',held)
    body=instance.WorkspaceUntrackedPreservationRequest(target=params['target'],relative_path='audit',
        attachment_card_id='cleanup-card',attachment_id='a'*32,attachment_sha256=params['attachment_sha256'],action='preserve')
    task=asyncio.create_task(instance.preserve_selected_untracked(req,body))
    try:
        async with asyncio.timeout(5):
            while not started.is_set():await asyncio.sleep(.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):await task
        assert agent._admitting_sessions=={selected.session_id} and directory.is_dir()
        release.set()
        async with asyncio.timeout(5):
            while agent._admitting_sessions:await asyncio.sleep(.01)
        assert not directory.exists()
    finally:release.set();await runtime.close()


@pytest.mark.parametrize('change', ['bytes', 'same_size_bytes', 'inode', 'mode', 'mtime', 'symlink', 'directory'])
def test_single_file_source_change_during_observation_retains_every_variant(tmp_path, monkeypatch, change):
    manager, _, other, source, request = fixture(tmp_path, single_file=True)
    original = source.read_bytes()
    info = source.stat()
    other_before = other.model_dump()

    def changed(_):
        if change == 'bytes': source.write_bytes(b'new valuable work')
        elif change == 'same_size_bytes':
            source.write_bytes(b'X' + original[1:])
            os.utime(source, ns=(info.st_atime_ns, info.st_mtime_ns))
        elif change == 'mode': source.chmod(0o640)
        elif change == 'mtime': os.utime(source, ns=(info.st_atime_ns, info.st_mtime_ns + 1_000_000))
        else:
            saved = tmp_path / 'retained-original'
            source.rename(saved)
            if change == 'inode':
                source.write_bytes(original); source.chmod(info.st_mode)
                os.utime(source, ns=(info.st_atime_ns, info.st_mtime_ns))
            elif change == 'symlink': source.symlink_to(saved)
            else:
                source.mkdir(); saved.rename(source / source.name)

    monkeypatch.setattr('pa.repository.untracked_preservation._no_open_files', changed)
    service = UntrackedPreservation(manager)
    with pytest.raises(WorkspaceProvisioningError): service.operate(**request, action='preserve')
    assert service.operate(**request, action='status')['effect'] == 'none'
    if change == 'directory': assert (source / source.name).read_bytes() == original
    elif change == 'bytes': assert source.read_bytes() == b'new valuable work'
    elif change == 'same_size_bytes': assert source.read_bytes() == b'X' + original[1:]
    else: assert source.read_bytes() == original
    assert manager.get_by_id(other.id).model_dump() == other_before


@pytest.mark.parametrize('relative', ['', '.', '..', '.git', '../audit.log', 'nested/audit.log', '/audit.log', '\x00'])
def test_single_file_unsafe_selection_never_moves_the_source(tmp_path, relative):
    manager, _, _, source, request = fixture(tmp_path, single_file=True)
    before = source.read_bytes(), source.stat()
    request['relative_path'] = relative
    with pytest.raises(WorkspaceProvisioningError, match='top-level'):
        UntrackedPreservation(manager).operate(**request, action='preserve')
    assert source.read_bytes() == before[0] and source.stat().st_ino == before[1].st_ino


@pytest.mark.parametrize('stage', ['before', 'after'])
def test_single_file_interrupted_rename_status_prevents_replay(tmp_path, monkeypatch, stage):
    manager, _, other, source, request = fixture(tmp_path, single_file=True)
    before = source.read_bytes(), source.stat()
    rename = os.rename
    def interrupted(src, dst):
        if stage == 'after': rename(src, dst)
        raise OSError('disposable fixture interruption')
    monkeypatch.setattr('pa.repository.untracked_preservation.os.rename', interrupted)
    service = UntrackedPreservation(manager)
    with pytest.raises(OSError): service.operate(**request, action='preserve')
    receipt = service.operate(**request, action='status')
    assert receipt['phase'] == 'relocating' and receipt['effect'] == 'unknown'
    retained = source if stage == 'before' else manager.root/'preserved-untracked'/receipt['operation_id']/'files'/source.name
    assert (retained.read_bytes(), retained.stat().st_ino, retained.stat().st_mode, retained.stat().st_mtime_ns) == (
        before[0], before[1].st_ino, before[1].st_mode, before[1].st_mtime_ns)
    with pytest.raises(WorkspaceProvisioningError, match='receipt'):
        service.operate(**request, action='preserve')
    assert Path(other.worktree_path).is_dir()

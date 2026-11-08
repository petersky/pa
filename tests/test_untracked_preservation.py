"""A durable copy never authorizes discard; relocation preserves exact originals."""
import hashlib
import json
import os
import subprocess
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from pa.repository.untracked_preservation import UntrackedPreservation, _no_open_files as real_open_check
from pa.repository.workspace import WorkspaceProvisioningError
from tests.test_selected_workspace_cleanup import setup, target
from tests.test_repository_workspaces import git


def fixture(tmp_path):
    manager, (selected, other) = setup(tmp_path)
    tree = Path(selected.worktree_path)
    directory = tree / 'audit'
    directory.mkdir()
    (directory / 'report.md').write_bytes(b'valuable audit\n')
    (directory / 'evidence.json').write_bytes(b'{"result":"retained"}\n')
    os.chmod(directory / 'report.md', 0o600)
    files = [{'name': p.name, 'bytes': p.stat().st_size,
              'sha256': hashlib.sha256(p.read_bytes()).hexdigest()} for p in sorted(directory.iterdir())]
    archive = tmp_path / 'durable.zip'
    manifest = {'source_card_id': selected.card_id, 'source_lease_id': selected.id, 'files': files}
    with zipfile.ZipFile(archive, 'w') as z:
        for p in directory.iterdir():
            z.write(p, p.name)
        z.writestr('preservation-manifest.json', json.dumps(manifest))
    request = dict(target=target(selected), relative_path='audit', attachment_path=archive,
                   attachment_sha256=hashlib.sha256(archive.read_bytes()).hexdigest(), principal='user:local')
    return manager, selected, other, directory, request


@pytest.fixture(autouse=True)
def no_external_observer(monkeypatch):
    # OS observer failures are tested separately; all Git operations remain real.
    monkeypatch.setattr('pa.repository.untracked_preservation._no_open_files', lambda _: None)


def test_preserve_then_selected_cleanup_retains_original_files_and_unselected_lease(tmp_path):
    manager, selected, other, directory, request = fixture(tmp_path)
    original = {p.name: (p.read_bytes(), p.stat()) for p in directory.iterdir()}
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
def test_guards_preserve_every_target_byte_and_unselected_state(tmp_path, monkeypatch, fault):
    manager, selected, other, directory, request = fixture(tmp_path)
    tree = Path(selected.worktree_path)
    if fault == 'fence': request['target'].expected_fence += 1
    if fault == 'timestamp': request['target'].expected_updated_at = datetime.now(UTC)
    if fault == 'head': request['target'].expected_head = 'a' * 40
    if fault == 'unfinished': selected.completed = False; manager._save(selected)
    if fault == 'unmerged': selected.merged = False; manager._save(selected)
    if fault == 'unexpired': selected.expires_at = datetime.now(UTC) + timedelta(days=1); manager._save(selected)
    if fault == 'acceptance': manager.store.card_completion_eligible.return_value = False
    if fault == 'restart': manager.store.pending_restart_continuation.return_value = True
    if fault == 'tracked': git(tree, 'add', 'audit')
    if fault == 'index_lock':
        index = Path(git(tree,'rev-parse','--path-format=absolute','--git-path','index').stdout.strip())
        index.with_name('index.lock').write_text('another writer')
    if fault == 'changed_bytes': (directory / 'report.md').write_text('new valuable work')
    if fault == 'extra_file': (directory / 'other').write_text('extra valuable work')
    if fault == 'symlink': (directory / 'report.md').unlink(); (directory / 'report.md').symlink_to('evidence.json')
    if fault == 'hardlink': os.link(directory / 'report.md', tmp_path / 'other-hardlink')
    if fault == 'wrong_attachment': request['attachment_sha256'] = 'a' * 64
    if fault == 'traversal': request['relative_path'] = '../audit'
    if fault == 'open_files':
        def busy(_): raise WorkspaceProvisioningError('open writer')
        monkeypatch.setattr('pa.repository.untracked_preservation._no_open_files', busy)
    before = {p.name: (os.readlink(p) if p.is_symlink() else p.read_bytes()) for p in directory.iterdir()}
    other_before = other.model_dump()
    with pytest.raises(WorkspaceProvisioningError): UntrackedPreservation(manager).operate(**request, action='preserve')
    assert directory.is_dir()
    assert {p.name: (os.readlink(p) if p.is_symlink() else p.read_bytes()) for p in directory.iterdir()} == before
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
def test_open_file_observer_is_fail_closed(monkeypatch, tmp_path, observation):
    def run(*a, **kw):
        if observation == 'missing': raise FileNotFoundError('lsof')
        if observation == 'timeout': raise subprocess.TimeoutExpired('lsof',5)
        return SimpleNamespace(returncode=0 if observation=='busy' else 1,
            stdout=b'p123\nf1\n' if observation=='busy' else b'',
            stderr=b'denied' if observation=='denied' else b'')
    monkeypatch.setattr('pa.repository.untracked_preservation.subprocess.run',run)
    if observation=='empty': real_open_check(tmp_path)
    else:
        with pytest.raises(WorkspaceProvisioningError): real_open_check(tmp_path)


@pytest.mark.asyncio
@pytest.mark.parametrize('fault', [None,'live','queued','inflight','missing_durable','binding','principal'])
async def test_api_owner_admission_and_durable_attachment_guards(tmp_path, monkeypatch, fault):
    from fastapi import HTTPException
    from pa.core.async_runtime import AsyncRuntime
    from pa.modules import instance, items
    manager, selected, _, directory, params = fixture(tmp_path)
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
    attachment=SimpleNamespace(sha256=params['attachment_sha256'])
    monkeypatch.setattr(items,'_card_attachment_or_404',lambda *_:(None,attachment))
    monkeypatch.setattr(items,'_attachment_store',lambda _:SimpleNamespace(blob_path=lambda _:params['attachment_path']))
    body=instance.WorkspaceUntrackedPreservationRequest(target=params['target'],relative_path='audit',
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

import hashlib
import json
import os
import shutil
import zipfile
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from contextlib import contextmanager, ExitStack

import pytest

from pa.repository.tracked_custody import TrackedPrefixCustody, JoinedArchive, metadata, source_file
from pa.repository.workspace import WorkspaceProvisioningError
from tests.test_untracked_preservation import setup
from tests.test_selected_workspace_cleanup import target
from tests.test_repository_workspaces import git


def fixture(tmp_path, monkeypatch, *, large=False):
    manager, (lease, other) = setup(tmp_path)
    tree = Path(lease.worktree_path)
    (tree/'prefix.bin').write_bytes(b'x' * ((48*1024*1024 if large else 1031)+7))
    for i in range(38 if large else 2):
        (tree/f'empty-{i}').write_bytes(b'original bytes')
    git(tree, 'add', '.'); git(tree, 'commit', '-m', 'tracked source')
    head = git(tree, 'rev-parse', 'HEAD').stdout.strip()
    git(tree, 'update-ref', 'refs/remotes/origin/main', head)
    lease.base_sha=head
    manager._save(lease)
    for p in tree.glob('empty-*'):
        p.write_bytes(b'')
    with (tree/'prefix.bin').open('r+b') as f:
        f.truncate(48*1024*1024 if large else 1031)
    git(tree, 'read-tree', '--empty')
    owner=SimpleNamespace(principal_id='user:local', card_id=lease.card_id,
        project_id=lease.project_id, origin_instance_id=manager.settings.instance_id,
        external_session_id=None,
        execution_binding={'lease_ids':[lease.id],'repository_ids':[lease.repository_id],
            'worktree_paths':[lease.worktree_path],'branch':lease.branch,'base_sha':lease.base_sha}, updated_at='owner-v1',
        config_json={'durable_runtime':{'in_flight':None,'queued_prompts':[]}})
    manager.store.get_session.return_value=owner
    monkeypatch.setattr('pa.repository.tracked_custody._no_open_files', lambda _:None)
    request=dict(target=target(lease),principal='user:local',idempotency_key='custody-v1')
    return manager, lease, other, tree, owner, request


def originals(tree):
    return {p.name:metadata(p) for p in tree.iterdir() if p.name!='.git'}


@contextmanager
def archive(prepared):
    with ExitStack() as stack:
        streams=[stack.enter_context(Path(p['source_path']).open('rb')) for p in prepared['attachment_parts']]
        yield stack.enter_context(zipfile.ZipFile(JoinedArchive(streams)))


def durable_parts(prepared,tmp_path):
    paths=[]
    for row in prepared['attachment_parts']:
        destination=tmp_path/f"durable-{row['part']}"
        shutil.copyfile(row['source_path'],destination)
        paths.append(destination)
    return paths


def test_preserves_39_changed_paths_48mib_and_index_without_recreating_head(tmp_path, monkeypatch):
    manager, lease, other, tree, _, request=fixture(tmp_path, monkeypatch,large=True)
    before=originals(tree)
    index=Path(git(tree,'rev-parse','--path-format=absolute','--git-path','index').stdout.strip())
    old_index=metadata(index)
    other_before=other.model_dump()
    service=TrackedPrefixCustody(manager)
    prepared=service.operate(**request,action='prepare')
    assert prepared['changed_files']==39 and prepared['preserved_bytes']==48*1024*1024
    assert len(prepared['attachment_parts'])==3
    assert all(p['bytes']<=24*1024*1024 for p in prepared['attachment_parts'])
    with archive(prepared) as z:
        assert len(z.namelist())==41 and 'files/README.md' not in z.namelist()
        manifest=json.loads(z.read('preservation-manifest.json'))
        assert len(manifest['changed'])==39
        assert manifest['index']==old_index
        assert manifest['changed'][0]['uid']==os.getuid()
    durable=durable_parts(prepared,tmp_path)
    with pytest.raises(WorkspaceProvisioningError,match='parts required'):
        service.operate(**request,action='verify',attachment_paths=durable[:-1],attachment_ids=[],attachment_guard=lambda:None)
    with pytest.raises(WorkspaceProvisioningError):
        service.operate(**request,action='verify',attachment_paths=list(reversed(durable)),attachment_ids=[],attachment_guard=lambda:None)
    result=service.operate(**request,action='verify',attachment_paths=durable,
        attachment_ids=['a'*32,'b'*32,'c'*32],attachment_guard=lambda:None)
    assert result['phase']=='verified' and result['reclaimed_bytes']==0 and not result['worktree_removed']
    assert originals(tree)==before and metadata(index)==old_index
    assert manager.get_by_id(other.id).model_dump()==other_before
    assert (Path(other.worktree_path)/'README.md').read_bytes()==b'base\n'
    # Status and a duplicate prepare observe the receipt, not a fresh archive.
    assert service.operate(**request,action='status')==result
    assert service.operate(**request,action='prepare')==result


@pytest.mark.parametrize('fault',['queue','inflight','permission','elicitation','interaction','binding','native','principal','checkpoint','expiry','acceptance','restart','fence','head','origin','nonprefix','untracked','symlink','hardlink','index','busy'])
def test_fail_closed_preserves_source_and_other_tree(tmp_path,monkeypatch,fault):
    manager, lease, other, tree, owner, request=fixture(tmp_path,monkeypatch)
    if fault in {'queue','inflight','permission','elicitation','interaction'}:
        field={'queue':'queued_prompts','inflight':'in_flight','permission':'pending_permissions','elicitation':'pending_elicitations','interaction':'pending_interaction'}[fault]
        owner.config_json['durable_runtime'][field]=['retained']
    if fault=='binding': owner.execution_binding={}
    if fault=='native':
        owner.external_session_id='native-id'; manager.store.native_history_session_ids.return_value=['other-session']
    if fault=='principal': owner.principal_id='other'
    if fault=='checkpoint': owner.config_json={}
    if fault=='expiry':
        lease.expires_at=datetime.now(UTC)+timedelta(hours=1); manager._save(lease)
    if fault=='acceptance': manager.store.card_completion_eligible.return_value=False
    if fault=='restart': manager.store.pending_restart_continuation.return_value=True
    if fault=='fence': request['target'].expected_fence+=1
    if fault=='head': request['target'].expected_head='a'*40
    if fault=='origin': manager._refresh_origin_for_cleanup=lambda _:'origin unavailable'
    if fault=='nonprefix': (tree/'prefix.bin').write_bytes(b'unique useful bytes')
    if fault=='untracked': (tree/'valuable-note').write_text('retain')
    if fault=='symlink': (tree/'link').symlink_to(tmp_path)
    if fault=='hardlink': os.link(tree/'prefix.bin',tmp_path/'hardlink')
    if fault=='index': git(tree,'read-tree','HEAD')
    if fault=='busy':
        def busy(_): raise WorkspaceProvisioningError('open files')
        monkeypatch.setattr('pa.repository.tracked_custody._no_open_files',busy)
    with pytest.raises((WorkspaceProvisioningError,OSError)):
        TrackedPrefixCustody(manager).operate(**request,action='prepare')
    assert tree.exists() and (tree/'prefix.bin').exists() and Path(other.worktree_path).exists()
    assert not (manager.root/'tracked-prefix-custody').exists()


def test_attachment_corruption_and_source_changes_do_not_settle_custody(tmp_path,monkeypatch):
    manager,lease,_,tree,_,request=fixture(tmp_path,monkeypatch)
    service=TrackedPrefixCustody(manager)
    p=service.operate(**request,action='prepare')
    paths=durable_parts(p,tmp_path)
    blob=paths[0]; blob.write_bytes(b'corrupted')
    kwargs=dict(action='verify',attachment_paths=paths,attachment_ids=['a'*32],attachment_guard=lambda:None)
    with pytest.raises(WorkspaceProvisioningError,match='changed'):service.operate(**request,**kwargs)
    shutil.copyfile(p['attachment_parts'][0]['source_path'],blob)
    (tree/'prefix.bin').write_bytes(b'x'*1000)
    with pytest.raises(WorkspaceProvisioningError,match='changed after'):service.operate(**request,**kwargs)
    assert service.operate(**request,action='status')['phase']=='prepared'
    assert (tree/'prefix.bin').read_bytes()==b'x'*1000


def test_final_guard_and_unknown_prepare_are_not_replayed(tmp_path,monkeypatch):
    manager,_,_,tree,owner,request=fixture(tmp_path,monkeypatch)
    service=TrackedPrefixCustody(manager)
    original=service.snapshot
    calls=0
    def snapshot(*args):
        nonlocal calls
        calls+=1
        if calls==2: owner.config_json['durable_runtime']['queued_prompts']=['new queue']
        return original(*args)
    monkeypatch.setattr(service,'snapshot',snapshot)
    with pytest.raises(WorkspaceProvisioningError):service.operate(**request,action='prepare')
    status=service.operate(**request,action='status')
    assert status['phase']=='writing' and status['effect']=='unknown'
    assert service.operate(**request,action='prepare')==status
    assert tree.exists()
    with pytest.raises(WorkspaceProvisioningError,match='identity'):
        service.operate(**{**request,'target':request['target'].model_copy(update={'expected_head':'a'*40})},action='status')


def test_component_symlinks_never_open_outside_tree(tmp_path):
    (tmp_path/'secret').write_text('private')
    root=tmp_path/'tree'; root.mkdir(); (root/'parent').symlink_to(tmp_path)
    with pytest.raises(OSError): source_file(root,'parent/secret')


@pytest.mark.parametrize('limit',['files','bytes'])
def test_prefix_capacity_is_checked_before_archive_creation(tmp_path,monkeypatch,limit):
    manager,_,_,tree,_,request=fixture(tmp_path,monkeypatch)
    monkeypatch.setattr('pa.repository.tracked_custody.MAX_FILES' if limit=='files' else 'pa.repository.tracked_custody.MAX_BYTES',1)
    with pytest.raises(WorkspaceProvisioningError,match='capacity'):
        TrackedPrefixCustody(manager).operate(**request,action='prepare')
    assert tree.exists() and not (manager.root/'tracked-prefix-custody').exists()


@pytest.mark.asyncio
@pytest.mark.parametrize('fault',['live','admitting','missing_blob','wrong_hash','attachment_race',None])
async def test_api_fences_owner_and_requires_durable_attachment(tmp_path,monkeypatch,fault):
    from fastapi import HTTPException
    from pa.core.async_runtime import AsyncRuntime
    from pa.modules import instance,items
    manager,lease,_,tree,_,params=fixture(tmp_path,monkeypatch)
    agent=SimpleNamespace(workspace_manager=manager,store=manager.store,
        _admitting_sessions={lease.session_id} if fault=='admitting' else set(),
        get=lambda _:SimpleNamespace(_closed=False) if fault=='live' else None)
    runtime=AsyncRuntime(max_workers=2,max_queue=2); await runtime.start()
    services={'instance_agent':agent,'async_runtime':runtime}
    req=SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(ctx=SimpleNamespace(require_service=services.__getitem__))))
    monkeypatch.setattr(instance,'require_user',lambda _:SimpleNamespace(role='user'))
    monkeypatch.setattr(instance,'get_principal_id',lambda _:'user:local')
    body=instance.WorkspaceTrackedCustodyRequest(target=params['target'],idempotency_key='api-custody',action='prepare')
    before=originals(tree)
    try:
        if fault in {'live','admitting'}:
            with pytest.raises(HTTPException): await instance.tracked_prefix_custody(req,body)
            return
        prepared=await instance.tracked_prefix_custody(req,body)
        blob=durable_parts(prepared,tmp_path)[0]
        attachment=SimpleNamespace(sha256='b'*64 if fault=='wrong_hash' else prepared['attachment_parts'][0]['sha256'])
        reads=0
        def retrieve(*_):
            nonlocal reads
            reads+=1
            if fault=='attachment_race' and reads==2:
                raise HTTPException(404,'attachment was removed')
            return None,attachment
        monkeypatch.setattr(items,'_card_attachment_or_404',retrieve)
        monkeypatch.setattr(items,'_attachment_store',lambda _:SimpleNamespace(blob_path=lambda _:tmp_path/'missing.zip' if fault=='missing_blob' else blob))
        body.action='verify'
        body.attachments=[instance.WorkspaceCustodyAttachment(part=0,card_id='cleanup-card',attachment_id='a'*32,sha256=prepared['attachment_parts'][0]['sha256'])]
        if fault:
            with pytest.raises(HTTPException): await instance.tracked_prefix_custody(req,body)
        else:
            result=await instance.tracked_prefix_custody(req,body)
            assert result['phase']=='verified' and reads==2
        assert originals(tree)==before
        assert not agent._admitting_sessions
    finally:
        await runtime.close()

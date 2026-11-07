"""Provenance inspection never turns a mismatched file into apply authority."""
from pathlib import Path
import pytest

from pa.repository import materialization
from pa.repository.materialization import MaterializationRepair
from pa.repository.workspace import WorkspaceProvisioningError
from tests.test_workspace_materialization import empty_checkout
from tests.test_repository_workspaces import git


def test_diagnose_identifies_changed_tracked_bytes_without_preservation_or_mutation(tmp_path,monkeypatch):
    manager,lease,tree,index,args=empty_checkout(tmp_path)
    precious=b'actual user edits\x00retain exactly'
    (tree/'README.md').write_bytes(precious)
    before=index.read_bytes();inode=(tree/'README.md').stat().st_ino
    def no_hydration(*a):raise AssertionError('diagnosis must not inventory untracked/ignored data')
    monkeypatch.setattr(materialization,'inventory',no_hydration)
    service=MaterializationRepair(manager)
    result=service.operate(**args,action='diagnose')
    d=result['diagnostic'];assert d['differing_tracked_paths']==1 and not d['apply_eligible']
    assert d['differences'][0]['path']=='README.md'
    assert d['differences'][0]['actual_oid']!=d['differences'][0]['expected_oid']
    assert d['tracked_bytes_read']==len(precious) and precious.decode(errors='replace') not in str(result)
    assert service.operate(**args,action='status')['phase']=='not_started'
    assert not (manager.root/'materialization-repairs'/result['operation_id']).exists()
    assert index.read_bytes()==before and (tree/'README.md').read_bytes()==precious
    assert (tree/'README.md').stat().st_ino==inode
    assert (tree/'.private'/'evidence').read_bytes()==b'untracked\x00retained'
    with pytest.raises(WorkspaceProvisioningError,match='bytes differ'):
        service.operate(**args,action='prepare')
    with pytest.raises(WorkspaceProvisioningError,match='No prepared'):
        service.operate(**args,action='apply',expected_revision='a'*64)


def test_diagnostic_bound_refuses_large_file_without_changing_it(tmp_path,monkeypatch):
    manager,_,tree,index,args=empty_checkout(tmp_path)
    (tree/'README.md').write_bytes(b'unchanged precious file')
    before=index.read_bytes();monkeypatch.setattr(materialization,'MAX_DIAGNOSTIC_BYTES',1)
    with pytest.raises(WorkspaceProvisioningError,match='bounded diagnostic'):
        MaterializationRepair(manager).operate(**args,action='diagnose')
    assert index.read_bytes()==before and (tree/'README.md').read_bytes()==b'unchanged precious file'


def test_diagnostic_keeps_existing_receipt_fingerprint_fence(tmp_path):
    manager,lease,tree,_,args=empty_checkout(tmp_path)
    service=MaterializationRepair(manager);service.operate(**args,action='prepare')
    with pytest.raises(WorkspaceProvisioningError,match='Idempotency'):
        service.operate(**{**args,'quarantine_untracked':True},action='diagnose')
    lease.fencing_token+=1;manager._save(lease)
    with pytest.raises(WorkspaceProvisioningError,match='fencing'):
        service.operate(**args,action='diagnose')


def test_diagnostic_reports_total_but_bounds_exported_path_metadata(tmp_path):
    manager,_,tree,index,args=empty_checkout(tmp_path)
    git(tree,'read-tree','HEAD');git(tree,'checkout-index','--all')
    for i in range(70):(tree/f'file-{i:02d}').write_text('original')
    git(tree,'add','--',*[f'file-{i:02d}' for i in range(70)])
    git(tree,'commit','-m','tracked provenance fixture')
    args['expected_head']=git(tree,'rev-parse','HEAD').stdout.strip()
    git(tree,'read-tree','--empty')
    for i in range(70):(tree/f'file-{i:02d}').write_text('retained edit')
    before=index.read_bytes()
    result=MaterializationRepair(manager).operate(**args,action='diagnose')['diagnostic']
    assert result['differing_tracked_paths']==70 and result['differences_truncated']
    assert len(result['differences'])==64 and index.read_bytes()==before
    assert all((tree/f'file-{i:02d}').read_text()=='retained edit' for i in range(70))

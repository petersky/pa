"""Read-only streaming evidence never relaxes preservation/apply guards."""
import hashlib
from pathlib import Path
import tracemalloc

import pytest

from pa.repository import materialization, provenance_pages as pages
from pa.repository.materialization import MaterializationRepair
from pa.repository.workspace import WorkspaceProvisioningError
from tests.test_workspace_materialization import empty_checkout
from tests.test_repository_workspaces import git


def fixture_tree(tmp_path, *, big=False, count=0):
    manager, lease, tree, index, args = empty_checkout(tmp_path)
    git(tree, 'read-tree', 'HEAD')
    git(tree, 'checkout-index', '--all')
    if big:
        with (tree / 'large').open('wb') as stream:
            for _ in range(40):
                stream.write(b'x' * (1024 * 1024))
    for i in range(count):
        (tree / f'file-{i:03d}').write_text(str(i))
    git(tree, 'add', '--', 'README.md', *(['large'] if big else []),
        *[f'file-{i:03d}' for i in range(count)])
    git(tree, 'commit', '--allow-empty', '-m', 'paged provenance fixture')
    args['expected_head'] = git(tree, 'rev-parse', 'HEAD').stdout.strip()
    git(tree, 'read-tree', '--empty')
    return manager, lease, tree, index, args


def test_large_file_streams_across_pages_without_copies_or_mutation(tmp_path, monkeypatch):
    manager, lease, tree, index, args = fixture_tree(tmp_path, big=True, count=130)
    with (tree / 'large').open('r+b') as stream:
        stream.seek(20 * 1024 * 1024)
        stream.write(b'precious edit')
    expected = git(tree, 'hash-object', str(tree / 'large')).stdout.strip()
    before = index.read_bytes()
    inodes = {p.name:p.lstat().st_ino for p in tree.iterdir()}
    def no_inventory(*a):
        raise AssertionError('no untracked/ignored hydration')
    monkeypatch.setattr(materialization, 'inventory', no_inventory)
    service = MaterializationRepair(manager)
    with pytest.raises(WorkspaceProvisioningError, match='bounded diagnostic'):
        service.operate(**args, action='diagnose')
    cursor = None
    rows = []
    identities = set()
    calls = 0
    maximum = 0
    tracemalloc.start()
    try:
        while True:
            result = service.operate(**args, action='diagnose_page', diagnostic_cursor=cursor)
            assert result['effect'] == 'none'
            d = result['diagnostic']
            maximum = max(maximum, tracemalloc.get_traced_memory()[1])
            calls += 1
            assert d['tracked_bytes_read'] <= materialization.MAX_DIAGNOSTIC_BYTES
            assert len(d['entries']) <= pages.MAX_ROWS
            assert not d['apply_eligible'] and not d['entries_truncated']
            identities.add(d['input_identity'])
            rows.extend(d['entries'])
            replay = service.operate(**args, action='diagnose_page', diagnostic_cursor=cursor)
            assert replay == result
            cursor = d['next_cursor']
            if d['complete']:
                assert cursor is None
                break
    finally:
        tracemalloc.stop()
    assert calls >= 5 and maximum < 8 * 1024 * 1024
    assert len(identities) == 1
    assert len(rows) == d['tracked_paths'] == d['completed_tracked_paths']
    assert [r['path'] for r in rows] == sorted(r['path'] for r in rows)
    assert next(r for r in rows if r['path'] == 'large')['actual_oid'] == expected
    assert d['differing_tracked_paths'] == 1
    assert index.read_bytes() == before
    assert inodes == {p.name:p.lstat().st_ino for p in tree.iterdir()}
    assert git(tree, 'hash-object', str(tree / 'large')).stdout.strip() == expected
    assert service.operate(**args, action='status')['phase'] == 'not_started'
    assert not (manager.root / 'materialization-repairs' / result['operation_id']).exists()
    with pytest.raises(WorkspaceProvisioningError, match='bytes differ'):
        service.operate(**args, action='prepare')


@pytest.mark.parametrize('change', ['file', 'index', 'head', 'fence', 'parent'])
def test_changed_inputs_reject_continuation(tmp_path, monkeypatch, change):
    manager, lease, tree, index, args = fixture_tree(tmp_path, count=3)
    monkeypatch.setattr(materialization, 'MAX_DIAGNOSTIC_BYTES', 1)
    service = MaterializationRepair(manager)
    first = service.operate(**args, action='diagnose_page')
    cursor = first['diagnostic']['next_cursor']
    assert cursor
    if change == 'file':
        (tree / 'README.md').write_bytes(b'new retained data')
    elif change == 'index':
        git(tree, 'read-tree', 'HEAD')
    elif change == 'head':
        git(tree, 'read-tree', 'HEAD')
        git(tree, 'commit', '--allow-empty', '-m', 'new head')
        git(tree, 'read-tree', '--empty')
    elif change == 'fence':
        lease.fencing_token += 1
        manager._save(lease)
    else:
        (tree / 'README.md').unlink()
        (tree / 'README.md').symlink_to('/dev/null')
    with pytest.raises(WorkspaceProvisioningError):
        service.operate(**args, action='diagnose_page', diagnostic_cursor=cursor)
    assert service.operate(**args, action='status')['phase'] == 'not_started'


def test_cursor_scope_expiry_and_stale_position(tmp_path, monkeypatch):
    manager, lease, tree, index, args = fixture_tree(tmp_path, count=3)
    monkeypatch.setattr(materialization, 'MAX_DIAGNOSTIC_BYTES', 1)
    service = MaterializationRepair(manager)
    first = service.operate(**args, action='diagnose_page')['diagnostic']['next_cursor']
    with pytest.raises(WorkspaceProvisioningError, match='different inputs'):
        service.operate(**{**args, 'principal':'other'}, action='diagnose_page', diagnostic_cursor=first)
    second = service.operate(**args, action='diagnose_page', diagnostic_cursor=first)['diagnostic']['next_cursor']
    service.operate(**args, action='diagnose_page', diagnostic_cursor=second)
    with pytest.raises(WorkspaceProvisioningError, match='stale'):
        service.operate(**args, action='diagnose_page', diagnostic_cursor=first)
    monkeypatch.setattr(pages, 'TTL_SECONDS', -1)
    with pytest.raises(WorkspaceProvisioningError, match='expired'):
        service.operate(**args, action='diagnose_page', diagnostic_cursor=second)


def test_read_race_invalidates_without_receipt_or_target_writes(tmp_path, monkeypatch):
    manager, lease, tree, index, args = fixture_tree(tmp_path)
    before = index.read_bytes()
    original = pages.snapshot
    calls = 0
    def racing(*a):
        nonlocal calls
        calls += 1
        if calls == 2:
            (tree / 'README.md').write_bytes(b'concurrent user edit')
        return original(*a)
    monkeypatch.setattr(pages, 'snapshot', racing)
    with pytest.raises(WorkspaceProvisioningError, match='changed during page'):
        MaterializationRepair(manager).operate(**args, action='diagnose_page')
    assert index.read_bytes() == before
    assert (tree / 'README.md').read_bytes() == b'concurrent user edit'
    assert not (manager.root / 'materialization-repairs').exists()


def test_metadata_and_index_bounds_are_not_raised(tmp_path, monkeypatch):
    manager, lease, tree, index, args = fixture_tree(tmp_path, count=3)
    monkeypatch.setattr(materialization, 'MAX_DIAGNOSTIC_PATHS', 1)
    with pytest.raises(WorkspaceProvisioningError, match='path capacity'):
        MaterializationRepair(manager).operate(**args, action='diagnose_page')
    monkeypatch.setattr(materialization, 'MAX_DIAGNOSTIC_PATHS', 10000)
    monkeypatch.setattr(pages, 'MAX_METADATA_BYTES', 1)
    with pytest.raises(WorkspaceProvisioningError, match='metadata.*capacity'):
        MaterializationRepair(manager).operate(**args, action='diagnose_page')


def test_fence_change_during_page_is_not_published(tmp_path, monkeypatch):
    manager, lease, tree, index, args = fixture_tree(tmp_path)
    original = pages.observe
    def racing(*a, **k):
        result = original(*a, **k)
        lease.fencing_token += 1
        manager._save(lease)
        return result
    monkeypatch.setattr(pages, 'observe', racing)
    with pytest.raises(WorkspaceProvisioningError, match='fencing token'):
        MaterializationRepair(manager).operate(**args, action='diagnose_page')


def test_small_tree_paged_hash_matches_legacy(tmp_path):
    manager, lease, tree, index, args = fixture_tree(tmp_path)
    (tree / 'README.md').write_bytes(b'preserved edit')
    service = MaterializationRepair(manager)
    old = service.operate(**args, action='diagnose')['diagnostic']
    new = service.operate(**args, action='diagnose_page')['diagnostic']
    assert new['complete'] and new['differing_tracked_paths'] == old['differing_tracked_paths']
    assert new['entries'][0]['actual_oid'] == old['differences'][0]['actual_oid']
    with pytest.raises(WorkspaceProvisioningError, match='only valid'):
        service.operate(**args, action='apply', diagnostic_cursor='a'*32)


def test_index_and_cache_capacity_fail_closed(tmp_path, monkeypatch):
    manager, lease, tree, index, args = fixture_tree(tmp_path)
    service = MaterializationRepair(manager)
    monkeypatch.setattr(pages, 'MAX_INDEX_BYTES', 1)
    with pytest.raises(WorkspaceProvisioningError, match='index exceeds'):
        service.operate(**args, action='diagnose_page')
    monkeypatch.setattr(pages, 'MAX_INDEX_BYTES', 1024 * 1024)
    monkeypatch.setattr(pages, 'MAX_SCANS', 1)
    service.operate(**args, action='diagnose_page')
    with pytest.raises(WorkspaceProvisioningError, match='cache capacity'):
        service.operate(**{**args, 'idempotency_key':'different-observation'}, action='diagnose_page')
    assert service.operate(**args, action='status')['phase'] == 'not_started'


@pytest.mark.asyncio
async def test_http_and_mcp_forward_cursor_under_exact_owner_guard(tmp_path, monkeypatch):
    import asyncio
    from types import SimpleNamespace
    from pa.core.async_runtime import AsyncRuntime
    from pa.modules.instance import repair_workspace_materialization, WorkspaceMaterializationRequest
    from pa.mcp.tools.instance import register_mcp
    from tests.test_instance_mcp import FakeMcp
    manager, lease, tree, index, args = fixture_tree(tmp_path, count=3)
    monkeypatch.setattr(materialization, 'MAX_DIAGNOSTIC_BYTES', 1)
    runtime = SimpleNamespace(session=SimpleNamespace(card_id='card', principal_id='user:local',
        execution_binding={'lease_ids':[lease.id]}), prompting=False, _queue=[],
        _prompt_lock=asyncio.Lock(), _prompt_admission_lock=asyncio.Lock())
    agent = SimpleNamespace(get=lambda _:runtime, _admitting_sessions=set(), workspace_manager=manager)
    executor = AsyncRuntime(max_workers=1)
    services = {'instance_agent':agent, 'async_runtime':executor}
    request = SimpleNamespace(state=SimpleNamespace(user=object(),principal_id='user:local'),
        app=SimpleNamespace(state=SimpleNamespace(ctx=SimpleNamespace(require_service=services.__getitem__))))
    body_args = {k:v for k,v in args.items() if k not in {'lease_id','principal'}}
    async def forward(*a, **kwargs):
        body = WorkspaceMaterializationRequest(**kwargs['json'])
        return await repair_workspace_materialization(request, lease.id, body)
    mcp = FakeMcp()
    register_mcp(mcp, SimpleNamespace(settings=SimpleNamespace(),
        require_service=lambda _:SimpleNamespace(run_blocking=forward)))
    try:
        fn = mcp.functions['workspace_materialization_repair']
        first = await fn(lease_id=lease.id, **body_args, action='diagnose_page')
        cursor = first['diagnostic']['next_cursor']
        assert cursor
        second = await fn(lease_id=lease.id, **body_args, action='diagnose_page', diagnostic_cursor=cursor)
        assert second['diagnostic']['continued_file_bytes'] > first['diagnostic']['continued_file_bytes']
        assert second['operation_id'] == first['operation_id']
        runtime._queue.append(object())
        from fastapi import HTTPException
        with pytest.raises(HTTPException) as denied:
            await fn(lease_id=lease.id, **body_args, action='diagnose_page',
                     diagnostic_cursor=second['diagnostic']['next_cursor'])
        assert denied.value.detail['code'] == 'workspace_materialization_owner_busy'
    finally:
        await executor.close()

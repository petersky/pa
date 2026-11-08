"""Real disposable Git cleanup with explicit scope and inherited safety gates."""
import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from pa.pr_supervisor.github import GitHubClient, GitHubCredentials
from pa.repository.workspace import WorkspaceCleanupTarget, WorkspaceProvisioningError
from tests.test_repository_workspaces import manager_for, git


def setup(tmp_path):
    manager, _, linked = manager_for(tmp_path)
    leases = [manager.provision_repository(linked, project_id='project-1',
        session_id=f'session-{i}', card_id=f'card-{i}') for i in range(2)]
    for lease in leases:
        manager.mark_card_completed(lease.card_id, merged=True)
        manager.expire_session(lease.session_id)
    return manager, [manager.get_by_id(lease.id) for lease in leases]


def target(lease, **updates):
    return WorkspaceCleanupTarget(lease_id=lease.id, expected_fence=lease.fencing_token,
        expected_head=lease.base_sha, expected_updated_at=lease.updated_at, **updates)


def test_selected_cleanup_removes_only_selected_and_reports_measured_bytes(tmp_path):
    manager, (selected, other) = setup(tmp_path)
    before = other.model_dump()
    result = manager.collect_garbage(targets=[target(selected)])
    assert result['cleaned'] == 1 and len(result['targets']) == 1
    outcome = result['targets'][0]
    assert outcome['lease_id'] == selected.id and outcome['state'] == 'cleaned'
    assert outcome['reclaimed_bytes'] > 0
    assert isinstance(outcome['filesystem_free_bytes_delta'], int)
    assert not Path(selected.worktree_path).exists()
    assert manager.get_by_id(other.id).model_dump() == before
    assert Path(other.worktree_path, 'README.md').read_text() == 'base\n'
    assert git(Path(other.cache_path), 'rev-parse', '--verify', other.branch).returncode == 0


@pytest.mark.parametrize('fault', ['unknown', 'fence', 'timestamp', 'duplicate', 'empty'])
def test_bad_selection_rejected_before_any_selected_or_unselected_mutation(tmp_path, fault):
    manager, leases = setup(tmp_path)
    inputs = [target(leases[0])]
    if fault == 'unknown': inputs.append(inputs[0].model_copy(update={'lease_id':'f'*64}))
    if fault == 'fence': inputs[0] = inputs[0].model_copy(update={'expected_fence':999})
    if fault == 'timestamp': inputs[0] = inputs[0].model_copy(update={'expected_updated_at':datetime.now(UTC)})
    if fault == 'duplicate': inputs *= 2
    if fault == 'empty': inputs = []
    before = [lease.model_dump() for lease in leases]
    with pytest.raises(WorkspaceProvisioningError): manager.collect_garbage(targets=inputs)
    assert [manager.get_by_id(lease.id).model_dump() for lease in leases] == before
    assert all(Path(lease.worktree_path).exists() for lease in leases)


def test_owner_change_during_fetch_is_rejected_under_lock_without_cleanup(tmp_path, monkeypatch):
    manager, (selected, other) = setup(tmp_path)
    original = manager._refresh_origin_for_cleanup
    def change(cache):
        result = original(cache)
        changed = manager.get_by_id(selected.id)
        changed.fencing_token += 1
        manager._save(changed)
        return result
    monkeypatch.setattr(manager, '_refresh_origin_for_cleanup', change)
    before = other.model_dump()
    result = manager.collect_garbage(targets=[target(selected)])
    assert result['targets'][0]['state'] == 'rejected'
    assert 'stale' in result['targets'][0]['reason']
    assert Path(selected.worktree_path).exists()
    assert manager.get_by_id(other.id).model_dump() == before


def test_last_boundary_head_change_preserves_unique_commit_and_tree(tmp_path):
    manager,(selected,other)=setup(tmp_path)
    tree=Path(selected.worktree_path)
    git(tree,'config','user.name','Fixture');git(tree,'config','user.email','fixture@pa.invalid')
    calls=0
    def accepted(_):
        nonlocal calls
        calls+=1
        if calls==3:git(tree,'commit','--allow-empty','-m','retain intervening history')
        return True
    manager.store.card_completion_eligible.side_effect=accepted
    result=manager.collect_garbage(targets=[target(selected)])
    assert result['cleaned']==0 and tree.exists()
    assert 'selected HEAD changed' in result['targets'][0]['reason']
    assert git(tree,'log','-1','--format=%s').stdout.strip()=='retain intervening history'
    assert Path(other.worktree_path).exists()


@pytest.mark.parametrize('guard', ['active','incomplete','unmerged','unexpired','acceptance','restart','dirty','untracked','unique','head'])
def test_selected_path_preserves_existing_guards_and_all_retained_bytes(tmp_path, guard):
    manager, (selected, other) = setup(tmp_path)
    tree = Path(selected.worktree_path)
    active = set()
    if guard == 'active': active.add(selected.session_id)
    if guard == 'incomplete': selected.completed = False
    if guard == 'unmerged': selected.merged = False
    if guard == 'unexpired': selected.expires_at = datetime.now(UTC)+timedelta(hours=1)
    if guard == 'acceptance': manager.store.card_completion_eligible.return_value = False
    if guard == 'restart': manager.store.pending_restart_continuation.return_value = True
    if guard == 'dirty': (tree/'README.md').write_bytes(b'valuable changed bytes\x00')
    if guard == 'untracked': (tree/'retained').write_bytes(b'valuable untracked bytes\x00')
    if guard in {'unique','head'}:
        git(tree,'config','user.name','Fixture');git(tree,'config','user.email','fixture@pa.invalid')
        (tree/'README.md').write_text('unique work\n')
        git(tree,'add','README.md');git(tree,'commit','-m','unique')
    manager._save(selected)
    request = target(manager.get_by_id(selected.id))
    if guard == 'unique': request.expected_head = git(tree,'rev-parse','HEAD').stdout.strip()
    before_other = other.model_dump()
    before = {p.name:p.read_bytes() for p in tree.iterdir() if p.is_file()}
    result = manager.collect_garbage(targets=[request], active_session_ids=active)
    assert result['cleaned'] == 0 and result['targets'][0]['state'] in {'retained','blocked','rejected'}
    assert result['targets'][0]['reclaimed_bytes'] == 0
    assert {p.name:p.read_bytes() for p in tree.iterdir() if p.is_file()} == before
    assert manager.get_by_id(other.id).model_dump() == before_other


@pytest.mark.asyncio
@pytest.mark.parametrize('fault', [None,'unmerged','foreign_repo','head','confirmed_head','merge_sha'])
async def test_primary_legacy_proof_is_exact_and_never_infers_historical_green(fault):
    client = GitHubClient(GitHubCredentials(token='fixture-only'))
    head, merge = 'a'*40, 'b'*40
    pr = {'number':359,'merged':True,'merged_at':'2026-08-22T23:20:46Z','state':'closed',
        'head':{'sha':head},'base':{'repo':{'full_name':'pa-test/repo'}},'merge_commit_sha':merge}
    if fault == 'unmerged': pr['merged'] = False
    if fault == 'foreign_repo': pr['base']['repo']['full_name'] = 'other/repo'
    if fault == 'head': pr['head']['sha'] = 'c'*40
    if fault == 'merge_sha': pr['merge_commit_sha'] = ''
    client._request = AsyncMock(return_value=(200,pr))
    client.get_pull_head = AsyncMock(return_value='c'*40 if fault == 'confirmed_head' else head)
    if fault:
        with pytest.raises(ValueError): await client.merged_cleanup_proof('pa-test/repo',359,head)
    else:
        proof = await client.merged_cleanup_proof('pa-test/repo',359,head)
        assert proof['watched_head_sha'] == proof['confirmed_head_sha'] == head
        assert proof['merge_commit_sha'] == merge
        assert 'stable_green_evidence' not in proof and 'green' not in proof


@pytest.mark.asyncio
@pytest.mark.parametrize('credentials', [GitHubCredentials(),GitHubCredentials(token='fixture-only',configuration_status='missing'),GitHubCredentials(token='fixture-only',allowed_repositories=['other/repo'])])
async def test_legacy_cleanup_revalidation_never_expands_credentials_or_repository_scope(credentials):
    client=GitHubClient(credentials);client._request=AsyncMock()
    with pytest.raises(ValueError):await client.merged_cleanup_proof('pa-test/repo',359,'a'*40)
    client._request.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize('fault', [None, 'http', 'errors', 'foreign_repo', 'number',
    'unmerged', 'head', 'merge', 'confirmed_head', 'null_commit', 'non_object'])
async def test_primary_cleanup_missing_rest_merge_requires_exact_graphql_identity(fault):
    import httpx
    from pa.pr_supervisor.github import GITHUB_API_VERSION
    head, merge = 'a' * 40, 'b' * 40
    rest = {'number':359, 'merged':True, 'merged_at':'2026-08-22T23:20:46Z',
        'state':'closed', 'head':{'sha':head}, 'base':{'repo':{'full_name':'pa-test/repo'}}}
    pr = {'number':359, 'merged':True, 'headRefOid':head, 'mergeCommit':{'oid':merge}}
    repo = {'nameWithOwner':'pa-test/repo', 'pullRequest':pr}
    payload = {'data':{'repository':repo}}
    if fault == 'errors': payload['errors'] = [{'message':'fixture unavailable'}]
    if fault == 'foreign_repo': repo['nameWithOwner'] = 'other/repo'
    if fault == 'number': pr['number'] = 388
    if fault == 'unmerged': pr['merged'] = False
    if fault == 'head': pr['headRefOid'] = 'c' * 40
    if fault == 'merge': pr['mergeCommit']['oid'] = 'invalid'
    if fault == 'null_commit': pr['mergeCommit'] = None
    if fault == 'non_object': payload = []
    requests = []
    def handler(request):
        requests.append(request)
        assert request.headers['X-GitHub-Api-Version'] == GITHUB_API_VERSION
        assert request.headers['Authorization'] == 'Bearer fixture-only'
        if request.method == 'POST':
            body = __import__('json').loads(request.content)
            assert body['variables'] == {'owner':'pa-test','name':'repo','number':359}
            assert 'reviewThreads' not in body['query']
            return httpx.Response(503 if fault == 'http' else 200, json=payload)
        response = {**rest, 'head':{'sha':'c' * 40}} if fault == 'confirmed_head' and len(requests) > 1 else rest
        return httpx.Response(200, json=response)
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as transport:
        client = GitHubClient(GitHubCredentials(token='fixture-only'), client=transport)
        if fault:
            with pytest.raises(ValueError): await client.merged_cleanup_proof('pa-test/repo',359,head)
        else:
            proof = await client.merged_cleanup_proof('pa-test/repo',359,head)
            assert proof['merge_commit_sha'] == merge
            assert proof['watched_head_sha'] == proof['confirmed_head_sha'] == head
            assert [r.method for r in requests] == ['GET','POST','GET']
            assert 'stable_green_evidence' not in proof and 'green' not in proof


@pytest.mark.asyncio
async def test_cleanup_does_not_replace_explicit_invalid_rest_merge_or_wrong_head():
    client = GitHubClient(GitHubCredentials(token='fixture-only'))
    client._cleanup_graphql_merge = AsyncMock(return_value='b' * 40)
    valid = {'number':359,'merged':True,'merged_at':'2026-08-22T23:20:46Z','state':'closed',
        'head':{'sha':'a' * 40},'base':{'repo':{'full_name':'pa-test/repo'}}}
    for invalid in [{**valid,'merge_commit_sha':''},{**valid,'head':{'sha':'c' * 40}}]:
        client._request = AsyncMock(return_value=(200,invalid))
        with pytest.raises(ValueError): await client.merged_cleanup_proof('pa-test/repo',359,'a' * 40)
    client._cleanup_graphql_merge.assert_not_called()


@pytest.mark.parametrize('fault', [None,'foreign_scope','merge_not_remote','different_content'])
def test_legacy_cleanup_still_requires_real_git_integration_and_preserves_unselected(tmp_path, fault):
    import subprocess
    manager, (selected, other) = setup(tmp_path)
    tree=Path(selected.worktree_path)
    git(tree,'config','user.name','Fixture');git(tree,'config','user.email','fixture@pa.invalid')
    (tree/'README.md').write_text('integrated feature\n')
    git(tree,'add','README.md');git(tree,'commit','-m','feature')
    head=git(tree,'rev-parse','HEAD').stdout.strip()
    git(tree,'push','origin',f'HEAD:refs/heads/{selected.branch}')
    integrator=tmp_path/'integrator'
    subprocess.run(['git','clone',selected.repository_url,str(integrator)],check=True,capture_output=True)
    git(integrator,'config','user.name','Fixture');git(integrator,'config','user.email','fixture@pa.invalid')
    git(integrator,'merge','--squash',f'origin/{selected.branch}')
    if fault=='different_content': (integrator/'README.md').write_text('different feature\n');git(integrator,'add','README.md')
    git(integrator,'commit','-m','squash feature')
    merge=git(integrator,'rev-parse','HEAD').stdout.strip()
    if fault!='merge_not_remote': git(integrator,'push','origin','main')
    git(integrator,'push','origin','--delete',selected.branch)
    # Fixture identity only: origin remains the disposable local bare repository.
    selected.repository_url='https://github.com/pa-test/repo'
    manager._save(selected)
    selected=manager.get_by_id(selected.id)
    request=target(selected);request.expected_head=head
    proof={'source':'github_merged_cleanup_revalidation','repository':'pa-test/repo',
        'pr_number':359,'watched_head_sha':head,'confirmed_head_sha':head,'merge_commit_sha':merge,
        'lease_id':selected.id,'session_id':selected.session_id,'fencing_token':selected.fencing_token}
    if fault=='foreign_scope': proof['lease_id']=other.id
    before=other.model_dump()
    result=manager.collect_garbage(targets=[request],merged_proofs={selected.id:proof})
    assert manager.get_by_id(other.id).model_dump()==before
    assert Path(other.worktree_path).exists()
    if fault:
        assert result['cleaned']==0 and tree.exists()
        assert (tree/'README.md').read_text()=='integrated feature\n'
    else:
        assert result['cleaned']==1 and not tree.exists()
        saved=manager.get_by_id(selected.id)
        assert saved.cleanup_evidence['source']=='github_merged_cleanup_revalidation'
        assert saved.cleanup_evidence['merge_method']=='squash'
        assert 'stable_green_evidence' not in saved.cleanup_evidence


@pytest.mark.asyncio
async def test_selected_api_does_not_reconcile_unselected_and_holds_admission(tmp_path, monkeypatch):
    from pa.core.async_runtime import AsyncRuntime
    from pa.modules import instance
    manager, leases = setup(tmp_path)
    manager.store.get_session.return_value = SimpleNamespace(principal_id='user:local')
    agent = SimpleNamespace(workspace_manager=manager, store=manager.store,
        _admitting_sessions=set(), list_runtimes=lambda:[])
    runtime = AsyncRuntime(max_workers=2,max_queue=2)
    await runtime.start()
    original = manager.collect_garbage
    def collect(**kwargs):
        assert agent._admitting_sessions == {leases[0].session_id}
        return original(**kwargs)
    monkeypatch.setattr(manager,'collect_garbage',collect)
    monkeypatch.setattr(manager,'reconcile_terminal_state',lambda:pytest.fail('global reconciliation'))
    monkeypatch.setattr(instance,'require_user',lambda _:SimpleNamespace(role='admin'))
    monkeypatch.setattr(instance,'get_principal_id',lambda _:'user:local')
    services={'instance_agent':agent,'async_runtime':runtime}
    request=SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(ctx=SimpleNamespace(require_service=services.__getitem__))))
    before=leases[1].model_dump()
    try:
        result=await instance.collect_selected_workspaces(request,instance.WorkspaceSelectedCleanupRequest(targets=[target(leases[0])]))
        assert result['cleaned']==1
        assert agent._admitting_sessions==set()
        assert manager.get_by_id(leases[1].id).model_dump()==before
    finally:
        await runtime.close()


@pytest.mark.asyncio
async def test_caller_cancellation_cannot_release_selected_owner_before_effect_finishes(tmp_path, monkeypatch):
    import threading
    from pa.core.async_runtime import AsyncRuntime
    from pa.modules import instance
    manager, leases=setup(tmp_path)
    manager.store.get_session.return_value=SimpleNamespace(principal_id='user:local')
    agent=SimpleNamespace(workspace_manager=manager,store=manager.store,_admitting_sessions=set(),list_runtimes=lambda:[])
    runtime=AsyncRuntime(max_workers=2,max_queue=2);await runtime.start()
    started,release,finished=threading.Event(),threading.Event(),threading.Event()
    original=manager.collect_garbage
    def held(**kwargs):
        started.set()
        assert release.wait(10)
        try:return original(**kwargs)
        finally:finished.set()
    monkeypatch.setattr(manager,'collect_garbage',held)
    monkeypatch.setattr(instance,'require_user',lambda _:SimpleNamespace(role='admin'))
    monkeypatch.setattr(instance,'get_principal_id',lambda _:'user:local')
    services={'instance_agent':agent,'async_runtime':runtime}
    request=SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(ctx=SimpleNamespace(require_service=services.__getitem__))))
    task=asyncio.create_task(instance.collect_selected_workspaces(request,instance.WorkspaceSelectedCleanupRequest(targets=[target(leases[0])])))
    try:
        async with asyncio.timeout(5):
            while not started.is_set():await asyncio.sleep(.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):await task
        assert agent._admitting_sessions=={leases[0].session_id}
        assert Path(leases[0].worktree_path).exists()
        release.set()
        async with asyncio.timeout(5):
            while agent._admitting_sessions:await asyncio.sleep(.01)
        assert finished.is_set() and not Path(leases[0].worktree_path).exists()
        assert Path(leases[1].worktree_path).exists()
    finally:
        release.set();await runtime.close()


def test_old_server_cannot_interpret_selected_cleanup_as_global_collection():
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    legacy=FastAPI();calls=[]
    @legacy.post('/api/workspaces/reconcile')
    def global_gc(body:dict):calls.append(body);return {'global':True}
    response=TestClient(legacy).post('/api/workspaces/collect-selected',json={'targets':[{'lease_id':'a'*64}]})
    assert response.status_code==404 and calls==[]

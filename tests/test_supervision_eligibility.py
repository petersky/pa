"""Exercise real policy producers, authority inventory and watch consumers."""
import json
from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from pa.config import Settings
from pa.pr_supervisor import scope
from pa.pr_supervisor.eligibility import evaluate, validate_advertisement
from pa.pr_supervisor.github import GitHubClient, GitHubCredentials
from pa.pr_supervisor.models import GitHubCapability, PRWatch, utcnow
from pa.pr_supervisor.policy import PolicyLoadError, parse_policy
from pa.pr_supervisor.service import PRSupervisor
from pa.pr_supervisor.store import PRSupervisorStore


def write_policy(root, payload):
    path = root / 'integrations' / 'github.json'
    path.parent.mkdir(exist_ok=True)
    path.write_text(json.dumps(payload))
    return path


@pytest.mark.parametrize('mode,repositories', [('none', []), ('allowlist', ['PeterSky/PA']), ('unrestricted', [])])
def test_explicit_modes_agree(tmp_path, monkeypatch, mode, repositories):
    monkeypatch.setenv('PA_GITHUB_TOKEN', 'environment-secret')
    write_policy(tmp_path, {'allowed_repositories': repositories, 'pa_supervision_scope': {'schema_version': 1, 'mode': mode}})
    current = scope.snapshot(tmp_path)
    credentials = GitHubCredentials.load(tmp_path)
    capability = credentials.capability('local')
    assert capability.scope_mode == current['scope_mode'] == mode
    assert capability.policy_revision == current['revision']
    assert capability.supports('petersky/pa') == (mode != 'none')
    assert capability.supports('petersky/other') == (mode == 'unrestricted')
    assert 'secret' not in capability.model_dump_json()


@pytest.mark.parametrize('payload', [[], {}, {'allowed_repositories': 'secret'}, {'allowed_repositories': [3]},
    {'allowed_repositories': ['https://secret@github.com/petersky/pa']},
    {'allowed_repositories': [], 'pa_supervision_scope': {'mode': 'allowlist'}},
    {'allowed_repositories': ['petersky/pa'], 'pa_supervision_scope': {'mode': 'none'}},
    {'allowed_repositories': [], 'pa_supervision_scope': {'schema_version': 999}},
    {'allowed_repositories': [], 'pa_supervision_scope': {'revision': []}},
    {'allowed_repositories': [], 'pa_supervision_scope': {'receipts': []}}])
def test_invalid_policy_never_grants_with_environment_token(tmp_path, monkeypatch, payload):
    monkeypatch.setenv('PA_GITHUB_TOKEN', 'environment-secret')
    write_policy(tmp_path, payload)
    credentials = GitHubCredentials.load(tmp_path)
    assert credentials.token_source == 'environment'
    assert credentials.configuration_status == 'invalid'
    assert not credentials.capability('local').supports('petersky/pa')
    with pytest.raises((scope.ScopeError, PolicyLoadError)):
        scope.snapshot(tmp_path)
    assert 'secret' not in credentials.capability('local').model_dump_json()


@pytest.mark.parametrize('failure', ['missing', 'unreadable', 'json'])
def test_unavailable_policy_preserves_auth_source(tmp_path, monkeypatch, failure):
    monkeypatch.setenv('PA_GITHUB_TOKEN', 'environment-secret')
    if failure == 'json':
        write_policy(tmp_path, {}).write_text('{secret')
    context = patch.object(Path, 'read_text', side_effect=PermissionError('secret')) if failure == 'unreadable' else patch.dict('os.environ', {})
    with context:
        credentials = GitHubCredentials.load(tmp_path)
    assert credentials.token_source == 'environment'
    assert not credentials.capability('local').supports('petersky/pa')
    assert 'secret' not in credentials.capability('local').model_dump_json()


@pytest.mark.parametrize('repositories,source', [(['PeterSky/PA'], 'legacy_explicit'), ([], 'legacy_unrestricted')])
def test_legacy_permissions_and_revision_preserved(tmp_path, monkeypatch, repositories, source):
    monkeypatch.setenv('PA_GITHUB_TOKEN', 'secret')
    path = write_policy(tmp_path, {'allowed_repositories': repositories, 'pa_supervision_scope': {'revision': 'existing-revision', 'receipts': {}}})
    original = path.read_bytes()
    current = scope.snapshot(tmp_path)
    capability = GitHubCredentials.load(tmp_path).capability('local')
    assert current['policy_source'] == capability.policy_source == source
    assert current['revision'] == capability.policy_revision == 'existing-revision'
    assert capability.supports('petersky/other') == (not repositories)
    assert path.read_bytes() == original


def cap(instance='peer', **kwargs):
    return GitHubCapability(instance_id=instance, authenticated=True,
        pr_watch_protocol_version=2, allowed_repositories=['petersky/pa'], **kwargs)


def test_mixed_fleet_retains_causes_and_unknown_revision():
    report = evaluate([cap('macmini'), GitHubCapability(instance_id='macbook', authenticated=True,
        allowed_repositories=['petersky/pa', 'petersky/eschaton'], pr_watch_protocol_version=2,
        scope_mode='allowlist', policy_revision='revision-two', policy_source='configured'),
        cap('stale', checked_at=utcnow()-timedelta(seconds=121)),
        cap('bad', configuration_status='invalid'),
        GitHubCapability(instance_id='no-auth', pr_watch_protocol_version=2)], 'petersky/eschaton', authority_instance_id='macbook')
    assert report.eligible == ['macbook']
    rows = {c.instance_id: c for c in report.candidates}
    assert rows['macmini'].reason_code == 'scope_denied'
    assert rows['macmini'].policy_revision is None
    assert rows['stale'].reason_code == 'capability_stale'
    assert rows['bad'].repositories is None
    assert rows['no-auth'].reason_code == 'credentials_unavailable'
    assert evaluate([], 'petersky/pa', authority_instance_id='a').reason_code == 'no_candidates'


def test_receiver_preserves_newer_observation_and_bounded_history(tmp_path):
    store = PRSupervisorStore(tmp_path/'supervisor.db')
    now = utcnow()
    store.save_capability(cap(checked_at=now, policy_revision='new'))
    store.save_capability(cap(checked_at=now-timedelta(seconds=60), policy_revision='old'))
    assert store.list_capabilities()[0].policy_revision == 'new'
    store.save_capability(cap('expired', checked_at=now-timedelta(seconds=121)))
    assert len(store.list_capabilities()) == 1
    assert len(store.list_capabilities(fresh_seconds=86400)) == 2
    with pytest.raises(ValueError):
        store.save_capability(cap(checked_at=now+timedelta(minutes=1)))
    for payload in [{}, {'allowed_repositories': 'invalid'}, {'allowed_repositories': []}]:
        with pytest.raises(ValueError):
            validate_advertisement(payload)


@pytest.mark.asyncio
@pytest.mark.parametrize('response,reason', [(httpx.ReadTimeout('provider-secret'), 'authority_unreachable'),
    ([], 'authority_response_invalid'), ({'instances': 'secret'}, 'authority_response_invalid'),
    ({'instances': [{}]}, 'authority_response_invalid')])
async def test_authority_failure_reaches_durable_watch_without_auth_blame(tmp_path, monkeypatch, response, reason):
    monkeypatch.setenv('PA_GITHUB_TOKEN', 'secret')
    write_policy(tmp_path, {'allowed_repositories': ['petersky/other']})
    settings = Settings(data_dir=tmp_path, instance_id='local', instance_url='http://local', fleet_owner_url='http://authority', peers=[])
    store = PRSupervisorStore(tmp_path/'supervisor.db')
    domain = MagicMock()
    domain.list_cards.return_value = []
    service = PRSupervisor(settings, domain, supervisor_store=store)
    service.github._request = AsyncMock(return_value=(200, {'login': 'test'}))
    service._post_json = AsyncMock(return_value={})
    service._get_json = AsyncMock(side_effect=response) if isinstance(response, Exception) else AsyncMock(return_value=response)
    service.eligibility_journal_hook = MagicMock()
    try:
        for number in range(1, 3):
            store.upsert_watch(PRWatch(id=f'watch-{number}', repository='petersky/pa', pr_number=number, pr_url=f'https://github.com/petersky/pa/pull/{number}'))
        await service.run_once()
        assert service._get_json.await_count == 1
        for row in store.list_watches():
            assert row.state['eligibility']['reason_code'] == reason
            assert row.state['eligibility']['evaluation_state'] == 'unavailable'
            assert 'authentication' not in row.last_error.lower()
            assert 'secret' not in row.model_dump_json()
            assert row.next_poll_at > utcnow()
        assert service.eligibility_journal_hook.call_count == 2
    finally:
        await service.http_client.aclose()


@pytest.mark.asyncio
async def test_refresh_fails_closed_and_recovers_without_restart(tmp_path, monkeypatch):
    monkeypatch.setenv('PA_GITHUB_TOKEN', 'secret')
    settings = Settings(data_dir=tmp_path, instance_id='local', peers=[])
    service = PRSupervisor(settings, MagicMock())
    service.github._request = AsyncMock(return_value=(200, {'login': 'test'}))
    try:
        first = await service.refresh_capability(force=True)
        assert first.state == 'scope_config_unavailable'
        assert not first.supports('petersky/pa')
        service.github._request.assert_not_awaited()
        path = write_policy(tmp_path, {'allowed_repositories': ['petersky/pa']})
        healthy = await service.refresh_capability()
        assert healthy.supports('petersky/pa')
        path.write_text('{secret')
        failed = await service.refresh_capability()
        assert failed.state == 'scope_config_invalid'
        assert not failed.supports('petersky/pa')
        assert not service.store.list_capabilities()[0].supports('petersky/pa')
    finally:
        await service.http_client.aclose()


def test_settings_script_renders_truthful_local_and_advertised_states():
    import shutil
    import subprocess
    node = shutil.which('node')
    if node is None:
        pytest.skip('Node is not installed')
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run([node, str(root/'tests/github_scope_node_harness.js'),
        str(root/'src/pa/server/static/js/github-scope.js')], capture_output=True, text=True, timeout=10)
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.asyncio
async def test_eligible_peer_resolves_watch_blocker_without_expanding_local_scope(tmp_path, monkeypatch):
    monkeypatch.setenv('PA_GITHUB_TOKEN', 'secret')
    path = write_policy(tmp_path, {'allowed_repositories': ['petersky/other']})
    original = path.read_bytes()
    service = PRSupervisor(Settings(data_dir=tmp_path, instance_id='local', peers=[]), MagicMock())
    service.domain_store.list_cards.return_value = []
    service.github._request = AsyncMock(return_value=(200, {'login': 'test'}))
    service.store.upsert_watch(PRWatch(id='recover', repository='petersky/pa', pr_number=1,
        pr_url='https://github.com/petersky/pa/pull/1'))
    try:
        await service.run_once()
        blocked = service.store.get_watch('recover')
        assert blocked.state['eligibility']['candidates'][0]['reason_code'] == 'scope_denied'
        service.store.save_capability(cap())
        service.store.schedule_now(watch_id='recover')
        await service.run_once()
        recovered = service.store.get_watch('recover')
        assert recovered.last_error is None
        assert recovered.state['eligibility']['eligible'] == ['peer']
        assert recovered.state['supervisor_state'] == 'eligible_instance_available'
        assert path.read_bytes() == original
        assert not service.capability.supports('petersky/pa')
    finally:
        await service.http_client.aclose()


@pytest.mark.asyncio
async def test_interrupted_authority_read_is_not_misreported_as_eligibility_failure(tmp_path):
    import asyncio
    service = PRSupervisor(Settings(data_dir=tmp_path, instance_id='local',
        instance_url='http://local', fleet_owner_url='http://authority'), MagicMock())
    service._get_json = AsyncMock(side_effect=asyncio.CancelledError())
    try:
        with pytest.raises(asyncio.CancelledError):
            await service._eligible_capabilities('petersky/pa')
        assert not hasattr(service, '_eligibility_inventory')
    finally:
        await service.http_client.aclose()


def test_stale_capability_cannot_acquire_effect_lease(tmp_path):
    store = PRSupervisorStore(tmp_path/'supervisor.db')
    store.upsert_watch(PRWatch(id='fence', repository='petersky/pa', pr_number=1,
        pr_url='https://github.com/petersky/pa/pull/1'))
    grant = store.try_acquire_lease('fence', 'peer', capability=cap(checked_at=utcnow()-timedelta(seconds=121)))
    assert not grant.acquired
    assert grant.reason == 'capability_stale'


@pytest.mark.asyncio
@pytest.mark.parametrize('status_code', [401, 403, 404])
async def test_repository_denial_only_heals_after_successful_observation(tmp_path, monkeypatch, status_code):
    from pa.pr_supervisor.github import GitHubAPIError
    from tests.test_pr_supervisor import snapshot
    monkeypatch.setenv('PA_GITHUB_TOKEN', 'secret')
    write_policy(tmp_path, {'allowed_repositories': ['owner/repo']})
    service = PRSupervisor(Settings(data_dir=tmp_path, instance_id='local', peers=[]), MagicMock())
    service.domain_store.list_cards.return_value = []
    service.github._request = AsyncMock(return_value=(200, {'login': 'test'}))
    service._notify = AsyncMock()
    service.eligibility_journal_hook = MagicMock()
    service.store.upsert_watch(PRWatch(id='access-recovery', repository='owner/repo', pr_number=17,
        pr_url='https://github.com/owner/repo/pull/17'))
    calls = 0

    async def observe(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls > 1:
            current = service.store.get_watch('access-recovery')
            assert current.status.value == 'blocked'
            assert current.last_error
            report = current.state['eligibility']
            assert report['dependency'] == 'github_repository_observation'
            assert not report['eligible']
            # Even immediately before successful network observation, neither
            # the watch nor the shared journal may have declared recovery.
            assert all(not call.args[0]['report']['eligible']
                       for call in service.eligibility_journal_hook.call_args_list
                       if call.args[0]['report']['dependency'] == 'github_repository_observation')
        if calls < 3:
            raise GitHubAPIError(status_code, 'snapshot', 'private-secret')
        return snapshot()

    service.github.snapshot = observe
    try:
        for attempt in range(3):
            service.store.schedule_now(watch_id='access-recovery')
            await service.run_once()
            current = service.store.get_watch('access-recovery')
            if attempt < 2:
                assert current.status.value == 'blocked'
                assert not current.state['eligibility']['eligible']
                assert 'secret' not in current.last_error
        assert calls == 3
        assert current.status.value == 'active'
        assert current.last_error is None
        emissions = [call.args[0] for call in service.eligibility_journal_hook.call_args_list
                     if call.args[0]['report']['dependency'] == 'github_repository_observation']
        assert len(emissions) == 3
        assert emissions[0]['issues'][0]['issue_key'] == emissions[1]['issues'][0]['issue_key']
        assert emissions[-1]['report']['dependency'] == 'github_repository_observation'
        assert emissions[-1]['report']['eligible'] == ['local']
        assert not emissions[-1]['issues']
    finally:
        await service.http_client.aclose()


@pytest.mark.asyncio
async def test_delayed_inventory_uses_response_time_for_freshness(tmp_path):
    service = PRSupervisor(Settings(data_dir=tmp_path, instance_id='local',
        instance_url='http://local', fleet_owner_url='http://authority'), MagicMock())
    started = utcnow()
    clock = [started]

    async def delayed_read(url):
        clock[0] = started + timedelta(seconds=10)
        heartbeat = cap('authority', checked_at=clock[0]).model_dump(mode='json')
        return {'local': heartbeat, 'instances': [heartbeat], 'history_seconds': 86400}

    service._get_json = AsyncMock(side_effect=delayed_read)
    try:
        with patch('pa.pr_supervisor.service.utcnow', side_effect=lambda: clock[0]):
            report = await service._eligible_capabilities('petersky/pa')
            assert report.eligible == ['authority']
            assert report.candidates[0].freshness == 'fresh'
            assert report.observed_at == started + timedelta(seconds=10)
            assert service._eligibility_inventory[0] == started + timedelta(seconds=15)
            clock[0] += timedelta(seconds=1)
            cached = await service._eligible_capabilities('petersky/pa')
            assert cached.eligible == ['authority']
            assert cached.observed_at == clock[0]
            assert service._get_json.await_count == 1
    finally:
        await service.http_client.aclose()


@pytest.mark.asyncio
async def test_inventory_and_repository_incidents_recover_independently(tmp_path, monkeypatch):
    from pa.pr_supervisor.github import GitHubAPIError
    from tests.test_pr_supervisor import snapshot
    monkeypatch.setenv('PA_GITHUB_TOKEN', 'secret')
    service = PRSupervisor(Settings(data_dir=tmp_path, instance_id='local', peers=[]), MagicMock())
    service.domain_store.list_cards.return_value = []
    service.github._request = AsyncMock(return_value=(200, {'login': 'test'}))
    service.github.snapshot = AsyncMock(side_effect=[GitHubAPIError(403, 'snapshot'),
        GitHubAPIError(403, 'snapshot'), snapshot()])
    service._notify = AsyncMock()
    service.store.upsert_watch(PRWatch(id='independent-recovery', repository='owner/repo', pr_number=17,
        pr_url='https://github.com/owner/repo/pull/17'))
    reports, incidents = [], set()

    def journal(event):
        report = event['report']
        reports.append(report)
        dependency = report['dependency']
        if report['evaluation_state'] == 'complete':
            for candidate in report['candidates']:
                if candidate['reason_code'] is None:
                    incidents.difference_update({key for key in incidents
                        if key[:2] == (dependency, candidate['instance_id'])})
        for issue in event['issues']:
            incidents.add((dependency, issue['instance_id'], issue['reason_code']))

    service.eligibility_journal_hook = journal
    inventory_issue = ('capability_inventory', 'local', 'scope_config_unavailable')
    repository_issue = ('github_repository_observation', 'local', 'repository_access_denied')
    try:
        await service.run_once()
        assert incidents == {inventory_issue}
        service.github.snapshot.assert_not_awaited()
        write_policy(tmp_path, {'allowed_repositories': ['owner/repo']})
        for attempt in range(3):
            service.store.schedule_now(watch_id='independent-recovery')
            await service.run_once()
            assert inventory_issue not in incidents
            if attempt < 2:
                assert incidents == {repository_issue}
                current = service.store.get_watch('independent-recovery')
                assert current.state['eligibility']['dependency'] == 'github_repository_observation'
                assert not current.state['eligibility']['eligible']
            else:
                assert not incidents
        assert [r['dependency'] for r in reports] == ['capability_inventory',
            'capability_inventory', 'github_repository_observation',
            'capability_inventory', 'github_repository_observation',
            'capability_inventory', 'github_repository_observation']
    finally:
        await service.http_client.aclose()


@pytest.mark.asyncio
async def test_local_capability_does_not_heal_unavailable_authority(tmp_path, monkeypatch):
    from pa.pr_supervisor.models import LeaseGrant
    monkeypatch.setenv('PA_GITHUB_TOKEN', 'secret')
    write_policy(tmp_path, {'allowed_repositories': ['owner/repo']})
    service = PRSupervisor(Settings(data_dir=tmp_path, instance_id='local',
        instance_url='http://local', fleet_owner_url='http://authority'), MagicMock())
    service.domain_store.list_cards.return_value = []
    service.github._request = AsyncMock(return_value=(200, {'login': 'test'}))
    service._post_json = AsyncMock(return_value={})
    service._get_json = AsyncMock(side_effect=httpx.ReadTimeout('secret'))
    service._acquire_lease = AsyncMock(return_value=LeaseGrant(acquired=False))
    service.eligibility_journal_hook = MagicMock()
    service.store.upsert_watch(PRWatch(id='authority-not-healed', repository='owner/repo', pr_number=17,
        pr_url='https://github.com/owner/repo/pull/17'))
    try:
        await service.run_once()
        report = service.eligibility_journal_hook.call_args.args[0]['report']
        assert service.capability.supports('owner/repo')
        assert report['dependency'] == 'capability_inventory'
        assert report['evaluation_state'] == 'unavailable'
        assert report['reason_code'] == 'authority_unreachable'
        assert not report['eligible']
    finally:
        await service.http_client.aclose()

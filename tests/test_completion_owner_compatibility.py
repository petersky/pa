from types import SimpleNamespace
from unittest.mock import patch

import pytest
from fastapi import HTTPException
from starlette.requests import Request

from pa.config import Settings
from pa.domain.completion import COMPLETION_CAPABILITY, CompletionConflict
from pa.domain.models import AgentSession, CardCreate, CardUpdate
from pa.domain.projection import CardProjection
from pa.modules.fleet import RemoteAgentStartBody, _admit_remote_agent_work
from pa.modules.items import _require_completion_owner_compatibility
from pa.modules.pr_supervisor import acquire_lease
from pa.pr_supervisor.models import GitHubCapability
from pa.pr_supervisor.store import PRSupervisorStore
from tests.test_pr_supervisor import watch


def context(tmp_path):
    cards = CardProjection(tmp_path / 'cards.db')
    watches = PRSupervisorStore(tmp_path / 'watches.db')
    watches.completion_capabilities = cards.card_completion_capabilities
    settings = Settings(data_dir=tmp_path, instance_id='authority')
    fleet = SimpleNamespace(get_instance=lambda owner: SimpleNamespace(capabilities=[]))
    services = {'pr_supervisor_store': watches, 'fleet_registry': fleet}
    ctx = SimpleNamespace(store=cards, settings=settings, services=services, require_service=services.__getitem__)
    request = Request({'type': 'http', 'headers': [(b'x-pa-origin-instance-id', b'old')], 'app': SimpleNamespace(state=SimpleNamespace(ctx=ctx))})
    return cards, watches, request


@pytest.mark.asyncio
async def test_old_owner_cannot_acquire_protected_lease_or_dispatch(tmp_path):
    cards, watches, request = context(tmp_path)
    protected = cards.create_card(CardCreate(title='acceptance pending', completion_requirement={'mode': 'explicit_acceptance', 'milestones': ['integrated', 'verified']}))
    source = cards.create_card(CardCreate(title='legacy source'))
    item = watch().model_copy(update={'card_id': protected.id, 'required_capabilities': []})
    # Old wire snapshot omits the new optional requirement. Authority reads the
    # canonical card, not a capability request chosen by the old worker.
    watches.upsert_watch(item)
    old = GitHubCapability(instance_id='old', authenticated=True, pr_watch_protocol_version=2)
    result = await acquire_lease(request, item.id, {'instance_id': 'old', 'capability': old.model_dump(mode='json')})
    assert result['acquired'] is False
    assert result['reason'] == 'completion_owner_incompatible'
    assert watches.get_watch(item.id).owner_instance_id is None
    # Actual named dispatch consumer rejects before ledger admission/provider
    # start/workspace provisioning, including the no-placement internal path.
    with patch('pa.modules.fleet.require_user', return_value=SimpleNamespace(role='admin')):
        with pytest.raises(HTTPException) as rejected:
            await _admit_remote_agent_work(request, 'old', RemoteAgentStartBody(card_id=protected.id, message='merge then clean'))
    assert rejected.value.detail['code'] == 'completion_owner_incompatible'
    assert cards.get_card(protected.id).completion_status['accepted'] is False
    compatible = old.model_copy(update={'capabilities': [COMPLETION_CAPABILITY]})
    granted = await acquire_lease(request, item.id, {'instance_id': 'old', 'capability': compatible.model_dump(mode='json')})
    assert granted['acquired'] is True
    denied_renewal = await acquire_lease(request, item.id, {'instance_id': 'old', 'capability': old.model_dump(mode='json')})
    assert denied_renewal['reason'] == 'completion_owner_incompatible'
    legacy = item.model_copy(update={'id': 'legacy', 'card_id': source.id, 'pr_number': 18})
    watches.upsert_watch(legacy)
    assert (await acquire_lease(request, legacy.id, {'instance_id': 'old', 'capability': old.model_dump(mode='json')}))['acquired'] is True


def test_declaration_rejects_existing_incompatible_card_owner(tmp_path):
    cards, watches, request = context(tmp_path)
    card = cards.create_card(CardCreate(title='already executed'))
    cards.save_session(AgentSession(agent_name='codex', card_id=card.id, origin_instance_id='old'))
    update = CardUpdate(completion_requirement={'mode': 'explicit_acceptance'})
    with pytest.raises(CompletionConflict, match='completion_existing_owner_incompatible'):
        _require_completion_owner_compatibility(request, card.id, update)
    assert cards.get_card(card.id).completion_requirement is None
    request.app.state.ctx.services['fleet_registry'].get_instance = lambda owner: SimpleNamespace(capabilities=[COMPLETION_CAPABILITY])
    _require_completion_owner_compatibility(request, card.id, update)


@pytest.mark.asyncio
async def test_incompatible_supervisor_cannot_reach_merge_completion_or_cleanup(tmp_path):
    from pathlib import Path
    from unittest.mock import AsyncMock
    from pa.pr_supervisor.service import PRSupervisor
    from tests.test_pr_supervisor import _FakeGitHub, _DedupeDispatcher, snapshot
    from tests.test_repository_workspaces import manager_for

    cards, watches, request = context(tmp_path)
    card = cards.create_card(CardCreate(title='acceptance', lane='waiting', completion_requirement={'mode': 'explicit_acceptance'}))
    fixture = tmp_path / 'workspace-fixture'
    fixture.mkdir()
    manager, _, linked = manager_for(fixture)
    manager.store = cards
    lease = manager.provision_repository(linked, project_id='project-1', session_id='session-1', card_id=card.id)
    item = watch().model_copy(update={'card_id': card.id, 'required_capabilities': []})
    watches.upsert_watch(item)
    service = PRSupervisor(Settings(data_dir=tmp_path, instance_id='old', instance_url='http://old', fleet_owner_url='http://old', peers=[]), cards, supervisor_store=watches, github_client=_FakeGitHub([snapshot(state='merged', merge_commit_sha='c' * 40)]), dispatcher=_DedupeDispatcher(), workspace_manager=manager)
    # Current PR protocol but no completion support, as emitted by the previous
    # binary. The authority's lease rejection prevents reaching its merge path.
    service.refresh_capability = AsyncMock(return_value=GitHubCapability(instance_id='old', authenticated=True, pr_watch_protocol_version=2))
    await service.run_once()
    assert watches.get_watch(item.id).owner_instance_id is None
    assert cards.get_card(card.id).lane.value == 'waiting'
    assert not manager.list()[0].completed
    assert Path(lease.worktree_path).is_dir()
    assert cards.get_card(card.id).completion_evidence == []

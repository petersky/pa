"""Failed effects stay fenced while durable, actionable notices survive restart."""
import asyncio
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pa.config import Settings
from pa.domain.models import AgentSession, RestartHandoff
from pa.domain.notifications import Notification, RESTART_FAILURE_NOTICE_CAPABILITY
from pa.domain.projection import CardProjection
from pa.instance.agent_session import AgentSessionManager
from pa.instance.restart_lifecycle import restart_observation_fields
from pa.modules.notifications import _presentation_metadata
from pa.notifications.service import NotificationService


def fixture(tmp_path):
    store = CardProjection(tmp_path / 'cards.db')
    settings = Settings(data_dir=tmp_path, instance_id='owner', instance_name='Owner')
    session = store.save_session(AgentSession(
        id='exact-session', agent_name='codex', realm_id='engineering',
        principal_id='user:owner', card_id='card', project_id='project',
    ))
    service = NotificationService(SimpleNamespace(settings=settings, store=store))
    service._publish = MagicMock()
    manager = AgentSessionManager(settings, store)
    manager.notification_service = service
    receipt = store.create_restart_handoff(RestartHandoff(
        session_id=session.id, card_id='card', project_id='project',
        instance_id='owner', idempotency_key='original',
        continuation_prompt='PRIVATE PROMPT MUST NOT APPEAR',
        continuation_prompt_id='original-prompt', status='failed',
        failure_stage='quiescing', error='PRIVATE RAW ERROR',
        reason_code='restart_operation_failed',
    ))
    return manager, store, service, receipt


def notices(store):
    return store.list_notifications(realm_id='engineering', principal_id='user:owner')


def test_failure_is_in_default_inbox_with_exact_owner_and_no_response_contract(tmp_path):
    manager, store, service, receipt = fixture(tmp_path)
    asyncio.run(manager._resume_pending_restart_handoffs())
    notice, = notices(store)
    assert notice.outstanding and notice.interaction is None
    assert notice.owner_instance_id == notice.source_instance_id == 'owner'
    assert notice.realm_id == 'engineering' and notice.principal_id == 'user:owner'
    assert notice.visibility.value == 'principal'
    assert notice.card_id == 'card' and notice.project_id == 'project'
    assert notice.session_id == receipt.session_id
    assert notice.destination_url == '/agent?session=exact-session'
    assert notice.capability == RESTART_FAILURE_NOTICE_CAPABILITY
    assert 'PRIVATE' not in notice.model_dump_json()
    assert not manager._restart_handoff_tasks and store.get_restart_handoff(receipt.id) == receipt
    assert restart_observation_fields(receipt)['effect'] == 'unknown'
    visible, count = service.list_inbox(
        principal_id='user:owner', realms={'engineering'}, realm_id='engineering', outstanding=True,
    )
    assert visible == [notice] and count == 1
    assert service.list_inbox(principal_id='user:other', realms={'engineering'}, realm_id='engineering', outstanding=True) == ([], 0)
    assert service.list_inbox(principal_id='user:owner', realms={'default'}, realm_id='engineering', outstanding=True) == ([], 0)
    presentation = _presentation_metadata(notice)
    assert presentation['category'] == 'blocker' and presentation['required_action']
    assert 'does not restart' in presentation['next_effect']


def test_many_sweeps_and_fresh_process_do_not_write_or_reopen_dismissed_notice(tmp_path):
    manager, store, service, receipt = fixture(tmp_path)
    async def scenario():
        await asyncio.gather(*(manager._surface_restart_failure(receipt) for _ in range(20)))
        notice, = notices(store)
        service.resolve(notice, principal_id='user:owner', idempotency_key='dismiss')
        dismissed, = notices(store)
        assert not dismissed.outstanding
        with patch.object(store, 'save_notification', wraps=store.save_notification) as save:
            for _ in range(20):
                await manager._resume_pending_restart_handoffs()
            fresh = AgentSessionManager(manager.settings, store)
            fresh.notification_service = service
            await fresh._resume_pending_restart_handoffs()
            save.assert_not_called()
        assert notices(store) == [dismissed]
        assert store.get_restart_handoff(receipt.id) == receipt
    asyncio.run(scenario())


@pytest.mark.parametrize('after_commit', [False, True])
def test_failed_notice_delivery_retries_custody_only_even_after_ambiguous_commit(tmp_path, after_commit):
    manager, store, service, receipt = fixture(tmp_path)
    original = service.create
    def fail(*args, **kwargs):
        if after_commit:
            original(*args, **kwargs)
        raise OSError('private fixture notification failure')
    async def scenario():
        with patch.object(service, 'create', side_effect=fail):
            await manager._surface_restart_failure(receipt)
        assert not manager._restart_failure_notices
        await manager._surface_restart_failure(receipt)
        notice, = notices(store)
        assert notice.coalesced_count == 1 and notice.version == 1
        assert store.get_restart_handoff(receipt.id) == receipt
        assert not manager._restart_handoff_tasks
    asyncio.run(scenario())


@pytest.mark.parametrize('condition', ['foreign_owner', 'missing_session', 'not_failed', 'stale_version'])
def test_unowned_or_stale_receipts_do_not_guess_notice_context(tmp_path, condition):
    manager, store, service, receipt = fixture(tmp_path)
    candidate = receipt.model_copy(deep=True)
    if condition == 'foreign_owner':candidate.instance_id = 'foreign'
    if condition == 'missing_session':
        store.get_session = MagicMock(return_value=None)
    if condition == 'not_failed':candidate.status = 'restarting'
    if condition == 'stale_version':candidate.phase_version += 1
    asyncio.run(manager._surface_restart_failure(candidate))
    assert not notices(store) and store.get_restart_handoff(receipt.id) == receipt


def test_notice_cache_is_bounded_and_does_not_change_normal_health_inbox_semantics(tmp_path):
    manager, store, service, receipt = fixture(tmp_path)
    for i in range(200):manager._restart_failure_notices.append((str(i), 0))
    assert len(manager._restart_failure_notices) == 128
    notice = Notification(title='Other health', type='service_health', severity='error')
    assert not notice.outstanding
    notice.capability = RESTART_FAILURE_NOTICE_CAPABILITY
    assert notice.outstanding
    notice.expires_at = datetime.now(UTC) - timedelta(seconds=1)
    assert not notice.outstanding


def test_executor_failure_persists_original_error_before_surface_and_never_restarts(tmp_path):
    manager, store, service, original = fixture(tmp_path)
    receipt = store.create_restart_handoff(RestartHandoff(
        session_id=original.session_id, instance_id='owner', idempotency_key='new',
        continuation_prompt='PRIVATE', continuation_prompt_id='new-prompt',
    ))
    manager.quiesce = AsyncMock(side_effect=TimeoutError('private quiesce deadline'))
    with patch('pa.cli.service.request_restart') as restart:
        asyncio.run(manager._execute_restart_handoff(receipt.id))
        restart.assert_not_called()
    failed = store.get_restart_handoff(receipt.id)
    assert failed.status == 'failed' and failed.failure_stage == 'quiescing'
    assert failed.error == 'private quiesce deadline' and failed.attempts == 0
    notice, = notices(store)
    assert 'private quiesce deadline' not in notice.body
    assert notice.deduplication_key.endswith(f':{failed.phase_version}')

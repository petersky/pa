"""Packet planning preserves origin authority without retained-history copies."""
import ast
from concurrent.futures import ThreadPoolExecutor
from dataclasses import FrozenInstanceError
from datetime import UTC, datetime
from pathlib import Path
import threading
from unittest.mock import patch

import pytest

from pa.core.async_runtime import AsyncRuntime
from pa.domain.completion import CompletionConflict
from pa.pr_supervisor.models import PRWatch, PRWatchStatus
from pa.health_journal.service import CANONICAL_PHASES
from tests.test_autonomous_verification import health_repair, ordinary, origin, repair_child_writer, system


def full_packet(target, card):
    ledger = target.ctx.services['dispatch_store']
    with patch.object(ledger, 'verification_card_origins', None), patch.object(ledger, 'verification_origin', None):
        return target.service.packet(card)


def test_native_packet_does_not_copy_retained_dispatch_history(system):
    card, _ = ordinary(system)
    origin(system, card.id, session_id='other-origin',
           materialization_plan={'steps': [{'proof': ['x' * 65536]} for _ in range(20)]})
    expected = full_packet(system, card)
    ledger = system.ctx.services['dispatch_store']
    with patch.object(ledger, '_snapshot', side_effect=AssertionError('Packet needs no full dispatch history')):
        assert system.service.packet(card) == expected
    assert not system.calls


@pytest.mark.parametrize('shape', ['ordinary', 'ordinary_retired_watch', 'health', 'health_child', 'health_bad_proof'])
def test_complete_packet_and_full_watch_proof_parity(system, shape):
    if shape.startswith('health'):
        card, _, _ = health_repair(system)
        if shape == 'health_child':
            repair_child_writer(system, card)
        if shape == 'health_bad_proof':
            watches = system.ctx.services['pr_supervisor_store']
            watch = watches.find_watches('test/pa', 1)[0]
            watch.state.pop('merge_commit_sha')
            watches.upsert_watch(watch)
    else:
        card, record = ordinary(system)
        if shape == 'ordinary_retired_watch':
            system.ctx.services['pr_supervisor_store'].upsert_watch(PRWatch(
                repository='fixture/repo', pr_number=1, pr_url='https://fixture.invalid/1',
                card_id=card.id, originating_session_id=record.session_id,
                dispatch_id=record.dispatch_id, status=PRWatchStatus.MERGED,
                retired_at=datetime.now(UTC), state={'proof': 'x' * 65536, 'merge_commit_sha': 'a' * 40}))
    expected = full_packet(system, card)
    assert system.service.packet(card) == expected
    if shape == 'health_bad_proof':
        assert expected['subject'] is None
    assert not system.calls


def test_health_retry_control_is_fresh_and_detached(system):
    card, action, lease = health_repair(system)
    journal = system.health.journal
    journal.action_result(action['id'], state='blocked', result={'retry_pending': True},
                          owner=system.health.owner, fence=lease['fence'])
    current = journal.repair_action(action['group_id'],
        expected_version=journal.group(action['group_id'], realms=['default'])['version'], realms=['default'])
    ledger = system.ctx.services['dispatch_store']
    record = ledger.get(current['result']['dispatch_id'])
    assert system.service.packet(card) == full_packet(system, card)
    assert system.service.packet(card)['pending_repair_controls'] == [action['id']]
    record.control_operations['health-start_retry:' + action['id']] = 'retry'
    ledger.put(record)
    view = ledger.verification_origin(record.dispatch_id)
    view.control_operations.clear()
    assert ledger.verification_origin(record.dispatch_id).control_operations
    assert system.service.packet(card) == full_packet(system, card)
    assert system.service.packet(card)['pending_repair_controls'] == []
    assert not system.calls


@pytest.mark.parametrize('card_id,realm_id,limit', [
    ('target', 'default', 101), ('target', 'other', 101), ('', '', 2),
    ('target', 'default', 0), ('target', 'default', -1),
])
def test_origin_window_matches_full_list_filter_order_and_limit(system, card_id, realm_id, limit):
    ledger = system.ctx.services['dispatch_store']
    for card, realm, session in [('target', 'default', 'a'), ('other', 'default', 'b'),
                                  ('target', 'other', 'c'), ('target', 'default', 'd')]:
        origin(system, card, realm_id=realm, session_id=session)
    expected = ledger.list(card_id=card_id, realm_id=realm_id, limit=limit)
    views = ledger.verification_card_origins(card_id=card_id, realm_id=realm_id, limit=limit)
    fields = ('dispatch_id', 'idempotency_key', 'session_id', 'card_id', 'realm_id', 'control_operations')
    assert [tuple(getattr(row, name) for name in fields) for row in views] == [
        tuple(getattr(row, name) for name in fields) for row in expected]


def test_origin_projection_is_detached_fresh_and_missing_is_none(system):
    card, record = ordinary(system)
    ledger = system.ctx.services['dispatch_store']
    before = ledger.verification_origin(record.dispatch_id)
    with pytest.raises(FrozenInstanceError):
        before.session_id = 'caller-mutation'
    record.session_id = 'fresh-session'
    record.control_operations['new-operation'] = 'retry'
    ledger.put(record)
    after = ledger.verification_origin(record.dispatch_id)
    assert before.session_id != after.session_id == ledger.get(record.dispatch_id).session_id
    assert not before.control_operations and after.control_operations == {'new-operation': 'retry'}
    assert ledger.verification_origin('missing') is None


def test_origin_projection_observes_only_committed_publication(system):
    card, record = ordinary(system)
    ledger = system.ctx.services['dispatch_store']
    before = ledger.verification_origin(record.dispatch_id)
    record.session_id = 'committed-session'
    entered, release = threading.Event(), threading.Event()
    def pause(boundary):
        if boundary == 'commit_before':
            entered.set()
            assert release.wait(2)
    ledger._fault_injector = pause  # Disposable transaction latch, no service fault.
    try:
        with ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(ledger.put, record)
            try:
                assert entered.wait(2)
                assert ledger.verification_origin(record.dispatch_id) == before
                assert ledger.verification_card_origins(card_id=card.id, realm_id=card.realm_id) == [before]
            finally:
                release.set()
            future.result(timeout=2)
        assert ledger.verification_origin(record.dispatch_id).session_id == 'committed-session'
    finally:
        release.set()
        ledger._fault_injector = None


def test_origin_bound_is_unchanged_and_does_not_require_history_copies(system):
    card, _ = ordinary(system)
    for index in range(100):
        origin(system, card.id, session_id=f'origin-{index}')
    ledger = system.ctx.services['dispatch_store']
    with patch.object(ledger, '_snapshot', side_effect=AssertionError('Bound checked before history copies')):
        with pytest.raises(CompletionConflict, match='verification_origin_bound_exceeded'):
            system.service.packet(card)
    assert not system.calls


@pytest.mark.parametrize('guard', ['realm', 'watch_card', 'watch_session'])
def test_origin_projections_preserve_realm_and_watch_binding_guards(system, guard):
    card, record = ordinary(system)
    if guard == 'realm':
        record.realm_id = 'other'
        system.ctx.services['dispatch_store'].put(record)
        card.completion_requirement.originating_dispatch_ids.append(record.dispatch_id)
        code = 'verification_origin_realm_mismatch'
    else:
        system.ctx.services['pr_supervisor_store'].upsert_watch(PRWatch(
            repository='fixture/repo', pr_number=1, pr_url='https://fixture.invalid/1',
            card_id=card.id, dispatch_id=record.dispatch_id,
            originating_session_id='wrong-session' if guard == 'watch_session' else record.session_id))
        if guard == 'watch_card':
            record.card_id = 'other-card'
            system.ctx.services['dispatch_store'].put(record)
        code = 'verification_watch_origin_mismatch'
    for full in [False, True]:
        with pytest.raises(CompletionConflict, match=code):
            full_packet(system, card) if full else system.service.packet(card)
    assert not system.calls


@pytest.mark.asyncio
async def test_real_planner_callbacks_have_fixed_leaf_metrics_without_admission(system):
    card, _ = ordinary(system)
    runtime = AsyncRuntime(max_workers=1)
    system.ctx.services['async_runtime'] = runtime
    try:
        job = await system.service.ensure(card)
        assert job['state'] == 'pending' and not system.calls
        metrics = runtime.work_metrics.snapshot()['metrics']
        for phase in ['verification_packet', 'verification_intent_read', 'verification_intent_write']:
            assert metrics[f'health.canonical.{phase}.worker_ms']['count'] == 1
            assert metrics[f'health.canonical.{phase}.worker_failed']['total'] == 0
        assert not any(name.startswith('health.canonical.unclassified.') for name in metrics)
    finally:
        await runtime.close()


def test_all_verification_worker_call_sites_use_fixed_bounded_labels():
    tree = ast.parse(Path('src/pa/execution/verification.py').read_text())
    calls = [n for n in ast.walk(tree) if isinstance(n, ast.Call)
             and isinstance(n.func, ast.Attribute) and isinstance(n.func.value, ast.Name)
             and n.func.value.id == 'self' and n.func.attr == 'call']
    assert calls
    for call in calls:
        phase = next((kw.value for kw in call.keywords if kw.arg == 'diagnostic_phase'), None)
        assert isinstance(phase, ast.Constant) and phase.value in CANONICAL_PHASES - {'unclassified'}

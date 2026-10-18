"""Fleet selectors retain identities/order/hints without full card payloads."""
from types import SimpleNamespace
from unittest.mock import Mock, patch

from pa.config import Settings
from pa.core.context import AppContext
from pa.core.hooks import HookBus
from pa.domain.models import CardCreate, CardLane
from pa.domain.projection import CardProjection
from pa.fleet.membership import MembershipStore
from pa.fleet.registry import FleetRegistry
from pa.modules.fleet import _fleet_context
from pa.network.peer_table import PeerTable


def fields(cards):
    return [(c.id, c.realm_id, c.kind, c.title, c.lane, c.project_id,
             c.created_at, c.updated_at) for c in cards]


def seed(store):
    for index, lane in enumerate(CardLane):
        store.create_card(CardCreate(title=f'z{index}', lane=lane,
            realm_id='default', project_id='fixture-project', body='x' * 65536))
    store.create_card(CardCreate(title='other realm', realm_id='other'))


def test_compact_recent_selector_preserves_legacy_full_card_order_and_fields(tmp_path):
    store = CardProjection(tmp_path / 'cards.db')
    seed(store)
    expected = fields(store.list_cards(realm_id='default'))
    with patch.object(store, '_row_to_card', side_effect=AssertionError('full hydration')):
        options = store.list_card_options(realm_id='default', recent_first=True)
    assert fields(options) == expected
    assert all(c.body == '' and not c.attachments and not c.completion_evidence
               for c in options)
    # Other selectors retain their existing alphabetical ordering.
    options = store.list_card_options(realm_id='default')
    assert [(c.title, c.id) for c in options] == sorted((c.title, c.id) for c in options)


def test_fleet_context_keeps_active_dispatch_hints_without_reading_card_bodies(tmp_path):
    store = CardProjection(tmp_path / 'cards.db')
    seed(store)
    settings = Settings(data_dir=tmp_path, instance_id='local',
        instance_name='local', instance_url='http://fixture', primary_realm='default',
        subscribed_realms=['default'], agent_enabled=False, peers=[])
    fleet = FleetRegistry(tmp_path, settings.fleet_id)
    fleet.register_self('local', 'local', 'http://fixture', zone=settings.zone)
    expected = fields(store.list_cards(realm_id='default'))
    active = SimpleNamespace(state='running')
    terminal = SimpleNamespace(state='completed')
    hints = Mock()
    hints.latest_by_card.return_value = {expected[0][0]: active, expected[1][0]: terminal}
    ctx = AppContext(settings=settings, store=store, hooks=HookBus(), services={
        'fleet_registry': fleet, 'membership': MembershipStore(tmp_path),
        'peer_table': PeerTable(tmp_path), 'dispatch_store': hints})
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(ctx=ctx)))
    hint = {'dispatch_id': 'active', 'session_id': 's',
            'target_instance_id': 'remote', 'target_instance_name': 'Remote'}
    with patch.object(store, 'list_cards', side_effect=AssertionError('full hydration')), \
         patch('pa.modules.fleet.build_overview', return_value={'nodes': []}), \
         patch('pa.modules.fleet.canonicalize_dispatch_public', return_value=hint) as canonical:
        context = _fleet_context(request)
    assert fields(context['cards']) == expected
    assert context['active_card_dispatches'] == {expected[0][0]: hint}
    hints.latest_by_card.assert_called_once_with({card[0] for card in expected})
    canonical.assert_called_once_with(ctx, active)

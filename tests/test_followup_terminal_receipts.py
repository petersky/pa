"""Terminal receipts must not keep ordinary verification permanently pending."""
from copy import deepcopy

import pytest

from tests.test_autonomous_verification import ordinary, system


@pytest.mark.asyncio
@pytest.mark.parametrize('error,accepted,may_admit', [
    ({'status_code':409, 'recoverable':False}, False, True),
    ({'status':409, 'recoverable':False}, False, True),
    ({'status_code':409, 'status':409, 'recoverable':False}, False, True),
    ({'status_code':409, 'recoverable':True}, False, False),
    ({'status_code':503, 'recoverable':False}, False, False),
    ({'status_code':409, 'recoverable':False}, True, False),
    ({'status_code':'409', 'recoverable':False}, False, False),
    ({'status_code':409, 'status':503, 'recoverable':False}, False, False),
    ({'status_code':503, 'status':409, 'recoverable':False}, False, False),
    ({'status_code':None, 'status':409, 'recoverable':False}, False, False),
    ({'status_code':409}, False, False),
    ({'recoverable':False}, False, False),
])
async def test_terminal_receipt_preserves_history_and_ordinary_verification_gates(
        system, error, accepted, may_admit):
    card, repair = ordinary(system)
    requirement = card.completion_requirement.model_dump(mode='json')
    receipt = {'state':'failed', 'prompt_id':'historical-prompt',
               'fingerprint':'f'*64, 'error':deepcopy(error)}
    if accepted:
        receipt['response'] = {'accepted':True}
    ledger = system.ctx.services['dispatch_store']

    def add(current):
        current.followup_operations['historical-followup'] = receipt

    ledger.mutate_current(repair.dispatch_id, mutate=add)
    before = deepcopy(ledger.get(repair.dispatch_id).followup_operations)
    await system.service.process(card)
    job = system.service.store.list()[0]

    assert ledger.get(repair.dispatch_id).followup_operations == before
    current = system.ctx.store.get_card(card.id)
    assert current.completion_requirement.model_dump(mode='json') == requirement
    assert current.completion_status['accepted'] is False
    if may_admit:
        assert len(system.calls) == 1 and job['state'] == 'dispatched'
        verifier = ledger.get(job['dispatch_id'])
        assert verifier.session_id != repair.session_id
        assert verifier.dispatch_id != repair.dispatch_id
        await system.service.process(card)
        assert len(system.calls) == 1
    else:
        assert not system.calls and job['diagnostic'] == 'repair_workflow_pending'

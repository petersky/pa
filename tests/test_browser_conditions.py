import asyncio
from unittest.mock import AsyncMock, Mock, patch

import pytest

from pa.browser.cdp import CdpError
from tests.browser_conditions import wait_for_read_condition


@pytest.mark.asyncio
async def test_navigation_read_retries_until_the_original_predicate_is_true():
    page = Mock(evaluate=AsyncMock(side_effect=[
        CdpError("Inspected target navigated or closed"), False, True]))
    with patch('tests.browser_conditions.asyncio.sleep', new_callable=AsyncMock) as sleep:
        await wait_for_read_condition(page, "location.pathname === '/login'")
    assert page.evaluate.await_count == 3
    assert all(call.args == ("location.pathname === '/login'",) for call in page.evaluate.await_args_list)
    assert sleep.await_count == 2 and all(call.args == (.05,) for call in sleep.await_args_list)


@pytest.mark.asyncio
async def test_persistent_closed_target_fails_at_unchanged_fifteen_second_budget():
    page = Mock(evaluate=AsyncMock(side_effect=CdpError("Inspected target navigated or closed")))
    # Start at100, one failed read at100, and no read once the115deadline arrives.
    with patch('tests.browser_conditions._now', side_effect=[100, 100, 115]), \
         patch('tests.browser_conditions.asyncio.sleep', new_callable=AsyncMock):
        with pytest.raises(pytest.fail.Exception, match='navigation-context errors: 1'):
            await wait_for_read_condition(page, "location.pathname === '/login'")
    assert page.evaluate.await_count == 1


@pytest.mark.asyncio
async def test_false_predicate_still_fails_without_navigation_error():
    page = Mock(evaluate=AsyncMock(return_value=False))
    with patch('tests.browser_conditions._now', side_effect=[100, 100, 115]), \
         patch('tests.browser_conditions.asyncio.sleep', new_callable=AsyncMock):
        with pytest.raises(pytest.fail.Exception, match='navigation-context errors: 0'):
            await wait_for_read_condition(page, 'false')


@pytest.mark.asyncio
@pytest.mark.parametrize('error', [CdpError('JavaScript evaluation failed'), asyncio.CancelledError()])
async def test_other_errors_and_cancellation_are_not_replayed(error):
    page = Mock(evaluate=AsyncMock(side_effect=error))
    with patch('tests.browser_conditions.asyncio.sleep', new_callable=AsyncMock) as sleep:
        with pytest.raises(type(error)):
            await wait_for_read_condition(page, 'document.readyState === "complete"')
    assert page.evaluate.await_count == 1 and sleep.await_count == 0

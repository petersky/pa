"""Poll pure browser reads across navigation; never replay actions or inputs."""
import asyncio
import time

import pytest

from pa.browser.cdp import CdpError

_now = time.monotonic


async def wait_for_read_condition(page, expression):
    """Only for side-effect-free predicates in these disposable browser tests."""
    deadline = _now() + 15
    navigation_errors = 0
    while _now() < deadline:
        try:
            if await page.evaluate(expression):
                return
        except CdpError as exc:
            # The old document can disappear between a click and this read.
            # A permanently closed target still fails at the same deadline;
            # unrelated CDP/JS errors and cancellation propagate immediately.
            if str(exc) != "Inspected target navigated or closed":
                raise
            navigation_errors += 1
        await asyncio.sleep(.05)
    pytest.fail(
        "Browser condition did not become true: " + expression
        + f" (navigation-context errors: {navigation_errors})"
    )

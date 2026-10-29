"""Verification readiness uses unchanged lifecycle guards without PR proof copies."""
from datetime import UTC, datetime
from unittest.mock import patch

import pytest

from pa.pr_supervisor.models import PRWatch, PRWatchStatus
from tests.test_autonomous_verification import ordinary, system


@pytest.mark.asyncio
async def test_readiness_never_hydrates_unneeded_proofs(system):
    card, record = ordinary(system)
    store = system.ctx.services["pr_supervisor_store"]
    store.upsert_watch(PRWatch(repository="fixture/unrelated", pr_number=1, pr_url="https://example.invalid/1",
        originating_session_id="other", card_id="other", state={"proof": "x"*65536}))
    with patch.object(store, "_row_to_watch", side_effect=AssertionError("Full PR proof hydration is unnecessary")):
        result = await system.service.origin_readiness(record)
    assert result["ready"] and result["reason"] == "dispatch_completed"
    assert not system.calls  # No verifier admission, close, prompt or acceptance.


@pytest.mark.asyncio
@pytest.mark.parametrize("relation,status,realm,retired", [
    ("session", PRWatchStatus.ACTIVE, "default", False),
    ("card", PRWatchStatus.BLOCKED, "other", False),
    ("session", PRWatchStatus.ACTIVE, "other", True),
    ("card", PRWatchStatus.BLOCKED, "default", True),
    ("both", PRWatchStatus.MERGED, "other", True),
    ("unrelated", PRWatchStatus.ACTIVE, "default", False),
])
async def test_readiness_matches_full_native_watch_guards(system, relation, status, realm, retired):
    card, record = ordinary(system)
    store = system.ctx.services["pr_supervisor_store"]
    store.upsert_watch(PRWatch(repository="fixture/repo", pr_number=1, pr_url="https://example.invalid/1",
        originating_session_id=record.session_id if relation in {"session", "both"} else "other-session",
        card_id=card.id if relation in {"card", "both"} else "other-card", realm_id=realm,
        status=status, retired_at=datetime.now(UTC) if retired else None, state={"proof": "x"*65536}))
    # Feed the unchanged policy its original complete watch inputs, then compare
    # with the fresh link-only inputs. This also covers cross-realm retired
    # active watches, which the default live-watch query would omit.
    with patch.object(store, "lifecycle_watch_links", return_value=store.list_watches(include_retired=True)):
        original = await system.service.origin_readiness(record)
    result = await system.service.origin_readiness(record)
    assert result == original
    active = relation != "unrelated" and status in {PRWatchStatus.ACTIVE, PRWatchStatus.BLOCKED}
    assert result["ready"] is (not active)
    assert result["reason"] == ("actionable_pr_watch" if active else "dispatch_completed")
    assert not system.calls


@pytest.mark.asyncio
async def test_readiness_rechecks_watch_changes_without_caching(system):
    card, record = ordinary(system)
    store = system.ctx.services["pr_supervisor_store"]
    watch = store.upsert_watch(PRWatch(repository="fixture/repo", pr_number=1, pr_url="https://example.invalid/1",
        card_id=card.id, status=PRWatchStatus.ACTIVE))
    assert not (await system.service.origin_readiness(record))["ready"]
    watch.status = PRWatchStatus.MERGED
    watch.retired_at = datetime.now(UTC)
    store.upsert_watch(watch)
    assert (await system.service.origin_readiness(record))["ready"]
    assert not system.calls

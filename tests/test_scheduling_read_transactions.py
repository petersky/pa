"""Passive ledger readers never reserve the writer slot; claims still do."""
from concurrent.futures import ThreadPoolExecutor
import sqlite3
from types import SimpleNamespace
from unittest.mock import Mock, call, patch

import pytest

from pa.domain.models import CardEvent, EventType, SyncCommit
from pa.scheduling.events import _records, poll_events
from pa.scheduling.store import Ledger, Schedule


@pytest.fixture
def populated(tmp_path):
    ledger = Ledger(tmp_path / "schedule.db")
    record = ledger.create("owner", "create", Schedule(name="original", payload={"synthetic": True},
        trigger={"kind": "manual"}, destination={"kind": "inbox", "target": "owner"}), 0)
    ledger.fire("owner", record["id"], "fire", 1)
    return ledger, record["id"]


@pytest.mark.parametrize("reader", [
    lambda ledger, sid: ledger.get("owner", sid),
    lambda ledger, sid: ledger.list_page("owner"),
    lambda ledger, sid: ledger.pending(2),
    lambda ledger, sid: ledger.history("owner", sid),
    lambda ledger, sid: ledger.status("owner", sid),
    lambda ledger, sid: ledger.delivery_health("owner", 2),
    lambda ledger, sid: _records(ledger),
], ids=["get", "list_page", "pending", "history", "status", "delivery_health", "owners"])
def test_readers_observe_committed_state_while_writer_is_reserved(populated, reader):
    ledger, sid = populated
    expected = reader(ledger, sid)
    with ThreadPoolExecutor(max_workers=1) as executor:
        with ledger.connect() as writer:
            writer.execute("UPDATE schedules SET deleted=1 WHERE id=?", (sid,))
            # The concurrent transaction has reserved the writer slot and changed
            # uncommitted bytes. Readers must return the exact old committed data.
            future = executor.submit(reader, ledger, sid)
            try:
                assert future.result(timeout=2) == expected
            finally:
                writer.rollback()
    assert reader(ledger, sid) == expected


def test_read_connection_rejects_writes_and_next_read_is_fresh(populated):
    ledger, sid = populated
    with ledger.connect(read_only=True) as db:
        assert db.execute("SELECT deleted FROM schedules WHERE id=?", (sid,)).fetchone()[0] == 0
        with pytest.raises(sqlite3.OperationalError, match="readonly"):
            db.execute("UPDATE schedules SET deleted=1 WHERE id=?", (sid,))
    with ledger.connect() as db:
        db.execute("UPDATE schedules SET deleted=1 WHERE id=?", (sid,))
    assert ledger.list_page("owner")["schedules"] == []
    with pytest.raises(KeyError): ledger.get("owner", sid)


def test_claim_rechecks_state_after_passive_pending_read(populated):
    ledger, sid = populated
    pending = ledger.pending(2)
    assert len(pending) == 1
    with ledger.connect() as db:
        db.execute("UPDATE schedules SET cancelled=1 WHERE id=?", (sid,))
    assert ledger.claim(pending[0]["id"], 2) is None
    assert ledger.pending(2) == []


def test_failed_mutation_rolls_back_and_does_not_leak_readonly_mode(populated):
    ledger, sid = populated
    with ledger.connect(read_only=True) as db:
        assert db.execute("PRAGMA query_only").fetchone()[0] == 1
    with pytest.raises(RuntimeError):
        with ledger.connect() as db:
            assert db.execute("PRAGMA query_only").fetchone()[0] == 0
            db.execute("UPDATE schedules SET deleted=1 WHERE id=?", (sid,))
            raise RuntimeError("synthetic rollback")
    assert ledger.get("owner", sid)["name"] == "original"


def event_source(tmp_path):
    ledger = Ledger(tmp_path / "events.db")
    ledger.create("owner", "create", Schedule(name="events", payload={"synthetic": True},
        trigger={"kind": "event", "event_type": EventType.CARD_CREATED.value},
        destination={"kind": "inbox", "target": "owner"}), 0)
    log = Mock(); log.get_head.return_value = "baseline"
    ctx = SimpleNamespace(settings=SimpleNamespace(subscribed_realms=["default"], data_dir=tmp_path),
        services={"event_log": log})
    poll_events(ctx, ledger)
    log.get_head.return_value = "new"
    log.get_commit.return_value = SyncCommit(hash="new", realm_id="default", instance_id="fixture",
        parent_hashes=["baseline"], event_hashes=["one", "two"])
    first, second = [CardEvent(id=name, type=EventType.CARD_CREATED, realm_id="default", card_id=name,
        author_principal="owner", author_instance="fixture") for name in ("first", "second")]
    return ledger, log, ctx, first, second


def test_event_poll_rechecks_user_and_realm_authorization_per_event(tmp_path):
    ledger, log, ctx, first, second = event_source(tmp_path)
    membership = Mock(); membership.has_role.side_effect = [True, False]
    ctx.services["membership"] = membership
    log.get_event.side_effect = [first, second]
    with patch("pa.auth.users.UserDirectory", side_effect=lambda *_:
            SimpleNamespace(get=lambda _: SimpleNamespace(role="editor"))) as users:
        poll_events(ctx, ledger)
    assert users.call_count == 2 and membership.has_role.call_count == 2
    pending = ledger.pending(2**40)
    assert [row["identity"] for row in pending] == ["event:first"]


def test_event_error_retains_prior_durable_occurrence_and_replays_without_duplicate(tmp_path):
    ledger, log, ctx, first, second = event_source(tmp_path)
    log.get_event.side_effect = [first, None]
    with patch("pa.auth.users.UserDirectory", side_effect=lambda *_:
            SimpleNamespace(get=lambda _: SimpleNamespace(role="admin"))):
        with pytest.raises(ValueError, match="scheduler_event_unavailable"):
            poll_events(ctx, ledger)
        assert [row["identity"] for row in ledger.pending(2**40)] == ["event:first"]
        with ledger.connect(read_only=True) as db:
            assert db.execute("SELECT processed FROM source_commits WHERE hash='new'").fetchone()[0] == 0
        log.get_event.side_effect = lambda key: first if key == "one" else second
        poll_events(ctx, Ledger(ledger.path))
    assert {row["identity"] for row in ledger.pending(2**40)} == {"event:first", "event:second"}
    assert log.get_event.call_args_list == [call("one"), call("two"), call("one"), call("two")]
    with ledger.connect(read_only=True) as db:
        assert db.execute("SELECT processed FROM source_commits WHERE hash='new'").fetchone()[0] == 1

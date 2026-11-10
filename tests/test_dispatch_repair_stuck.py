"""`pa dispatch repair-stuck` closes stranded dispatch records, dry-run first."""
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

from pa.cli.main import app
from pa.config import Settings
from pa.domain.models import AgentSession, CardCreate, CardLane
from pa.domain.projection import CardProjection
from pa.execution.dispatch import DispatchEvent, DispatchRecord, DispatchStore
from pa.execution.dispatch_repair import _classify, repair_stuck_dispatches

LOCAL = "instance-local"
NOW = datetime.now(UTC)


def record(name, *, state="running", days=0, target=LOCAL, card_id=None, session_id=None, **extra):
    stamp = NOW - timedelta(days=days)
    return DispatchRecord(
        dispatch_id=name, mutation_id=f"m-{name}", authority_instance_id=LOCAL,
        authority_url="http://local.invalid", target_instance_id=target, state=state,
        card_id=card_id, session_id=session_id, created_at=stamp, updated_at=stamp,
        events=[DispatchEvent(seq=1, state=state, message="x", created_at=stamp)], **extra,
    )


@pytest.fixture
def stuck(tmp_path):
    settings = Settings(data_dir=tmp_path / "data", instance_id=LOCAL)
    settings.data_dir.mkdir()
    store = CardProjection(settings.data_dir / "pa.db")

    def card(title, lane):
        return store.create_card(CardCreate(title=title, lane=lane), via_log=False).id

    done = [card(f"done-{i}", CardLane.DONE) for i in range(3)]
    active = card("active", CardLane.ACTIVE)
    store.save_session(AgentSession(id="closed-session", agent_name="codex", status="closed"))
    store.save_session(AgentSession(id="live-session", agent_name="codex", status="idle"))
    ledger = DispatchStore(settings.data_dir)
    for index, days in enumerate((26, 50, 80)):
        ledger.put(record(
            f"local-prompted-{index}", days=days, card_id=done[index], session_id=f"s{index}",
            completion_payload={"stop_reason": "end_turn"}, reconciliation_state="prompted",
            capacity_reserved_at=NOW - timedelta(days=days),
        ))
    ledger.put(record("legacy-acknowledged", state="acknowledged", days=120, acknowledged_at=NOW - timedelta(days=120)))
    ledger.put(record("remote-96", days=96, target="peer", card_id=active, session_id="remote-session"))
    # Healthy / out of scope:
    ledger.put(record("remote-recent", days=2, target="peer", card_id=active))
    ledger.put(record("closed-session-work", days=3, card_id=active, session_id="closed-session"))
    ledger.put(record("live-work", days=3, card_id=active, session_id="live-session"))
    ledger.put(record("already-failed", state="failed", days=200))
    ledger.close()
    return SimpleNamespace(settings=settings, store=store)


def ledger_of(stuck, **kw):
    return DispatchStore(stuck.settings.data_dir, **kw)


def states(stuck):
    ledger = ledger_of(stuck, read_only=True)
    try:
        return {r.dispatch_id: r.state for r in ledger.list(limit=1000)}
    finally:
        ledger.close()


def test_dry_run_reports_the_zombies_and_changes_nothing(stuck):
    before = states(stuck)
    ledger = ledger_of(stuck, read_only=True)
    report = repair_stuck_dispatches(ledger, stuck.store, instance_id=LOCAL, stale_days=30)
    ledger.close()
    planned = {a["dispatch_id"]: a["action"] for a in report["actions"]}
    assert planned == {
        "local-prompted-0": "retire_card_done", "local-prompted-1": "retire_card_done",
        "local-prompted-2": "retire_card_done", "legacy-acknowledged": "normalize_acknowledged",
        "remote-96": "fail_unreachable", "closed-session-work": "retire_dead_session",
    }
    assert report["applied"] is False and not any(a["applied"] for a in report["actions"])
    assert states(stuck) == before


def test_apply_closes_each_through_the_ledger_without_inferring_an_outcome(stuck):
    ledger = ledger_of(stuck)
    report = repair_stuck_dispatches(ledger, stuck.store, instance_id=LOCAL, apply=True)
    assert all(a["applied"] for a in report["actions"])
    for index in range(3):
        retired = ledger.get(f"local-prompted-{index}")
        assert retired.state == "failed" and retired.recoverable is False  # completion evidence kept
        assert retired.completion_payload == {"stop_reason": "end_turn"}
        assert retired.reconciliation_state == "failed"
        audit = retired.lifecycle_inconsistencies[-1]
        assert audit["kind"] == "stranded_dispatch_repaired" and audit["outcome_inferred"] is False
    assert ledger.get("legacy-acknowledged").state == "completed"
    remote = ledger.get("remote-96")
    assert remote.state == "failed" and remote.recoverable is True and remote.retryable
    assert remote.error_code == "remote_dispatch_unreachable"
    assert ledger.get("closed-session-work").state == "failed"
    # Untouched:
    assert ledger.get("remote-recent").state == "running"
    assert ledger.get("live-work").state == "running"
    # Idempotent: a second pass finds nothing left to do.
    again = repair_stuck_dispatches(ledger, stuck.store, instance_id=LOCAL, apply=True)
    assert again["actions"] == []
    ledger.close()


def test_card_done_without_completion_evidence_is_cancelled(stuck):
    ledger = ledger_of(stuck)
    done_card = next(c for c in stuck.store.list_card_lanes().items() if c[1] == "done")[0]
    ledger.put(record("bare", days=1, card_id=done_card))
    repair_stuck_dispatches(ledger, stuck.store, instance_id=LOCAL, apply=True)
    assert ledger.get("bare").state == "cancelled"
    ledger.close()


def test_governed_dispatches_are_reported_not_closed(stuck):
    ledger = ledger_of(stuck)
    done_card = next(c for c in stuck.store.list_card_lanes().items() if c[1] == "done")[0]
    governed = record("governed", days=1, card_id=done_card)
    governed.goal_provenance = SimpleNamespace()  # only truthiness matters to the classifier
    verdict = _classify(
        governed, lanes=stuck.store.list_card_lanes(), session_status=lambda _s: None,
        instance_id=LOCAL, stale_before=NOW,
    )
    assert verdict[0] == "needs_operator"
    ledger.close()


def test_cli_is_dry_run_unless_apply_is_given(stuck, monkeypatch):
    monkeypatch.setattr("pa.cli.dispatch_repair.get_settings", lambda: stuck.settings)
    monkeypatch.setattr("pa.domain.store.get_store", lambda settings=None: stuck.store)
    before = states(stuck)
    result = CliRunner().invoke(app, ["dispatch", "repair-stuck"])
    assert result.exit_code == 0, result.output
    assert "Dry run" in result.output and "--apply" in result.output
    assert "retire_card_done" in result.output and "dispatch=remote-96" in result.output
    assert states(stuck) == before
    applied = CliRunner().invoke(app, ["dispatch", "repair-stuck", "--apply"])
    assert applied.exit_code == 0, applied.output
    assert "Applied" in applied.output
    assert states(stuck)["legacy-acknowledged"] == "completed"
    assert states(stuck)["remote-96"] == "failed"


def test_apply_refuses_while_the_server_owns_the_data_dir(stuck, monkeypatch):
    from pa.core.writer_lock import DataDirWriterLock

    monkeypatch.setattr("pa.cli.dispatch_repair.get_settings", lambda: stuck.settings)
    monkeypatch.setattr("pa.domain.store.get_store", lambda settings=None: stuck.store)
    server = DataDirWriterLock(stuck.settings.data_dir)
    server.acquire()
    try:
        before = states(stuck)
        refused = CliRunner().invoke(app, ["dispatch", "repair-stuck", "--apply"])
        assert refused.exit_code == 1 and "Stop the PA server" in refused.output
        assert states(stuck) == before
        dry = CliRunner().invoke(app, ["dispatch", "repair-stuck"])
        assert dry.exit_code == 0 and "Dry run" in dry.output
    finally:
        server.release()

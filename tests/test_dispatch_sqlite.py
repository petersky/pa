from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from pa.acp.mcp_config import probe_pa_mcp_stdio
from pa.config import Settings
from pa.core.kernel import Kernel
from pa.core.writer_lock import DataDirWriterLock
from pa.execution.dispatch import (
    DispatchReceiptConflict,
    DispatchRecord,
    DispatchStore,
    DispatchStoreReadOnlyError,
)
from pa.execution.post_turn import (
    FollowupActionName,
    FollowupActionV1,
    PostTurnDecision,
    PostTurnEvaluationV1,
    SafetyClassification,
)
from pa.execution.progress import (
    CompletionReportV1,
    DispatchProgressEventV1,
    DispatchProgressHeartbeatV1,
    OperatorInputRequestV1,
    ProgressKind,
    ProgressPhase,
)

AUTHORITY = "authority-1"
TARGET = "target-1"


def _record(index: int = 0, **updates) -> DispatchRecord:
    values = {
        "dispatch_id": f"dispatch-{index}",
        "mutation_id": f"mutation-{index}",
        "idempotency_key": f"admission-{index}",
        "card_id": f"card-{index}",
        "project_id": f"project-{index % 5}",
        "session_id": f"session-{index}",
        "authority_instance_id": AUTHORITY,
        "authority_url": "https://authority.example",
        "target_instance_id": TARGET,
        "state": "running",
        "progress_protocol_version": 1,
    }
    values.update(updates)
    return DispatchRecord(**values)


def _event(
    record: DispatchRecord,
    sequence: int,
    *,
    key: str | None = None,
    occurred_at: datetime | None = None,
    operator_input: OperatorInputRequestV1 | None = None,
    kind: ProgressKind = ProgressKind.CHECKPOINT,
) -> DispatchProgressEventV1:
    return DispatchProgressEventV1(
        kind=kind,
        card_id=record.card_id,
        dispatch_id=record.dispatch_id,
        acp_session_id=record.session_id or "",
        originating_instance_id=record.target_instance_id,
        authority_instance_id=record.authority_instance_id,
        sequence=sequence,
        idempotency_key=key or f"event-{record.dispatch_id}-{sequence}",
        occurred_at=occurred_at or datetime.now(UTC),
        phase=ProgressPhase.IMPLEMENTING,
        summary=f"checkpoint {sequence}",
        operator_input=operator_input,
    )


def _heartbeat(record: DispatchRecord, sequence: int) -> DispatchProgressHeartbeatV1:
    return DispatchProgressHeartbeatV1(
        card_id=record.card_id,
        dispatch_id=record.dispatch_id,
        acp_session_id=record.session_id or "",
        originating_instance_id=record.target_instance_id,
        authority_instance_id=record.authority_instance_id,
        sequence=sequence,
        idempotency_key=f"heartbeat-{record.dispatch_id}-{sequence}",
        phase=ProgressPhase.IMPLEMENTING,
        summary="still active",
    )


def _write_legacy(path: Path, records: list[DispatchRecord]) -> bytes:
    raw = json.dumps(
        {record.dispatch_id: record.model_dump(mode="json") for record in records},
        separators=(",", ":"),
    ).encode()
    path.write_bytes(raw)
    return raw


def _content_state(paths: list[Path]) -> dict[str, tuple[int, str]]:
    return {
        path.name: (
            path.stat().st_size,
            hashlib.sha256(path.read_bytes()).hexdigest(),
        )
        for path in paths
        if path.exists()
    }


def test_migration_reconciles_counts_keeps_backup_and_exports_all_receipts() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        first = _record(1)
        first.progress_events = [_event(first, 1), _event(first, 2)]
        first.progress_heartbeat = _heartbeat(first, 3)
        first.progress_seen_keys = [
            first.progress_events[0].idempotency_key,
            first.progress_events[1].idempotency_key,
            first.progress_heartbeat.idempotency_key,
            "coalesced-receipt",
        ]
        second = _record(2)
        second.progress_seen_keys = ["receipt-a", "receipt-b"]
        source = _write_legacy(root / "dispatch_mutations.json", [first, second])

        store = DispatchStore(root)
        metrics = store.storage_metrics()

        assert metrics["migration"]["state"] == "verified"
        assert metrics["migration"]["counts"] == {
            "dispatches": 2,
            "final_reports": 0,
            "heartbeats": 1,
            "progress_events": 2,
            "receipts": 6,
        }
        backup = root / "dispatch_mutations.json.pre-sqlite-backup"
        assert backup.read_bytes() == source
        assert (
            metrics["migration"]["source_sha256"] == hashlib.sha256(source).hexdigest()
        )

        exported = root / "rollback-export.json"
        evidence = store.export_legacy_json(exported)
        assert evidence["dispatches"] == 2
        assert evidence["progress_events"] == 2
        assert evidence["receipts"] == 6
        assert (
            len(
                json.loads(exported.read_text())[first.dispatch_id][
                    "progress_seen_keys"
                ]
            )
            == 4
        )


@pytest.mark.parametrize(
    "boundary",
    ["migration_backup_verified", "migration_before_commit", "migration_after_commit"],
)
def test_migration_is_resumable_at_every_boundary(boundary: str) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source = _write_legacy(root / "dispatch_mutations.json", [_record(1)])

        def fail(observed: str) -> None:
            if observed == boundary:
                raise RuntimeError(f"killed at {boundary}")

        with pytest.raises(RuntimeError, match=boundary):
            DispatchStore(root, fault_injector=fail)

        resumed = DispatchStore(root)
        assert resumed.get("dispatch-1") is not None
        assert resumed.storage_metrics()["migration"]["counts"]["dispatches"] == 1
        assert (
            root / "dispatch_mutations.json.pre-sqlite-backup"
        ).read_bytes() == source


def test_mixed_version_writer_is_fenced_and_source_remains_recoverable() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source_path = root / "dispatch_mutations.json"
        original = _write_legacy(source_path, [_record(1)])
        store = DispatchStore(root)
        store.close()

        source_path.write_text(json.dumps({"old-binary-write": {}}))
        with pytest.raises(RuntimeError, match="mixed-version"):
            DispatchStore(root)
        assert (
            root / "dispatch_mutations.json.pre-sqlite-backup"
        ).read_bytes() == original


def test_newer_sqlite_schema_is_fenced_from_unsafe_downgrade() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        store = DispatchStore(root)
        store.close()
        with sqlite3.connect(root / "dispatch_mutations.db") as conn:
            conn.execute(
                "UPDATE dispatch_meta SET value='999' WHERE key='schema_version'"
            )
        with pytest.raises(RuntimeError, match="unsafe downgrade"):
            DispatchStore(root)


@pytest.mark.parametrize(
    "boundary,committed", [("commit_before", False), ("commit_after", True)]
)
def test_admission_fault_never_publishes_a_ghost_and_replays_canonical_state(
    boundary: str, committed: bool
) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        store = DispatchStore(root)
        record = _record(1, state="queued", request_fingerprint="request-1")

        def fail(observed: str) -> None:
            if observed == boundary:
                raise RuntimeError(f"killed at {boundary}")

        store._fault_injector = fail
        with pytest.raises(RuntimeError, match=boundary):
            store.admit(record)

        visible = store.get(record.dispatch_id)
        assert (visible is not None) is committed
        assert bool(store.latest_by_card({record.card_id or ""})) is committed
        assert (
            bool(
                store.latest_by_session(
                    {record.session_id or ""}, realm_id=record.realm_id
                )
            )
            is committed
        )
        assert store.history_counts({record.card_id or ""}, realm_id=record.realm_id)[
            record.card_id or ""
        ] == int(committed)
        assert store.capacity_snapshot(TARGET)["dispatch_reservations"] == int(
            committed
        )
        # The caller's object is not a hidden pre-commit cache entry either.
        assert record.events == []

        store._fault_injector = None
        canonical, duplicate = store.admit(record)
        assert duplicate is committed
        assert canonical == store.get(record.dispatch_id)
        store.close()

        resumed = DispatchStore(root)
        replayed, duplicate = resumed.admit(record)
        assert duplicate is True
        assert replayed == canonical
        assert resumed.storage_metrics()["rows"]["dispatches"] == 1


def test_failed_put_keeps_all_indexes_on_the_committed_record() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        store = DispatchStore(root)
        original = store.put(_record(1, state="failed"))
        candidate = original.model_copy(deep=True)
        candidate.card_id = "replacement-card"
        candidate.session_id = "replacement-session"
        candidate.state = "queued"

        def fail(observed: str) -> None:
            if observed == "commit_before":
                raise RuntimeError("killed before put commit")

        store._fault_injector = fail
        with pytest.raises(RuntimeError, match="before put commit"):
            store.put(candidate)

        assert store.get(original.dispatch_id) == original
        assert (
            store.latest_by_card({original.card_id or ""})[original.card_id or ""]
            == original
        )
        assert store.latest_by_card({"replacement-card"}) == {}
        assert store.by_session(original.session_id or "") == original
        assert store.by_session("replacement-session") is None
        store._fault_injector = None
        store.close()

        resumed = DispatchStore(root)
        assert resumed.get(original.dispatch_id).model_dump(
            mode="json"
        ) == original.model_dump(mode="json")
        assert resumed.latest_by_card({"replacement-card"}) == {}


def test_read_only_auxiliary_kernel_boot_does_not_touch_or_wait_on_writer_wal() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        writer_lock = DataDirWriterLock(root)
        writer_lock.acquire()
        writer = DispatchStore(root)
        writer.put(_record(1))
        paths = [
            writer.db_path,
            Path(str(writer.db_path) + "-wal"),
            Path(str(writer.db_path) + "-shm"),
        ]

        writer._conn.execute("BEGIN IMMEDIATE")
        before = _content_state(paths)
        try:
            facade_started = time.perf_counter()
            facade = DispatchStore(root, read_only=True)
            facade_elapsed = time.perf_counter() - facade_started
            assert facade.get("dispatch-1") is not None
            facade.close()
            assert facade_elapsed < 0.5

            started = time.perf_counter()
            kernel = Kernel.boot(
                settings=Settings(
                    data_dir=root,
                    instance_id="auxiliary",
                    instance_name="Auxiliary",
                    instance_url="http://auxiliary.test:8080",
                    agent_enabled=False,
                    subscribed_realms=["default"],
                    peers=[],
                )
            )
            elapsed = time.perf_counter() - started
            auxiliary = kernel.ctx.require_service("dispatch_store")
            assert auxiliary.read_only is True
            assert auxiliary.deferred_read_only is True
            assert auxiliary._conn is None
            assert auxiliary.storage_metrics()["mode"] == "deferred_read_only"
            assert auxiliary.storage_metrics()["rows"]["available"] is False
            with pytest.raises(DispatchStoreReadOnlyError, match="running PA server API"):
                auxiliary.get("dispatch-1")
            with pytest.raises(DispatchStoreReadOnlyError):
                auxiliary.put(_record(2))
            with pytest.raises(DispatchStoreReadOnlyError):
                auxiliary.checkpoint()
            with pytest.raises(DispatchStoreReadOnlyError):
                auxiliary.compact()
            auxiliary.close()
            # A mistaken writer facade waits for SQLite's 30-second busy
            # timeout here. Module discovery itself is comparatively expensive,
            # so the direct facade assertion above carries the tight p99 bound.
            assert elapsed < 25.0
            assert _content_state(paths) == before
        finally:
            writer._conn.rollback()
            writer.close()
            writer_lock.release()


def test_auxiliary_commands_never_open_or_touch_live_dispatch_wal() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        writer_lock = DataDirWriterLock(root)
        writer_lock.acquire()
        writer = DispatchStore(root)
        writer.put(_record(1))
        paths = [
            writer.db_path,
            Path(str(writer.db_path) + "-wal"),
            Path(str(writer.db_path) + "-shm"),
        ]
        environment = {
            **os.environ,
            "PA_DATA_DIR": str(root),
            "PA_INSTANCE_ID": "auxiliary-subprocess",
            "PA_INSTANCE_NAME": "Auxiliary subprocess",
            "PA_INSTANCE_URL": "http://127.0.0.1:1",
            "PA_AGENT_ENABLED": "false",
            "PA_PEERS": "[]",
        }
        settings = Settings(
            data_dir=root,
            instance_id="auxiliary-subprocess",
            instance_name="Auxiliary subprocess",
            instance_url="http://127.0.0.1:1",
            agent_enabled=False,
            peers=[],
        )

        writer._conn.execute("BEGIN IMMEDIATE")
        try:
            for command in (
                ("status",),
                ("plugins", "list"),
                ("doctor", "--json"),
            ):
                before = _content_state(paths)
                completed = subprocess.run(
                    [sys.executable, "-m", "pa", *command],
                    env=environment,
                    capture_output=True,
                    text=True,
                    timeout=60,
                    check=False,
                )
                if command[0] != "doctor":
                    assert completed.returncode == 0, completed.stderr
                assert _content_state(paths) == before, command

            before = _content_state(paths)
            handshake = probe_pa_mcp_stdio(
                settings,
                timeout=45.0,
                owner_environment=environment,
            )
            assert handshake["state"] == "connected"
            assert handshake["tool_count"] > 0
            assert _content_state(paths) == before
        finally:
            writer._conn.rollback()
            writer.close()
            writer_lock.release()


def test_read_only_legacy_facade_never_creates_sqlite_or_migration_artifacts() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        record = _record(1)
        source = _write_legacy(root / "dispatch_mutations.json", [record])

        store = DispatchStore(root, read_only=True)
        assert store.get(record.dispatch_id) == record
        store.close()

        assert (root / "dispatch_mutations.json").read_bytes() == source
        assert not (root / "dispatch_mutations.db").exists()
        assert not (root / "dispatch_mutations.json.pre-sqlite-backup").exists()


def test_read_only_facade_promotes_only_after_writer_ownership_and_migrates() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        record = _record(1)
        source = _write_legacy(root / "dispatch_mutations.json", [record])
        store = DispatchStore(root, read_only=True, deferred_read_only=True)
        assert store.read_only is True
        assert store.deferred_read_only is True
        assert store._conn is None
        assert not store.db_path.exists()

        writer_lock = DataDirWriterLock(root)
        writer_lock.acquire()
        try:
            store.promote_writer()
            assert store.read_only is False
            assert store.deferred_read_only is False
            assert store.get(record.dispatch_id).model_dump(
                mode="json"
            ) == record.model_dump(mode="json")
            assert store.db_path.exists()
            assert store.backup_path.read_bytes() == source
            store.put(_record(2))
            assert store.get("dispatch-2") is not None
        finally:
            store.close()
            writer_lock.release()


def test_failed_deferred_promotion_rolls_back_mode_and_can_retry() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        record = _record(1)
        _write_legacy(root / "dispatch_mutations.json", [record])
        store = DispatchStore(root, read_only=True, deferred_read_only=True)

        def fail(boundary: str) -> None:
            if boundary == "migration_before_commit":
                raise RuntimeError("promotion interrupted")

        writer_lock = DataDirWriterLock(root)
        writer_lock.acquire()
        try:
            store._fault_injector = fail
            with pytest.raises(RuntimeError, match="promotion interrupted"):
                store.promote_writer()
            assert store.read_only is True
            assert store.deferred_read_only is True
            assert store._conn is None
            with pytest.raises(DispatchStoreReadOnlyError, match="running PA server API"):
                store.get(record.dispatch_id)

            store._fault_injector = None
            store.promote_writer()
            assert store.read_only is False
            assert store.deferred_read_only is False
            assert store.get(record.dispatch_id) is not None
        finally:
            store.close()
            writer_lock.release()


@pytest.mark.parametrize(
    "boundary,committed", [("commit_before", False), ("commit_after", True)]
)
def test_commit_kill_never_false_acknowledges_or_partially_persists(
    boundary: str, committed: bool
) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        store = DispatchStore(root)
        record = _record(1)
        store.put(record)

        def fail(observed: str) -> None:
            if observed == boundary:
                raise RuntimeError(f"killed at {boundary}")

        store._fault_injector = fail
        event = _event(record, 1)
        with pytest.raises(RuntimeError, match=boundary):
            store.ingest_progress(event)
        store._conn.close()

        resumed = DispatchStore(root)
        persisted = resumed.get(record.dispatch_id)
        assert persisted is not None
        assert len(persisted.progress_events) == int(committed)
        result = resumed.ingest_progress(event)
        assert result.status == ("duplicate" if committed else "accepted")
        assert len(resumed.get(record.dispatch_id).progress_events) == 1


def test_disk_full_rolls_back_receipt_event_and_watermark() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        store = DispatchStore(root)
        record = _record(1)
        store.put(record)

        def disk_full(boundary: str) -> None:
            if boundary == "commit_before":
                raise sqlite3.OperationalError("database or disk is full")

        store._fault_injector = disk_full
        with pytest.raises(sqlite3.OperationalError, match="disk is full"):
            store.ingest_progress(_event(record, 1))
        store._conn.close()

        resumed = DispatchStore(root)
        persisted = resumed.get(record.dispatch_id)
        assert persisted is not None
        assert persisted.progress_events == []
        assert persisted.progress_next_sequence == 1
        assert resumed.storage_metrics()["rows"]["receipts"] == 0


def test_duplicate_conflict_and_late_semantics_survive_restart() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        store = DispatchStore(root)
        record = _record(1)
        store.put(record)
        third = _event(record, 3)
        first = _event(record, 1)
        assert store.ingest_progress(third).status == "accepted"
        assert store.ingest_progress(first).status == "late"
        conflict = _event(record, 3, key="different-payload-same-sequence")
        assert store.ingest_progress(conflict).status == "conflict"

        resumed = DispatchStore(root)
        assert resumed.ingest_progress(third).status == "duplicate"
        assert resumed.ingest_progress(conflict).status == "duplicate"
        persisted = resumed.get(record.dispatch_id)
        assert [item.sequence for item in persisted.progress_events] == [1, 3]
        assert persisted.progress_conflicts == 1


@pytest.mark.parametrize("mutation_kind", ["progress", "final", "heartbeat"])
def test_exact_accepted_receipts_and_payload_conflicts_survive_restart(
    mutation_kind: str,
) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        store = DispatchStore(root)
        record = _record(1)
        store.put(record)
        if mutation_kind == "heartbeat":
            payload = _heartbeat(record, 1)
            ingest = store.ingest_heartbeat
        else:
            payload = _event(
                record,
                1,
                kind=(
                    ProgressKind.FINAL
                    if mutation_kind == "final"
                    else ProgressKind.CHECKPOINT
                ),
            )
            ingest = store.ingest_progress

        accepted = ingest(payload)
        assert accepted.accepted is True
        duplicate = ingest(payload)
        assert duplicate.status == "duplicate"
        assert duplicate.replay_of_status == accepted.status
        changed = payload.model_copy(deep=True)
        changed.summary = "same key, different canonical payload"
        with pytest.raises(DispatchReceiptConflict, match="different payload"):
            ingest(changed)
        store.close()

        resumed = DispatchStore(root)
        resumed_ingest = (
            resumed.ingest_heartbeat
            if mutation_kind == "heartbeat"
            else resumed.ingest_progress
        )
        replay = resumed_ingest(payload)
        assert replay.status == "duplicate"
        assert replay.replay_of_status == accepted.status
        with pytest.raises(DispatchReceiptConflict, match="different payload"):
            resumed_ingest(changed)


def test_final_evidence_is_never_coalesced_with_a_matching_checkpoint() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        store = DispatchStore(Path(tmp))
        record = _record(1)
        store.put(record)
        checkpoint = _event(record, 1)
        final = _event(record, 2, kind=ProgressKind.FINAL)
        final.summary = checkpoint.summary
        final.occurred_at = checkpoint.occurred_at + timedelta(seconds=1)

        assert store.ingest_progress(checkpoint).status == "accepted"
        result = store.ingest_progress(final)
        assert result.status == "accepted"
        assert [
            event.kind for event in store.get(record.dispatch_id).progress_events
        ] == [
            ProgressKind.CHECKPOINT,
            ProgressKind.FINAL,
        ]


def test_exact_rejected_receipt_and_payload_conflict_survive_restart() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        store = DispatchStore(root)
        record = _record(1)
        store.put(record)
        accepted = _event(record, 1)
        rejected = _event(record, 1, key="conflicting-sequence-receipt")
        assert store.ingest_progress(accepted).accepted is True

        result = store.ingest_progress(rejected)
        assert result.accepted is False
        assert result.status == "conflict"
        assert store.ingest_progress(rejected) == result
        changed = rejected.model_copy(deep=True)
        changed.summary = "changed rejected payload"
        with pytest.raises(DispatchReceiptConflict, match="different payload"):
            store.ingest_progress(changed)
        store.close()

        resumed = DispatchStore(root)
        assert resumed.ingest_progress(rejected) == result
        with pytest.raises(DispatchReceiptConflict, match="different payload"):
            resumed.ingest_progress(changed)


def test_control_receipt_replays_exact_result_and_fences_changed_parameters() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        store = DispatchStore(root)
        record = store.put(
            _record(
                1,
                state="waiting_capacity",
                queue_admitted_at=datetime.now(UTC),
            )
        )
        key = "reprioritize-receipt"
        canonical = store.reprioritize(
            record,
            priority=5,
            principal_id="user:operator",
            idempotency_key=key,
        )
        assert (
            store.reprioritize(
                record,
                priority=5,
                principal_id="user:operator",
                idempotency_key=key,
            )
            == canonical
        )
        with pytest.raises(DispatchReceiptConflict, match="different parameters"):
            store.reprioritize(
                record,
                priority=4,
                principal_id="user:operator",
                idempotency_key=key,
            )
        store.close()

        resumed = DispatchStore(root)
        assert (
            resumed.reprioritize(
                record,
                priority=5,
                principal_id="user:operator",
                idempotency_key=key,
            )
            == canonical
        )
        with pytest.raises(DispatchReceiptConflict, match="different parameters"):
            resumed.reprioritize(
                record,
                priority=5,
                principal_id="user:different",
                idempotency_key=key,
            )


@pytest.mark.parametrize(
    "boundary,committed", [("commit_before", False), ("commit_after", True)]
)
def test_control_receipt_fault_boundary_replays_only_committed_result(
    boundary: str, committed: bool
) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        store = DispatchStore(root)
        record = store.put(
            _record(
                1,
                state="waiting_capacity",
                queue_admitted_at=datetime.now(UTC),
            )
        )
        key = "faulted-control-receipt"

        def fail(observed: str) -> None:
            if observed == boundary:
                raise RuntimeError(f"killed at {boundary}")

        store._fault_injector = fail
        with pytest.raises(RuntimeError, match=boundary):
            store.reprioritize(
                record,
                priority=5,
                principal_id="user:operator",
                idempotency_key=key,
            )
        assert store.get(record.dispatch_id).requested_priority == (
            5 if committed else 0
        )

        store._fault_injector = None
        canonical = store.reprioritize(
            record,
            priority=5,
            principal_id="user:operator",
            idempotency_key=key,
        )
        assert canonical.requested_priority == 5
        store.close()

        resumed = DispatchStore(root)
        assert (
            resumed.reprioritize(
                record,
                priority=5,
                principal_id="user:operator",
                idempotency_key=key,
            )
            == canonical
        )


def test_one_heartbeat_is_delta_only_and_never_rewrites_legacy_or_history() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        legacy_record = _record(1)
        legacy_record.progress_events = [
            _event(legacy_record, index) for index in range(1, 51)
        ]
        source = _write_legacy(root / "dispatch_mutations.json", [legacy_record])
        store = DispatchStore(root)
        before = (root / "dispatch_mutations.json").stat().st_mtime_ns
        traced: list[str] = []
        store._conn.set_trace_callback(traced.append)
        store._core_payload = MagicMock(wraps=store._core_payload)

        store.ingest_heartbeat(_heartbeat(legacy_record, 51))

        assert (root / "dispatch_mutations.json").read_bytes() == source
        assert (root / "dispatch_mutations.json").stat().st_mtime_ns == before
        assert store._core_payload.call_count == 1
        mutations = [
            statement.upper()
            for statement in traced
            if statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
        ]
        assert len(mutations) == 3
        assert sum("DISPATCH_HEARTBEATS" in statement for statement in mutations) == 1
        assert not any(
            "DELETE FROM DISPATCH_PROGRESS_EVENTS" in statement
            for statement in mutations
        )


def test_concurrent_readers_writers_checkpoint_and_shutdown_drain() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        store = DispatchStore(root)
        records = [_record(index) for index in range(20)]
        for record in records:
            store.put(record)
        read_latencies: list[float] = []
        stop = threading.Event()

        def reader() -> None:
            while not stop.is_set():
                started = time.perf_counter()
                store.by_session(records[0].session_id or "")
                store.latest_by_card({record.card_id or "" for record in records})
                read_latencies.append((time.perf_counter() - started) * 1000)

        thread = threading.Thread(target=reader)
        thread.start()
        try:
            with ThreadPoolExecutor(max_workers=8) as pool:
                futures = []
                for index in range(200):
                    record = records[index % len(records)]
                    futures.append(
                        pool.submit(
                            store.ingest_heartbeat, _heartbeat(record, index + 1)
                        )
                    )
                for future in futures:
                    future.result()
            checkpoint = store.checkpoint()
        finally:
            stop.set()
            thread.join(timeout=5)
        assert checkpoint["busy"] == 0
        assert read_latencies
        assert statistics.quantiles(read_latencies, n=100)[98] < 25
        store.close()

        resumed = DispatchStore(root)
        assert resumed.storage_metrics()["rows"]["heartbeats"] == len(records)


def test_corrupt_database_fails_closed_without_touching_legacy_source() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        source = _write_legacy(root / "dispatch_mutations.json", [_record(1)])
        (root / "dispatch_mutations.db").write_bytes(b"not a sqlite database")
        with pytest.raises(sqlite3.DatabaseError):
            DispatchStore(root)
        assert (root / "dispatch_mutations.json").read_bytes() == source


def test_slow_commit_is_not_acknowledged_until_after_boundary() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        store = DispatchStore(Path(tmp))
        record = _record(1)
        store.put(record)
        reached = threading.Event()

        def slow_commit(boundary: str) -> None:
            if boundary == "commit_before":
                reached.set()
                time.sleep(0.03)

        store._fault_injector = slow_commit
        started = time.perf_counter()
        result = store.ingest_heartbeat(_heartbeat(record, 1))
        elapsed = time.perf_counter() - started
        assert reached.is_set()
        assert result.accepted
        assert elapsed >= 0.03


def test_retention_keeps_operator_final_and_active_dispatch_evidence() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        store = DispatchStore(Path(tmp))
        record = _record(1)
        store.put(record)
        old = datetime.now(UTC) - timedelta(days=60)
        store.ingest_progress(_event(record, 1, occurred_at=old))
        request = OperatorInputRequestV1(prompt="Choose a safe recovery action")
        store.ingest_progress(
            _event(record, 2, occurred_at=old, operator_input=request)
        )
        store.ingest_progress(
            _event(record, 3, occurred_at=old, kind=ProgressKind.FINAL)
        )

        assert store.compact(now=datetime.now(UTC)) == {"events": 0, "receipts": 0}
        persisted = store.get(record.dispatch_id)
        assert [item.sequence for item in persisted.progress_events] == [2, 3]
        assert persisted.progress_compacted_ranges == [[1, 1]]

        for event in persisted.progress_events:
            store.mark_progress_delivered(record.dispatch_id, event.idempotency_key)
        persisted.state = "completed"
        persisted.acknowledged_at = datetime.now(UTC)
        persisted.final_report = CompletionReportV1(outcome="Completed safely")
        persisted.post_turn_evaluations = [
            PostTurnEvaluationV1(
                snapshot_id="snapshot-1",
                context_digest="a" * 64,
                decision=PostTurnDecision.OUTCOME_ACHIEVED,
                rationale="The final report and retained evidence establish completion.",
                confidence=1.0,
                recommended_actions=[
                    FollowupActionV1(
                        name=FollowupActionName.NO_ACTION,
                        parameters={"reason": "No follow-up is needed."},
                        idempotency_key_inputs=["dispatch-1"],
                        safety=SafetyClassification.RECORD_ONLY,
                        human_approval_required=False,
                    )
                ],
                operator_status_text="Completed safely.",
            )
        ]
        store.put(persisted)

        removed = store.compact(now=datetime.now(UTC) + timedelta(days=60))
        assert removed == {"events": 0, "receipts": 1}
        retained = store.get(record.dispatch_id).progress_events
        assert [item.sequence for item in retained] == [2, 3]


def test_reference_stress_200_dispatches_25000_receipts_50_writes_per_second() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp)
        records = [_record(index) for index in range(200)]
        for index in range(25_000):
            records[index % len(records)].progress_seen_keys.append(
                f"prior-receipt-{index}"
            )
        _write_legacy(root / "dispatch_mutations.json", records)
        store = DispatchStore(root)

        started = time.perf_counter()
        for index in range(500):
            record = records[index % len(records)]
            sequence = index + 1
            if index % 2:
                store.ingest_progress(
                    _event(record, sequence, key=f"stress-event-{index}")
                )
            else:
                store.ingest_heartbeat(_heartbeat(record, sequence))
        elapsed = time.perf_counter() - started
        metrics = store.storage_metrics()
        throughput = 500 / elapsed

        assert metrics["rows"]["dispatches"] == 200
        assert metrics["rows"]["receipts"] == 25_500
        assert throughput >= 50
        # Aggregate throughput is the service-level requirement. Keep a wider
        # p99 guard for gross regressions without coupling the benchmark to
        # shared CI runner disk jitter.
        assert metrics["writes"]["latency_ms"]["p99"] < 75
        assert (root / "dispatch_mutations.json").stat().st_size > 1_000_000

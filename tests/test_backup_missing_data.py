"""A historical absence cannot prevent future capture or masquerade as intact."""
import contextlib
import json
from pathlib import Path
import sqlite3
import tarfile
from unittest.mock import Mock, patch

import pytest

from pa.backup.models import BackupManifest, BackupRun
from pa.backup.service import BackupError, BackupService
from pa.domain.models import CardCreate, TranscriptEvent
from tests import test_backups as fixtures


@pytest.fixture
def case():
    value = fixtures.BackupTestCase()
    value.setUp()
    try:
        yield value
    finally:
        value.tearDown()


def missing_payload(case):
    event = TranscriptEvent(session_id="historical-gap", seq=1, event_type="prompt_rendered",
                            payload={"text": "isolated historical payload " * 300})
    case.store.append_transcript_events([event])
    digest = next(iter(case.store.transcripts.referenced_hashes()))
    path = case.store.transcripts._object_path(digest)
    encoded = path.read_bytes()
    path.unlink()  # Disposable source fixture; never a production recovery action.
    return digest, encoded


def rewrite_archive(case, backup_id, change):
    path = case.service._path_for_backup(backup_id)
    root = case.root / "edited-archive"
    root.mkdir()
    with tarfile.open(path, "r:*") as archive:
        archive.extractall(root, filter="data")
    manifest = json.loads((root / "manifest.json").read_text())
    change(root, manifest)
    (root / "manifest.json").write_text(json.dumps(manifest))
    with tarfile.open(path, "w:gz") as archive:
        for source in [root / "manifest.json", *sorted(root.rglob("*"))]:
            if source == root / "manifest.json" and archive.getmembers():
                continue
            archive.add(source, arcname=source.relative_to(root).as_posix(), recursive=False)


def test_limited_capture_preserves_new_data_refs_history_and_intact_points(case):
    good = case._run("intact-before-gap")
    last_success = case.service._state.last_success
    case.service.config.retention_count = 1
    digest, _ = missing_payload(case)
    current = case.store.create_card(CardCreate(title="new state after historical gap"))
    old_failure = BackupRun(trigger="manual", status="failed", verification="failed",
                            failure_reason="retained original failure", finished_at=last_success)
    case.service._state.runs.append(old_failure)
    case.service._state.consecutive_failures = 207
    limited = case.service.run_backup(idempotency_key="limited", preserve_existing=True)
    assert limited.status == "success" and limited.integrity_verified
    assert limited.data_complete is False and limited.verification == "failed"
    assert limited.missing_transcript_object_count == 1
    assert limited.pruned_backup_ids == []
    assert case.service._state.last_success == last_success
    assert case.service._state.last_completed >= last_success
    assert old_failure in case.service._state.runs and old_failure.status == "failed"
    assert case.store.transcripts.referenced_hashes() == {digest}
    assert not case.store.transcripts._object_path(digest).exists()
    checked = case.service.verify_backup(limited.backup_id)
    assert checked.integrity_verified and not checked.verified
    assert checked.verification_status == "verified_with_missing_data"
    assert checked.manifest.format_version == 2
    assert checked.manifest.missing_transcript_objects[0].sha256 == digest
    assert not checked.manifest.data_complete
    with tarfile.open(checked.path, "r:*") as archive:
        root = case.root / "captured"
        archive.extractall(root, filter="data")
    with contextlib.closing(sqlite3.connect(root / "transcripts.sqlite3")) as conn:
        assert conn.execute("SELECT cold_hash FROM transcript_events").fetchone()[0] == digest
        assert conn.execute("SELECT ref_count FROM transcript_objects WHERE hash=?", (digest,)).fetchone()[0] == 1
    with contextlib.closing(sqlite3.connect(root / "projection.sqlite3")) as conn:
        assert conn.execute("SELECT COUNT(*) FROM cards WHERE id=?", (current.id,)).fetchone()[0] == 1
    status = case.service.status()
    assert status["verified_backup_count"] == 1 and status["limited_backup_count"] == 1
    assert status["last_known_good"]["backup_id"] == good.backup_id
    assert status["last_completed_backup"]["backup_id"] == limited.backup_id
    assert status["backup_state"] == "limited" and not status["automatic_backups_paused"]
    assert status["latest_failure_reason"] == "retained original failure"
    assert status["historical_data_warning"] and status["next_actor"] == "PA"


def test_future_scheduled_captures_continue_and_limited_retention_is_bounded(case):
    digest, _ = missing_payload(case)
    before = case.service.config.model_dump()
    case.service.config.retention_count = 1
    case.service.missing_data_notifier = Mock()
    first = case.service.run_backup(idempotency_key="manual-limited")
    case.store.create_card(CardCreate(title="future card is protected"))
    second = case.service.run_backup(trigger="scheduled", idempotency_key="scheduled-limited")
    assert first.integrity_verified and second.integrity_verified
    assert second.data_complete is False and second.status == "success"
    assert second.pruned_backup_ids == [first.backup_id]
    assert case.service.missing_data_notifier.call_count == 1
    assert case.service._state.last_success is None
    assert case.service._state.last_completed is not None
    status = case.service.status()
    assert status["verified_backup_count"] == 0 and status["limited_backup_count"] == 1
    assert status["last_known_good"] is None and status["next_scheduled_run"]
    assert case.store.transcripts.referenced_hashes() == {digest}
    after = case.service.config.model_dump()
    assert {k:v for k,v in before.items() if k != "retention_count"} == {k:v for k,v in after.items() if k != "retention_count"}
    # Historical run state remains readable by old enum contracts; the failed
    # intact-verification flag and explicit reason cannot become an intact proof.
    assert second.verification == "failed" and second.failure_reason


def test_retention_never_replaces_intact_points_with_limited_captures(case):
    points = {case._run("intact-one").backup_id, case._run("intact-two").backup_id}
    case.service.config.retention_count = 1
    missing_payload(case)
    first = case.service.run_backup(idempotency_key="limited-one")
    second = case.service.run_backup(idempotency_key="limited-two")
    assert first.pruned_backup_ids == [] and second.pruned_backup_ids == [first.backup_id]
    assert {r.backup_id for r in case.service.list_backups()} == points | {second.backup_id}


def test_corrupt_present_payload_cannot_be_disclosed_as_missing(case):
    missing_payload(case)
    case.store.append_transcript_events([TranscriptEvent(session_id="present-corrupt", seq=1,
        event_type="chunk", payload={"text":"different present payload " * 300})])
    for source in case.store.transcripts.objects_dir.glob("*/*.zlib"):
        source.write_bytes(b"corrupt fixture")
    failed = case.service.run_backup(idempotency_key="corrupt-and-missing")
    assert failed.status == "failed" and not failed.integrity_verified
    assert "transcript_object_corrupt" in failed.failure_reason
    assert case.service.list_backups() == []


@pytest.mark.parametrize("damage", ["undeclared", "wrong_size", "wrong_count", "extra_gap", "present_as_gap", "lost_present", "event_graph", "wrong_format"])
def test_limitation_declarations_cannot_hide_other_damage(case, damage):
    missing_payload(case)
    case.store.append_transcript_events([TranscriptEvent(session_id="present", seq=1,
        event_type="chunk", payload={"text":"available payload " * 300})])
    case.store.create_card(CardCreate(title="event graph must remain whole"))
    run = case.service.run_backup(idempotency_key="before-tamper")
    assert run.integrity_verified and run.data_complete is False
    def change(root, manifest):
        gap = manifest["missing_transcript_objects"][0]
        if damage == "undeclared":
            manifest.update(missing_transcript_objects=[], format="pa.metadata-backup/v1", format_version=1)
        elif damage == "wrong_size": gap["uncompressed_bytes"] += 1
        elif damage == "wrong_count": gap["reference_count"] += 1
        elif damage == "extra_gap": manifest["missing_transcript_objects"].append({**gap,"sha256":"f"*64})
        elif damage == "wrong_format": manifest.update(format="pa.metadata-backup/v1", format_version=1)
        else:
            prefix = "objects/" if damage == "event_graph" else "transcript_objects/"
            item = next(f for f in manifest["files"] if f["path"].startswith(prefix))
            if damage == "present_as_gap":
                parts = Path(item["path"]).parts
                manifest["missing_transcript_objects"].append({**gap,"sha256":parts[1]+Path(parts[2]).stem})
            else:
                (root / item["path"]).unlink()
                manifest["files"].remove(item)
    rewrite_archive(case, run.backup_id, change)
    checked = case.service.verify_backup(run.backup_id)
    assert not checked.verified and not checked.integrity_verified
    assert checked.verification_status == "failed" and checked.verification_error


@pytest.mark.parametrize("damage", ["ref_count", "metadata_missing", "payload_hash"])
def test_inconsistent_index_cannot_become_a_missing_data_declaration(case, damage):
    digest, _ = missing_payload(case)
    with case.store.transcripts._conn() as conn:
        if damage == "ref_count": conn.execute("UPDATE transcript_objects SET ref_count=2")
        elif damage == "metadata_missing": conn.execute("DELETE FROM transcript_objects")
        else: conn.execute("UPDATE transcript_events SET payload_hash=?", ("f"*64,))
    failed = case.service.run_backup(idempotency_key="bad-index")
    assert failed.status == "failed" and "transcript_object_corrupt" in failed.failure_reason
    assert case.service.list_backups() == []


def test_existing_unpinned_payload_cannot_be_declared_a_source_absence(case):
    case.store.append_transcript_events([TranscriptEvent(session_id="unpinned", seq=1,
        event_type="chunk", payload={"text":"still present " * 300})])
    with patch.object(case.service, "_pin_cold_objects", return_value=None):
        failed = case.service.run_backup(idempotency_key="incomplete-pins")
    assert failed.status == "failed" and "transcript_inventory_changed" in failed.failure_reason
    assert not failed.integrity_verified and case.service.list_backups() == []
    assert list(case.store.transcripts.objects_dir.glob("*/*.zlib"))


def test_full_restore_refuses_limited_capture_before_changing_projection(case):
    missing_payload(case)
    run = case.service.run_backup(idempotency_key="limited-restore-guard")
    before = case.settings.db_path.read_bytes()
    with pytest.raises(BackupError, match="fully intact restore") as raised:
        case.service.initiate_restore(run.backup_id, requested_by="fixture", confirm_instance_id=case.settings.instance_id)
    assert raised.value.code == "backup_contains_missing_data"
    offline = BackupService(case.settings, None)
    result = offline.restore_offline(run.backup_id)
    assert result.status == "failed" and "backup_contains_missing_data" in result.failure_reason
    assert case.settings.db_path.read_bytes() == before
    assert case.service.download_path(run.backup_id).is_file()  # Explicitly limited v2 export.


def test_limited_pre_restore_capture_cannot_weaken_full_restore_guard(case):
    good = case._run("intact-restore-target")
    missing_payload(case)
    before = case.settings.db_path.read_bytes()
    offline = BackupService(case.settings, None)
    result = offline.restore_offline(good.backup_id)
    assert result.status == "failed" and "pre-restore backup failed" in result.failure_reason
    assert case.settings.db_path.read_bytes() == before
    assert {r.backup_id for r in offline.list_backups()} >= {good.backup_id}


def test_limited_verification_survives_restart_and_inventory_does_not_scan(case):
    missing_payload(case)
    run = case.service.run_backup(idempotency_key="limited-cache")
    restarted = BackupService(case.settings, case.store)
    with patch("pa.backup.service.tarfile.open", side_effect=AssertionError("no archive scan")):
        overview = restarted.overview()
        record = overview["records"][0]
        assert record["integrity_verified"] and not record["verified"]
        assert record["data_complete"] is False and record["coverage_warning"]
        assert overview["status"]["verified_backup_count"] == 0
        assert overview["status"]["limited_backup_count"] == 1
        assert overview["status"]["last_success"] is None
    # Replaced archive bytes invalidate the limited proof too.
    restarted._path_for_backup(run.backup_id).write_bytes(b"changed fixture")
    assert restarted.list_backups()[0].verification_status == "unverified"
    assert restarted.status()["last_completed_backup"] is None


def test_scrub_does_not_convert_limited_coverage_into_legacy_intact_proof(case):
    missing_payload(case)
    run = case.service.run_backup(idempotency_key="scrub-limited")
    assert case.service.deep_scrub() == {run.backup_id: False}
    assert case.service.list_backups()[0].integrity_verified
    assert case.service.list_backups()[0].data_complete is False


def test_v1_manifest_rejects_declarations_and_v2_requires_them(case):
    good = case._run("v1-compatible")
    manifest = case.service.verify_backup(good.backup_id).manifest.model_dump()
    assert manifest["format_version"] == 1
    with pytest.raises(ValueError):
        BackupManifest.model_validate({**manifest,"format":"pa.metadata-backup/v2","format_version":2})


@pytest.mark.parametrize("damage", ["duplicate", "overlap", "index_absent", "invalid_hash"])
def test_manifest_rejects_ambiguous_or_unbound_missing_data(case, damage):
    digest, encoded = missing_payload(case)
    run = case.service.run_backup(idempotency_key="bounded-declarations")
    manifest = case.service.verify_backup(run.backup_id).manifest.model_dump()
    gap = manifest["missing_transcript_objects"][0]
    if damage == "duplicate": manifest["missing_transcript_objects"].append(gap)
    elif damage == "overlap":
        manifest["files"].append({"path":f"transcript_objects/{digest[:2]}/{digest[2:]}.zlib",
                                  "size_bytes":len(encoded), "sha256":"a"*64})
    elif damage == "index_absent":
        manifest["files"] = [f for f in manifest["files"] if f["path"] != "transcripts.sqlite3"]
    else: gap["sha256"] = "../invalid"
    with pytest.raises(ValueError):
        BackupManifest.model_validate(manifest)

import gc
import json
import time
from concurrent.futures import ThreadPoolExecutor
from threading import Event

import pytest

from tests.deadline_evidence import DeadlineEvidence


def test_evidence_retains_bounds_and_no_call_values_or_gc_policy_changes(tmp_path, monkeypatch):
    monkeypatch.setenv("PA_TEST_TIMING_ARTIFACT_DIR", str(tmp_path))
    before = list(gc.callbacks)
    thresholds, enabled = gc.get_threshold(), gc.isenabled()
    marker = "fixture-value-must-not-be-recorded"
    with DeadlineEvidence("evidence-contract", capacity=2, minimum_pause_ms=0) as evidence:
        evidence.begin(time.monotonic())
        wrapped = evidence.wrap(lambda value: value)
        assert [wrapped(marker) for _ in range(3)] == [marker] * 3
        # Real collections in this private test prove callback publication.
        gc.collect()
        evidence.finish(time.monotonic())
        summary = evidence.summary()
    assert gc.callbacks == before
    assert gc.get_threshold() == thresholds and gc.isenabled() == enabled
    assert summary["worker_calls_total"] == 3
    assert len(summary["worker_intervals"]) == 2
    assert summary["worker_events_dropped"] == 1
    assert summary["request_gc_overlaps"]
    assert summary["gc_events_retained"] <= 2
    assert summary["gc_events_total"] - summary["gc_events_dropped"] == summary["gc_events_retained"]
    artifact, = tmp_path.glob("evidence-contract-*.json")
    assert json.loads(artifact.read_text())["contract"] == "pa.test-deadline-evidence/v1"
    assert marker not in artifact.read_text()


def test_evidence_cleanup_keeps_original_failure_if_artifact_write_fails(tmp_path, monkeypatch):
    occupied = tmp_path / "file"
    occupied.write_text("occupied")
    monkeypatch.setenv("PA_TEST_TIMING_ARTIFACT_DIR", str(occupied))
    before = list(gc.callbacks)
    original = RuntimeError("original fixture failure")
    with pytest.raises(RuntimeError) as caught:
        with DeadlineEvidence("original-failure") as evidence:
            evidence.begin(time.monotonic())
            try:
                evidence.wrap(lambda: (_ for _ in ()).throw(original))()
            finally:
                evidence.finish(time.monotonic())
    assert caught.value is original
    assert gc.callbacks == before
    assert original.__notes__ == ["Deadline evidence publication failed; original failure retained."]


def test_requested_artifact_failure_is_visible_on_an_otherwise_successful_call(tmp_path, monkeypatch):
    occupied = tmp_path / "file"
    occupied.write_text("occupied")
    monkeypatch.setenv("PA_TEST_TIMING_ARTIFACT_DIR", str(occupied))
    before = list(gc.callbacks)
    with pytest.raises(FileExistsError):
        with DeadlineEvidence("artifact-failure") as evidence:
            evidence.begin(time.monotonic())
            evidence.finish(time.monotonic())
    assert gc.callbacks == before


def test_worker_running_at_deadline_is_retained_without_waiting_or_cancelling_it():
    entered, release = Event(), Event()
    with ThreadPoolExecutor(max_workers=1) as pool:
        with DeadlineEvidence("incomplete-worker") as evidence:
            evidence.begin(time.monotonic())
            def work():
                entered.set()
                release.wait()
            pending = pool.submit(evidence.wrap(work))
            try:
                assert entered.wait(timeout=2)
                evidence.finish(time.monotonic())
                summary = evidence.summary()
                assert summary["worker_calls_started"] == 1
                assert summary["worker_calls_total"] == 0
                assert summary["worker_calls_incomplete"] == 1
                assert summary["worker_intervals"][0]["end_relative_ms"] is None
                assert summary["worker_intervals"][0]["thread_cpu_ms"] is None
            finally:
                release.set()
                pending.result(timeout=2)

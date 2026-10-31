from __future__ import annotations

import io
import json
from pathlib import Path

import pytest

from pa.cli.logs import show_logs
from pa.config import Settings


@pytest.mark.parametrize("structured_first", [False, True])
def test_json_combined_sources_preserve_diagnostic_fields(tmp_path: Path, structured_first: bool):
    logs = tmp_path / "logs"
    logs.mkdir()
    message = "Slow off-loop operation operation=fixture.read elapsed_ms=600.0"
    (logs / "server.err.log").write_text(
        f"2026-10-31T00:00:01Z WARNING [pa.fixture] {message}\n"
    )
    phase = {"operation": "fixture.read", "sqlite_calls": 4, "elapsed_ms": 600.0}
    stamp = "2026-10-31T00:00:00.750Z" if structured_first else "2026-10-31T00:00:01.250Z"
    (logs / "pa.jsonl").write_text(json.dumps({
        "timestamp": stamp, "level": "WARNING", "logger": "pa.fixture",
        "message": message, "performance_phases": phase,
    }) + "\n")
    settings = Settings(data_dir=tmp_path, instance_id="fixture", instance_name="fixture")
    output = io.StringIO()
    show_logs(settings=settings, sources=["stderr", "structured"], lines=20,
              json_output=True, output=output, diagnostics=io.StringIO())
    records = [json.loads(line) for line in output.getvalue().splitlines()]
    assert [r["performance_phases"] for r in records if "performance_phases" in r] == [phase]
    assert len(records) == (1 if structured_first else 2)
    # The human display retains its existing duplicate suppression contract.
    human = io.StringIO()
    show_logs(settings=settings, sources=["stderr", "structured"], lines=20,
              output=human, diagnostics=io.StringIO())
    assert human.getvalue().count(message) == 1


def test_json_keeps_distinct_numeric_observations_with_identical_messages(tmp_path: Path):
    logs = tmp_path / "logs"
    logs.mkdir()
    rows = [{"timestamp": f"2026-10-31T00:00:01.{i}00Z", "level": "WARNING",
             "logger": "pa.fixture", "message": "same rounded warning",
             "performance_phases": {"sqlite_rows": i}} for i in (1, 2)]
    (logs / "pa.jsonl").write_text("".join(json.dumps(row) + "\n" for row in rows))
    output = io.StringIO()
    show_logs(settings=Settings(data_dir=tmp_path), sources=["structured"], lines=20,
              json_output=True, output=output, diagnostics=io.StringIO())
    assert [json.loads(line)["performance_phases"] for line in output.getvalue().splitlines()] == [
        row["performance_phases"] for row in rows
    ]


def test_json_follow_keeps_fields_when_human_copy_was_in_initial_tail(tmp_path: Path, monkeypatch):
    from datetime import UTC, datetime
    from pa.cli import logs as module

    directory = tmp_path / "logs"
    directory.mkdir()
    (directory / "server.err.log").write_text("2026-10-31T00:00:01Z WARNING [pa.fixture] same warning\n")
    (directory / "pa.jsonl").touch()
    record = module.parse_line(json.dumps({
        "timestamp": "2026-10-31T00:00:01.250Z", "level": "WARNING",
        "message": "same warning", "performance_phases": {"sqlite_rows": 3},
    }), "structured", datetime(2026, 10, 31, tzinfo=UTC))
    closed = []

    class Follower:
        def __init__(self, path, source):
            self.source = source

        def poll(self):
            return [record] if self.source == "structured" else []

        def close(self):
            closed.append(self.source)

    def stop(_):
        raise KeyboardInterrupt

    monkeypatch.setattr(module, "_FollowFile", Follower)
    monkeypatch.setattr(module.time, "sleep", stop)
    output = io.StringIO()
    show_logs(settings=Settings(data_dir=tmp_path), sources=["stderr", "structured"],
              lines=20, follow=True, json_output=True, output=output, diagnostics=io.StringIO())
    assert [json.loads(line).get("performance_phases") for line in output.getvalue().splitlines()] == [
        None, {"sqlite_rows": 3},
    ]
    assert sorted(closed) == ["stderr", "structured"]

from pathlib import Path

from pa.instance.quiesce import (
    consume_skip_resume,
    mark_snapshot_no_resume,
    quiesce_path,
    skip_resume_path,
)


def test_no_resume_marker_is_durable_and_consumed_once(tmp_path: Path) -> None:
    quiesce_path(tmp_path).write_text("{}")

    mark_snapshot_no_resume(tmp_path)

    assert not quiesce_path(tmp_path).exists()
    assert skip_resume_path(tmp_path).exists()
    assert consume_skip_resume(tmp_path) is True
    assert consume_skip_resume(tmp_path) is False

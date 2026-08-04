from __future__ import annotations

import os
import signal
import subprocess
from collections import defaultdict
from unittest.mock import Mock, patch

import pytest

from pa.cli.dispatch_wait import (
    DispatchFetchError,
    KeepAwakeError,
    MacOSKeepAwake,
    wait_for_dispatches,
)


def test_multiple_dispatches_wait_for_all_and_report_mixed_terminal_states() -> None:
    states = {
        "success": iter(
            [
                {"state": "queued", "target_instance_name": "one"},
                {"state": "completed", "target_instance_name": "one"},
            ]
        ),
        "failure": iter(
            [
                {"state": "running", "target_instance_name": "two"},
                {
                    "state": "failed",
                    "target_instance_name": "two",
                    "last_error": "agent failed",
                },
            ]
        ),
        "cancelled": iter(
            [{"state": "cancelled", "target_instance_name": "three"}]
        ),
    }
    calls: defaultdict[str, int] = defaultdict(int)

    def fetch(dispatch_id: str) -> dict:
        calls[dispatch_id] += 1
        return next(states[dispatch_id])

    with patch("pa.cli.dispatch_wait.time.sleep"):
        result = wait_for_dispatches(
            ["success", "failure", "cancelled"],
            fetch=fetch,
            timeout_seconds=10,
            poll_interval_seconds=0.1,
            keep_awake=False,
            emit=None,
        )

    assert result["exit_code"] == 1
    assert [item["outcome"] for item in result["dispatches"]] == [
        "succeeded",
        "failed",
        "cancelled",
    ]
    assert calls == {"success": 2, "failure": 2, "cancelled": 1}


def test_temporary_api_loss_recovers_and_records_transition() -> None:
    responses = iter(
        [
            DispatchFetchError("server restarting"),
            {"state": "running", "target_instance_id": "worker"},
            {"state": "completed", "target_instance_id": "worker"},
        ]
    )

    def fetch(_dispatch_id: str) -> dict:
        value = next(responses)
        if isinstance(value, Exception):
            raise value
        return value

    with patch("pa.cli.dispatch_wait.time.sleep"):
        result = wait_for_dispatches(
            ["dispatch-1"],
            fetch=fetch,
            timeout_seconds=10,
            poll_interval_seconds=0.1,
            keep_awake=False,
            emit=None,
        )

    assert result["exit_code"] == 0
    assert [item["state"] for item in result["dispatches"][0]["transitions"]] == [
        "api-unavailable",
        "running",
        "completed",
    ]


def test_missing_dispatch_is_terminally_unavailable() -> None:
    def fetch(_dispatch_id: str) -> dict:
        raise DispatchFetchError("PA rejected request (404)", status_code=404)

    result = wait_for_dispatches(
        ["missing"],
        fetch=fetch,
        timeout_seconds=10,
        poll_interval_seconds=0.1,
        keep_awake=False,
        emit=None,
    )

    assert result["exit_code"] == 1
    assert result["dispatches"][0]["state"] == "unavailable"
    assert result["dispatches"][0]["outcome"] == "unavailable"


def test_wait_timeout_marks_only_nonterminal_dispatches() -> None:
    with patch(
        "pa.cli.dispatch_wait.time.monotonic", side_effect=[0.0, 2.0, 2.25]
    ):
        result = wait_for_dispatches(
            ["running"],
            fetch=lambda _dispatch_id: {"state": "running"},
            timeout_seconds=1,
            poll_interval_seconds=0.1,
            keep_awake=False,
            emit=None,
        )

    assert result["exit_code"] == 124
    assert result["status"] == "timed_out"
    assert result["dispatches"][0]["outcome"] == "timed_out"


def test_ctrl_c_releases_caffeinate_and_returns_130() -> None:
    process = Mock()
    process.poll.return_value = None
    process.wait.return_value = 0
    with (
        patch("pa.cli.dispatch_wait.sys.platform", "darwin"),
        patch("pa.cli.dispatch_wait.shutil.which", return_value="/usr/bin/caffeinate"),
        patch("pa.cli.dispatch_wait.subprocess.Popen", return_value=process) as popen,
        patch("pa.cli.dispatch_wait.time.sleep", side_effect=KeyboardInterrupt),
    ):
        result = wait_for_dispatches(
            ["running"],
            fetch=lambda _dispatch_id: {"state": "running"},
            timeout_seconds=10,
            poll_interval_seconds=0.1,
            keep_awake=True,
            emit=None,
        )

    assert result["exit_code"] == 130
    assert result["dispatches"][0]["outcome"] == "interrupted"
    process.terminate.assert_called_once_with()
    process.wait.assert_called_once_with(timeout=5.0)
    command = popen.call_args.args[0]
    assert command == ["/usr/bin/caffeinate", "-dimsu", "-w", str(os.getpid())]
    assert popen.call_args.kwargs["start_new_session"] is True


def test_sigterm_is_graceful_and_returns_shell_signal_status() -> None:
    def raise_sigterm(_seconds: float) -> None:
        signal.raise_signal(signal.SIGTERM)

    with patch("pa.cli.dispatch_wait.time.sleep", side_effect=raise_sigterm):
        result = wait_for_dispatches(
            ["running"],
            fetch=lambda _dispatch_id: {"state": "running"},
            timeout_seconds=10,
            poll_interval_seconds=0.1,
            keep_awake=False,
            emit=None,
        )

    assert result["exit_code"] == 143
    assert result["status"] == "interrupted"


def test_caffeinate_cleanup_escalates_to_kill() -> None:
    process = Mock()
    process.poll.return_value = None
    process.wait.side_effect = [subprocess.TimeoutExpired("caffeinate", 5), 0]
    with (
        patch("pa.cli.dispatch_wait.sys.platform", "darwin"),
        patch("pa.cli.dispatch_wait.shutil.which", return_value="/usr/bin/caffeinate"),
        patch("pa.cli.dispatch_wait.subprocess.Popen", return_value=process),
        MacOSKeepAwake(),
    ):
        pass

    process.terminate.assert_called_once_with()
    process.kill.assert_called_once_with()
    assert process.wait.call_count == 2


def test_keep_awake_fails_explicitly_off_macos() -> None:
    with (
        patch("pa.cli.dispatch_wait.sys.platform", "linux"),
        pytest.raises(KeepAwakeError, match="supported on macOS only"),
        MacOSKeepAwake(),
    ):
        pass

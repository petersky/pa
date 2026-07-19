import subprocess
from unittest.mock import call, patch

from pa.release.runner import ROOT, create_release, ensure_release_pr, merge_release_pr


def test_create_release_uses_version_resolved_before_branch_switch() -> None:
    with (
        patch("pa.release.runner.read_version", return_value="9.9.9"),
        patch("pa.release.runner.resolve_version") as resolve_version,
        patch("pa.release.runner.ensure_tag_available"),
        patch("pa.release.runner.set_version") as set_version,
        patch("pa.release.runner._run"),
    ):
        result = create_release(
            "patch",
            target_version="1.2.4",
            commit=False,
            check_tag=False,
        )

    resolve_version.assert_not_called()
    set_version.assert_called_once_with("1.2.4")
    assert result.old_version == "9.9.9"
    assert result.new_version == "1.2.4"
    assert result.tag == "v1.2.4"


def test_ensure_release_pr_reuses_open_pr() -> None:
    completed = subprocess.CompletedProcess(
        [], 0, stdout='[{"url":"https://example.test/pr/7"}]\n', stderr=""
    )
    with patch("pa.release.runner._capture", return_value=completed) as capture:
        url = ensure_release_pr("v1.2.3", "release/v1.2.3")

    assert url == "https://example.test/pr/7"
    assert capture.call_count == 1


def test_merge_release_pr_waits_for_checks_before_merge() -> None:
    checks = subprocess.CompletedProcess([], 0, stdout="checks passed", stderr="")
    with (
        patch("pa.release.runner._capture", return_value=checks) as capture,
        patch("pa.release.runner._run") as run,
    ):
        merge_release_pr("https://example.test/pr/7", head_commit="abc123")

    capture.assert_called_once_with(
        [
            "gh",
            "pr",
            "checks",
            "https://example.test/pr/7",
            "--watch",
            "--fail-fast",
        ],
        cwd=ROOT,
    )
    assert run.call_args_list == [
        call(
            [
                "gh",
                "pr",
                "merge",
                "https://example.test/pr/7",
                "--merge",
                "--match-head-commit",
                "abc123",
            ],
            cwd=ROOT,
        ),
    ]


def test_merge_release_pr_retries_until_checks_are_registered() -> None:
    missing = subprocess.CompletedProcess(
        [], 1, stdout="", stderr="no checks reported on the 'release' branch"
    )
    passed = subprocess.CompletedProcess([], 0, stdout="checks passed", stderr="")
    with (
        patch("pa.release.runner._capture", side_effect=[missing, passed]) as capture,
        patch("pa.release.runner._run"),
        patch("pa.release.runner.time.sleep") as sleep,
    ):
        merge_release_pr("https://example.test/pr/7", head_commit="abc123")

    assert capture.call_count == 2
    sleep.assert_called_once_with(2.0)

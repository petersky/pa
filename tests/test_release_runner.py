import subprocess
from unittest.mock import call, patch

import pytest

from pa.release.runner import (
    ROOT,
    ReleaseError,
    cleanup_release_branch,
    create_release,
    ensure_release_pr,
    merge_release_pr,
    resolve_release_target,
)


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


def test_ensure_release_pr_retries_transient_create_then_succeeds() -> None:
    list_empty = subprocess.CompletedProcess([], 0, stdout="[]\n", stderr="")
    create_timeout = subprocess.CompletedProcess(
        [],
        1,
        stdout="",
        stderr=(
            "pull request create failed: HTTP 504: We couldn't respond to your "
            "request in time. Sorry about that. Please try resubmitting your request"
        ),
    )
    created = subprocess.CompletedProcess(
        [], 0, stdout="https://example.test/pr/8\n", stderr=""
    )
    with (
        patch(
            "pa.release.runner._capture",
            side_effect=[list_empty, create_timeout, list_empty, created],
        ),
        patch("pa.release.runner.time.sleep") as sleep,
    ):
        url = ensure_release_pr("v1.2.3", "release/v1.2.3")

    assert url == "https://example.test/pr/8"
    sleep.assert_called_once_with(1)


def test_ensure_release_pr_uses_pr_created_before_timeout() -> None:
    list_empty = subprocess.CompletedProcess([], 0, stdout="[]\n", stderr="")
    create_timeout = subprocess.CompletedProcess(
        [], 1, stdout="", stderr="HTTP 504: gateway timeout"
    )
    list_found = subprocess.CompletedProcess(
        [], 0, stdout='[{"url":"https://example.test/pr/9"}]\n', stderr=""
    )
    with (
        patch(
            "pa.release.runner._capture",
            side_effect=[list_empty, create_timeout, list_found],
        ),
        patch("pa.release.runner.time.sleep"),
    ):
        url = ensure_release_pr("v1.2.3", "release/v1.2.3")

    assert url == "https://example.test/pr/9"


def test_ensure_release_pr_wraps_exhausted_failures() -> None:
    list_empty = subprocess.CompletedProcess([], 0, stdout="[]\n", stderr="")
    create_timeout = subprocess.CompletedProcess(
        [], 1, stdout="", stderr="HTTP 504: gateway timeout"
    )
    with (
        patch(
            "pa.release.runner._capture",
            side_effect=[list_empty, create_timeout] * 3,
        ),
        patch("pa.release.runner.time.sleep"),
        pytest.raises(ReleaseError, match="could not create the release PR") as raised,
    ):
        ensure_release_pr("v1.2.3", "release/v1.2.3")

    assert raised.value.hints
    assert "Rerun the same release command" in raised.value.hints[0]


def test_resolve_release_target_resumes_prepared_branch() -> None:
    with (
        patch("pa.release.runner.current_branch", return_value="release/v1.0.6"),
        patch("pa.release.runner.read_version", return_value="1.0.6"),
    ):
        version, resuming = resolve_release_target("patch")

    assert version == "1.0.6"
    assert resuming is True


def test_resolve_release_target_accepts_matching_explicit_version() -> None:
    with (
        patch("pa.release.runner.current_branch", return_value="release/v1.0.6"),
        patch("pa.release.runner.read_version", return_value="1.0.6"),
    ):
        version, resuming = resolve_release_target("v1.0.6")

    assert version == "1.0.6"
    assert resuming is True


def test_resolve_release_target_from_main_still_bumps() -> None:
    with (
        patch("pa.release.runner.current_branch", return_value="main"),
        patch("pa.release.runner.read_version", return_value="1.0.5"),
    ):
        version, resuming = resolve_release_target("patch")

    assert version == "1.0.6"
    assert resuming is False


def test_resolve_release_target_rejects_mismatched_explicit_version() -> None:
    with (
        patch("pa.release.runner.current_branch", return_value="release/v1.0.6"),
        patch("pa.release.runner.read_version", return_value="1.0.6"),
        pytest.raises(ReleaseError, match="already prepared as 1.0.6"),
    ):
        resolve_release_target("1.0.7")


def test_create_release_skips_bump_and_empty_commit_when_already_prepared() -> None:
    clean = subprocess.CompletedProcess([], 0, stdout="", stderr="")
    cached_empty = subprocess.CompletedProcess([], 0, stdout="", stderr="")
    with (
        patch("pa.release.runner.read_version", return_value="1.2.4"),
        patch("pa.release.runner.set_version") as set_version,
        patch("pa.release.runner._run") as run,
        patch(
            "pa.release.runner._capture",
            side_effect=[clean, cached_empty],
        ),
    ):
        result = create_release(
            "patch",
            target_version="1.2.4",
            commit=True,
            check_tag=False,
        )

    set_version.assert_not_called()
    assert call(["uv", "lock"], cwd=ROOT) not in run.call_args_list
    assert call(["git", "commit", "-m", "Release v1.2.4"]) not in run.call_args_list
    assert result.old_version == "1.2.4"
    assert result.new_version == "1.2.4"


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


def test_cleanup_release_branch_switches_to_main_and_deletes_local_and_remote() -> None:
    present = subprocess.CompletedProcess([], 0, stdout="", stderr="")
    remote_present = subprocess.CompletedProcess(
        [], 0, stdout="abc123\trefs/heads/release/v1.2.3\n", stderr=""
    )
    ok = subprocess.CompletedProcess([], 0, stdout="", stderr="")
    captures = [
        present,  # local show-ref
        remote_present,  # ls-remote
        ok,  # merge --ff-only
        ok,  # branch -d
        ok,  # push --delete
        ok,  # fetch --prune
    ]
    with (
        patch("pa.release.runner._run") as run,
        patch("pa.release.runner._capture", side_effect=captures) as capture,
        patch(
            "pa.release.runner.current_branch",
            side_effect=["release/v1.2.3", "main"],
        ),
    ):
        cleanup_release_branch("release/v1.2.3")

    assert run.call_args_list == [
        call(["git", "fetch", "origin", "main"], cwd=ROOT),
        call(["git", "switch", "main"], cwd=ROOT),
    ]
    assert capture.call_args_list == [
        call(
            ["git", "show-ref", "--verify", "--quiet", "refs/heads/release/v1.2.3"],
            cwd=ROOT,
        ),
        call(["git", "ls-remote", "--heads", "origin", "release/v1.2.3"], cwd=ROOT),
        call(["git", "merge", "--ff-only", "origin/main"], cwd=ROOT),
        call(["git", "branch", "-d", "release/v1.2.3"], cwd=ROOT),
        call(["git", "push", "origin", "--delete", "release/v1.2.3"], cwd=ROOT),
        call(["git", "fetch", "--prune", "origin"], cwd=ROOT),
    ]


def test_cleanup_release_branch_refuses_non_release_names() -> None:
    with pytest.raises(ReleaseError, match="non-release branch"):
        cleanup_release_branch("agent/feature")


def test_cleanup_release_branch_retries_delete_when_ls_remote_fails() -> None:
    local_missing = subprocess.CompletedProcess([], 1, stdout="", stderr="")
    probe_failed = subprocess.CompletedProcess(
        [], 1, stdout="", stderr="ssh: connect to host github.com port 22: Connection refused"
    )
    ok = subprocess.CompletedProcess([], 0, stdout="", stderr="")
    captures = [
        local_missing,  # local show-ref
        probe_failed,  # ls-remote
        ok,  # merge --ff-only
        ok,  # push --delete
        ok,  # fetch --prune
    ]
    with (
        patch("pa.release.runner._run") as run,
        patch("pa.release.runner._capture", side_effect=captures) as capture,
        patch("pa.release.runner.current_branch", side_effect=["main", "main"]),
    ):
        cleanup_release_branch("release/v1.2.3")

    assert run.call_args_list == [
        call(["git", "fetch", "origin", "main"], cwd=ROOT),
    ]
    assert call(
        ["git", "push", "origin", "--delete", "release/v1.2.3"], cwd=ROOT
    ) in capture.call_args_list
    assert call(
        ["git", "branch", "-d", "release/v1.2.3"], cwd=ROOT
    ) not in capture.call_args_list

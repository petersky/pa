from argparse import Namespace
from types import SimpleNamespace
from unittest.mock import patch

from pa.release import script


def test_confirm_ship_accepts_yes() -> None:
    with (
        patch.object(script.sys.stdin, "isatty", return_value=True),
        patch("builtins.input", return_value="yes"),
    ):
        assert script._confirm_ship("v1.2.3", "https://example.test/pr/1") is True


def test_confirm_ship_defaults_to_no_without_tty() -> None:
    with patch.object(script.sys.stdin, "isatty", return_value=False):
        assert script._confirm_ship("v1.2.3", "https://example.test/pr/1") is False


def test_publish_uses_merged_release_notes() -> None:
    args = Namespace(message=None, notes_file=None, skip_gh=False, wait_ci=30)
    with (
        patch("pa.release.script.tag_merged_release") as tag_release,
        patch(
            "pa.release.script._notes_path_from_merged_main",
            return_value=(script.Path("/tmp/notes.md"), None),
        ),
        patch("pa.release.script._wait_then_publish") as wait_publish,
    ):
        script._publish("v1.2.3", args, do_push=True)

    tag_release.assert_called_once_with("v1.2.3", message=None, push=True)
    wait_publish.assert_called_once_with(
        "v1.2.3", script.Path("/tmp/notes.md"), wait_ci=30
    )


def test_ship_uses_head_captured_before_confirmation() -> None:
    confirmation_started = False

    def confirm(_tag: str, _pr_url: str) -> bool:
        nonlocal confirmation_started
        confirmation_started = True
        return True

    def current_head() -> str:
        return "later-local-sha" if confirmation_started else "pushed-pr-sha"

    with (
        patch("pa.release.script._warn_if_behind_origin_main", return_value=0),
        patch("pa.release.script.read_version", return_value="1.2.2"),
        patch("pa.release.script.resolve_version", return_value="1.2.3"),
        patch("pa.release.script.latest_tag", return_value="v1.2.2"),
        patch("pa.release.script.ensure_tag_available"),
        patch("pa.release.script.ensure_release_branch", return_value="release/v1.2.3"),
        patch("pa.release.script.generate_release_notes", return_value="notes"),
        patch(
            "pa.release.script.write_release_notes",
            return_value=script.Path("/tmp/v1.2.3.md"),
        ),
        patch(
            "pa.release.script.create_release",
            return_value=SimpleNamespace(
                old_version="1.2.2", new_version="1.2.3", tag="v1.2.3"
            ),
        ),
        patch(
            "pa.release.script.ensure_release_pr",
            return_value="https://example.test/pr/1",
        ),
        patch("pa.release.script.head_commit", side_effect=current_head),
        patch("pa.release.script._confirm_ship", side_effect=confirm),
        patch("pa.release.script.merge_release_pr") as merge_pr,
        patch("pa.release.script._publish"),
    ):
        assert script._run(["1.2.3", "--no-agent"]) == 0

    merge_pr.assert_called_once_with(
        "https://example.test/pr/1", head_commit="pushed-pr-sha"
    )

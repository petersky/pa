from argparse import Namespace
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

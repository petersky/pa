from unittest.mock import patch

from pa.release.runner import create_release


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

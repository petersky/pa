"""State, rendering, fallback, and terminal integration tests for config TUI."""

from __future__ import annotations

import io
import os
import pty
import tempfile
import unittest
from pathlib import Path

from pa.cli.config_tui import (
    EditorState,
    ExitCode,
    detect_terminal,
    render_text,
    run_line_editor,
    state_marker,
)
from pa.domain.config_edit import ConfigConflictError, ConfigError, get_field_spec
from pa.domain.instance_config import (
    InstanceConfig,
    load_instance_config,
    save_instance_config,
    update_instance_config,
)


class ConfigTuiStateTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.data_dir = Path(self._tmp.name)
        save_instance_config(
            self.data_dir,
            InstanceConfig(
                instance_name="tui-test",
                data_dir=str(self.data_dir),
                host="127.0.0.1",
                zone="local",
                subscribed_realms=["default"],
                sync_token="super-secret-value",
            ),
        )
        self.state = EditorState.load(self.data_dir)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_multiple_edits_are_staged_without_writing(self) -> None:
        self.state.stage_raw("zone", "west")
        self.state.toggle("relay_enabled")
        persisted = load_instance_config(self.data_dir)
        self.assertEqual(persisted.zone, "local")
        self.assertFalse(persisted.relay_enabled)
        self.assertEqual(self.state.staged, {"zone": "west", "relay_enabled": True})

    def test_invalid_edit_is_marked_and_does_not_stage(self) -> None:
        with self.assertRaises(ConfigError):
            self.state.stage_raw("dispatch_capacity", "0")
        self.assertIn("dispatch_capacity", self.state.errors)
        self.assertNotIn("dispatch_capacity", self.state.staged)

    def test_search_sections_and_non_color_markers(self) -> None:
        self.state.query = "bind"
        self.assertEqual(
            [item.name for item in self.state.visible_specs],
            ["host", "web_listeners"],
        )
        self.state.query = ""
        self.state.section = "Network"
        self.assertIn("host", [item.name for item in self.state.visible_specs])
        self.state.stage_raw("host", "0.0.0.0")
        self.assertIn("*", state_marker(self.state, get_field_spec("host")))

    def test_review_redacts_secrets(self) -> None:
        self.state.stage_raw("sync_token", "replacement-secret")
        review = "\n".join(" ".join(row) for row in self.state.review_rows())
        self.assertNotIn("super-secret-value", review)
        self.assertNotIn("replacement-secret", review)
        self.assertIn("<redacted>", review)

    def test_apply_is_atomic_and_reports_impacts(self) -> None:
        self.state.stage_raw("zone", "west")
        self.state.stage_raw("host", "0.0.0.0")
        summary = self.state.apply()
        persisted = load_instance_config(self.data_dir)
        self.assertEqual(persisted.zone, "west")
        self.assertEqual(persisted.host, "0.0.0.0")
        self.assertEqual(summary.reload, frozenset({"zone"}))
        self.assertEqual(summary.restart, frozenset({"host"}))

    def test_external_change_conflicts_instead_of_overwriting(self) -> None:
        self.state.stage_raw("zone", "staged")
        update_instance_config(self.data_dir, zone="external")
        with self.assertRaises(ConfigConflictError):
            self.state.apply()
        self.assertEqual(load_instance_config(self.data_dir).zone, "external")

    def test_refresh_preserves_non_conflicting_edits_and_marks_conflicts(self) -> None:
        self.state.stage_raw("zone", "staged")
        self.state.stage_raw("instance_name", "renamed")
        update_instance_config(self.data_dir, zone="external")
        conflicts = self.state.refresh()
        self.assertEqual(conflicts, {"zone"})
        self.assertEqual(self.state.staged["instance_name"], "renamed")
        self.assertIn("zone", self.state.errors)

    def test_discard_selected_and_all(self) -> None:
        self.state.stage_raw("zone", "west")
        self.state.stage_raw("instance_name", "renamed")
        self.state.revert("zone")
        self.assertEqual(set(self.state.staged), {"instance_name"})
        self.state.discard_all()
        self.assertFalse(self.state.staged)

    def test_small_and_large_rendering_preserve_status_and_help(self) -> None:
        self.state.stage_raw("zone", "west")
        for width, height in ((40, 10), (80, 24), (140, 40)):
            with self.subTest(size=(width, height)):
                lines = render_text(self.state, width, height)
                self.assertLessEqual(len(lines), height)
                self.assertTrue(all(len(line) <= width for line in lines))
                self.assertTrue(any("staged" in line for line in lines))
                self.assertTrue(any("j/k" in line for line in lines))


class ConfigLineFallbackTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.data_dir = Path(self._tmp.name)
        save_instance_config(
            self.data_dir,
            InstanceConfig(
                instance_name="line-test",
                data_dir=str(self.data_dir),
                zone="local",
                sync_token="do-not-print-this",
            ),
        )

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_line_mode_stages_reviews_and_applies(self) -> None:
        stdin = io.StringIO("set zone remote\nreview\napply\n")
        stdout = io.StringIO()
        code = run_line_editor(
            EditorState.load(self.data_dir), stdin=stdin, stdout=stdout
        )
        self.assertEqual(code, ExitCode.APPLIED)
        self.assertEqual(load_instance_config(self.data_dir).zone, "remote")
        self.assertIn("zone: local -> remote [reload]", stdout.getvalue())

    def test_line_mode_never_accepts_or_prints_secret_values(self) -> None:
        stdin = io.StringIO("list\nset sync_token leaked\nquit\n")
        stdout = io.StringIO()
        code = run_line_editor(
            EditorState.load(self.data_dir), stdin=stdin, stdout=stdout
        )
        output = stdout.getvalue()
        self.assertEqual(code, ExitCode.CANCELLED)
        self.assertNotIn("do-not-print-this", output)
        self.assertNotIn("leaked", output)
        self.assertIn("interactive terminal", output)

    def test_eof_with_staged_values_returns_no_write_code(self) -> None:
        stdout = io.StringIO()
        code = run_line_editor(
            EditorState.load(self.data_dir),
            stdin=io.StringIO("set zone staged\n"),
            stdout=stdout,
        )
        self.assertEqual(code, ExitCode.STAGED_NO_WRITE)
        self.assertEqual(load_instance_config(self.data_dir).zone, "local")


class TerminalCapabilityTests(unittest.TestCase):
    def test_non_tty_and_dumb_term_select_fallback(self) -> None:
        caps = detect_terminal(
            stdin=io.StringIO(), stdout=io.StringIO(), env={"TERM": "xterm-256color"}
        )
        self.assertFalse(caps.curses)
        self.assertIn("not a terminal", caps.reason)

    def test_pseudo_terminal_is_recognized_and_no_color_respected(self) -> None:
        master, slave = pty.openpty()
        try:
            with os.fdopen(os.dup(slave), "r", encoding="utf-8") as stdin:
                with os.fdopen(os.dup(slave), "w", encoding="utf-8") as stdout:
                    caps = detect_terminal(
                        stdin=stdin,
                        stdout=stdout,
                        env={"TERM": "xterm-256color", "NO_COLOR": "1"},
                    )
            self.assertTrue(caps.interactive)
            self.assertTrue(caps.curses)
            self.assertFalse(caps.color)
            self.assertTrue(caps.unicode)
        finally:
            os.close(master)
            os.close(slave)


if __name__ == "__main__":
    unittest.main()

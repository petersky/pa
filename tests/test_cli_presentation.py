from __future__ import annotations

import io
import json

from typer.testing import CliRunner

from pa.cli import presentation as ui
from pa.cli.main import app


class Stream(io.StringIO):
    def __init__(self, *, tty: bool, encoding: str = "utf-8") -> None:
        super().__init__()
        self._tty = tty
        self._encoding = encoding

    def isatty(self) -> bool:
        return self._tty

    @property
    def encoding(self) -> str:
        return self._encoding


def test_terminal_policy_matrix() -> None:
    tty = Stream(tty=True)
    pipe = Stream(tty=False)
    assert ui.TerminalPolicy.detect(tty, {}).color
    assert not ui.TerminalPolicy.detect(pipe, {}).color
    assert not ui.TerminalPolicy.detect(tty, {"NO_COLOR": "1"}).color
    assert not ui.TerminalPolicy.detect(tty, {"TERM": "dumb"}).color
    assert not ui.TerminalPolicy.detect(tty, {"CI": "true"}).color
    assert ui.TerminalPolicy.detect(pipe, {"FORCE_COLOR": "1"}).color
    assert not ui.TerminalPolicy.detect(pipe, {"FORCE_COLOR": "0"}).color


def test_unicode_and_ascii_fallback_policy() -> None:
    assert ui.TerminalPolicy.detect(Stream(tty=True), {}).unicode
    assert not ui.TerminalPolicy.detect(Stream(tty=True, encoding="ascii"), {}).unicode


def test_semantic_status_keeps_label_in_monochrome(monkeypatch) -> None:
    output = Stream(tty=False)
    monkeypatch.setattr(ui.sys, "stdout", output)
    ui.status("WARN", "peer unavailable")
    assert output.getvalue() == "  [WARN] peer unavailable\n"
    assert "\x1b[" not in output.getvalue()


def test_force_color_and_no_color(monkeypatch) -> None:
    output = Stream(tty=False)
    monkeypatch.setattr(ui.sys, "stdout", output)
    monkeypatch.delenv("NO_COLOR", raising=False)
    monkeypatch.delenv("TERM", raising=False)
    monkeypatch.setenv("FORCE_COLOR", "1")
    ui.echo("complete", style="success")
    assert "\x1b[" in output.getvalue()

    output = Stream(tty=True)
    monkeypatch.setattr(ui.sys, "stdout", output)
    monkeypatch.setenv("NO_COLOR", "1")
    ui.echo("complete", style="success")
    assert "\x1b[" not in output.getvalue()


def test_structured_console_is_always_unstyled(monkeypatch) -> None:
    output = Stream(tty=True)
    monkeypatch.setattr(ui.sys, "stdout", output)
    monkeypatch.setenv("FORCE_COLOR", "1")
    payload = {"state": "OK", "name": "München"}
    ui.console(structured=True).print(json.dumps(payload, ensure_ascii=False))
    assert output.getvalue() == '{"state": "OK", "name": "München"}\n'
    assert "\x1b[" not in output.getvalue()


def test_help_error_and_representative_command_are_stable_without_color() -> None:
    runner = CliRunner()
    help_result = runner.invoke(app, ["--help"], env={"NO_COLOR": "1"})
    assert help_result.exit_code == 0
    assert "PA — human–agent orchestration" in help_result.stdout
    assert "\x1b[" not in help_result.stdout

    command_result = runner.invoke(app, ["version"], env={"NO_COLOR": "1"})
    assert command_result.exit_code == 0
    assert command_result.stdout.startswith("pa ")
    assert "\x1b[" not in command_result.stdout

    error_result = runner.invoke(app, ["not-a-command"], env={"NO_COLOR": "1"})
    assert error_result.exit_code != 0
    assert "No such command" in error_result.output
    assert "\x1b[" not in error_result.output


def test_theme_uses_more_than_red_green_state_distinctions() -> None:
    assert ui.THEME.styles["success"].color.name == "bright_cyan"
    assert ui.THEME.styles["warning"].color.name == "yellow"
    assert ui.THEME.styles["failure"].color.name == "bright_red"
    assert (
        len(
            {
                ui.THEME.styles[name].color.name
                for name in ("success", "warning", "failure", "skipped")
            }
        )
        == 4
    )

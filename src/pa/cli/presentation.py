"""Shared terminal presentation policy and semantic theme for the PA CLI."""

from __future__ import annotations

import os
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from typing import IO, Literal

from rich.console import Console
from rich.style import Style
from rich.text import Text
from rich.theme import Theme

SemanticStyle = Literal[
    "success",
    "information",
    "progress",
    "warning",
    "failure",
    "skipped",
    "heading",
    "command",
    "path",
    "identifier",
    "muted",
]

# Bright blue/cyan/magenta/yellow remain distinguishable without relying on a
# red/green pair, and retain useful contrast on common dark and light themes.
THEME = Theme(
    {
        "success": Style(color="bright_cyan", bold=True),
        "information": Style(color="blue", bold=True),
        "progress": Style(color="magenta"),
        "warning": Style(color="yellow", bold=True),
        "failure": Style(color="bright_red", bold=True),
        "skipped": Style(color="bright_black", italic=True),
        "heading": Style(color="bright_blue", bold=True),
        "command": Style(color="bright_magenta", bold=True),
        "path": Style(color="cyan", underline=True),
        "identifier": Style(color="bright_blue"),
        "muted": Style(dim=True),
    },
    inherit=False,
)

STATUS_STYLES: Mapping[str, SemanticStyle] = {
    "OK": "success",
    "INFO": "information",
    "..": "progress",
    "WARN": "warning",
    "FAIL": "failure",
    "SKIP": "skipped",
}


def _truthy(value: str | None) -> bool:
    return value is not None and value.strip().lower() not in {"", "0", "false", "no"}


@dataclass(frozen=True)
class TerminalPolicy:
    color: bool
    interactive: bool
    unicode: bool

    @classmethod
    def detect(
        cls,
        stream: IO[str] = sys.stdout,
        environ: Mapping[str, str] | None = None,
    ) -> TerminalPolicy:
        env = os.environ if environ is None else environ
        interactive = bool(getattr(stream, "isatty", lambda: False)())
        forced = _truthy(env.get("FORCE_COLOR"))
        disabled = "NO_COLOR" in env or env.get("TERM", "").lower() == "dumb"
        # An explicit force wins over redirection and CI, but never over NO_COLOR
        # or TERM=dumb. This keeps policy deterministic across Unix and Windows.
        color = not disabled and (
            forced or (interactive and not _truthy(env.get("CI")))
        )
        encoding = (getattr(stream, "encoding", None) or "utf-8").lower()
        unicode = "utf" in encoding
        return cls(color=color, interactive=interactive, unicode=unicode)


def console(*, stderr: bool = False, structured: bool = False) -> Console:
    stream = sys.stderr if stderr else sys.stdout
    policy = TerminalPolicy.detect(stream)
    return Console(
        file=stream,
        theme=THEME,
        force_terminal=False if structured else policy.color,
        no_color=structured or not policy.color,
        color_system=None if structured or not policy.color else "standard",
        highlight=False,
        soft_wrap=True,
    )


def echo(
    message: object = "", *, style: SemanticStyle | None = None, err: bool = False
) -> None:
    """Print human output with a semantic style; text stays stable in monochrome."""
    console(stderr=err).print(str(message), style=style, markup=False)


def styled(label: str, style: SemanticStyle) -> Text:
    return Text(label, style=style)


def status(label: str, message: str, *, indent: int = 2, err: bool = False) -> None:
    normalized = label.upper()
    line = Text(" " * indent)
    line.append(f"[{normalized:<4}]", style=STATUS_STYLES.get(normalized, "muted"))
    line.append(f" {message}")
    console(stderr=err).print(line)


def heading(message: str, *, err: bool = False) -> None:
    echo(message, style="heading", err=err)


def progress(message: str) -> None:
    """Stable progress line; callers may animate only when policy.interactive."""
    status("..", message)


def render_command(command: str) -> Text:
    return styled(command, "command")


def configure_typer_help() -> None:
    """Apply the PA theme to Typer's Rich help and error renderer."""
    import typer
    from typer import rich_utils

    # Existing commands keep their stable strings while all human output flows
    # through the same terminal capability policy. JSON calls have no style and
    # therefore remain byte-stable even when color is forced.
    typer.echo = echo

    rich_utils.STYLE_HELPTEXT = ""
    # Typer owns its help Console, so use the same concrete palette rather than
    # theme aliases that only PA-owned consoles can resolve.
    rich_utils.STYLE_HELPTEXT_FIRST_LINE = "bold bright_blue"
    rich_utils.STYLE_OPTION = "bold bright_magenta"
    rich_utils.STYLE_ARGUMENT = "bright_blue"
    rich_utils.STYLE_SWITCH = "bold bright_magenta"
    rich_utils.STYLE_NEGATIVE_OPTION = "bold yellow"
    rich_utils.STYLE_NEGATIVE_SWITCH = "bold yellow"
    rich_utils.STYLE_USAGE = "bold bright_blue"
    rich_utils.STYLE_USAGE_COMMAND = "bold bright_magenta"
    rich_utils.STYLE_ERRORS_SUGGESTION = "bold yellow"
    rich_utils.STYLE_ERRORS_PANEL_BORDER = "bold bright_red"

"""Staged, conflict-safe terminal editor for PA instance configuration."""

from __future__ import annotations

import locale
import os
import shutil
import sys
from dataclasses import dataclass, field
from enum import IntEnum
from pathlib import Path
from typing import Any, TextIO

from pa.domain.config_edit import (
    ConfigConflictError,
    ConfigError,
    FieldSpec,
    RESTART_KEYS,
    SERVICE_KEYS,
    apply_config_changes,
    config_revision,
    default_for_unset,
    format_value,
    get_field_spec,
    list_field_specs,
    parse_value,
    require_config,
    validate_config_changes,
    validate_field_value,
)
from pa.domain.instance_config import InstanceConfig


class ExitCode(IntEnum):
    APPLIED = 0
    CANCELLED = 2
    VALIDATION_FAILED = 3
    CONNECTION_FAILED = 4
    STAGED_NO_WRITE = 5


@dataclass(frozen=True)
class TerminalCapabilities:
    curses: bool
    interactive: bool
    color: bool
    unicode: bool
    width: int
    height: int
    reason: str = ""


def detect_terminal(
    *,
    stdin: TextIO = sys.stdin,
    stdout: TextIO = sys.stdout,
    env: dict[str, str] | None = None,
) -> TerminalCapabilities:
    values = os.environ if env is None else env
    size = shutil.get_terminal_size((80, 24))
    interactive = bool(stdin.isatty() and stdout.isatty())
    term = values.get("TERM", "")
    reason = ""
    curses_ok = interactive and term.lower() not in {"", "dumb", "unknown"}
    try:
        import curses  # noqa: F401
    except (ImportError, ModuleNotFoundError):
        curses_ok = False
        reason = "Python curses is unavailable"
    if not interactive:
        curses_ok = False
        reason = "input or output is not a terminal"
    elif term.lower() in {"", "dumb", "unknown"}:
        reason = f"TERM={term or '(unset)'} lacks cursor capabilities"
    encoding = (stdout.encoding or locale.getpreferredencoding(False) or "").lower()
    unicode_ok = "utf" in encoding
    color = (
        curses_ok
        and "NO_COLOR" not in values
        and values.get("PA_CONFIG_COLOR", "auto").lower() != "never"
    )
    return TerminalCapabilities(
        curses=curses_ok,
        interactive=interactive,
        color=color,
        unicode=unicode_ok,
        width=size.columns,
        height=size.lines,
        reason=reason,
    )


@dataclass
class ApplySummary:
    changed: frozenset[str]
    live: frozenset[str]
    reload: frozenset[str]
    restart: frozenset[str]


@dataclass
class EditorState:
    data_dir: Path
    base: InstanceConfig
    instance_name: str
    target_scope: str = "instance-local"
    staged: dict[str, Any] = field(default_factory=dict)
    errors: dict[str, str] = field(default_factory=dict)
    query: str = ""
    section: str = "All"
    cursor: int = 0
    focus: int = 1
    status: str = "Ready"

    @classmethod
    def load(cls, data_dir: Path) -> EditorState:
        config = require_config(data_dir)
        return cls(
            data_dir=data_dir,
            base=config,
            instance_name=config.instance_name,
        )

    @property
    def revision(self) -> str:
        return config_revision(self.base)

    @property
    def sections(self) -> list[str]:
        categories = sorted({spec.category for spec in list_field_specs()})
        return ["All", *categories]

    @property
    def visible_specs(self) -> list[FieldSpec]:
        needle = self.query.casefold().strip()
        specs = [
            spec
            for spec in list_field_specs()
            if (self.section == "All" or spec.category == self.section)
            and (
                not needle
                or needle in spec.name.casefold()
                or needle in spec.description.casefold()
            )
        ]
        return specs

    @property
    def selected(self) -> FieldSpec | None:
        specs = self.visible_specs
        if not specs:
            return None
        self.cursor = min(max(self.cursor, 0), len(specs) - 1)
        return specs[self.cursor]

    def value(self, key: str) -> Any:
        return self.staged.get(key, getattr(self.base, key))

    def stage_raw(self, key: str, raw: str) -> None:
        spec = get_field_spec(key)
        if not spec.editable:
            raise ConfigError(f"{key} is read-only")
        try:
            parsed = parse_value(spec, raw)
            parsed = validate_field_value(key, parsed)
            candidate = dict(self.staged)
            candidate[key] = parsed
            validate_config_changes(self.base, candidate)
        except ConfigError as exc:
            self.errors[key] = str(exc)
            raise
        self.errors.pop(key, None)
        if parsed == getattr(self.base, key):
            self.staged.pop(key, None)
        else:
            self.staged[key] = parsed
        self.status = f"Staged {key}" if key in self.staged else f"Reverted {key}"

    def toggle(self, key: str) -> None:
        spec = get_field_spec(key)
        if spec.kind != "bool":
            raise ConfigError(f"{key} is not a boolean")
        self.stage_raw(key, "false" if self.value(key) else "true")

    def unset(self, key: str) -> None:
        spec = get_field_spec(key)
        value = default_for_unset(spec)
        validate_config_changes(self.base, {**self.staged, key: value})
        if value == getattr(self.base, key):
            self.staged.pop(key, None)
        else:
            self.staged[key] = value
        self.errors.pop(key, None)
        self.status = f"Staged reset for {key}"

    def revert(self, key: str) -> None:
        self.staged.pop(key, None)
        self.errors.pop(key, None)
        self.status = f"Reverted {key}"

    def discard_all(self) -> None:
        self.staged.clear()
        self.errors.clear()
        self.status = "Discarded all staged changes"

    def refresh(self) -> set[str]:
        latest = require_config(self.data_dir)
        conflicts = {
            key
            for key in self.staged
            if getattr(self.base, key) != getattr(latest, key)
            and self.staged[key] != getattr(latest, key)
        }
        self.base = latest
        for key in list(self.staged):
            if self.staged[key] == getattr(latest, key):
                self.staged.pop(key)
        for key in conflicts:
            self.errors[key] = "external change conflicts with staged value"
        self.instance_name = latest.instance_name
        self.status = (
            f"Refreshed; {len(conflicts)} conflict(s) need review"
            if conflicts
            else "Refreshed configuration snapshot"
        )
        return conflicts

    def review_rows(self) -> list[tuple[str, str, str, str]]:
        rows = []
        for key in sorted(self.staged):
            spec = get_field_spec(key)
            before = format_value(
                getattr(self.base, key), reveal=False, sensitive=spec.sensitive
            )
            after = format_value(
                self.staged[key], reveal=False, sensitive=spec.sensitive
            )
            impact = "restart" if key in RESTART_KEYS else (
                "reload" if key in SERVICE_KEYS else "live"
            )
            rows.append((key, before, after, impact))
        return rows

    def apply(self) -> ApplySummary:
        if self.errors:
            raise ConfigError("Resolve validation/conflict errors before applying")
        if not self.staged:
            return ApplySummary(frozenset(), frozenset(), frozenset(), frozenset())
        candidate = validate_config_changes(self.base, self.staged)
        changed = frozenset(
            key
            for key in self.staged
            if getattr(self.base, key) != getattr(candidate, key)
        )
        saved, reload_keys, restart_keys = apply_config_changes(
            self.data_dir, self.staged, expected_revision=self.revision
        )
        live = changed - reload_keys - restart_keys
        self.base = saved
        self.staged.clear()
        self.errors.clear()
        self.status = f"Applied {len(changed)} change(s) atomically"
        return ApplySummary(changed, live, reload_keys - restart_keys, restart_keys)


def state_marker(state: EditorState, spec: FieldSpec) -> str:
    markers = []
    if spec.name in state.errors:
        markers.append("!")
    if spec.name in state.staged:
        markers.append("*")
    if not spec.editable:
        markers.append("R")
    if spec.sensitive:
        markers.append("S")
    return "".join(markers) or "-"


def render_text(state: EditorState, width: int = 80, height: int = 24) -> list[str]:
    """Pure text renderer used by curses and golden tests."""
    width = max(20, width)
    height = max(8, height)
    compact = width < 72 or height < 18
    title = f"PA config > {state.instance_name} [{state.target_scope}]"
    lines = [title[:width], f"Section: {state.section}  Search: {state.query or '(none)'}"[:width]]
    specs = state.visible_specs
    available = max(1, height - (7 if compact else 11))
    start = max(0, min(state.cursor - available // 2, max(0, len(specs) - available)))
    for index, spec in enumerate(specs[start : start + available], start=start):
        selected = ">" if index == state.cursor else " "
        marker = state_marker(state, spec)
        value = format_value(
            state.value(spec.name), reveal=False, sensitive=spec.sensitive
        )
        lines.append(f"{selected} [{marker:<3}] {spec.name}: {value}"[:width])
    if not specs:
        lines.append("  No settings match this filter."[:width])
    selected_spec = state.selected
    if selected_spec:
        configured = format_value(
            getattr(state.base, selected_spec.name),
            sensitive=selected_spec.sensitive,
        )
        effective = format_value(
            state.value(selected_spec.name), sensitive=selected_spec.sensitive
        )
        default = (
            format_value(
                default_for_unset(selected_spec), sensitive=selected_spec.sensitive
            )
            if selected_spec.editable
            else "(managed)"
        )
        impact = "restart" if selected_spec.name in RESTART_KEYS else (
            "reload" if selected_spec.name in SERVICE_KEYS else "live"
        )
        lines.append(
            (
                f"cfg={configured} effective={effective} default={default} "
                f"source=config.json impact={impact}"
            )[:width]
        )
    if selected_spec and not compact:
        lines.extend(
            [
                "-" * min(width, 60),
                selected_spec.description[:width],
                f"type={selected_spec.kind} example={selected_spec.example or '(none)'}"[
                    :width
                ],
                (
                    "allowed=" + ", ".join(selected_spec.allowed)
                    if selected_spec.allowed
                    else "configured/effective values shown above"
                )[:width],
            ]
        )
    lines.append(
        f"{len(state.staged)} staged  {len(state.errors)} invalid | {state.status}"[:width]
    )
    lines.append(
        "j/k arrows move  Tab focus  Enter/Space edit  / search  s review  u revert  r refresh  ? help  q quit"[
            :width
        ]
    )
    return lines[-height:]


def _safe_addstr(screen: Any, row: int, col: int, text: str, attr: int = 0) -> None:
    try:
        height, width = screen.getmaxyx()
        if row < height and col < width:
            screen.addnstr(row, col, text, max(0, width - col - 1), attr)
    except Exception:
        pass


class CursesEditor:
    def __init__(self, state: EditorState, *, color: bool = True) -> None:
        self.state = state
        self.color = color
        self._running = True
        self._exit = ExitCode.CANCELLED

    def run(self) -> ExitCode:
        import curses

        curses.wrapper(self._main)
        return self._exit

    def _main(self, screen: Any) -> None:
        import curses

        try:
            curses.curs_set(0)
        except curses.error:
            pass
        screen.keypad(True)
        if self.color and curses.has_colors():
            curses.start_color()
            curses.use_default_colors()
            curses.init_pair(1, curses.COLOR_CYAN, -1)
            curses.init_pair(2, curses.COLOR_YELLOW, -1)
            curses.init_pair(3, curses.COLOR_RED, -1)
        while self._running:
            screen.erase()
            height, width = screen.getmaxyx()
            for row, line in enumerate(render_text(self.state, width, height)):
                attr = curses.A_BOLD if line.startswith(">") else 0
                if "[!" in line:
                    attr |= curses.color_pair(3)
                elif "[*" in line:
                    attr |= curses.color_pair(2)
                _safe_addstr(screen, row, 0, line, attr)
            screen.refresh()
            try:
                key = screen.get_wch()
            except curses.error:
                continue
            self._handle_key(screen, key)

    def _handle_key(self, screen: Any, key: Any) -> None:
        import curses

        specs = self.state.visible_specs
        if key in (curses.KEY_UP, "k"):
            self.state.cursor = max(0, self.state.cursor - 1)
        elif key in (curses.KEY_DOWN, "j"):
            self.state.cursor = min(max(0, len(specs) - 1), self.state.cursor + 1)
        elif key in ("\t", curses.KEY_BTAB):
            delta = -1 if key == curses.KEY_BTAB else 1
            self.state.focus = (self.state.focus + delta) % 3
        elif key == curses.KEY_RESIZE:
            self.state.status = "Terminal resized; staged changes preserved"
        elif key == "/":
            self.state.query = self._prompt(screen, "Search", self.state.query)
            self.state.cursor = 0
        elif key in ("?", "h"):
            self._overlay(
                screen,
                [
                    "Keyboard help",
                    "Arrows/j/k move; Tab/Shift-Tab focus",
                    "Enter edits; Space toggles booleans",
                    "/ searches; 1-9 select section",
                    "s reviews/applies; u reverts selected; U discards all",
                    "r refreshes; q quits (with discard confirmation)",
                    "R=read-only S=secret *=changed !=invalid",
                    "Press any key",
                ],
            )
        elif isinstance(key, str) and key.isdigit() and key != "0":
            index = int(key) - 1
            if index < len(self.state.sections):
                self.state.section = self.state.sections[index]
                self.state.cursor = 0
        elif key in ("\n", "\r", curses.KEY_ENTER, " "):
            spec = self.state.selected
            if spec:
                self._edit(screen, spec, toggle=(key == " "))
        elif key == "u":
            spec = self.state.selected
            if spec:
                self.state.revert(spec.name)
        elif key == "U":
            if self.state.staged and self._confirm(screen, "Discard ALL staged changes?"):
                self.state.discard_all()
        elif key == "r":
            self.state.refresh()
        elif key == "s":
            self._review(screen)
        elif key == "q":
            if not self.state.staged or self._confirm(
                screen, "Quit and discard staged changes?"
            ):
                self._exit = (
                    ExitCode.STAGED_NO_WRITE
                    if self.state.staged
                    else ExitCode.CANCELLED
                )
                self._running = False

    def _edit(self, screen: Any, spec: FieldSpec, *, toggle: bool) -> None:
        if not spec.editable:
            self.state.status = f"{spec.name} is read-only"
            return
        try:
            if spec.kind == "bool" and toggle:
                self.state.toggle(spec.name)
                return
            if spec.sensitive:
                action = self._prompt(
                    screen, f"{spec.name}: type replace, clear, or cancel", "cancel"
                ).lower()
                if action == "clear":
                    self.state.stage_raw(spec.name, "")
                elif action == "replace":
                    self.state.stage_raw(
                        spec.name,
                        self._prompt(screen, "New secret (hidden)", "", secret=True),
                    )
                return
            value = self.state.value(spec.name)
            if isinstance(value, (list, dict)):
                import json

                current = json.dumps(value)
            elif value is None:
                current = "-"
            else:
                current = str(value).lower() if isinstance(value, bool) else str(value)
            raw = self._prompt(
                screen,
                f"{spec.name} ({spec.kind}; '-' unsets optional)",
                current,
            )
            self.state.stage_raw(spec.name, raw)
        except ConfigError as exc:
            self.state.status = str(exc)

    def _review(self, screen: Any) -> None:
        if not self.state.staged:
            self.state.status = "Nothing is staged"
            return
        rows = ["Review staged changes"]
        dangerous = False
        for key, before, after, impact in self.state.review_rows():
            rows.append(f"{key}: {before} -> {after} [{impact}]")
            dangerous |= get_field_spec(key).dangerous
        rows.append(f"Target: {self.state.instance_name} ({self.state.target_scope})")
        rows.append("Secrets are redacted. Press a to apply; any other key cancels.")
        choice = self._overlay(screen, rows, return_key=True)
        if choice != "a":
            return
        if dangerous:
            phrase = self._prompt(
                screen,
                f"Disruptive change: type APPLY {self.state.instance_name}",
                "",
            )
            if phrase != f"APPLY {self.state.instance_name}":
                self.state.status = "Strong confirmation did not match"
                return
        try:
            summary = self.state.apply()
        except ConfigConflictError:
            self.state.refresh()
            return
        except ConfigError as exc:
            self.state.status = str(exc)
            self._exit = ExitCode.VALIDATION_FAILED
            return
        self._overlay(
            screen,
            [
                f"Applied {len(summary.changed)} changes to {self.state.instance_name}",
                f"live: {', '.join(sorted(summary.live)) or 'none'}",
                f"reload: {', '.join(sorted(summary.reload)) or 'none'}",
                f"restart: {', '.join(sorted(summary.restart)) or 'none'}",
                "PA was not restarted automatically. Press any key.",
            ],
        )
        self._exit = ExitCode.APPLIED

    def _prompt(
        self, screen: Any, label: str, default: str = "", *, secret: bool = False
    ) -> str:
        import curses

        height, width = screen.getmaxyx()
        value = list(default)
        while True:
            screen.move(max(0, height - 2), 0)
            screen.clrtoeol()
            shown = "*" * len(value) if secret else "".join(value)
            _safe_addstr(screen, max(0, height - 2), 0, f"{label}: {shown}")
            screen.refresh()
            try:
                curses.curs_set(1)
                key = screen.get_wch()
            finally:
                try:
                    curses.curs_set(0)
                except curses.error:
                    pass
            if key in ("\n", "\r", curses.KEY_ENTER):
                return "".join(value)
            if key == "\x1b":
                return default
            if key in (curses.KEY_BACKSPACE, "\b", "\x7f"):
                if value:
                    value.pop()
            elif isinstance(key, str) and key.isprintable():
                value.append(key)

    def _confirm(self, screen: Any, prompt: str) -> bool:
        return self._prompt(screen, f"{prompt} [y/N]", "").lower() == "y"

    def _overlay(
        self, screen: Any, lines: list[str], *, return_key: bool = False
    ) -> Any:
        screen.erase()
        height, width = screen.getmaxyx()
        for row, line in enumerate(lines[: max(1, height - 1)]):
            _safe_addstr(screen, row, 0, line[:width])
        screen.refresh()
        key = screen.get_wch()
        return key if return_key else None


def run_line_editor(
    state: EditorState,
    *,
    stdin: TextIO = sys.stdin,
    stdout: TextIO = sys.stdout,
) -> ExitCode:
    """Accessible line-oriented fallback with the same staged state model."""
    print(
        f"PA config > {state.instance_name} [{state.target_scope}] (line mode)",
        file=stdout,
    )
    print(
        "Commands: list, search TEXT, set KEY VALUE, toggle KEY, unset KEY, "
        "revert KEY|all, review, apply, refresh, help, quit",
        file=stdout,
    )
    while True:
        stdout.write("config> ")
        stdout.flush()
        raw = stdin.readline()
        if raw == "":
            return ExitCode.STAGED_NO_WRITE if state.staged else ExitCode.CANCELLED
        command, _, rest = raw.strip().partition(" ")
        try:
            if command in {"list", "l"}:
                for line in render_text(state, width=120, height=200)[2:-2]:
                    print(line, file=stdout)
            elif command == "search":
                state.query = rest
                state.cursor = 0
            elif command == "set":
                key, separator, value = rest.partition(" ")
                if not separator:
                    raise ConfigError("usage: set KEY VALUE")
                spec = get_field_spec(key)
                if spec.sensitive:
                    raise ConfigError(
                        "secret values require an interactive terminal to avoid scrollback"
                    )
                state.stage_raw(key, value)
            elif command == "toggle":
                state.toggle(rest)
            elif command == "unset":
                state.unset(rest)
            elif command == "revert":
                state.discard_all() if rest == "all" else state.revert(rest)
            elif command == "review":
                for key, before, after, impact in state.review_rows():
                    print(f"{key}: {before} -> {after} [{impact}]", file=stdout)
            elif command == "refresh":
                state.refresh()
            elif command == "apply":
                if any(get_field_spec(key).dangerous for key in state.staged):
                    print(
                        f"Disruptive change. Type APPLY {state.instance_name}: ",
                        end="",
                        file=stdout,
                    )
                    stdout.flush()
                    if stdin.readline().rstrip("\n") != f"APPLY {state.instance_name}":
                        print("Confirmation did not match.", file=stdout)
                        continue
                summary = state.apply()
                print(
                    f"Applied {len(summary.changed)} change(s); "
                    f"restart: {', '.join(sorted(summary.restart)) or 'none'}",
                    file=stdout,
                )
                return ExitCode.APPLIED
            elif command in {"help", "?"}:
                print(
                    "All edits are staged. review redacts secrets; apply is atomic. "
                    "External changes require refresh and explicit review.",
                    file=stdout,
                )
            elif command in {"quit", "q"}:
                if state.staged:
                    print("Discard staged changes? [y/N] ", end="", file=stdout)
                    stdout.flush()
                    if stdin.readline().strip().lower() != "y":
                        continue
                    return ExitCode.STAGED_NO_WRITE
                return ExitCode.CANCELLED
            elif command:
                raise ConfigError(f"unknown command: {command}")
        except ConfigConflictError as exc:
            print(f"Conflict: {exc}", file=stdout)
            state.refresh()
        except ConfigError as exc:
            print(f"Error: {exc}", file=stdout)


def run_config_editor(
    data_dir: Path,
    *,
    force_line: bool = False,
    stdin: TextIO = sys.stdin,
    stdout: TextIO = sys.stdout,
    env: dict[str, str] | None = None,
) -> ExitCode:
    state = EditorState.load(data_dir)
    capabilities = detect_terminal(stdin=stdin, stdout=stdout, env=env)
    if force_line or not capabilities.curses:
        if capabilities.reason:
            print(f"Using line-oriented config editor: {capabilities.reason}.", file=stdout)
        return run_line_editor(state, stdin=stdin, stdout=stdout)
    try:
        return CursesEditor(state, color=capabilities.color).run()
    except Exception as exc:
        # Curses setup failures are common in minimal SSH/container terminals.
        print(f"Curses unavailable ({exc}); using line-oriented editor.", file=stdout)
        return run_line_editor(state, stdin=stdin, stdout=stdout)

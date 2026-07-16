"""CLI for viewing and editing instance config.json."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Confirm, Prompt
from rich.table import Table

from pa.config import get_settings
from pa.domain.config_edit import (
    ConfigError,
    FIELD_SPECS,
    MutateResult,
    add_config_value,
    config_as_dict,
    format_value,
    get_field_spec,
    list_field_specs,
    refresh_after_mutate,
    remove_config_value,
    require_config,
    set_config_value,
    unset_config_value,
)
from pa.domain.instance_config import config_path

config_app = typer.Typer(
    help="View and edit instance config.json",
    no_args_is_help=False,
    invoke_without_command=True,
)

console = Console(stderr=False)


def _data_dir() -> Path:
    return get_settings().data_dir


def _echo_result(result: MutateResult, *, reveal: bool = False) -> None:
    spec = get_field_spec(result.key)
    before = format_value(result.before, reveal=reveal, sensitive=spec.sensitive)
    after = format_value(result.after, reveal=reveal, sensitive=spec.sensitive)
    typer.echo(f"{result.op.value} {result.key}: {before} → {after}")
    refreshed = refresh_after_mutate(_data_dir(), result)
    if refreshed:
        typer.echo("  Service unit environment refreshed.")
    if result.restart_required:
        typer.echo("  Restart required for bind host change — run: pa restart")


def _print_table(*, reveal: bool = False) -> None:
    data_dir = _data_dir()
    config = require_config(data_dir)
    data = config_as_dict(config)
    path = config_path(data_dir)

    table = Table(
        title=f"PA config — {path}",
        show_header=True,
        header_style="bold",
    )
    table.add_column("Key", style="cyan", no_wrap=True)
    table.add_column("Value")
    table.add_column("Notes", style="dim")

    for spec in list_field_specs():
        raw = data.get(spec.name)
        value = format_value(raw, reveal=reveal, sensitive=spec.sensitive)
        notes: list[str] = []
        if not spec.editable:
            notes.append("read-only")
        if spec.list_ops:
            notes.append("list")
        if spec.sensitive and not reveal:
            notes.append("masked")
        table.add_row(spec.name, value, ", ".join(notes))

    console.print(table)


@config_app.callback()
def config_callback(
    ctx: typer.Context,
    interactive: Annotated[
        bool,
        typer.Option(
            "--interactive",
            "-i",
            help="Open interactive config TUI",
        ),
    ] = False,
) -> None:
    """Manage instance configuration (config.json).

    Run with no subcommand (or ``-i``) for an interactive terminal UI.
    """
    if ctx.invoked_subcommand is not None:
        return
    if interactive or ctx.invoked_subcommand is None:
        try:
            run_interactive(reveal=False)
        except ConfigError as exc:
            typer.echo(str(exc), err=True)
            raise typer.Exit(1) from exc
        raise typer.Exit(0)


@config_app.command("show")
def show_cmd(
    reveal: Annotated[
        bool,
        typer.Option("--reveal", help="Show sensitive values in full"),
    ] = False,
    as_json: Annotated[
        bool,
        typer.Option("--json", help="Print raw JSON"),
    ] = False,
) -> None:
    """Show all config keys and values."""
    try:
        if as_json:
            config = require_config(_data_dir())
            data = config_as_dict(config)
            if not reveal:
                for name, spec in FIELD_SPECS.items():
                    if spec.sensitive and data.get(name):
                        data[name] = format_value(
                            data[name], reveal=False, sensitive=True
                        )
            typer.echo(json.dumps(data, indent=2))
        else:
            _print_table(reveal=reveal)
    except ConfigError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc


@config_app.command("get")
def get_cmd(
    key: Annotated[str, typer.Argument(help="Config key")],
    reveal: Annotated[
        bool,
        typer.Option("--reveal", help="Show sensitive values in full"),
    ] = False,
) -> None:
    """Print one config value."""
    try:
        spec = get_field_spec(key)
        config = require_config(_data_dir())
        value = getattr(config, key)
        typer.echo(format_value(value, reveal=reveal, sensitive=spec.sensitive))
    except ConfigError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc


@config_app.command("set")
def set_cmd(
    key: Annotated[str, typer.Argument(help="Config key")],
    value: Annotated[str, typer.Argument(help="New value (lists: comma-separated or JSON)")],
) -> None:
    """Set a config key (validated)."""
    try:
        result = set_config_value(_data_dir(), key, value)
        _echo_result(result)
    except ConfigError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc


@config_app.command("add")
def add_cmd(
    key: Annotated[str, typer.Argument(help="List config key")],
    value: Annotated[str, typer.Argument(help="Item to append")],
) -> None:
    """Append an item to a list config key."""
    try:
        result = add_config_value(_data_dir(), key, value)
        _echo_result(result)
    except ConfigError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc


@config_app.command("remove")
def remove_cmd(
    key: Annotated[str, typer.Argument(help="List config key")],
    value: Annotated[str, typer.Argument(help="Item to remove")],
) -> None:
    """Remove an item from a list config key."""
    try:
        result = remove_config_value(_data_dir(), key, value)
        _echo_result(result)
    except ConfigError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc


@config_app.command("unset")
def unset_cmd(
    key: Annotated[str, typer.Argument(help="Config key to reset")],
) -> None:
    """Reset a config key to its default / empty value."""
    try:
        result = unset_config_value(_data_dir(), key)
        _echo_result(result)
    except ConfigError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc


@config_app.command("keys")
def keys_cmd() -> None:
    """List known config keys and whether they are editable."""
    table = Table(show_header=True, header_style="bold")
    table.add_column("Key", style="cyan")
    table.add_column("Type")
    table.add_column("Editable")
    table.add_column("Description")
    for spec in list_field_specs():
        table.add_row(
            spec.name,
            spec.kind,
            "yes" if spec.editable else "no",
            spec.description,
        )
    console.print(table)


@config_app.command("edit")
def edit_cmd(
    reveal: Annotated[
        bool,
        typer.Option("--reveal", help="Show sensitive values in full"),
    ] = False,
) -> None:
    """Interactive terminal UI for managing config.json."""
    try:
        run_interactive(reveal=reveal)
    except ConfigError as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(1) from exc


def run_interactive(*, reveal: bool = False) -> None:
    """Rich-based interactive config manager."""
    data_dir = _data_dir()
    require_config(data_dir)  # fail fast if missing

    console.print(
        Panel.fit(
            "[bold]PA config[/bold]\n"
            "Commands: [cyan]s[/cyan]et  [cyan]a[/cyan]dd  [cyan]r[/cyan]emove  "
            "[cyan]u[/cyan]nset  [cyan]g[/cyan]et  [cyan]l[/cyan]ist  "
            "[cyan]v[/cyan]reveal  [cyan]h[/cyan]elp  [cyan]q[/cyan]uit",
            border_style="blue",
        )
    )
    _print_table(reveal=reveal)

    aliases = {
        "s": "set",
        "a": "add",
        "r": "remove",
        "u": "unset",
        "g": "get",
        "l": "list",
        "v": "reveal",
        "h": "help",
        "q": "quit",
    }

    while True:
        try:
            raw_choice = Prompt.ask(
                "\n[bold]config[/bold] [dim](s/a/r/u/g/l/v/h/q)[/dim]",
                default="l",
            ).strip().lower()
        except (EOFError, KeyboardInterrupt):
            console.print("\nBye.")
            return

        choice = aliases.get(raw_choice, raw_choice)

        if choice == "quit":
            console.print("Bye.")
            return

        if choice == "help":
            console.print(
                Panel(
                    "[bold]s / set[/bold]     Set a key to a value\n"
                    "[bold]a / add[/bold]     Append to a list key (peers, realms, …)\n"
                    "[bold]r / remove[/bold]  Remove an item from a list key\n"
                    "[bold]u / unset[/bold]   Reset a key to default/empty\n"
                    "[bold]g / get[/bold]     Print one key\n"
                    "[bold]l / list[/bold]    Refresh the config table\n"
                    "[bold]v / reveal[/bold]  Toggle masking of secrets\n"
                    "[bold]q / quit[/bold]    Exit",
                    title="Help",
                    border_style="dim",
                )
            )
            continue

        if choice == "list":
            _print_table(reveal=reveal)
            continue

        if choice == "reveal":
            reveal = not reveal
            console.print(f"Sensitive values {'shown' if reveal else 'masked'}.")
            _print_table(reveal=reveal)
            continue

        if choice == "get":
            key = Prompt.ask("Key")
            try:
                spec = get_field_spec(key)
                config = require_config(data_dir)
                value = getattr(config, key)
                console.print(
                    f"[cyan]{key}[/cyan] = "
                    f"{format_value(value, reveal=reveal, sensitive=spec.sensitive)}"
                )
            except ConfigError as exc:
                console.print(f"[red]{exc}[/red]")
            continue

        if choice == "set":
            key = Prompt.ask("Key")
            try:
                spec = get_field_spec(key)
            except ConfigError as exc:
                console.print(f"[red]{exc}[/red]")
                continue
            if not spec.editable:
                console.print(f"[red]{key} is read-only[/red]")
                continue
            hint = "true/false" if spec.kind == "bool" else (
                "comma-separated or JSON list" if "list" in spec.kind else "value"
            )
            if spec.kind.startswith("optional"):
                hint += " (empty/null to clear)"
            raw = Prompt.ask(f"Value ({hint})", password=spec.sensitive and not reveal)
            try:
                result = set_config_value(data_dir, key, raw)
                _print_mutate(result, reveal=reveal)
                _print_table(reveal=reveal)
            except ConfigError as exc:
                console.print(f"[red]{exc}[/red]")
            continue

        if choice == "add":
            key = Prompt.ask("List key", default="peers")
            raw = Prompt.ask("Item to add")
            try:
                result = add_config_value(data_dir, key, raw)
                _print_mutate(result, reveal=reveal)
                _print_table(reveal=reveal)
            except ConfigError as exc:
                console.print(f"[red]{exc}[/red]")
            continue

        if choice == "remove":
            key = Prompt.ask("List key", default="peers")
            raw = Prompt.ask("Item to remove")
            try:
                result = remove_config_value(data_dir, key, raw)
                _print_mutate(result, reveal=reveal)
                _print_table(reveal=reveal)
            except ConfigError as exc:
                console.print(f"[red]{exc}[/red]")
            continue

        if choice == "unset":
            key = Prompt.ask("Key to reset")
            try:
                get_field_spec(key)
                if not Confirm.ask(f"Reset [cyan]{key}[/cyan] to default?", default=False):
                    continue
                result = unset_config_value(data_dir, key)
                _print_mutate(result, reveal=reveal)
                _print_table(reveal=reveal)
            except ConfigError as exc:
                console.print(f"[red]{exc}[/red]")
            continue

        console.print(
            f"[red]Unknown command {raw_choice!r}[/red] — type [cyan]h[/cyan] for help"
        )


def _print_mutate(result: MutateResult, *, reveal: bool) -> None:
    spec = get_field_spec(result.key)
    before = format_value(result.before, reveal=reveal, sensitive=spec.sensitive)
    after = format_value(result.after, reveal=reveal, sensitive=spec.sensitive)
    console.print(f"[green]{result.op.value}[/green] {result.key}: {before} → {after}")
    refreshed = refresh_after_mutate(_data_dir(), result)
    if refreshed:
        console.print("[dim]Service unit environment refreshed.[/dim]")
    if result.restart_required:
        console.print("[yellow]Restart required — run: pa restart[/yellow]")

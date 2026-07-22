"""Release notes generation via configurable agent."""

from __future__ import annotations

import os
import re
import shlex
import subprocess
from datetime import UTC, datetime
from pathlib import Path

from pa.release.version import ROOT, tag_for_version
from pa.prompts import PROMPTS

TEMPLATE_PATH = ROOT / "docs" / "RELEASE_NOTES_TEMPLATE.md"
RELEASES_DIR = ROOT / "releases"

DEFAULT_AGENT_TIMEOUT = 300


def releases_dir() -> Path:
    RELEASES_DIR.mkdir(parents=True, exist_ok=True)
    return RELEASES_DIR


def notes_path_for_tag(tag: str) -> Path:
    return releases_dir() / f"{tag}.md"


def _run_git(*args: str) -> str:
    result = subprocess.run(
        ["git", *args],
        cwd=ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        msg = result.stderr.strip() or result.stdout.strip() or "git failed"
        raise RuntimeError(f"git {' '.join(args)}: {msg}")
    return result.stdout.strip()


def latest_tag() -> str | None:
    out = _run_git("describe", "--tags", "--abbrev=0")
    return out or None


def previous_tag(before: str | None = None) -> str | None:
    if before:
        try:
            return _run_git("describe", "--tags", "--abbrev=0", f"{before}^")
        except RuntimeError:
            return None
    tag = latest_tag()
    if not tag:
        return None
    try:
        return _run_git("describe", "--tags", "--abbrev=0", f"{tag}^")
    except RuntimeError:
        return None


def changelog_since(ref: str | None = None, until: str = "HEAD") -> str:
    """Return git log formatted for release notes."""
    args = ["log", "--pretty=format:- %s (%h)", until]
    if ref:
        args.append(f"{ref}..{until}")
    try:
        return _run_git(*args)
    except RuntimeError:
        return "(no commits)"


def render_template(
    *,
    version: str,
    tag: str,
    channel: str,
    changelog: str,
    template_path: Path | None = None,
) -> str:
    path = template_path or TEMPLATE_PATH
    if not path.exists():
        raise RuntimeError(f"Release notes template not found: {path}")
    text = path.read_text()
    today = datetime.now(UTC).strftime("%Y-%m-%d")
    return (
        text.replace("{{VERSION}}", version)
        .replace("{{TAG}}", tag)
        .replace("{{CHANNEL}}", channel)
        .replace("{{DATE}}", today)
        .replace("{{CHANGELOG}}", changelog)
    )


def build_agent_prompt(prefilled_template: str) -> str:
    return PROMPTS.render(
        "release.notes.generate",
        {"release": {"prefilled_template": prefilled_template}},
    ).text


def resolve_agent_timeout(timeout: float | None = None) -> float:
    """Resolve agent timeout in seconds (CLI/arg > env > default)."""
    if timeout is not None:
        return float(timeout)
    env = os.environ.get("PA_RELEASE_AGENT_TIMEOUT")
    if env:
        try:
            return float(env)
        except ValueError as exc:
            raise RuntimeError(
                f"Invalid PA_RELEASE_AGENT_TIMEOUT={env!r}; expected seconds"
            ) from exc
    return float(DEFAULT_AGENT_TIMEOUT)


def invoke_agent(
    prompt: str,
    *,
    agent_cmd: str | None = None,
    agent_args: str | None = None,
    timeout: float | None = None,
) -> str:
    """Run a configurable agent to generate release notes."""
    cmd = agent_cmd or os.environ.get("PA_RELEASE_AGENT", "agent")
    # --trust is required for headless Cursor agent; without it, --print hangs
    # waiting for an interactive workspace-trust prompt that never arrives.
    args_str = (
        agent_args
        if agent_args is not None
        else os.environ.get("PA_RELEASE_AGENT_ARGS", "--print --trust")
    )
    args = shlex.split(args_str)
    timeout_s = resolve_agent_timeout(timeout)

    use_stdin = os.environ.get("PA_RELEASE_AGENT_USE_STDIN", "").lower() in {
        "1",
        "true",
        "yes",
    }
    argv = [cmd, *args] if use_stdin else [cmd, *args, prompt]
    run_kwargs: dict = {
        "cwd": ROOT,
        "check": False,
        "capture_output": True,
        "text": True,
        "timeout": timeout_s,
    }
    if use_stdin:
        run_kwargs["input"] = prompt

    print(
        f"  Running: {cmd} {' '.join(args)}  (timeout {timeout_s:.0f}s)",
        flush=True,
    )
    try:
        result = subprocess.run(argv, **run_kwargs)
    except subprocess.TimeoutExpired as exc:
        raise RuntimeError(
            f"{cmd} timed out after {timeout_s:.0f}s while generating release notes. "
            "Retry with --no-agent, or raise the limit via --agent-timeout / "
            "PA_RELEASE_AGENT_TIMEOUT."
        ) from exc

    if result.returncode != 0:
        msg = result.stderr.strip() or result.stdout.strip() or "agent failed"
        raise RuntimeError(f"{cmd} {' '.join(args)}: {msg}")

    output = result.stdout.strip()
    if not output:
        raise RuntimeError("Agent returned empty release notes")
    return _strip_code_fences(output)


def _strip_code_fences(text: str) -> str:
    """Remove accidental markdown code fences wrapping the output."""
    stripped = text.strip()
    match = re.match(r"^```(?:markdown|md)?\s*\n(.*)\n```\s*$", stripped, re.DOTALL)
    if match:
        return match.group(1).strip()
    return stripped


def generate_release_notes(
    *,
    version: str,
    channel: str,
    tag: str | None = None,
    since_tag: str | None = None,
    template_path: Path | None = None,
    use_agent: bool = True,
    agent_cmd: str | None = None,
    agent_args: str | None = None,
    agent_timeout: float | None = None,
) -> str:
    tag = tag or tag_for_version(version)
    prev = since_tag or previous_tag()
    since_label = prev or "the beginning of history"
    print(f"==> Gathering changelog since {since_label}...", flush=True)
    changelog = changelog_since(prev)
    commit_lines = [line for line in changelog.splitlines() if line.startswith("- ")]
    print(f"  Found {len(commit_lines)} commit(s).", flush=True)

    prefilled = render_template(
        version=version,
        tag=tag,
        channel=channel,
        changelog=changelog,
        template_path=template_path,
    )
    if not use_agent:
        print("==> Skipping agent; using prefilled template.", flush=True)
        return prefilled

    timeout_s = resolve_agent_timeout(agent_timeout)
    print(
        f"==> Generating release notes via agent (up to {timeout_s:.0f}s)...",
        flush=True,
    )
    prompt = build_agent_prompt(prefilled)
    notes = invoke_agent(
        prompt,
        agent_cmd=agent_cmd,
        agent_args=agent_args,
        timeout=timeout_s,
    )
    print("  Agent finished.", flush=True)
    return notes


def write_release_notes(tag: str, content: str, *, path: Path | None = None) -> Path:
    dest = path or notes_path_for_tag(tag)
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(content.rstrip() + "\n")
    return dest


def read_release_notes(tag: str) -> str | None:
    path = notes_path_for_tag(tag)
    if not path.exists():
        return None
    return path.read_text()

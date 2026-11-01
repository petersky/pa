"""Real Git regression tests, isolated from host config and credential stores."""
from __future__ import annotations

import asyncio
import os
import shlex
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from pa.config import Settings
from pa.repository.state import GitInspectionError, GitInspector
from pa.repository.workspace import WorkspaceManager


@pytest.fixture
def isolated_git(tmp_path: Path, monkeypatch):
    # Redirect all config sources; never invoke a real credential helper.
    for key in list(os.environ):
        if key.startswith("GIT_") or key in {"SSH_ASKPASS", "DISPLAY"}:
            monkeypatch.delenv(key)
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HOME", str(home))
    monkeypatch.setenv("XDG_CONFIG_HOME", str(home))
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", str(tmp_path / "empty-global"))
    system = tmp_path / "system.gitconfig"
    monkeypatch.setenv("GIT_CONFIG_SYSTEM", str(system))
    monkeypatch.setenv("LC_ALL", "C")
    return system


def run_git(kind: str, tmp_path: Path, *args: str, check: bool = True) -> str:
    if kind == "workspace":
        manager = WorkspaceManager(
            Settings(data_dir=tmp_path / "data", workspace_root=tmp_path / "workspaces"),
            MagicMock(),
        )
        result = manager._git(*args, cwd=tmp_path, timeout=5, check=check)
        return result.stdout if result.returncode == 0 else result.stderr
    inspector = GitInspector(timeout=5)
    if kind == "inspector_async":
        return asyncio.run(inspector._run_async(tmp_path, *args, check=check))
    return inspector._run(tmp_path, *args, check=check)


@pytest.mark.parametrize("kind", ["workspace", "inspector", "inspector_async"])
def test_system_credential_helper_is_used(kind, tmp_path, isolated_git):
    # `credential fill` exercises Git's real helper lookup without a network or
    # secrets. The helper also proves terminal prompting remains disabled.
    helper = tmp_path / "dummy-helper"
    helper.write_text(
        '#!/bin/sh\n'
        '[ "$1" = get ] || exit 1\n'
        '[ "$GIT_TERMINAL_PROMPT" = 0 ] || exit 2\n'
        'printf "username=isolated-test\\npassword=dummy-only\\n"\n'
    )
    helper.chmod(0o700)
    isolated_git.write_text(
        '[credential]\n'
        f'    helper = "{shlex.quote(str(helper))}"\n'
        '[http]\n    proxy = http://proxy.invalid:8080\n'
        '    sslCAInfo = /isolated/test-ca.pem\n'
    )
    # Supply the protocol through a fixed command-line helper wrapper because
    # the production wrappers intentionally do not expose subprocess stdin.
    output = run_git(
        kind, tmp_path, "-c",
        "alias.test-credentials=!printf 'protocol=https\\nhost=fixture.invalid\\n\\n' | git credential fill",
        "test-credentials",
    )
    assert "username=isolated-test" in output
    assert "password=dummy-only" in output
    assert run_git(kind, tmp_path, "config", "--get", "http.proxy").strip() == "http://proxy.invalid:8080"
    assert run_git(kind, tmp_path, "config", "--get", "http.sslCAInfo").strip() == "/isolated/test-ca.pem"


@pytest.mark.parametrize("kind", ["workspace", "inspector", "inspector_async"])
def test_missing_credentials_fail_without_terminal_prompt(kind, tmp_path, isolated_git):
    isolated_git.write_text("")
    args = (
        "-c", "alias.test-credentials=!printf 'protocol=https\\nhost=fixture.invalid\\n\\n' | git credential fill",
        "test-credentials",
    )
    if kind == "workspace":
        error = run_git(kind, tmp_path, *args, check=False)
        assert any(message in error for message in (
            "unable to get password from user", "terminal prompts disabled",
        ))
    else:
        with pytest.raises(GitInspectionError, match="terminal prompts disabled"):
            run_git(kind, tmp_path, *args)

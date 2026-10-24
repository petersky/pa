"""Status timing includes every completed status probe, without real CLIs."""
from pathlib import Path
from types import SimpleNamespace

import pytest

import pa.acp.providers.codex as module


@pytest.mark.parametrize('cli_installed', [False, True])
def test_codex_status_duration_includes_final_cli_version(tmp_path, monkeypatch, cli_installed):
    elapsed = [0.0]
    calls = []
    monkeypatch.setattr(module, 'time', SimpleNamespace(perf_counter=lambda: elapsed[0]))
    monkeypatch.setattr(module.CodexProvider, 'resolve_spawn', lambda self, **kwargs:
                        SimpleNamespace(env={}, command='fixture-adapter', args=[]))
    monkeypatch.setattr(module, 'resolve_executable', lambda command: Path('/fixture/adapter'))
    monkeypatch.setattr(module, 'load_metadata', lambda *args: None)
    monkeypatch.setattr(module, 'load_credentials', lambda *args: {})
    monkeypatch.setattr(module, 'resolve_codex_cli', lambda *args:
                        '/fixture/cli' if cli_installed else None)
    monkeypatch.setattr(module, 'get_codex_login_store', lambda *args:
                        SimpleNamespace(latest_active=lambda: None))

    def auth(*args, **kwargs):
        calls.append('auth')
        elapsed[0] += .013
        return False, 'none', 'not signed in', None

    def version(command):
        assert command[1:] == ['--version']
        cli = command[0] == '/fixture/cli'
        calls.append('cli-version' if cli else 'adapter-version')
        elapsed[0] += .200 if cli else .030
        return 'fixture-cli-version' if cli else 'fixture-adapter-version'

    monkeypatch.setattr(module, '_codex_auth_status', auth)
    monkeypatch.setattr(module, '_run_version', version)
    result = module.CodexProvider().status(tmp_path)

    assert result.probe_duration_ms == pytest.approx(elapsed[0] * 1000)
    assert result.probe_duration_ms == pytest.approx(243 if cli_installed else 43)
    assert calls == ['auth', 'adapter-version'] + (['cli-version'] if cli_installed else [])
    assert result.version == 'fixture-adapter-version'
    assert result.codex_cli_version == ('fixture-cli-version' if cli_installed else None)
    assert result.auth_state == ('signed_out' if cli_installed else 'unavailable')
    assert result.auth_configured is False
    assert result.available is True and result.installed is True

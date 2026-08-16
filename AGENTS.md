# AGENTS.md

## Cursor Cloud specific instructions

PA is a single Python application (package `pa` under `src/pa/`) that exposes four peer
interfaces to one core engine: a FastAPI + HTMX web server (`pa serve`), a CLI (`pa`),
an MCP server (`pa mcp`), and an ACP client. Persistence is embedded (SQLite + JSON
files under the data dir) — there is no separate database, cache, or broker to start.

### Environment
- Python **3.14** managed by **uv** (see `.python-version`, `pyproject.toml`). The update
  script installs `uv`, Python 3.14, and runs `uv sync`, so dependencies are ready on boot.
- `uv` lives in `~/.local/bin`; if `uv` is not found, run `export PATH="$HOME/.local/bin:$PATH"`.
- Prefix commands with `uv run` so they use the project virtualenv (`.venv`).

### Run / lint / test / build
- Run the server (dev): `uv run pa serve --host 127.0.0.1 --port 8081`. There is no
  separate lint step configured in CI; the CI "checks" are the boot smoke test + pytest
  (see `.github/workflows/ci.yml`). Release PRs (`release/v*`) skip the pytest shards.
- Boot smoke test: `uv run python -c "from pa.core.kernel import Kernel; Kernel.boot().build_app()"`.
- Tests: `uv run pytest -q` (serial) or `uv run pytest -n auto --dist worksteal`
  (parallel; CI uses this plus two shards). Chrome dump-dom layout tests are
  ignored in CI.
- Build artifacts: `uv build`.

### Non-obvious caveats
- **Isolate instance state with `PA_DATA_DIR`.** For a throwaway dev instance, set
  `PA_DATA_DIR=.dev/pa-data`, then `uv run pa init --name dev` once before `pa serve`.
  This avoids touching the host default `~/.pa`.
- **Disable the ACP agent in this VM:** set `PA_AGENT_ENABLED=false` before `pa serve`.
  The default (`true`) tries to spawn an external `agent`/`codex` CLI that is not installed
  here and is unnecessary for web/API testing.
- **Mutating REST endpoints require CSRF + idempotency.** POST/PATCH need a double-submit
  CSRF token (cookie `pa_csrf` echoed in the `X-CSRF-Token` header — grab it from any GET
  with a cookie jar) and creation endpoints also require an `Idempotency-Key` header. The
  web UI handles both automatically, so prefer the UI for manual testing.
- **Headless-Chrome layout tests are excluded from CI.** `tests/test_header_layout.py`
  and `tests/test_fleet_topology_layout.py` spawn `google-chrome --dump-dom`; Chrome
  can hang until timeout under Firecracker and is not part of the GitHub suite.
  Run them locally only when Chrome is installed.

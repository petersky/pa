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
  (parallel; CI uses this plus four shards). Chrome dump-dom layout tests are
  ignored in CI.
- Local package artifacts: `uv build` (wheel/sdist already exclude repository evidence).
- Exact revision builds across workers: `uv run python scripts/build_verified.py --revision <commit> --output .dev/wheels-<short-sha>`.
  Use this selective streaming exporter for integrated/rollback builds; it exports only package/build inputs,
  enforces a 64 MiB source budget, removes temporary source on success/failure, and retains the wheel
  plus source SHA/input bytes/wheel hash attestation. Reuse uv's shared dependency cache.
  Never create a full repository tar plus extracted tree for a package build. For baseline test runs,
  select only the particular tests/scripts needed in a disposable context; do not copy benchmark evidence.
- Historical benchmark retention: see `docs/benchmarks/README.md`. Keep compact useful summaries;
  raw dumps and large historical snapshots belong outside the tracked tree during a run and should
  be removed after summarizing. Do not recreate retired archives, chain retained parents, or attach
  replacement copies. Existing Git history provides recovery. Each worker retains its isolated leased
  worktree; update idle owners through supported PA lifecycle rather than editing their checkout.

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

## Releasing

**Always cut a release with `./scripts/release.sh`; never hand-roll the version bump, PR or tag.**
The script (`src/pa/release/script.py`) does the whole flow: bumps the version
(`pyproject.toml`, `src/pa/__init__.py`), writes `releases/vX.Y.Z.md` with agent-generated
notes, creates the `release/vX.Y.Z` branch and PR, then (with `--ship`) waits for checks,
merges, tags `vX.Y.Z`, and publishes the GitHub release. Pushing the tag is what publishes, so
it is outward-facing: only run `--ship` or `--publish` when the user has asked for a release.

```bash
./scripts/release.sh patch|minor|major --dry-run   # show the plan without doing anything
./scripts/release.sh minor                         # prepare the release PR, then prompt to ship
./scripts/release.sh minor --ship                  # PR, checks, merge, tag, publish
./scripts/release.sh --publish --tag v1.7.0        # tag + publish an already-merged release PR
./scripts/release.sh --amend --tag v1.7.0          # amend notes for an existing release
```

- It is resumable: re-running it for the same version reuses the existing `release/v*` branch,
  commit and notes. It refuses to run if the checkout is behind `origin/main`.
- Choose the bump by behavior: `patch` for fixes, `minor` for user-visible changes or changed
  defaults. Pre-release tracks: `alpha`, `beta`, `rc`, or an explicit semver with `--channel`.
- `--no-agent` skips the notes agent and uses the template (`docs/RELEASE_NOTES_TEMPLATE.md`);
  other flags are listed in `./scripts/release.sh --help`.
- After the tag is pushed, `.github/workflows/release.yml` builds the wheel, creates the release
  and opens a `chore/channels-vX.Y.Z` PR that updates `channels.json` (it cannot be in the release
  PR: the pointer must name a tag that already exists). That PR's CI is held for approval, which
  does not block merging it. `--ship` merges it for you after checking it changes only that one
  pointer; pass `--skip-channels` to leave it. Without `--ship`, merge it yourself, or `pa update`
  will not see the new version.
- `release/v*` PRs skip the pytest shards in CI (only the smoke check runs), so run the tests
  before releasing.

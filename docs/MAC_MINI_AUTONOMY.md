# Mac mini autonomous-operation runbook

This runbook separates controls PA can continuously enforce from assertions that
require a real login, reboot, network, or credential provider. Run it as the
dedicated macOS account that owns the PA service and `PA_DATA_DIR`.

## Enforced controls

- The user LaunchAgent uses `RunAtLoad`, unconditional `KeepAlive`, a 10-second
  restart throttle, a 300-second supervisor exit ceiling, and bounded file
  descriptors. The ceiling covers PA's per-module teardown, completion drain,
  ACP quiesce, and provider/browser stop budgets without cutting them short.
- Application events are written as rotating JSON Lines to
  `~/.pa/logs/pa.jsonl` (10 MiB, five backups). launchd stdout/stderr remain in
  `server.log` and `server.err.log` for early-start failures.
- `config.json` and provider credential files are atomically written with mode
  `0600`. Credentials stay on the target host and are never included in fleet
  events, card context, or capability responses.
- Module shutdown, ACP quiesce, provider process shutdown, and completion-outbox
  draining are time-bounded. launchd restarts unexpected exits.
- Remote card dispatch fails closed unless the exact authoritative card version
  is materialized on the selected target. Completion is idempotent and only the
  authority moves the card to done.
- Updates require an interactive confirmation unless `--yes` is explicit. A
  service restart requires `--restart` or a separate operator decision. Agents
  must not install, restart, update, change credentials, or widen repository
  capability merely because a card requests it.

## Credential readiness

Provider credentials must be configured for the LaunchAgent's owning account,
not only an interactive shell:

```bash
pa agent-provider status codex
pa doctor
```

GitHub supervision requires a token in `PA_GITHUB_TOKEN` at service launch or in
`~/.pa/integrations/github.json`, plus an explicit `allowed_repositories` entry
for `petersky/pa`. Use the minimum repository permissions needed to read checks,
reviews, branch protection and threads, create/update pull requests, push the
card branch, and merge. Capability output is safe to record; never record token
values or credential-file contents.

## Manual acceptance after deployment

These assertions intentionally are not automated in CI because they alter the
production login session, power state, credentials, or network:

1. Log out and back in. Confirm `launchctl print gui/$(id -u)/com.pa.server`,
   `pa status`, and `/api/health` all report the service running.
2. Reboot the Mac mini. Without opening a terminal, confirm the same checks from
   another fleet peer. A user LaunchAgent starts after that user logs in; enable
   automatic login only if its physical-security tradeoff is accepted. For true
   pre-login service, migrate deliberately to a system LaunchDaemon.
3. Kill only the PA server PID and verify launchd restarts it after throttling.
   Never run this while an update or active card session is quiescing.
4. From a remote peer, start a disposable card-scoped Codex session and verify
   exact card title/body, target worktree path, authoritative completion, and
   cleanup. CI covers the same contract with disposable data/repository/worktree.
5. Run `pa doctor`; verify Codex authentication and GitHub capability for
   `petersky/pa`, then open a harmless supervised PR and observe a full stable
   green refresh before merge.
6. Temporarily remove network access and verify health failure is observable,
   logs remain bounded, pending completion survives restart, and recovery does
   not require deleting `PA_DATA_DIR`.

## Recovery

Preserve `PA_DATA_DIR` before repair. Inspect `pa.jsonl`, `server.err.log`,
`pa status`, and `pa doctor`; fix ownership or credentials in place. Reinstalling
the service unit is safe, but do not delete dispatch, event-log, or supervisor
databases. Roll back by installing the last known-good release tag, re-registering
the service unit, and restarting after bounded quiesce.

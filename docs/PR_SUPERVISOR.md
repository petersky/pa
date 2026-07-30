# Pull request supervisor

PA's pull request supervisor is an always-on control-plane service. It owns the
long-running observation and executor-wake lifecycle; an advisor or coordinator
session is not involved and may be offline.

## What is persisted

Each verified watch persists the exact canonical realm, repository, project,
card, dispatch, originating principal, executing instance, and agent-session IDs,
alongside the GitHub PR, current head SHA, executor worktree, copied policy,
required capabilities, lease owner/fence, current GitHub snapshot, polling state,
and append-only audit history. State lives in
`<PA_DATA_DIR>/pr_supervisor.db` and is recovered on restart.

The canonical live/actionable view contains only `active` or `blocked` watches
whose `retired_at` is null. `merged` and `closed` preserve the terminal GitHub
outcome, but terminalization also sets `retired_at` atomically and releases the
owner and lease. Generic operator retirement uses `retired`. All three terminal
outcomes remain available through `include_retired=true`, the Pull requests
audit UI, linked cards/sessions, and watch event history. Historical
`last_error`, snapshots, provenance, and evidence are retained; they do not
contribute Fleet topology edges or health.

At startup PA may discover an exact
`https://github.com/OWNER/REPO/pull/NUMBER` URL in an open card. Discovery can
create an **unlinked** legacy watch, but it never turns the surrounding card,
branch, path, label, prompt, or URL into provenance. An operator must explicitly
relink an old watch to one canonical session before merged-card disposition is
allowed.

## Canonical provenance and repair

Workspace directories and Git branch names intentionally remain compact. Their
card/session components are non-authoritative display and storage slugs; they
may be shortened and hash-suffixed. No durable identity is parsed from them.

A session-backed watch is accepted only through the session's owning/executing
instance. The server resolves version-1 provenance from the durable session,
its structured execution context, and (for remote work) the durable dispatch
record. Caller-supplied IDs are comparison assertions only. PA rejects before
creating a watch when an ID is shortened, malformed, noncanonical, absent from
the named realm, or mismatched across session/card/project/repository/dispatch.
The accepting lease authority never replaces the true executing instance ID.
The same validation runs for fleet replicas, authority lease recovery,
retirement forwarding, and executor dispatch.

Use this read-only diagnostic endpoint to locate historical unverified or
shortened values without rewriting Store or EventLog history:

```
GET /api/pr-supervisor/provenance/issues
```

Repair requires an explicit, full canonical session ID and an idempotency key:

```
POST /api/pr-supervisor/watches/WATCH_ID/provenance/repair
{
  "originating_session_id": "FULL-SESSION-UUID",
  "idempotency_key": "operator-ticket-or-command-id"
}
```

The server resolves every linked entity again, refuses ambiguous or mismatched
relationships, updates only the watch projection, and appends a
`provenance_repaired` audit event containing before/after values and
`guessed: false`. Repeating an already-applied repair is a no-op. MCP exposes the
same workflow as `diagnose_pr_watch_provenance` and
`repair_pr_watch_provenance`.

## Lease authority and worker failover

`PA_PR_SUPERVISOR_AUTHORITY_URL` (or the persisted
`pr_supervisor_authority_url`) selects the single lease authority independently
of the fleet owner. When unset, PA retains the legacy `fleet_owner_url` behavior.
Point it at an always-on Mac mini or Monica; do not use a laptop that routinely
sleeps. The configured authority treats its own advertised URL as local and
uses its replicated SQLite state as the compare-and-swap boundary.

Every capable instance advertises a
secret-free GitHub capability heartbeat. A worker obtains an atomic renewable
lease from the authority before polling or changing watch state:

- lease acquisition uses SQLite `BEGIN IMMEDIATE` compare-and-swap;
- a new owner receives a monotonically increasing fencing token;
- every observation/terminal update must present the current unexpired fence;
- an expired worker cannot publish stale results;
- replica updates preserve newer and terminal authority state;
- executor dispatch uses a fleet-stable event key and an atomic destination claim.

Ordinary replicas never let an older terminal snapshot overwrite newer active
state. Operator retirement is propagated as a separate idempotent fleet
transition, so it does not depend on snapshot timestamps to take effect. The
transition preserves each peer's richer observation data and never downgrades
an already merged or closed watch. Terminal replicas and retirement transitions
cannot reattach an owner or lease.

An explicit connection failure may fail over to a replacement executor. An
ambiguous response failure never falls back to a second instance because the
origin may already have queued the prompt; PA retries the same idempotent event
at the intended destination instead.

If the worker owner disappears, another eligible authenticated instance claims
the expired lease. If the fleet authority or every eligible credential is
unavailable, the watch becomes visibly blocked with an actionable reason. PA
does not silently drop supervision.

`GET /api/pr-supervisor/health` exposes the authority URL and role, explicit vs
legacy selection, authority reachability, last successful contact, active/local
watch counts, historical/archive counts, the terminal-retirement backlog, lease
TTL, and the largest observed fence token. It never returns tokens.
`authority_unreachable` is actionable; `authority_unverified` is normal only
before the first heartbeat or lease request after startup.

Credentials remain instance-local. Tokens and webhook secrets are never copied
into watches, audit events, prompts, fleet heartbeats, or sync objects.

### Safe authority migration

Authority selection is intentionally not automatic: two independent SQLite
authorities could both grant a valid-looking lease. Migrate in a short,
fail-closed maintenance window so no split brain is possible:

1. Deploy this PA version to every instance without changing authority config.
   On the intended always-on authority, configure its own GitHub credential and
   repository allowlist; do not copy the MacBook credential.
2. Verify every active watch is replicated to the new authority. Compare active
   watch count, watch IDs, and `max_fence_token` from
   `/api/pr-supervisor/health` and `/api/pr-supervisor/watches` on old and new.
   Stop if the new authority is missing a watch or has a lower fence.
3. Quiesce PA on all supervisor-eligible instances with bounded `pa stop`.
   This is the fencing barrier. Record the stop time and wait at least 45 seconds
   (the reported `lease_ttl_seconds`) after the last instance stopped. If the
   MacBook is already unreachable, its last leases still must age past TTL.
4. While services are stopped, persist the same always-on URL everywhere:

   ```bash
   pa config set pr_supervisor_authority_url http://always-on-mini:8080
   pa install --service-only --no-start
   ```

   `--no-start` is required here: it regenerates the unit environment without
   bootstrapping the stopped service during the fencing barrier.
5. Start the new authority first. Confirm health says `role=lease_authority`,
   `state=ready`, and its fence baseline is no lower than the recorded value.
   Start one authenticated worker and force a watch refresh. Its newly granted
   fence must be greater than the pre-migration fence.
6. Start remaining workers one at a time. Verify each reports the same
   `authority_url`. Leave the former MacBook stopped until its config has been
   changed; an old authority must never rejoin with legacy configuration.
7. Observe an active watch for longer than 45 seconds with the MacBook offline.
   Confirm fresh observation timestamps, successful renewals/polls, and no
   `authority_unreachable` state. Only then end the maintenance window.

Rollback uses the same stop-all, TTL-drain barrier. Never point a subset back to
the old authority while any worker can still reach the new one.

### Backfill legacy terminal watches

After every fleet instance is running this version, invoke the migration on the
configured PR-supervisor lease authority. It re-reads each legacy `merged` or
`closed` PR from GitHub and archives only when the observed terminal outcome
matches the stored outcome. The first request is a read-only preview:

```http
POST /api/pr-supervisor/migrations/terminal-retirements
{"realm_id": "default", "dry_run": true}
```

Review `counts`, `results`, and
`GET /api/pr-supervisor/health`. For the reported legacy realm, the preview
should report 85 `would_archive` candidates and the health backlog should be
85. Apply the same operation with `"dry_run": false`; the resulting
`watch_archived` events record the migration reason, revalidated GitHub state,
terminal outcome, and retirement time. The authority replicates each archived
watch and broadcasts a separate idempotent retirement transition so peers
preserve richer local evidence while converging on the same archive time.

Rerunning the apply is safe: archived watches are no longer candidates and
stable audit event keys prevent duplicates. Verify the backlog is zero, the
default watch list contains only actionable watches, and
`include_retired=true` still returns all history. This migration writes only
the PR-supervisor projection through its service/control-plane API; it does not
write realm EventLog/Store data and therefore does not create competing realm
sync heads. MCP exposes the same preview/apply operation as
`backfill_terminal_pr_watches`.

## GitHub authentication

Configure either environment variables:

```bash
PA_GITHUB_TOKEN=github_pat_...
PA_GITHUB_WEBHOOK_SECRET=replace-with-a-random-secret
```

or an owner-only file at
`<PA_DATA_DIR>/integrations/github.json`:

```json
{
  "token": "github_pat_...",
  "webhook_secret": "replace-with-a-random-secret",
  "allowed_repositories": ["owner/repo"]
}
```

`allowed_repositories` is optional. When present, this instance will not claim
watches outside the allowlist. The capability API/UI reports authentication,
webhook configuration, allowlist, and corrective guidance without secrets:

```
GET /api/pr-supervisor/capabilities
```

## Webhooks and polling

Configure the repository webhook URL as:

```
https://PA-INSTANCE/api/pr-supervisor/webhook/github
```

Use `application/json`, the same webhook secret, and subscribe to pull request,
review, check run/suite, workflow run, and status events. PA verifies the raw
body with `X-Hub-Signature-256` HMAC-SHA256 using constant-time comparison.
Invalid, unsigned, or oversized deliveries are rejected.

Webhooks schedule an immediate observation. Bounded polling remains enabled as
the reliable fallback, with exponential backoff, jitter, and policy-controlled
minimum/maximum intervals. A webhook is an invalidation hint, not trusted state;
the supervisor always re-reads GitHub. Every realm watch associated with that
same repository and PR is independently audited and the invalidation is
replicated across the fleet.

## Gate and executor behavior

For a fixed, stable head SHA, PA observes:

- draft/open/closed/merged state;
- required and optional check runs plus status contexts, conclusions, output
  excerpts, and details/log URLs;
- latest review decisions and approval count;
- unresolved review threads with file, line, author, comment, and URL;
- branch protection requirements;
- mergeability/conflicts and merge commit.

Notifications are keyed by head SHA, condition fingerprint, and transition
version. Unchanged conditions do not duplicate prompts. A condition changing
away and later returning is re-armed. Results are discarded if the head changes
during an observation.

When work is required, PA resumes the originating executor session, queues the
prompt if it is busy/idle, or starts a card-scoped replacement on the responsible
instance. If that instance is unavailable, the supervising eligible worker
starts the replacement. Prompts include exact failing checks and inline review
context.

GitHub text is untrusted. PA bounds and redacts it, escapes delimiter-breaking
text, and places it inside an explicit `github_external_content` data boundary.
External comments/check logs are never used as privileged instructions.

PA only emits the green instruction after:

- the PR is non-draft and open;
- the head is unchanged for the configured time and observation count;
- required checks are terminal success or an explicitly allowed neutral result;
- required approvals are satisfied;
- no unresolved actionable review thread remains;
- branch protection is known;
- GitHub reports a clean, non-ambiguous merge state.

The executor must independently re-fetch and revalidate all signals and the exact
head before merging. The service never bypasses branch protection and never
merges an ambiguous/pending PR itself. After GitHub reports the merge, PA records
the merge commit, moves the card to Done, notifies the executor, and retires
active polling. Worktree cleanup remains the executor's responsibility under the
project repository rules. The fenced terminal watch update happens before card
completion; a failed card projection update is durably visible and retried by
the always-on supervisor.

## Policy and controls

Project defaults live in `Project.tool_config.pr_policy`; repository overrides
live in `Project.tool_config.pr_repository_policies[OWNER/REPO]`:

```json
{
  "ready_by_default": true,
  "auto_notify": true,
  "agent_merge_on_green": true,
  "integration_branch": "main",
  "required_checks": [],
  "allowed_neutral_conclusions": ["neutral", "skipped"],
  "required_approvals": null,
  "stable_head_seconds": 15,
  "stable_observations": 2,
  "poll_min_seconds": 15,
  "poll_max_seconds": 300
}
```

Associated PR creation defaults to ready for review. Draft creation remains
possible only when a caller explicitly passes `draft: true`.

REST controls are under `/api/pr-supervisor` for watch CRUD/refresh, history,
policy, ready PR creation, capabilities, metrics, fleet replica/lease/dispatch,
webhook receipt, and terminal-retirement migration. Linked watch state is also
included in durable agent-session history responses. The Pull requests UI
exposes status and audit history on the PR page, linked card, and linked
agent-session list. MCP provides:

- `list_pr_watches`, `get_pr_watch`, `create_pr_watch`;
- `refresh_pr_watch`, `retire_pr_watch`, `backfill_terminal_pr_watches`;
- `create_supervised_pull_request`;
- `set_project_pr_policy`;
- `diagnose_pr_watch_provenance`, `repair_pr_watch_provenance`;
- `github_integration_capability`.

Operational counters include active watches, polls, leases, webhooks, audit
events, executor prompts, merged and backfilled watches, historical/archive
counts, the terminal-retirement backlog, stale fences, and poll/dispatch/
replication/loop failures.

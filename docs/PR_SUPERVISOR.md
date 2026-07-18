# Pull request supervisor

PA's pull request supervisor is an always-on control-plane service. It owns the
long-running observation and executor-wake lifecycle; an advisor or coordinator
session is not involved and may be offline.

## What is persisted

Each watch links a realm, project, card, GitHub repository and PR, current head
SHA, originating PA instance/session/agent, executor worktree, copied policy,
required capabilities, lease owner/fence, current GitHub snapshot, polling state,
and a complete append-only audit history. State lives in
`<PA_DATA_DIR>/pr_supervisor.db` and is recovered on restart.

PA safely migrates discoverable associations at startup. An open card containing
an exact `https://github.com/OWNER/REPO/pull/NUMBER` URL gets a watch if none
exists. PA does not guess from branch names or issue URLs.

## Fleet ownership and failover

The fleet owner is the lease authority. Every capable instance advertises a
secret-free GitHub capability heartbeat. A worker obtains an atomic renewable
lease from the authority before polling or changing watch state:

- lease acquisition uses SQLite `BEGIN IMMEDIATE` compare-and-swap;
- a new owner receives a monotonically increasing fencing token;
- every observation/terminal update must present the current unexpired fence;
- an expired worker cannot publish stale results;
- replica updates preserve newer and terminal authority state;
- executor dispatch uses a fleet-stable event key and an atomic destination claim.

If the worker owner disappears, another eligible authenticated instance claims
the expired lease. If the fleet authority or every eligible credential is
unavailable, the watch becomes visibly blocked with an actionable reason. PA
does not silently drop supervision.

Credentials remain instance-local. Tokens and webhook secrets are never copied
into watches, audit events, prompts, fleet heartbeats, or sync objects.

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
the supervisor always re-reads GitHub.

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
project repository rules.

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
and webhook receipt. Linked watch state is also included in durable agent-session
history responses. The Pull requests UI exposes status and audit history on the
PR page, linked card, and linked agent-session list. MCP provides:

- `list_pr_watches`, `get_pr_watch`, `create_pr_watch`;
- `refresh_pr_watch`, `retire_pr_watch`;
- `create_supervised_pull_request`;
- `set_project_pr_policy`;
- `github_integration_capability`.

Operational counters include active watches, polls, leases, webhooks, audit
events, executor prompts, merged watches, stale fences, and poll/dispatch/
replication/loop failures.

# Operational problem journal

The PA server owns `health_journal.db` alongside its other instance files. The
journal uses separate SQLite FULL/WAL transactions and a reserved two-thread
executor. It does not read canonical history to record, page, gather, or display
an observation. Module startup opens it only under the normal server writer lock;
stdio MCP registration does not open it. A storage failure reports an unavailable
journal without preventing the rest of PA from starting.

## Reporting and custody

Use `report_pa_problem(idempotency_key, observation)` for unexpected PA failures,
repeated timeouts, inconsistent state, manual workarounds, or unresolved systemic
risk. Use the same occurrence key for related new evidence and the same
idempotency key when retrying an uncertain write. Each new accepted observation
appends a revision and hash; retries return the original receipt. Exclude secrets,
private answers, and transcript dumps. Reporting failure must not recursively
report itself or block the primary task. Every provider passes through the common
session prompt boundary containing this instruction.

`list_pa_problems` and `get_pa_problem` read original records.
`get_pa_problem_group` reads an authority grouping, its current CAS version and
history. `update_pa_problem` records a reasoned disposition and canonical repair
links using that version. Report and group IDs are distinct. Source UI at
`/health-journal` displays custody, immutable history, disposition, card, PR,
commit and acceptance. Gathered means durable delivery, not resolved.

HTTP routes live under `/api/health-journal`. Normal user/CLI and assigned-session
authentication bind the reporting principal on the server. Operational records are shared among
readers authorized for that realm; a signed assigned session remains bound to its
exact live session realm, principal and dispatch. Group triage/configuration is
administrator-only. Cookie writes retain normal CSRF protection. Shared fleet
authentication admits only narrow outbox/receipt/handover routes; the collector
binds responses to configured peer endpoints and source identity. A source
verifies gathered receipts at its configured authority endpoint before recording
custody. Agent-supplied identity or authority fields cannot override this binding.

Requests are capped at 32 KiB, revisions at 16,000 encoded characters, report
pages at 32 entries, collection pages at 8 entries, and peer responses at 256 KiB.
Collection takes at most two pages per source, defaults to 16 available sources
per tick, rotates remaining sources fairly, and uses bounded network/cycle
timeouts. Inbox commit precedes source acknowledgement. Lost replies replay the
same revision identity. Custody changes propagate independently after gathering,
so later no-fix, duplicate, repair and acceptance decisions reach original records.

## Explicit authority and normal repair workflow

New instances have no configured authority and scheduling disabled. Activation
is an explicit administrator operation, not election or partition failover:

1. Read `GET /status` for the current policy/version.
2. `PATCH /config` with `expected_version` and a complete policy selecting the
   same authority instance on every fleet member. Configure its existing repair
   project. The server binds the dispatch principal to the authenticated admin.
3. Enable the designated authority after the fleet configuration is consistent.
   Default collection is 60 seconds (allowed range 30–3600). `paused` suspends new
   cycles. `POST /run` requests one bounded cycle and rejects overlap.

The durable lease/epoch/fence protects one coordinator. Exactly one nonterminal
health action is retained globally. Each action durably reserves its exact normal
card/dispatch idempotency key and payload before the external effect. A previously
admitted effect may finish after pause or lease loss; its slot stays occupied and
recovery reuses its identity. Pause does not falsely claim cancellation of work
already admitted. Dispatch uses the existing fleet engine and fresh repository
workspace. Existing session lifecycle, completion delivery, recoverability and PR
supervisor obligations retain the action until authoritative terminal evidence.
Queued, permission-waiting and CI time never cause dispatch-age cancellation.
The triage prompt requests a bounded assessment before edits; execution/followup
budgets remain owned by existing PA/provider policy.

Health-created investigation cards initially use ordinary source-only completion.
The same worker records a reasoned no-fix disposition and can finish that ordinary
card with the existing no-integration Done outcome. It does not manufacture
acceptance evidence or erase requirements on a previously protected repair.

Before acknowledging reproduced, linked, in_progress or merged (or any transition
supplying commit/PR evidence), PA installs or proves an explicit canonical
verification requirement on the actual action's bound card and realm. It preserves
existing owners, criteria and milestones and adds the health group verification
scope. The exact normal API/CAS declaration is reserved in the existing durable
action before submission; unknown/lost replies replay that request and cannot
acknowledge repair permission. The prompt requires successful transition before
edits/PR work. This is the authorized workflow contract, not an OS security boundary.
The initial repair declaration grants no automatic acceptance principals.
Until an owner is declared, a verification attempt returns typed
`acceptance_owner_unconfigured`; declared but unaccepted work returns
`awaiting_acceptance`. An already-authorized coordinator reads the group's retained action and the actual
referenced durable dispatch record. Through the ordinary authorized current-card
completion requirement update (with current expected version), it declares BOTH
eligible `acceptance_principals` and the actual `originating_session_id` /
`originating_dispatch_id`; an owner-principal-only update is insufficient. Preserve
the current requirement's other fields and use its resulting canonical revision.
No extra human permission step or automatic deployment is implied. If actual repair
identity or an eligible deployment owner is unavailable, leave acceptance pending.
An independent live ordinary session/dispatch for the same card and realm can then
call `record_card_acceptance(card_id, realm, expected_version,
requirement_revision, subject_revision, milestones, references, idempotency_key)`.
Its private SessionAcceptance bridge derives and stamps the actor identity without
Goal provenance. Original repair identities, including retained prior group action
origins, remain ineligible; callers cannot supply an actor identity. This does
not block source-fix construction, ready PRs, or immediate repair backlinks. The journal requires a current canonical accepted receipt with
matching subject and `health-group:`, `scenario:`, `instance:` references. A merge,
legacy Done lane or forged reference cannot certify deployment. Canonical receipts
must identify a verified human actor or an authenticated bound session/dispatch.
The journal rejects the originating repair session or dispatch, including earlier
actions for the same group, and fails closed when origin evidence exceeds its bound
or is unavailable. Shared bearer credentials alone do not prove independence. Whole-group acceptance
requires the actual current source-instance set to be covered by the canonical
receipt. Every novel inbox revision advances the group CAS version, so an assessment
based on an earlier membership snapshot is rejected; exact replay does not advance
the version. The journal stamps an inbox watermark in that same transaction. A later
observation outside that snapshot remains awaiting acceptance, while previously
covered records retain their verified scope. Uncovered source receipts do not
present another instance's acceptance reference as their own. The companion
canonical completion contract must be installed before repair card admission;
otherwise admission fails closed with `repair_completion_contract_unavailable`.

Authority transfer uses `POST /transfer`: freeze the old authority, receive on the
configured new target, then follow on remaining sources. Both endpoints verify the
old frozen handover and target receipt. The old owner is permanently fenced before
activation; the new owner starts paused. Active/unknown repair obligations,
nonempty target inboxes or oversized snapshots block transfer honestly. This first
version bounds handover to 1,000 rows per transferred table and 192 KiB; it does
not silently truncate or independently elect a replacement.

The PR supervisor eligibility hook uses this same reporting service. Its identity
includes bound realm and exact dependency/authority/repository/affected instance;
repeated identical failures do not produce a new revision every poll. Recovery
requires successful fresh authenticated evidence for that same dependency and
instance. Collector failures similarly update bounded retry status with backoff,
without recursively generating journal reports.

## Integration and validation

`tests/test_health_journal.py` exercises two authenticated ASGI instances, offline
recovery, lost acknowledgements, revision races, fenced action replay, authority
handover, redaction, realm/principal checks, custody history and runtime recovery.
It includes actual localhost stdio MCP and real Kernel card/dispatch admission.
Companion canonical completion and eligibility producer tests skip explicitly on
older component branches and run in the recorded combined integration fixture.
The global degraded-history middleware's explicit authenticated
`local_operational` route classification is owned by the foundations component;
canonical-store independence alone does not prove that integration gate.

Release acceptance remains a root-owned packaged boot/provider-stub check followed
by a harmless real two-instance report/collection/visibility check. No production
fault injection, automatic deployment, secret inspection, permission expansion or
alternate PR merge engine is part of this service.

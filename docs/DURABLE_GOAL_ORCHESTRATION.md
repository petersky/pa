# Durable goal orchestration

## Status

Proposed architecture for durable, long-running, fleet-wide goal pursuit.

This design deliberately separates a **goal** from a card, dispatch, agent
session, or provider-native goal mode. A goal is durable organizational intent.
Cards are work packages, dispatches are execution attempts, sessions are
replaceable workers, and provider-native goal modes are optional execution
strategies.

## Motivation

PA can already record work, dispatch cards, supervise pull requests, communicate
with operators, and synchronize state across a fleet. It does not yet have a
durable entity that remains responsible for an outcome across many plans,
sessions, machines, failures, pauses, and changes of strategy.

Current coding agents expose variants of `/goal` that repeatedly run an agent
and evaluate whether a session-scoped completion condition has been met. That is
useful for a bounded task, but PA needs a higher-level control plane capable of:

- pursuing an outcome for days or months without an immortal chat session;
- refining vague intent without silently expanding authority;
- decomposing a goal into dependent or competing work streams;
- assigning work according to fleet capacity and capabilities;
- reacting to repository, PR, fleet, operator, and external-channel events;
- maintaining durable short- and long-term memory across worker replacement;
- independently verifying completion instead of trusting a worker's final text;
- proposing proactive goals while keeping their activation within policy; and
- sleeping safely between events rather than continuously consuming a model.

## Design principles

1. **Intent is durable; execution is replaceable.** Losing a process, machine,
   provider session, or context window must not lose the goal.
2. **Authority never follows ambition.** Goals and subgoals do not expand
   filesystem, network, credential, approval, financial, or communication
   authority.
3. **Evidence closes goals.** An agent assertion is a claim, not proof.
4. **Fast reactions and slow deliberation are separate.** Most events should be
   classified cheaply; only consequential events should invoke deeper planning.
5. **Every action is attributable.** Decisions identify the source goal,
   triggering observations, policy decision, actor, authority, and result.
6. **Autonomy is bounded and inspectable.** Operators can understand why PA is
   acting, what it may do next, and how to pause or constrain it.
7. **The fleet has one logical goal state.** Work may execute anywhere, but
   leases, fencing, sync, and conflict resolution prevent duplicate controllers.
8. **Channels do not become authorities.** Telegram, Discord, web, CLI, and
   future inputs share a canonical intake contract and explicit identity policy.

## Conceptual model

```text
Goal
├── charter and policy envelope
├── strategies, hypotheses, and derived subgoals
├── work graph
│   └── cards / work packages
│       └── dispatch attempts
│           └── agent sessions and tools
├── observations and evidence
├── decisions and operator interactions
├── memory: working, episodic, semantic, and procedural
└── completion audit
```

### Goal record

A versioned goal record should include:

- `goal_id`, realm, project, owner, creation source, and parent goal;
- objective, motivation, constraints, and explicit non-goals;
- success criteria with an evidence requirement for each criterion;
- assumptions, ambiguities, risks, and open questions;
- repository, fleet, integration, and data scope;
- autonomy policy and permitted action classes;
- time, cost, token, API, storage, dispatch, and concurrency budgets;
- deadlines, review cadence, retry limits, and stop conditions;
- current lifecycle state, strategy revision, progress summary, and next wake;
- controller lease/fencing data and responsible instance;
- linked cards, dispatches, sessions, artifacts, interactions, and notifications;
- immutable decision, observation, and evidence references; and
- retention, sensitivity, provenance, and redaction classifications.

Success criteria should be structured rather than stored only as prose. Each
criterion needs a stable ID, verification method, current verdict, evidence
references, freshness requirement, and explanation. This allows a strategy to
change without moving the finish line invisibly.

### Goal lifecycle

```text
draft → shaping → ready → active → verifying → achieved
                     ↘ waiting_operator
                     ↘ waiting_external
                     ↘ paused
                     ↘ blocked
                     ↘ abandoned
```

- **Shaping** permits reversible discovery and converts vague intent into an
  explicit charter.
- **Active** permits actions within the policy envelope.
- **Waiting states** record a durable wake condition; they are not failures.
- **Verifying** freezes ordinary execution while an independent completion audit
  evaluates current evidence.
- **Achieved** records a completion report and evidence snapshot. Materially
  changing the objective creates a new revision or successor goal.
- **Blocked** requires evidence and recovery guidance, not merely an agent saying
  it is stuck.

## Goal supervisor: the PA brain

The goal supervisor should be an event-driven durable workflow, not one
permanently running ACP conversation. Each controller cycle is bounded,
idempotent, recoverable, and recorded:

1. **Observe** relevant state changes and refresh stale evidence.
2. **Orient** using the limbic classifier and current goal memory.
3. **Deliberate** only when the event requires planning or judgment.
4. **Propose** structured actions, expected outcomes, risks, and wake conditions.
5. **Authorize** every action through deterministic policy and current authority.
6. **Execute** bounded operations or create work packages for executor agents.
7. **Verify** immediate results and record observations and evidence.
8. **Reflect** on progress, drift, failed assumptions, and strategy quality.
9. **Schedule** the next event-driven or timed wake, then release resources.

Typical actions include refining a plan, creating or updating a subgoal, creating
cards, dispatching work, requesting operator input, waiting for an external event,
running a verification job, changing strategy, pausing, or proposing completion.

The planner returns a typed proposal. It does not directly mutate PA. A policy
executor validates expected versions, scope, budgets, authority, idempotency keys,
and conflict risks before applying mutations through PA's public services.

### Roles

PA should support distinct logical roles even when one model fills several roles:

- **Supervisor/planner:** chooses and revises strategy.
- **Executor:** performs a bounded work package.
- **Verifier:** tests claims and maps criteria to evidence.
- **Critic/red-team reviewer:** looks for missing requirements, unsafe shortcuts,
  reward hacking, and premature completion.
- **Memory curator:** promotes durable facts and expires or supersedes stale ones.

Higher-risk goals should require role separation. A worker should not be the only
authority that decides its own output is correct.

## The limbic system

PA's **limbic system** is a lightweight, low-latency receptor and triage layer
for state changes and incoming messages. The term describes its architectural
role, not a claim of biological equivalence.

It consumes normalized signals such as:

- card, dispatch, session, repository, PR, CI, fleet, and sync transitions;
- timers, deadlines, resource pressure, repeated failures, and lack of progress;
- web, Telegram, Discord, CLI, webhook, email, voice, image, and file inputs;
- operator responses, approvals, corrections, and explicit interrupts; and
- output or interaction requests produced by agents and integrations.

For each signal it produces a small structured **appraisal**, for example:

```json
{
  "salience": 0.82,
  "urgency": "high",
  "valence": "risk",
  "novelty": "new",
  "confidence": 0.91,
  "goal_refs": ["goal-id"],
  "intent": "operator_reports_regression",
  "risk_classes": ["production", "credential"],
  "recommended_path": "slow_deliberation",
  "wake": ["goal_supervisor", "notification_service"],
  "dedupe_key": "...",
  "reason": "..."
}
```

The limbic layer may use rules, embeddings, small local models, inexpensive model
APIs, or short-lived ACP sessions. Its implementation is replaceable behind a
versioned contract. It must have strict latency, token, privacy, and cost budgets.

It is **not** the final authorization layer and must not independently perform
high-impact actions. Depending on policy, it can:

- ignore or coalesce noise;
- append a low-risk observation;
- acknowledge receipt with an explicitly limited response;
- wake an existing goal supervisor;
- create a notification;
- request fast deterministic handling;
- route to deeper deliberation; or
- trigger an emergency policy action already authorized by deterministic rules.

Some events bypass or supplement LLM appraisal: security revocation, explicit
operator stop, data-integrity alarms, lease fencing, and hard resource limits must
have deterministic handling. Appraisals are logged with model/rule version and
inputs after required redaction, so routing decisions can be audited and replayed.

### Limbic safeguards

- Never include secrets in prompts when metadata or a local rule is sufficient.
- Treat all channel content and external artifacts as untrusted input.
- Detect prompt injection and keep content separate from control instructions.
- Rate-limit by identity, channel, goal, realm, and event class.
- Coalesce event storms and suppress duplicate notifications.
- Measure false escalation, missed escalation, latency, and cost.
- Fall back to deterministic conservative routing when models are unavailable.
- Require a deeper or deterministic check before destructive or external actions.

## Multichannel, multimodal intake

The web UI, Telegram, and Discord should be the first channel adapters, with a
contract designed for later email, SMS, voice, mobile push, and other systems.
Every inbound item is converted to a canonical envelope before any agent sees it:

```text
channel adapter
  → authenticate and resolve identity
  → preserve raw content in controlled storage
  → malware/type/size and prompt-injection checks
  → transcribe/OCR/describe/index multimodal parts
  → limbic appraisal or deterministic bypass
  → fast-path response, slow-path deliberation, or durable queue
  → correlated response through an authorized channel
```

The envelope should carry channel and message IDs, thread/reply relationships,
sender identity and confidence, realm, visibility, timestamps, locale, modality,
attachments, content hashes, sensitivity, reply capabilities, delivery receipts,
and correlation IDs. Channel-specific payloads must not leak into domain logic.

### Telegram and Discord

Initial support should include:

- text, images, audio/voice, video, files, captions, reactions, and replies;
- bot commands and discoverable PA commands without requiring commands for normal
  language input;
- direct messages, configured groups/channels, and thread/topic mapping;
- allowlists and realm/project/goal routing policies;
- explicit account linking and channel identity verification;
- typing/progress indicators, message edits, delivery failure handling, and
  links back to the authoritative PA view;
- safe size limits, content quarantine, retention controls, and redaction; and
- outbound policy that prevents PA from posting to a broader audience than the
  initiating context permits.

Multimodal processing must retain the original artifact and record every derived
representation. A transcript, OCR result, image description, or embedding is an
observation with provenance and confidence, not a replacement for the source.

## Fast and slow processing

Inputs should not go directly to an omnipresent agent session. PA should implement
a two-layer path inspired by fast and slow cognition:

### Fast path

The fast path uses deterministic handlers and limbic appraisal to perform cheap,
bounded work:

- resolve identity and conversation context;
- classify intent, urgency, sensitivity, and related goals;
- retrieve a small working-memory packet;
- answer safe factual/status questions from authoritative PA state;
- acknowledge an event and state what will happen next;
- deduplicate, route, notify, or wake a controller; and
- escalate uncertainty or consequential requests.

Fast responses must declare when they are preliminary. They may not fabricate a
commitment or claim that slow-path work has completed.

### Slow path

The slow path starts a bounded deliberation with reconstructed context. It can
compare strategies, inspect repositories and evidence, ask correlated questions,
create work, and update goal state. It writes decisions and durable memory before
ending. A later turn may use a fresh agent rather than resume the same session.

The routing decision should consider uncertainty, consequence, reversibility,
novelty, operator expectation, required tools, and whether an existing goal owns
the issue. Operators can explicitly request deeper review or suppress it.

## Memory architecture

PA should model memory explicitly instead of treating chat transcripts as memory:

- **Sensory buffer:** short-retention raw events and channel artifacts.
- **Working memory:** the compact context packet for one appraisal or controller
  cycle, including current goal state and immediately relevant observations.
- **Episodic memory:** timestamped events, attempts, outcomes, and decisions.
- **Semantic memory:** durable facts, relationships, constraints, preferences,
  capabilities, and learned environment knowledge with provenance.
- **Procedural memory:** policies, skills, playbooks, prompts, and verified
  workflows.

Memory promotion is deliberate. A curator proposes semantic facts from episodes;
policy checks provenance, sensitivity, contradiction, confidence, and retention.
Facts can be superseded rather than overwritten. Retrieval is scoped by authority,
realm, goal, recency, relevance, and provenance, with defenses against poisoned
memory and cross-tenant leakage.

Each agent invocation receives a generated **goal packet**, not an unbounded
transcript. It contains the charter, current strategy, assigned work, relevant
decisions, evidence, constraints, open interactions, and exact completion/reporting
contract. Context generation is versioned and reproducible for audit.

## Planning, cards, and dispatch

The supervisor maintains a dependency graph of subgoals and work packages. Cards
remain useful for human-visible, independently trackable work, but not every
internal observation or one-step probe needs a card. PA should support:

- card-backed work packages for durable implementation and review;
- lightweight jobs for probes, verification, and bounded analysis;
- competing strategy branches with explicit experiment budgets;
- dependency, mutex, repository/worktree, and artifact relationships;
- capability-aware placement, leases, queueing, retry, and backoff;
- duplicate-work and conflicting-write detection;
- cancellation and compensation when a strategy is abandoned; and
- reconciliation when an agent ends without the required durable result.

Provider-native `/goal` support belongs in an adapter. PA may assign a bounded
subgoal to a Codex, Claude, or Kimi goal loop, but must ingest its progress,
blockers, interactions, artifacts, and completion evidence into the canonical PA
goal. Providers without goal mode use ordinary recoverable ACP turns.

## Policy and autonomy

Suggested autonomy levels are:

1. **Observe:** collect and summarize only.
2. **Propose:** suggest plans and actions for operator approval.
3. **Reversible execution:** perform local/reversible actions within budgets.
4. **Delegated execution:** create and dispatch work under an approved charter.
5. **Policy autonomy:** pursue the goal without routine approval while remaining
   inside explicit action, risk, audience, cost, and time boundaries.

Policies apply per goal and action, not merely per agent session. They should
address code changes, merges, releases, credentials, purchases, deletion,
external communication, personal data, production access, and creation of new
goals. Policy changes are versioned and take effect at a clear event boundary.

Agent-created subgoals must be traceable to a parent criterion or risk. New
top-level goals are proposals by default. Automatic activation requires an
operator-authored standing policy specifying categories, scope, budgets,
priority, and expiry. Depth limits, quotas, cooldowns, and portfolio review guard
against recursive goal explosion and reward hacking.

## Human interaction and notifications

Questions, approvals, external actions, and choices use PA's durable interaction
and notification contracts. They retain correlation, required responder,
deadline, authorized response channels, and resume target. A session's ordinary
final text is never the only record of a required response.

The goal dashboard and notification surfaces should show:

- why attention is needed and what is blocked;
- which goal, instance, channel, and agent originated the request;
- response choices and consequences;
- whether the response can be handled on any fleet member or only at an authority;
- deadlines, default behavior, and escalation path; and
- confirmation that the response reached the recovering worker or controller.

## Fleet correctness and recovery

Only one controller lease may make decisions for a goal revision at a time.
Leases require fencing tokens so a partitioned or resumed controller cannot apply
stale actions. Goal events use idempotency keys and optimistic expected versions.
Durable and projection head mismatches use PA's normal reconciliation path;
divergence is resolved by explicit merge events rather than ref manipulation.

The system must recover from host sleep, restart, provider outage, expired auth,
network partition, partial dispatch, duplicate webhook, and interrupted model
response. Recovery revalidates external state before repeating an action.

Wake scheduling should be durable and transferable between eligible fleet
members. Keeping a machine awake is an execution concern used only when active
local work requires it, not the persistence mechanism for the goal.

## Completion, drift, and no-progress detection

Completion uses an independent audit that produces a verdict for every success
criterion and records the evidence snapshot. The auditor checks evidence
freshness, contradictory observations, unresolved high-severity risks, and any
required human acceptance. Ambiguous evidence returns the goal to active or
waiting state with a specific remediation plan.

The supervisor should also detect:

- repeated equivalent actions without new evidence;
- oscillation between strategies;
- growing work scope without criterion changes;
- excessive retries or resource consumption;
- outputs optimized for proxy metrics rather than the objective;
- stale assumptions and invalidated dependencies; and
- prolonged activity with no measurable outcome progress.

Responses include replanning, narrowing, independent review, budget reduction,
operator escalation, pause, or abandonment. Progress reporting should describe
criterion movement and evidence, not turns or token consumption alone.

## Interfaces

### Web

A goal portfolio and detail view should expose objective, lifecycle, owner,
strategy/subgoal graph, active work, evidence coverage, budgets, decisions,
memory, notifications, channel activity, controller location, next wake, and an
audit timeline. Operators can pause, resume, edit through a new revision, change
policy, answer interactions, request review, or stop a goal.

### CLI

Proposed commands:

```text
pa goal create
pa goal list
pa goal show <goal-id>
pa goal run <goal-id>
pa goal pause|resume|stop <goal-id>
pa goal edit <goal-id>
pa goal events|evidence|memory <goal-id>
pa goal audit <goal-id>
```

### Slash commands and MCP/API

`/goal` in PA-owned input surfaces creates or manages a PA goal. It should not be
blindly forwarded to a provider. The type-ahead menu explains whether a command
is handled by PA or by the current provider. Versioned REST/MCP contracts expose
goal creation, inspection, proposals, progress, evidence, interactions, and
audits. Mutating tools require authority provenance and idempotency keys.

## Observability and evaluation

Metrics should cover goal lead time, criterion progress, completion-audit
reversals, operator interruption rate, blocked duration, planning cost, dispatch
success, duplicate action suppression, limbic routing latency/cost/accuracy,
memory retrieval quality, channel delivery, and autonomy-policy denials.

Evaluation needs replayable event suites, simulated fleet partitions, adversarial
channel inputs, prompt-injection tests, ambiguous-goal tests, long-horizon drift
scenarios, crash recovery, and shadow-mode comparison of limbic classifications.
Recorded production events used for evaluation must be consented, minimized, and
redacted.

## Delivery plan

### Phase 1: durable goals

- Goal schema, event model, lifecycle, revisions, and sync semantics.
- Web/CLI/API/MCP CRUD and goal dashboard.
- Structured success criteria, evidence ledger, and manual audit.
- Controller leases, durable wakeups, budgets, and operator controls.

### Phase 2: supervised orchestration

- Event-driven supervisor and typed action proposals.
- Card/work-package graph and fleet-aware dispatch integration.
- Independent verifier and no-progress/drift detection.
- Durable interactions and recovery into replacement sessions.

### Phase 3: limbic and memory systems

- Canonical signal envelope and deterministic routing baseline.
- Lightweight appraisal service with shadow mode and evaluation harness.
- Working, episodic, semantic, and procedural memory services.
- Fast/slow path routing with provenance and policy enforcement.

### Phase 4: multichannel operation

- Web intake migration to the canonical channel contract.
- Telegram and Discord adapters with multimodal artifacts and identity linking.
- Cross-channel correlated responses, notification routing, and delivery receipts.
- Channel security, retention, moderation, and abuse controls.

### Phase 5: advanced autonomy

- Provider-native goal adapters and strategy portfolios.
- Policy-controlled agent-proposed goals and derived subgoals.
- Cross-goal priority, resource allocation, and conflict management.
- Organization-level portfolio review and governance.

The implemented Phase 5 contracts and authorization order are documented in
[Goal autonomy and governance](GOAL_AUTONOMY_AND_GOVERNANCE.md).

## Initial acceptance criteria

The first production milestone is complete when PA can accept a goal, survive a
full fleet restart, reconstruct its context in a replacement agent session,
create and dispatch dependent work, pause for a correlated operator response,
resume on another eligible instance, and reach an independently audited result
whose evidence is visible in the web UI and CLI. Every mutation must be
attributable to the goal and authorized by the policy revision active at the time.

## Related architecture

- [Architecture](ARCHITECTURE.md)
- [Interactions and fleet notifications](INTERACTIONS_AND_NOTIFICATIONS.md)
- [Dispatch progress](DISPATCH_PROGRESS.md)
- [Post-turn evaluation](POST_TURN_EVALUATION.md)
- [Fleet capacity](FLEET_CAPACITY.md)
- [Session lifecycle](SESSION_LIFECYCLE.md)
- [Collaboration modes](COLLABORATION_MODES.md)

## External reference patterns

The provider mechanisms below inform the worker-loop adapter but do not define
PA's canonical goal model:

- [OpenAI: Long-running work](https://learn.chatgpt.com/docs/long-running-work)
- [OpenAI: Follow a goal](https://learn.chatgpt.com/use-cases/follow-goals)
- [Claude Code: Keep Claude working toward a goal](https://code.claude.com/docs/en/goal)
- [Kimi Code: Goals](https://www.kimi.com/help/kimi-code/cli-goals)

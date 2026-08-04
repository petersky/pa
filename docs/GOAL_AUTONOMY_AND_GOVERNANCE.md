# Goal autonomy and governance

Phase 5 adds deterministic autonomy controls around PA's durable goal record. A
provider goal loop remains a replaceable worker. It cannot change the canonical
objective, policy, budget, evidence, or completion verdict.

## Durable records

Advanced state is synchronized through `goal_governance_upserted` events and is
projected separately from the Phase 1 goal snapshot. Each mutation carries an
actor, authority instance, policy revision, idempotency key, and optimistic
version. The governance projection contains four entity classes:

- `goal_autonomy`: priority, strategies, provider runs, usage, rolling windows,
  action decisions, resource reservations, and derived-goal references;
- `goal_governance_policy`: the realm's organization-level limits, standing
  proposal policies, provider limits, and resource capacities;
- `goal_proposal`: traceable derived and proactive top-level proposals; and
- `goal_portfolio_review`: the current independent organization allocation
  review. Prior reviews remain in the immutable governance event ledger.

An autonomy mutation also validates the canonical goal version, active goal
policy revision, and controller fencing token when a controller lease is live.
The governance entity has its own optimistic version so retries cannot apply a
decision twice.

## Action authorization

`POST /api/goals/{goal_id}/actions/authorize` is the reservation boundary. The
caller describes the action class, risk, reversibility, delegation, external
audience, provider, repository/data scope, expected usage, and resource claims.
PA evaluates, in order:

1. prohibited and permitted action classes;
2. provider, repository, data, and audience scope;
3. autonomy-level, risk, and explicit operator-approval gates;
4. goal and organization cost, token, API, storage, action, dispatch, and time
   budgets;
5. per-goal rolling limits and organization provider limits; and
6. portfolio allocations, exclusive claims, and quantitative capacity.

The result is `authorized`, `requires_approval`, `denied`, `budget_exhausted`,
`rate_limited`, or `resource_conflict`. Every result is attributable. Only an
authorized result reserves usage and resources. A prohibited action or scope
expansion is a hard denial and cannot be converted into authority by an agent's
approval claim.

Operator approval is accepted only when the approval principal matches the
mutation actor and the actor is an operator principal. Approval can satisfy a
review gate; it does not override prohibited actions or scope.

## Provider adapters

The adapter registry publishes capabilities for Codex, Claude, Kimi, Cursor,
and OpenInterpreter, and accepts plugin adapters. Native mode is selected only
when the live provider command catalog advertises a configured goal command.
Otherwise, PA emits a recoverable ordinary-turn assignment when session loading
is supported. If neither path is available, assignment fails closed.

Each invocation contains a versioned goal packet with the canonical goal ID,
criteria, constraints, non-goals, policy revision, budget, selected strategy,
and progress contract. Provider progress is normalized into usage, blocker,
interaction, artifact, and evidence-claim fields. Evidence claims do not enter
the canonical evidence ledger automatically, and provider completion never
satisfies the independent Phase 1 completion audit.

## Strategies and proposals

A goal can maintain multiple scored strategies with explicit cost/token
allocations and selected branches. Aggregate allocations may not exceed the
goal budget.

Derived subgoals must reference a parent criterion or recorded risk. They must
remain in the same realm and project, cannot increase autonomy, cannot expand
permitted actions, repositories, data, providers, or budgets, and must inherit
the parent's prohibitions. Parent policy controls automatic activation, depth,
quota, and cooldown. A disallowed automatic activation remains a proposal for
operator review.

Agent-proposed top-level goals remain pending by default. Automatic activation
requires a current, enabled, unexpired, operator-authored standing policy whose
category, project, priority, cost, and token envelope all match. Proposal and
activation use separate idempotency keys so an interrupted activation can be
recovered without creating a duplicate goal.

## Portfolio allocation and review

Portfolio review scores nonterminal goals by declared priority, lifecycle, and
deadline urgency, then allocates in stable score/ID order. It enforces the
organization's active-goal ceiling, exclusive resource mutexes, and quantitative
resource capacities. Previously active goals displaced by higher priority work
are marked `preempted`; paused or blocked goals remain visible as `blocked`.

An organization review must identify an independent reviewer distinct from the
requesting actor. The durable result includes every allocation, aggregate usage,
pending proposals, budget/capacity findings, and whether operator review is
required. Action authorization consults the current portfolio allocation before
granting new resource claims.

## Interfaces

The REST surface is rooted at `/api/goal-governance` and the per-goal advanced
routes under `/api/goals/{goal_id}`. MCP exposes portfolio reads and reviews,
action authorization, provider assignment, and goal proposals. The Goals page
shows organization policy/review status plus each goal's priority, autonomy,
usage, and provider-run count.

All mutations require `Idempotency-Key`, `expected_version`, and
`policy_revision`. Per-goal mutations also accept `goal_version` and
`X-PA-Goal-Fencing-Token`; callers should always supply both when operating as a
controller.

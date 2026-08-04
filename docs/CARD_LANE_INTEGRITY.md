# Card lane integrity and the 2026-08 resurrection incident

This document records the immutable evidence for `Ship PA v0.0.1`
(`7b3176fa-ca9a-4546-839e-6265ae36bbb6`) and `Get agents working`
(`6989b229-666b-4854-8db8-d2e8799a9f93`). The evidence was read through
PA's authenticated sync/card APIs; no database, object, ref, or projection was
edited during the investigation.

## Causal findings

`Get agents working` had two accepted Done updates before the regression:

- commit `2cd30b5bf0b0228c2c9d074f48871a155393e566cceb75a14b6a28f61403a967`
  at `2026-07-26T02:16:47.863742Z`;
- commit `fa99af6ee0665be226dc231beb8d0ffe0cdcee97853f70c07f72a0cfab53fc85`
  at `2026-07-26T05:21:11.366290Z`.

Mac mini then authored commit
`2037e3f61e6da1cbebaa7f9c40d1f9246b532a6e0339451a1a347288c9d88f57`
at `2026-08-01T21:26:56.867997Z`. Its sole event,
`688e2291-cd9f-4ef3-b911-9b305f8d50f8`, was a full `card_created` payload
with `lane=inbox`, principal `fleet:dispatch`, and authority instance recorded
as MacBook (`0c7d8ecb-7e45-4579-8fa0-35159492d3f1`). The commit instance was
Mac mini (`02dbcd47-8f40-44eb-8403-5eb57545afc8`) and its parent was
`fd212ff4550cbc7e1c8f85fa8a9dca7e6ef79eda2e33bb14da1179634ad0cc01`.
Both Done commits were ancestors of that parent. The transition was therefore
a linear stale full-entity dispatch upsert, not a new lane patch, divergent
merge choice, legacy status conversion, or conflict resolution.

`Ship PA v0.0.1` had Done events, including commits
`a13dbf7ef3b7ef43210c5511115f0a4d66a0a20512f08e866b5d9517fcd0c83d`
and `1b87dad7ff364358d01ca7e83a5046a23c9961e2311a1b4814898393619d8d67`,
but no reachable durable create event or complete snapshot. Its older `items`
row remained `status=open`. `CardProjection._init_db()` ran
`_migrate_items_to_cards()` on every process start and used `INSERT OR IGNORE`
to copy that row to `cards` as `lane=inbox`. When the durable and projection
checkpoint heads already matched, startup reconciliation correctly skipped a
rebuild, leaving this projection-only resurrection visible and unaudited.

## Previous fixes and why they were insufficient

PR #176 (`c1a9a1b9f80095eb83e7025bce007375b1609270`, merged as
`ce6c434e533d6a29969ad3123d28e596e7e53a2b`) removed dispatch
materialization's missing-card `CARD_CREATED` side-load and required the target
durable and projection heads to match the authority. It merged at
`2026-08-01T22:12:54Z`, about 46 minutes after the bad Mac mini event had
already been authored. It prevented new writes from the updated path, but did
not make replay reject an already-durable duplicate create and did not cover
legacy SQLite migration.

Commit `7e895c25a4d48658040f182919771bbde29ff1e1` intentionally changed legacy
item migration to run even when cards already existed, to prevent card loss on
restart. `INSERT OR IGNORE` protected existing rows but supplied no durable
version fence. Once replay or another repair removed a projection-only card,
the retained stale item row became eligible to recreate it on every restart.

The earlier sync timestamp fix in PR #42
(`5f6d16dbfc188cd5aa37792941f771d5af7ca0d4`) preserved `updated_at` across
peer projections for dispatch version comparison. It did not reject duplicate
`CARD_CREATED` events, convert full payload timestamps into update
preconditions, or make legacy bootstrap one-shot. PR #58
(`29bb530f3745fe8c897d8a86a50a73d31529c8ae`) made the browser editor send
partial updates, but fleet tools and older/full-entity clients remained able to
submit stale fields.

## Enforced invariants

- A normal `CARD_CREATED` is rejected when its causal parent already contains
  state for that card. Explicit repair/conflict restoration uses the distinct
  `CARD_UPSERTED` event.
- Replay classifies and ignores historical duplicate creates, including a
  create preceded by legacy field events, so parent/merge traversal order
  cannot select their stale lane.
- Legacy item projection migration is versioned and one-shot. If a durable
  realm head exists, replay is authoritative and legacy rows are not imported.
- Full-card PATCH payloads use echoed `updated_at` as an optimistic causal
  precondition. `field_intent` lets a client send a full snapshot while changing
  only named fields.
- The idempotent legacy-history repair appends a complete `CARD_UPSERTED` base,
  overlays accepted durable field events (including legacy `status` mapping),
  and never rewrites immutable history.
- Card history exposes event and commit hashes, both parents, actor, author and
  commit instances, timestamps, source operation, causal parent/card version,
  field intent, and whether replay applied or ignored an event.

## Fleet observation

During investigation MacBook and Monica converged at durable head
`290353c3526842d6f79fc68ec1fd3e1f9c8efe16575e3256628146b6e81e2ee5`
with matching projection heads and no conflicts. Mac mini was unavailable with
a read timeout; its last reported durable head was
`96a781c3b42941079e336d7723ce415712037c4f20dbbb4e653866c7e1b7fa66`.
Both reachable peers projected `Get agents working` as Done at version
`2026-08-04T00:00:53.055537Z`; `Ship PA v0.0.1` was absent because its history
lacked a canonical base. After rollout, run the canonical legacy-history repair
for both IDs, then normal sync/reconciliation. The repair is safe to repeat.

# Execution selection

PA separates execution intent, capability evidence, admission, and native confirmation.
The policy contract is `pa.execution-selection/v1`. `selection.py` is the single
pure resolver; `selection_service.py` supplies owned defaults, catalogs, feedback,
and fleet admission. Requested preferences are never presentation truth for a
running provider. The existing `acp/configuration.py` normalization and
`session_presentation` remain authoritative after rejection or deferral.

## Intent and precedence

Each field (`harness`, `connection`, `model_provider`, `model`, `reasoning`, and
each native option) has an independent intent:

| Intent | Meaning |
| --- | --- |
| `inherit` / omitted | Consult the next less-specific source. |
| `automatic` | Stop inheritance for this field and let policy decide. |
| `required` | Exact native value must be supported by the complete tuple; otherwise reject. |
| `preferred` | Favor this value, but record an unmet preference when choosing a compatible alternative. |

Field precedence is dispatch/new-session input, card, project, user surface,
installation surface, installation policy. Existing legacy provider/model/effort
fields are translated at their original layer, not flattened over modern intent.
`provider` still means harness in legacy dispatch APIs; `model_provider` means
backend. A contradictory modern and legacy explicit pin is rejected. Modern
Automatic is not an alias for omitted legacy input.

Hard constraints from every applicable layer are conjunctive. A dispatch override
cannot relax project or installation routing, ownership, tool, context, modality,
or budget requirements. Unknown capability/cost evidence cannot satisfy a hard
requirement. Permission mode, sandbox, approval policy, collaboration, and network
authority are not native model options; the selection API cannot grant them.

Example preferences (replace IDs with actual catalog values):

```json
{
  "version": 1,
  "harness": {"intent": "required", "value": "codex"},
  "connection": {"intent": "required", "value": "research-account"},
  "model": {"intent": "preferred", "value": "advertised-model-id"},
  "reasoning": {"intent": "automatic"},
  "task": {"role": "reviewer", "complexity": "complex", "risk": "high"}
}
```

Card create/edit persist these preferences. Dispatch overrides are one-run input;
only the separate **Save card defaults** action changes the card. Card and session
versions reject stale edits. Changing parent selectors visibly clears incompatible
explicit children. Inherited values and their source remain visible.
The card's durable project is authoritative: a different request project cannot
substitute its defaults or restrictions. New admission fails if the originating
card/project policy has not been materialized on the execution instance.

## Catalogs, connections, and readiness

A candidate is scoped to an instance, harness, connection revision/account/endpoint,
backend, model/version, and native options. Identical model strings on different
accounts are not the same candidate. Missing actual model identity stays
`provider_default` or `unknown`; configured backend routing is not a native model
provider confirmation.

Read-only catalog calls use cached evidence. Explicit/admission refresh is
coalesced (60-second minimum interval); entries older than 300 seconds are stale.
Named discovery has an eight-second bound, a 256 KiB response limit, at most 500
backend model IDs, and no redirects. ACP initialization and backend authentication
are separate evidence. An installed adapter is not required to be reported as
installed when an authenticated active session proves availability. Conversely,
credentials alone do not prove health, and a failed initialization probe is not
hidden by configured credentials. Mixed-version evidence remains unusable/unknown
when it cannot be validated by this resolver.

Named profiles use existing adapter credential references; no secret is accepted
in a model option or stored in a receipt. Profile and credential revisions fence
retries, caches, and next-turn routing. Selecting a named account excludes other
backend account credentials from its child process environment. It does not edit
global provider configuration, homes, permissions, or another worker.

Native transport support is deliberately adapter-specific:

| Harness | Configured connection contract |
| --- | --- |
| Codex | Responses endpoint via invocation-scoped `MODEL_PROVIDER` and `CODEX_CONFIG`. |
| Cursor | Account API key and account-exposed models; arbitrary backend endpoints are unsupported. |
| OpenInterpreter | Invocation-scoped native provider configuration (`responses`, `chat`, or `messages`). Initialization/backend failure remains visible. |
| Card-summary HTTP job | Existing configured OpenAI-compatible or native Messages transport, its own scoped connection and model. |

The native overlays follow the [Codex ACP configuration contract](https://github.com/agentclientprotocol/codex-acp/blob/main/README.md)
and [OpenInterpreter provider configuration](https://github.com/openinterpreter/openinterpreter/blob/main/docs/config.md).
No harness/provider Cartesian product, cross-provider effort equivalence, model
price list, or model competence ranking is synthesized. Model-specific native
reasoning is taken from advertisements/configOptions, not a model's name.
Adapters may additionally advertise version-1 `execution_capabilities.models`
evidence for tools, modalities, context size and model version. Unknown extension
versions are ignored, never translated into support. A native option's explicit
`_meta["pa.mutableBetweenTurns"] = false` requires a context boundary.

## Automatic policy and evidence

Policy is editable by an administrator with compare-and-swap revision checking,
through HTTP, MCP, or `pa execution policy`. Rules match a bounded structured task
assessment: role, complexity, ambiguity, risk, scope, and cost/latency/quality
objective. Explicit assessment has provenance; otherwise a versioned deterministic
text heuristic supplies conservative values. Repository text can inform this
advisory assessment but cannot supply permission or routing authority. There is no
optional LLM classifier or unbounded classifier call in this version.

Cold start prefers the configured default among eligible candidates, not the
largest/newest model. Native rule weights, task assessment, and all alternatives
are recorded. Explicit preferences dominate these small policy weights. Known
cost/latency can affect an objective; unknown costs remain unknown. Scarce health
or capacity evidence is recorded rather than replaced by guessed success.

Outcome evidence is append-only and tied to an owned decision and candidate.
Operator-validated task completion, test and review results are separate from a
successful provider protocol return or summary JSON schema validation. Feedback
reports sample count, age, and a confidence interval. Policy-controlled reliability
use is disabled by default and requires at least the configured minimum sample
count. The score is not advertised as learned competence; contradictory and sparse
observations remain explicit. There is no autonomous budget escalation.

## Receipts, attempts, and context boundaries

Receipts contain field-level input provenance, task assessment, policy revision and
digest/owning instance, catalog metadata, candidate rejection reasons, selected
native settings, known/unknown tradeoffs, and fallback authority. Provider
configuration attempts and prompt outcomes are separate records. A selection
receipt says what was requested, not that it was applied.
The decision includes its policy snapshot and a verified content digest. Receipt
reads return the immutable `decision` separately from `attempts` and
`attempts_page` (`offset`, `limit` 1–100, `total`, `next_offset`); HTTP, CLI and MCP
can access every attempt without an unbounded response. Historical session details
show the last confirmed values explicitly as historical, without live-setting controls.

Persisted attempts are reused before consulting mutable defaults. Current hard
authority is revalidated at admission/next turn without silently reselecting the
model. A prompt ID is bound to its original content digest. A verified between-turn
setting change retains the original decision and a linked receipt history, so
replayed dispatch input does not undo the later confirmed change.
An unmet soft preference may be replayed unchanged; it does not become a hard pin.
Additional retry restrictions are checked against the saved tuple. Previously
unspecified native model/reasoning defaults are bound after provider readback,
separately from the original intent. A changed provider default cannot silently
replace this binding on a later turn or restore. Unsolicited native drift retains
the original prompt in the durable queue and requests a verified correction.

Deferred settings are durable, versioned, and idempotent. They run under the prompt
lock before the next prompt. Rejection preserves the latest native state and
blocks the original queued prompt; it never invents rollback after partial native
application. Failure is saved before the existing durable interaction contract is
used. Acknowledging the notification is not approval, application, or queue resume.

Changing harness/account/context requires an explicit linked attempt. Source
ownership, instance, card/project/realm, queue, pending settings, and native turn
boundary are checked. The source is durably fenced before a replacement starts;
its workspace and transcript are retained. The replacement gets a new leased
workspace, source linkage, and an explicitly labeled bounded saved transcript
excerpt. Transport retries reuse the same replacement identity. A dispatch-owned
source requires a terminal predecessor and a new dispatch on the named source
instance; it cannot be converted into an untracked competing standalone runtime.

Automatic fallback is off by default and capped at three preauthorized attempts.
It cannot cross instance, harness, account, connection revision, backend, original
hard pins, or prompt identity. Budget reservation is durable and idempotent; a
derived fallback cannot replenish the original budget. PR-supervisor fixed-
destination authorization remains an independent gate and cannot be broadened by
selection policy. When that authority does not permit replacement, operator
coordination is required.
Legacy/missing receipts have no implied fallback allowance. Summary retries also
recheck current hard restrictions; a response-reported model identity different
from the request is recorded as different, with no invented alias equivalence or
claim that schema validation constitutes repository tests.

## Execution creation inventory

Inventory searches: `create_session`, `attach_default`, ACP `new_session`/
`load_session`, direct HTTP completion endpoints. All application ACP creation
converges on `AgentSessionManager.create_session`; raw ACP protocol creation lives
only in `AgentConnection.connect` below that admission gate.

| Surface | Selection/attempt handling |
| --- | --- |
| Web card create/edit; card HTTP/MCP; `pa card create` / `execution-defaults` | Durable typed preferences and migration-compatible default. No provider application. |
| Web dispatch modal; fleet HTTP/MCP; `pa card dispatch --preview/--selection-json` | Joint fleet/tuple preview and admission; card version and one-run input retained. |
| Direct web/API session, labeled/default session; `pa execution start` | Common manager gate; existing labeled/default native identity is retained. |
| Local and remote materialization/session allocation | Authority-owned receipt persisted in target ledger; target revalidates its own policy/capabilities and exact instance before native startup. |
| Remote allocation retry / admission replay | Target must retain receipt lineage and confirm normalized native settings before the prompt is sent. Incompatible peers fail closed. |
| Manager retry, recovery, default reconnect, startup restore, queued/direct prompts | Reuse receipt/native context; current hard constraints and original prompt digest are checked. |
| Card enrichment one-shot job | Manager gate with originating card/project context. |
| Card summary generation/retry/forced regeneration | Same pure resolver; durable summary input/model/connection binding and native response audit. Summary transport cannot satisfy unsupported harness/native pins; such constraints fail visibly. |
| PR supervisor live/resume/authorized recovery | Retained session receipt; bounded selection fallback cannot override supervisor destination authority. |
| Explicit linked standalone attempt; terminal successor dispatch | Fenced source, distinct leased target, bounded immutable saved context and explicit boundary. |
| Auxiliary diagnostic ACP initialize/MCP handshake/provider connection probe | Discovery/diagnostic only, not a task worker or model competence outcome. |

API roots: `/api/execution/catalog`, `/connections`, `/defaults`, `/preview`,
`/policy`, `/decisions/{id}`, `/evidence`, and `/sessions/{id}/settings` (including
cancel) or `/boundary`. Existing `/api/agent` aliases preserve remote proxy routing.
All mutations go through the running PA server; these commands do not edit PA's
data files from an agent script.

## Requirement-to-test/evidence checklist

This checklist is maintained until complete; unchecked items are not a completion claim.

- [x] Field precedence, inherit/Automatic/required/preferred, native tuple scoping,
  hard permission/routing/tool/context/modality/budget restrictions:
  `tests/test_execution_selection.py`.
- [x] Deterministic role/risk/complexity rules, explicit cold start, unknown prices,
  stale catalogs, scarce evidence and bounded fallback/prompt identity:
  `tests/test_execution_selection.py`.
- [x] Codex/OpenAI, Cursor account Grok, OpenInterpreter/MiniMax native fixtures,
  transport incompatibility, failed auth/initialize, named revision/credential
  rotation and process-local overlays: `tests/test_execution_selection_connections.py`.
- [x] Card create/edit/preview and stale card revision; no render-time probes;
  policy CAS and narrow controls' markup: `tests/test_execution_selection_api.py`.
- [x] Deferred/cancel/conflict behavior and preserved confirmation contract:
  `tests/test_execution_selection_settings.py`, `tests/test_acp_client.py`.
- [x] Real tool-free Codex/Cursor ACP processes: requested→native-confirmed,
  reasoning change and lineage, distinct linked workspace, no competing queued
  source, idempotent replay: `tests/test_execution_selection_runtime.py`.
- [x] 390px create and dispatch inspection: inherited xhigh versus one-run low is
  labeled pending; horizontal dispatch overflow found and corrected.
- [x] Successful real tool-free browser dispatch/admission/native confirmation,
  sidebar/details/receipt, explicit save-defaults and visible parent-choice clearing:
  fixture card `bc5c3b64-b681-470a-8e88-9e935f9adffc`, dispatch
  `fbf171fa-8ea7-457d-b35b-2891b045c9a7`, session
  `87a8a564-6578-4dc6-8d8a-2280b2b94c2b`; provider confirmed low at
  `2026-09-05T23:44:52.886776+00:00` while the card stayed xhigh. Separate UI save
  persisted low at `23:58:34.282982Z`. No production vendor health/competence claim.
- [x] Remote authority/target real ACP confirmation and mocked inter-instance
  transport rejects missing lineage/conflicting metadata before prompt ACK;
  same-native-identity recovery after changed defaults and verified settings;
  bounded derived-summary retry/routing/feedback; administrator settings retain
  worker/card ownership; old-column projection migration and CLI/MCP forwarding:
  `tests/test_execution_selection_runtime.py`, `tests/test_execution_selection_surfaces.py`,
  `tests/test_execution_selection_settings.py`, `tests/test_execution_selection_jobs.py`.
- [ ] Full repository regression suite, isolated boot smoke, build, final diff
  review and wide/narrow browser evidence at the exact final commit.
- [ ] Scoped commit/push, ready PR with exact requirement/evidence checklist,
  durable PA watch, stable-green exact head, independent review/CI/mergeability
  revalidation, merge and matching merge-commit evidence on the card.

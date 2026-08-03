# Collaboration modes and commands

PA treats collaboration mode and execution authority as independent controls.
`default` and `plan` change how an agent collaborates. Provider `mode_id`, the
sandbox, filesystem/network access, approval rules, dispatch scope, placement,
and capacity remain unchanged when collaboration mode changes.

## Dispatch policy

The deterministic precedence order is:

1. mandatory fleet/realm/project constraints;
2. explicit dispatch selection or user preference;
3. project/card policy;
4. realm, instance, and provider defaults;
5. backward-compatible `default` fallback.

Strategies are `always_default`, `always_plan_first`, `automatic`, and
`conditional`. Automatic selection uses recorded card kind/tags/capabilities,
dispatch intent, risk, and an explicit ambiguity signal. It does not rely on an
opaque model classification. Every decision stores its inputs, matching policy
IDs and versions, effective source, rationale, and dispatch/card linkage.

Plan-first policy includes maximum turns, an expiry, a question budget, approval
behavior, and an unattended fallback (`default`, `escalate`, or `cancel`). ACP
elicitation and PA notifications provide fleet-visible user interaction. A
pending or recovered session revalidates policy and authority before the next
turn; PA never rewrites an in-flight turn.

## Agent transition contract

Agents call `request_collaboration_mode` with requested mode, purpose, intended
next action, session/dispatch/card/authority provenance, authority version, and
an idempotency key. The agent cannot update its own session configuration and
must not claim a transition happened until PA returns one of:

- `approved_applied`;
- `approved_pending_next_turn`;
- `rejected`;
- `unsupported`;
- `stale`;
- `failed`.

PA validates the current provider advertisement, policy edge, owner, active-turn
state, card authority version, and execution-authority invariants. Pending work
is kept in the authority-local recovery ledger and applied only at a safe turn
boundary. Duplicate keys return the original durable result; reusing a key for
different content is a conflict.

## Command catalog

ACP `available_commands_update` is a full provider snapshot. PA normalizes and
versions it with PA-native commands while preserving the raw record, provider,
requirements, input metadata, availability, disabled reason, and executable
`commandAction`. Provider commands own unqualified names; PA commands use the
`pa:` namespace.

A recognized `setConfigOption(collaboration_mode=...)` action uses the same
policy transition contract. Other recognized configuration actions run through
the PA-controlled session API. Recognized failures are returned as structured
results and are never silently sent as prompt text. Provider commands without
an executable action retain the ACP-compatible prompt-forwarding behavior.

The prompt composer opens an accessible type-ahead only when `/` is the first
character. `//` sends literal leading-slash text. Catalog generation and full
session/dispatch/card/authority provenance fence stale or remote execution.

## Surfaces

- HTTP: `/api/agent/collaboration/policies`,
  `/api/agent/collaboration/policy/resolve`,
  `/api/agent/sessions/{id}/collaboration`, and
  `/api/agent/sessions/{id}/commands`.
- MCP: `get_collaboration_mode_state`, `request_collaboration_mode`,
  `list_session_commands`, and `execute_agent_session_command`.
- CLI: `pa collaboration inspect`, `commands`, `policy-list`, and `policy-set`,
  each with JSON where applicable.
- Web: scoped policy settings, dispatch overrides/risk signals, session command
  type-ahead, transcript results, and fleet notifications.

Legacy dispatch `mode_id` remains the provider execution-mode field. Providers
or sessions that advertise only Default continue to operate and reject/fallback
from Plan with an explicit unsupported rationale.

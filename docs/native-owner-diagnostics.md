# Native owner diagnostics and requester withdrawal

PA intentionally removes `PA_LOCAL_API_TOKEN`, `PA_INSTANCE_ID` and other private
service variables from general ACP provider environments, including overrides.
The dedicated PA MCP bridge receives its own owner connection separately.
Missing variables in a provider shell do not establish a broken binding. Do not
ask an operator to inject credentials or copy the bridge environment into a shell.

`get_instance_diagnostics(section)` reads one fixed owner endpoint:

| Section | Endpoint | Interpretation |
| --- | --- | --- |
| `status` | `GET /api/status` | Installed metadata and separately startup-captured loaded build, PID and startup ID |
| `runtime` | `GET /api/runtime` | Complete returned counters, bounded distributions and lifetime maxima; these populations differ |
| `maintenance` | `GET /api/instance/maintenance` | Existing observations; does not start maintenance |

These ordinary tools use existing owner authentication and instance fencing.
Assigned-service and completion bindings gain no access. Each read retains a
five-second transport budget and at most 2 MiB of decoded response data. An
oversized response raises `diagnostics_response_too_large` with correlation and
limit; it is never presented as a complete or passing truncated snapshot.

## Bootstrap before these tools are installed

The existing native `instance_info`, session liveness, journal, configuration,
workspace lease, PR supervisor and backup tools remain supported. The PA browser
can navigate the owner's ordinary web endpoints under its existing web access.
For this session, `http://127.0.0.1:8080/api/status` visibly returned the complete
status JSON, including the loaded `f789c0bb` build, package identity, PID and
startup ID, without a token being read or transferred. Match its instance ID to
the native identity. Browser text snapshots truncate JSON: use an actual visible
screenshot when assessing displayed fields and do not claim a complete numeric
runtime inventory from a truncated text excerpt. Browser access does not grant
additional permissions; an authentication failure must remain a failure.

This provides a supported preinstallation status observation. It does not waive
rollout checks. A successor still requires its exact protected PR/supervisor
gate, independently revalidated signals, terminal head and main CI, fresh sole
lease, configuration, backup and original handoff state, and independently
attested installed files, RECORD, interpreter, dependency constraints and cache.
Use new immutable evidence paths. Metadata subprocesses and builds set private
`PA_DATA_DIR` and `PA_WORKSPACE_ROOT` before importing PA. Do not execute old
shell API helpers that would fall back to private credential files.

The previously used constrained installation route is `uv tool install --force
--python <attested-existing-python> --no-python-downloads --constraints
<verified-frozen-constraints> git+https://github.com/petersky/pa@<exact-green-merge>`.
Prepare and review the concrete target and command, then obtain any required
provider sandbox approval for that installation. This document grants no install
approval. Never substitute moving `main`, another interpreter or older targets.
Installation alone does not prove what the live listener loads. The supported
managed restart is `request_agent_restart_handoff` with the exact session's
durable key and continuation, followed by ending the turn after its receipt.
Never issue synchronous `pa restart` or use `--no-acp-quiesce`. On continuation,
verify actual native tool availability, loaded files/build and every declared
scoped retest. Never assume that merging, installing or restarting refreshed a
provider's tool catalog.

## Withdraw an erroneous question without answering it

`withdraw_operator_input` forwards to
`POST /api/notifications/{id}/withdraw-operator-input`. Supply the original
notification, request and session IDs, current version, stable withdrawal key and
a bounded factual reason. The owner accepts only its own principal's original
unanswered MCP progress question. Native ACP permissions/elicitation, remote
owners, transferred continuations, changed versions, recorded responses and any
delivery attempt are rejected. Normal operator identity and realm authorization
remain mandatory; shared fleet credentials cannot impersonate an operator.

The operation serializes with responses and uses the existing durable CAS write.
It preserves the question/response fields, changes the state to `superseded`,
and appends the reason and an input/principal/authority digest to the existing
supplemental notice body. The event audit records actor, version and time. No
notification/event schema changes, user answer, cancellation, provider delivery
or continuation prompt are manufactured. A full notice is rejected rather than
truncated. An exact key/input replay is read-only; a changed request conflicts.

For the systemic performance session, the erroneous request
`systemic-pr591-owner-api-binding-recovery-20261025-v1` / notification
`a99ee1db-df41-564e-910d-8c3f0be182d8` is still unanswered at source preparation.
Withdraw it only after the supported operation is actually available and a fresh
read confirms its version and unanswered state. Do not use `respond(cancel=true)`
or answer a choice on the user's behalf. Keep the original request, false report
revision and subsequent correction as history. No user credential recovery is
required. Whole-card independent acceptance and all performance residuals remain
open.

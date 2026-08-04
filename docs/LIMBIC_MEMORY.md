# Limbic appraisal and tiered memory

PA normalizes state changes and inbound items into `SignalEnvelope` before any
optional model sees them. The envelope has a stable schema, content hash, dedupe
key, realm and goal scope, source/subject identity, timestamps, sensitivity, and
correlation metadata. Model features are minimized; confidential and restricted
content never enters the optional appraisal provider.

`LimbicService` applies versioned rules first. Security revocation, operator stop,
data-integrity alarms, lease fencing, and hard resource limits use a deterministic
bypass only when the server supplies verified provenance from an authenticated
operator, integration, or authority boundary. Signal-body trust flags, event-class
lookalikes, hashes, and dedupe keys are never authoritative. The service records a
content-free spoof diagnostic and routes unverified control lookalikes to slow
deliberation. The general `/api/limbic/appraise` and MCP entry point never infer
trusted provenance from request fields or headers; authenticated adapters must
construct it server-side. The service revalidates proof objects at use time,
including objects produced through unchecked model-copy/construct helpers.
Content hashes and dedupe keys are recomputed from normalized content and an
authority-specific identity fingerprint, so trusted and untrusted copies do not
collide and separate integrations cannot share a dedupe scope.

Known bounded status requests can use the preliminary fast path. Unknown,
consequential, repeated-failure, or prompt-injection inputs route to slow
deliberation. A model may supplement non-sensitive inputs, but its output is
schema-allowlisted and can only escalate the baseline. Model-proposed bypasses,
wake targets, actions, and unknown fields are rejected. Provider calls have a
strict deadline and circuit breaker; timeouts, provider/network errors, and
malformed output return the deterministic baseline with code-only diagnostics.
One provider invocation remains admitted after a timeout until its worker actually
exits, preventing a hung provider from accumulating threads on circuit retries.
Shadow mode records a policy-validated proposal without changing the effective
route.

Replay fixtures report exact agreement and a per-case escalation confusion matrix
without mutating durable state. Empty suites return `no_data` with no accuracy;
malformed suites are `invalid`, never perfect scores. Durable appraisal events use
redacted signal content, minimized metadata, and content-free audit features.

The memory service stores sensory, working, episodic, semantic, and procedural
records in the realm event log. Every record carries source provenance, actor,
content hash, transformation, sensitivity, confidence, ownership, goal scope,
retention, and principal grants. Sensory records default to one-hour retention;
working records default to 24 hours. Expired or superseded records remain auditable
but are omitted from ordinary retrieval.

Semantic and procedural facts with the same scoped subject and predicate are
marked contradictory when values differ. A curator resolves the conflict by
recording a new fact with `supersedes`; the prior value is retained and linked,
never overwritten. Retrieval requires an explicit realm and requester, honors goal
and principal scope plus a maximum sensitivity, excludes contradictions by
default, and labels content as untrusted instructions unless it is a verified
procedural record. Working-memory packets cap each tier to resist noisy or poisoned
sources crowding out the context.

REST endpoints live under `/api/limbic` and `/api/memory`; matching MCP tools use
the server-owned local API so the running PA process remains the sole data writer.

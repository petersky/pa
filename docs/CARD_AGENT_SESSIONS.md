# Card agent sessions

A card may own multiple durable local agent sessions. PA preserves each session's
canonical ID, transcript, dispatch ID, authority/owner instance, repository
context, and provider thread ID. Starting a fresh session creates a new canonical
identity; resuming never replaces the old identity or provenance.

The card Agent view sorts sessions by most recent update. Its deterministic
default is the newest active session owned by the local instance, then the newest
closed-but-resumable local session. Users can select every session and can start a
fresh one explicitly. Multiple active sessions are supported, including concurrent
sessions, because each has an independent runtime, queue, transcript, and worktree
lease; PA's existing capacity and lease controls still apply.

Statuses have product meaning:

- **active**: locally owned and not terminal; it can be selected immediately.
- **resumable**: closed locally but has a provider thread ID. PA first attempts
  `session/resume`, falls back to `session/load`, and only then creates a new
  provider thread while retaining the PA session identity and history.
- **unavailable**: owned by another instance, or closed without a provider thread
  ID. Durable history remains visible, but local live controls are disabled.
- **failed**: recovery is blocked or failed; history and provenance remain intact.

Resume and fresh-start operations are serialized by session/label locks. Repeated
resume requests return the existing live runtime, while `fresh` start bypasses
label reuse. Cross-instance sessions are routed to their owner when available and
are never recovered locally.

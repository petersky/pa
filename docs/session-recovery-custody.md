# Read recovery custody metadata

`GET /api/agent/observability/v1/sessions/{session_id}/recovery-custody`
and ordinary MCP `get_agent_session_recovery_custody` expose checkpoint identity
and saved native-alias registry facts. They do not audit, reconcile, restore,
renew a lease, change a queue, connect/load a provider or release a close/fence.

Supply the same exact authority, session version, native identity, binding hash
and raw-string lease snapshot hash as the binding inspection reader. An empty
expected native identity explicitly asserts no saved identity. Owner/admin access
and all binding inspection preconditions are checked before custody metadata is
read, and again afterwards. Checkpoint/alias metadata is also read twice. These
checks detect observed changes; they are not an admission lock, transactional
global ownership fence, filesystem custody proof or permission to resume work.

SQL returns only JSON types, bounded prompt IDs and queue order. It never returns
config JSON, private settings, prompt/answer text, error bodies or transcripts to
the application reader. Config larger than256KiB, duplicate JSON keys, malformed/unsupported roots,
unsupported checkpoint shapes and invalid IDs yield explicit unavailable states.
This budget may intentionally make a large valid checkpoint uninspectable.

Checkpoint `absent`, JSON `null`, and `object` remain distinct. An object's
in-flight field is separately `absent`, `null`, `identified` or `invalid`; an empty
object or missing ID never becomes null. Queues distinguish absent/null/invalid,
capacity exceeded and observed arrays (including an empty array). At most32 IDs
of200 characters each are returned, preserving duplicates and order. Invalid
items never produce a partial queue that could be mistaken for the whole queue.

The native registry lookup uses the existing native-history index and exact
saved agent name/native ID, includes closed sessions, and reads at most33 matches
to detect the32-row display budget. It never normalizes provider/repository
aliases. Owners see only their own session IDs, authorities and versions; hidden
matches are indicated without exposing foreign principal/session identity.
Administrators may see those bounded identity fields. Counts are lower bounds
when truncated; uniqueness is then unknown. Missing native identity is distinct
from a unique registry entry. Registry uniqueness never proves exclusive native
ownership, provider inactivity or history custody. Those remain unknown/not
observed, including when exactly one registry row is present.

Responses are private/no-store and limited to64KiB, with a hash of the returned
metadata observation. This hash is not mutation authority or a full-config hash.
Foreign ownership yields403, missing identity404, stale/unsupported/oversized
observations409 and unavailable storage/deadline503 with unknown effect. No raw
exceptions or private values are echoed. The assigned-service allowlist and
existing tool schemas remain unchanged.

This capability supplies facts needed to investigate the retained9d/3b original
bindings and be010 recovery. It cannot settle historical delivery from missing
evidence, restore a lease base, prove current Git HEAD/dirty state, or replace the
provider's passive no-execution/exclusive-ownership protocol. Keep queued work,
original identities/defaults and deliberate close/dismissal dispositions intact.
Cleanup owner1b remains the single executor for coordinated runtime activation;
source integration does not itself activate this reader or satisfy recovery.

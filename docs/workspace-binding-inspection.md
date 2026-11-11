# Inspect retained workspace provenance

`GET /api/agent/observability/v1/sessions/{session_id}/workspace-binding`
and the ordinary MCP tool `get_agent_session_workspace_binding` inspect saved
binding facts, a bounded audited history page, and current session lease metadata.
They do not provision, renew a lease, read provider files, load history into a
provider, retry a session, change a binding, or deliver queued work. There is no
restoration/apply endpoint in this change.

Supply these read preconditions from the existing authenticated session history
and workspace lease APIs:

- `expected_authority_instance_id`: exact serving authority and saved authority.
- `expected_session_version`: saved `updated_at`, including timezone.
- `expected_native_id`: exact saved ID; the empty string explicitly asserts absence.
- `expected_binding_hash`: SHA256 of the complete saved binding serialized with
  `json.dumps(binding, sort_keys=True, separators=(',', ':'))` and UTF-8.
- `expected_lease_hash`: the same hash of the session's lease rows, ordered by ID,
  with exactly these fields: `id`, `session_id`, `repository_id`, `card_id`,
  `project_id`, `worktree_path`, `branch`, `base_sha`, `fencing_token`, `state`,
  `updated_at`. Include cleaned leases. Timestamps use the stored ISO string
  (normalize API UTC `Z` to `+00:00` before hashing).

Only the authenticated session owner or an authenticated administrator may read.
Authority, session version, native identity, complete binding hash and lease
identity/fence/version must match. The session, lease snapshot and history page
are read again before returning; a concurrent change yields409. Read preconditions
are not mutation CAS tokens or admission permission. Responses are private/no-store.

`history_limit` defaults to16, maximum32; `history_offset` is bounded to4096.
The next offset continues the same fixed session-version read. A stale precondition
requires a new independently observed snapshot, never a retry of a mutation.
Bindings above16KiB, JSON nesting beyond32, malformed JSON, unsupported shapes and
unrecognized fields are explicit gaps. Unknown fields and raw reasons are not
emitted. At most32 session leases and256KiB total output are allowed. Exceeding
capacity fails closed rather than silently omitting evidence. History exhaustion
does not establish complete historical retention.

The comparison identifies a unique non-cleaned lease when one exists, otherwise
the unique retained original lease. It separately identifies the bound original
lease(s), including cleaned ones. Repository aliases remain distinct; an old
HTTPS identity is not silently replaced with an SSH identity. An unresolved
`pending` base remains unresolved. Saved binding and history rows are observations,
not proof that the original workspace was correct or that a provider did not act.

Every restore plan remains `unsupported`, `mutation_authorized=false`,
`delivery_allowed=false`, `restored=false`, and `historical_effect=unknown`.
Its required proof covers original historical provenance, exact repository/branch/
base/HEAD, exclusive native and lease ownership, dirty/queued-work custody, and a
separately reviewed admission-serialized CAS prepare/apply/status contract.
The maintainer must establish that proof before proposing any production repair.
Four queued prompts across the two PA owners and the five-field Eschaton mismatch
therefore remain preserved; this reader does not make those roots collectible.

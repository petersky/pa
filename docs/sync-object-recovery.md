# Missing sync object recovery

Incremental projection and canonical index traversal read events through
`EventLog.read_referenced_event`. A missing or invalid event raises the original
`EventHistoryObjectError`, retaining only its realm, object hash/kind, canonical
head, and referencing commit. The recovery observer persists this evidence and
closes ordinary mutation admission before the projection worker returns.

`POST /api/sync/recovery` accepts only `realm_id`, under the existing instance
authentication and realm membership checks. It accepts no object bytes, hashes,
ref changes, or caller-supplied proof. The existing degraded admission allowlist
is unchanged: ordinary push (including object-only push) and ordinary writes
remain blocked. Repair installation is internal to the owned recovery job.

For exact event evidence, recovery checks the current durable head and walks
canonical commit links to the referencing commit. It verifies that commit's
schema, realm, and reference to the object. This requires no event-history scan.
A configured authenticated peer supplies exactly that hash through the normal
`/api/sync/get` protocol. SHA256, supported schema, model, and realm validation
precede installation; the ref lock guards the final stale-head check and object
repair. Neither parent links nor durable refs are changed. Legacy diagnostics
without canonical reference evidence must first obtain evidence by verification.

There is one process-owned task per realm. Its UUID identifies the recovery
generation. Up to 64 concurrent request-key SHA256 digests can join it; raw
keys are never retained. Retired key digests remain fenced to their prior head. An HTTP wait expires independently: `recovered: null`,
`pending: true`, and the same `operation_id` describe unfinished work. A repeated
key joins the job or returns its completed result without another scan. A new
key can retry a terminal failure. Verification and reprojection use the actual
`AsyncRuntime` `wait_for_completion=True` contract: `timeout=None` alone would
still apply the runtime's default timeout. Cancelling a request does not cancel
the owner. Shutdown drains owners before closing their peer client.

Late success/failure updates `sync_recovery.json` and readiness automatically.
Completion checks the current durable head and projection head while holding the
ref fence. Per-realm failure records prevent success in one realm from clearing
another realm's gate. Diagnostics include operation identity, phase, bounded work
counts, and whether owned work is still active, without payloads, URLs, request
keys, or credentials. Process interruption is reported as an interrupted phase with no live worker.
Actual SyncModule startup automatically starts authoritative verification for
persisted degraded subscribed realms, including terminal failures whose objects
were subsequently restored. An interrupted same-head operation retains its UUID
and key aliases, with an explicit resume count. Terminal same-key receipts also
survive process exit. Startup verification may resume a failed operation; it does
not claim historical success. A changed head starts a linked new generation,
discards stale object evidence, and rejects keys belonging to the old head.
Unsubscribed realm failures remain gated and are not silently erased. No saved
scan cursor is trusted across process exit: a new process verifies canonical
history again, while concurrent requests within that process join one owner.

`tests/test_sync_recovery_owned.py` exercises the real merge/suffix producer,
public router and admission middleware, authenticated peer endpoint, canonical
object store and index, projection, and `AsyncRuntime`. Tiny caller deadlines
cover concurrent calls, cancellation, late results, and same-operation retries;
rejection cases cover hash/schema/realm/reference failures and head changes.
Startup integration persists an actual timed-out or failed operation, restores
complete canonical history, then runs SyncModule startup with the real runtime
and public gate. It covers missing legacy proof, changed heads, unrelated realm
failures, same-key restart receipts, and late automatic readiness.

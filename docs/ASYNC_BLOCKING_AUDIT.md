# PA event-loop blocking audit

This audit covers every coroutine and every HTTP or MCP entry point under `src/pa`, including endpoint-local stream producers, module startup/shutdown hooks, background workers, agent/provider callbacks, and their transitive I/O paths. The generated companion [inventory](ASYNC_BOUNDARY_INVENTORY.md) contains 511 concrete call paths and can be refreshed with:

```console
python scripts/audit_async_boundaries.py > docs/ASYNC_BOUNDARY_INVENTORY.md
```

The generator is a conservative index, not proof that a syntax match blocks. Each direct signal was traced through its callees and classified below. Tests deliberately stop representative Git, disk, SQLite, HTTP, provider, subprocess, browser, dispatch, transcript, Fleet, sync, and PR-supervisor operations while an unrelated coroutine or request continues.

## Repository and coordination record

The audit began at `c3808111fd21bb66a13d665842b221b37576c65b` in the PA-provisioned worktree and branch recorded on card `ee5b637b-21fc-4682-a094-b18dbec11a4c`. Repository, remote, branch, base, cleanliness, divergence, and worktree uniqueness were checked before editing. PA's lease endpoint could not return its durable record because the running service reported `Service not registered: instance_agent`; the supplied card-scoped worktree nevertheless existed as its own Git worktree and no other worktree used this branch. No PA data files were read or written directly.

The overlapping work was integrated before its files were touched:

| Owned work | Integration evidence | Audit handling |
|---|---|---|
| Inline card editing (`d807ff1f…`) | `29bb530`, merged before the current base | No template or inline-edit behavior changed |
| Dispatch completion (`2df5e742…`) | PR 60, `20eaff1` | Rebased first; retained completion semantics and changed only blocking boundaries |
| ACP model configuration (`12ddbed6…`) | PR 61, `1da671a` | Rebased first; retained atomic configuration semantics |
| Monica's Fleet Overview (`0a3ad27b…`) | PR 62, `bc7f2f0` | Rebased after merge; no Fleet templates, assets, topology state, or health rendering changed. Only post-merge I/O boundaries in `fleet/overview.py` and server orchestration were changed |

## Reviewed call-path matrix

“Before” describes cancellation behavior at the start of this card. Threaded calls now return promptly to a cancelled caller but remain charged to the bounded executor until the underlying function actually ends.

| Call path and execution context | Latency/timeout risk and visible impact | Before | Remediation |
|---|---|---|---|
| `Kernel.startup/shutdown -> Module hooks`, ASGI lifespan | Provider discovery, SQLite/file initialization, and shutdown waits could delay readiness or freeze health during quiesce | Mostly unbounded; cancellation did not stop synchronous work | A process-owned bounded runtime starts before modules. Blocking construction is off-loop, hooks have deadlines, shutdown rejects new legacy work, drains briefly, and reports lifecycle state |
| `HookBus.emit -> sync subscribers`, agent/integration callback | Arbitrary synchronous callbacks could stall the caller, including SSE and agent updates | Caller cancellation waited behind the callback | Sync subscribers use the bounded runtime; async subscribers remain native async |
| Repository HTTP/MCP/agent paths -> `RepositoryStateService` / workspace manager -> Git, filesystem, SQLite | Git commands and traversal could stop all requests; unbounded output could consume memory | `subprocess.run`/filesystem calls were synchronous and process cancellation was absent | Git uses asyncio subprocesses with 120-second call deadlines, a 4 MiB combined output limit, and TERM/KILL process-group cleanup. Legacy workspace/SQLite/file work uses the bounded runtime |
| Sync endpoints/MCP/background convergence -> event log, refs, object traversal, peer HTTP | Slow disk or a peer could stall unrelated requests; repeated sync could exhaust workers or duplicate writes | Sync file/database work and some response decoding ran on-loop; peer clients were short-lived | Durable operations cross named bounded calls, peer HTTP uses a shared pooled async client with connect/read/write/pool deadlines, JSON decoding is off-loop, and existing per-realm locks preserve ordering and idempotency |
| Browser HTTP/MCP -> `BrowserManager` -> socket/CDP/process startup | DNS/socket connect and browser startup could freeze MCP/HTTP and leak a child after cancellation | Blocking socket/process discovery; partial cleanup | Startup is single-flight and concurrency-bounded, CDP uses bounded async HTTP, filesystem discovery is off-loop, child cancellation performs process-group cleanup, and stop has finite TERM/KILL waits |
| File browser UI/raw endpoint -> authorization roots, traversal, reads, Git diff, template render | Slow disks, repositories, or large diffs could consume the framework pool and delay unrelated frontend rendering | The synchronous route used Starlette's shared worker pool | Browse authorization, traversal, reads, Git subprocesses, diffing, and rendering use one named bounded call with a 30-second total deadline; raw-file resolution uses a 10-second boundary and Starlette streams the bounded file response |
| Integration sync endpoint -> integration registry/config/hooks | File-backed registry reads and synchronous hook subscribers could delay the request | Direct synchronous registry access | Registry work and sync callbacks cross the runtime; connector/network work stays async |
| Execution router -> remote peer prompt / local agent launch | Sync payload/state work and one-client-per-call HTTP increased head-of-line delay | Cancellation did not bound all preparation | State/serialization uses the runtime; remote prompts use one pooled async client, explicit connect/read/write/pool deadlines, and cancellation propagation |
| Dispatch worker/outbox -> dispatch SQLite -> local handler or completion HTTP | A locked database or slow completion owner could stop the worker and other requests; unbounded retry queues could grow | SQLite ran in async tasks and HTTP clients were recreated | Store mutations are serialized by the existing store transaction/lock but executed off-loop. Worker concurrency and active work are bounded, wakeups are awaitable, completion HTTP is pooled/deadlined, cancellation is recorded, and idempotency keys remain authoritative |
| ACP connection/provider callbacks -> provider resolution, wire log, filesystem, child process | Provider discovery, capture writes, and SQLite/file activity could stall agent sessions and SSE | Synchronous resolver/file/log work inside callbacks; global environment mutation during spawn | Resolution and files are off-loop. Child environment is copied per process. Wire-log writes are serialized and bounded. The ACP process remains native async with shutdown deadlines |
| Agent session manager/runtime -> workspace/config/session SQLite; provider callbacks -> transcript | Slow workspace/Git/SQLite could freeze all sessions; transcript writes could reorder or grow without bound | Direct SQLite/file calls and per-event writes in callbacks | Blocking work uses named runtime calls. Transcript persistence uses a bounded ordered queue and batches; `(session_id, seq)` is unique and `INSERT OR REPLACE` makes timeout retries idempotent. Durability boundaries drain with deadlines; excessive backlog pauses the session |
| Agent chat HTTP/SSE -> session manager, transcript catch-up, JSON serialization | Disk/SQLite or large serialization could interrupt live token delivery and unrelated UI/MCP | Catch-up/store/serialization work ran directly in async handlers | Session/store work and CPU serialization are off-loop, live delivery stays on awaitable bounded queues, keepalive polling uses async timers, and disconnect cancellation propagates |
| PR-supervisor HTTP/MCP/startup/worker -> credentials, policy/domain store, supervisor SQLite, GitHub | Credential reads, SQLite leases/audit writes, JSON, and GitHub requests could block status and other supervisors | Sync store/policy work in workers and some async MCP paths; startup reconciliation delayed startup | Store, lease, audit, card, policy, JSON, and credential work is off-loop. One bounded async GitHub client is shared. Startup reconciliation is backgrounded; each refresh/network operation has a deadline and preserves ownership fencing |
| Fleet overview async probes -> local dimensions/cache/SQLite/Git/files and peer HTTP | A slow local dimension or peer could freeze Fleet rendering and global request delivery | Post-PR-62 local dimensions/cache still executed synchronous work in probe coroutines | Monica's incremental state contract is preserved. Local dimensions and cache reads/writes cross the runtime, per-dimension single-flight remains, and peer probes share the Fleet client with aggregate/per-detail deadlines |
| Local/remote provider HTTP and MCP -> provider resolution/config/install/probe/login -> files, subprocess, HTTP | Provider install/probe and the 120-second synchronous remote client could consume the shared framework pool, delaying unrelated MCP/UI work; thread cancellation could leave installer children alive | Local tools were synchronous and remote MCP used `httpx.Client` | Status/config/login files use named bounded operations. Install/update/probe run in an isolated asyncio-owned process group with 60/900-second deadlines, a 1 MiB result limit, two active slots, and eight queued slots, so cancellation kills all legacy descendants and refresh storms fail explicitly. Remote calls share the Fleet async pool with a 125-second deadline and off-loop JSON decoding |
| Fleet health/status/join/agent-provider routes -> registry/files/config/peer HTTP | File/config writes, response decoding, and the provider proxy's synchronous HTTP could block health or login flows | Several async routes performed direct file work; two async provider routes called `httpx.Client` | File/config/registry mutation is off-loop. Provider/status/dispatch requests use pooled async HTTP and off-loop JSON decoding. Explicit partial failures retain per-peer error state |
| Remote install worker -> job files/token/script generation -> asyncssh -> remote process -> health | SSH/DNS/connect or remote install could hang for minutes and survive cancellation | File writes were synchronous and process lifetime was weakly bounded | Durable job operations are off-loop, SSH connect is bounded to 30 seconds, install to 900 seconds, health HTTP is pooled/deadlined, cancellation terminates the remote process/context, and restart recovery reads persisted jobs |
| Fleet update/quiesce worker -> job store/config/release/subprocess/peer polling | Update and restart could freeze normal health/status; new work could be accepted after quiesce | Durable writes and release/config work could run in the task; client creation and phase waits were scattered | Store/release/config/apply/restart work is off-loop, phase polling uses `asyncio.sleep`, HTTP has per-phase deadlines, persisted operation IDs make retries idempotent, recovery planning runs off-loop, and lifecycle state rejects new work at explicit boundaries |
| SSE producers for install/update/agent streams | Serialization, store reads, or blocking polling could delay event delivery | Mixed direct serialization/store calls | Durable reads and JSON serialization are off-loop; in-memory job event stores use short locks; waits are awaitable and disconnect/shutdown cancels producers |

## Bounded ownership and cancellation

The compatibility executor defaults to 8 workers plus 64 queued calls. Admission is atomic; overflow returns HTTP 503 with `Retry-After`, expired work returns 504, and queued plus running work is accounted per operation. Timeout is a total deadline including queue time. Python threads cannot be killed safely, so timeout/cancellation releases the caller but not the capacity slot; this prevents an apparent capacity increase and thread-pool exhaustion.

Native subprocesses start in a process group. Timeout, output-limit failure, and cancellation send TERM, wait two seconds, then KILL and reap. Native HTTP paths use shared clients with explicit connection limits and operation deadlines. Durable SQLite/file writers keep their existing locks, transactions, fencing tokens, operation IDs, audit sequence, and event ordering; only the wait moved off the event loop.

Agent transcripts, dispatch completion, and Fleet/PR-supervisor job state have bounded queues or active sets. Shutdown first marks lifecycle state, rejects new work, drains durable queues within a deadline, cancels async workers, closes pooled clients, then closes the executor without waiting indefinitely for an uncooperative native thread.

## Safe synchronous boundaries

The audit deliberately retained synchronous code where it does not execute on the ASGI loop:

- FastAPI `def` endpoints execute in Starlette's worker pool, and synchronous FastMCP tools execute in the framework's worker boundary. Their transitive calls were still reviewed for finite external timeouts.
- Module `configure` and kernel construction happen before the server accepts requests. Small in-memory registration remains synchronous; startup work that may touch providers, disk, SQLite, or the network was moved to the runtime or a background task.
- CLI commands, release/install commands, migration helpers, and synchronous compatibility APIs remain synchronous. Async server callers use their explicit async wrappers; CLI callers gain no responsiveness from conversion.
- Pure in-memory registry/table reads and short lock-protected event-buffer reads remain direct.

The static inventory has ten conservative “reviewed transitively” rows. `cli.main.peers_list.discover` is CLI-only. `ExecutionRouter._remote_prompt`, `fleet_health`, and `instance.list_peers` only read in-memory peer tables before native async HTTP. Fleet install/update SSE rows read in-memory event buffers. `integrations.sync_binding` reads an in-memory registry before async hooks. `SyncEngine._push_peer` and `converge_realm` read in-memory peer state; every durable/log/object call below them crosses a named runtime boundary.

## Measurements

The repeatable benchmark injects a 100 ms slow operation at each proven boundary and times an unrelated request-sized coroutine (`asyncio.sleep(5 ms)`). It is a regression benchmark rather than a production capacity claim. Raw output is checked in at [async-responsiveness-2026-07-22.json](benchmarks/async-responsiveness-2026-07-22.json).

| Scenario | Historical direct-on-loop unrelated latency | Bounded/native-async unrelated latency |
|---|---:|---:|
| Git | 299.410 ms | 6.105 ms |
| Disk | 182.372 ms | 6.208 ms |
| SQLite | 290.880 ms | 6.220 ms |
| Provider | 243.365 ms | 6.225 ms |
| Sync | 175.093 ms | 6.230 ms |
| PR supervisor | 296.994 ms | 6.249 ms |
| Subprocess | 234.912 ms | 6.178 ms |
| Peer HTTP | n/a (native async target) | 5.444 ms |
| Browser startup | n/a (native async target) | 6.358 ms |

During the mixed run, loop-lag p95 was 1.422 ms. The 294.281 ms maximum was recorded during the intentionally blocking historical-control phases; bounded phases kept unrelated work near the 5 ms probe floor.

Run the benchmark with:

```console
uv run --offline python scripts/measure_async_responsiveness.py
```

## Observability and regression guard

`GET /api/runtime` is intentionally in-memory and reports executor active/queued/limits, loop-lag latest/p95/max, request count/average/max and slow attribution, per-operation submit/complete/fail/timeout/cancel/reject/queue/runtime metrics, lifecycle state, transcript batches/buffered events, prompt depth, active dispatch count, and provider-action active/queued limits. Slow off-loop calls log their operation name and duration.

`tests/test_async_runtime.py`, `tests/test_async_subprocess.py`, and `tests/test_async_responsiveness.py` cover hard admission limits, total deadlines, charged cancellation, shutdown rejection, process-group cleanup/output bounds, blocked Git/disk/SQLite/provider/dispatch/transcript/PR/Fleet work, SSE responsiveness, concurrent health requests, and runtime telemetry. Subsystem suites cover update recovery, browser cancellation, sync fencing, dispatch idempotency, and supervisor ownership.

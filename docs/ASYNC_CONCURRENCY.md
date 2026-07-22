# Async concurrency ownership

PA has one explicit boundary for legacy blocking work and native async ownership for network and process lifecycles. New server code must make that ownership visible at the call site.

## Choose the boundary

Use native async APIs for HTTP, socket/DNS, subprocesses, timers, and queue/event waits. Use the application `AsyncRuntime.run_blocking(operation, callable, ...)` for bounded legacy filesystem, SQLite, Git-library, provider, serialization/diffing, or configuration calls. Give every operation a stable low-cardinality name and a deadline appropriate to the resource.

Do not call `asyncio.to_thread` from server code when the application runtime is available: the default executor has no PA queue limit, saturation metrics, shutdown gate, or total deadline. A `to_thread` fallback is acceptable only in a reusable helper whose production server caller always injects `AsyncRuntime`, and the fallback must be documented as CLI/test-only.

Synchronous FastAPI endpoints and synchronous MCP handlers already have a framework worker boundary. Keep them synchronous when the entire handler is bounded legacy work. Convert them to async when they need pooled async HTTP, streaming, cancellation-aware processes, or coordination with async queues.

## Resource ownership

- The kernel owns `AsyncRuntime`. Module startup may borrow it; kernel shutdown closes it after module and agent drains.
- A module that opens an `httpx.AsyncClient` owns and closes that client in `on_shutdown`. Set connection/pool/read/write deadlines and finite connection limits. Decode potentially large JSON off-loop.
- `run_process` owns its process group through exit, timeout, output-limit failure, or cancellation. Do not wrap long-lived processes in a worker thread.
- If a legacy library internally launches long-lived subprocesses, isolate the whole action behind `run_process` (as provider install/update/probe do) so cancellation terminates the descendant process group. Never mutate global `os.environ`; pass a copied child environment.
- The component that creates a background task stores its handle, rejects duplicate starts, cancels and awaits it on shutdown, and exposes queue/active depth.
- Durable queues preserve their existing transaction, operation ID, fencing, and sequence semantics. Moving a write to a thread does not permit concurrent writers or fire-and-forget durability.

## Cancellation and deadlines

Treat a deadline as including admission/queue time. Caller cancellation of a Python thread is not worker cancellation: never release its capacity slot until the real future ends, and make retries idempotent. Prefer operations that are naturally short or accept their own database/socket timeout.

On cancellation, subprocess owners terminate the process group and await reaping. HTTP cancellation closes the response or stream and returns the pooled connection. A producer that owns durable buffered data must restore an uncommitted batch or drain it at an explicit durability boundary.

Long lifecycle operations report persisted phases and keep health/runtime endpoints independent. Quiesce must reject new work explicitly; it must not obtain a global lock that health/status needs. Shutdown waits are finite and must leave enough persisted state for restart recovery.

## Capacity and backpressure

Bound all three dimensions independently:

1. concurrency (workers, connections, active tasks);
2. queued work (executor admission, prompt/transcript/job queues);
3. payload size (subprocess output, buffered request/response bodies, SSE batches).

When capacity is unavailable, fail explicitly with a retryable error or pause the owning session. Do not create another executor/client, silently enqueue without a limit, or launch duplicate refresh work. Use single-flight keys for cache refresh and idempotency keys for durable dispatch/update work.

## Review checklist

- Does any async function directly call filesystem, SQLite, synchronous HTTP, Git/subprocess, provider discovery, serialization/diffing, or a lock whose holder can perform I/O?
- Is every transitive external operation cancellable or off-loop and protected by a total deadline?
- Are client, executor, task, process, and queue ownership plus shutdown behavior explicit?
- Can timeout/cancellation cause a late thread completion to duplicate or reorder a durable write?
- Are concurrency, queue depth, and payload bytes bounded?
- Does `/api/runtime` expose enough operation attribution to diagnose saturation or lag?
- Does a regression test deliberately block this boundary while unrelated health, HTTP/MCP, SSE, and agent work remains responsive?
- Did `python scripts/audit_async_boundaries.py` regenerate `docs/ASYNC_BOUNDARY_INVENTORY.md` after adding the new endpoint/worker/callback?

# Database and API performance

This change targets the runtime health request, dispatch polling, and transcript
ingestion identified on card `89957d94-db93-4bf4-b534-3fbea8ba783c`. The investigation
uses disposable fixtures, without reading or changing the installed PA database,
configuration, or running service. The source observation's cumulative operation
maxima are not percentiles and do not establish a production locking root cause.

## Findings and changes

`DispatchWorker.snapshot()` called `queue_snapshot()` only to read its total.
That constructed public queue records, copied up to 1,000 historical dispatches
twice, and ran storage diagnostics. Runtime then ran the storage diagnostics
again. A 200-dispatch fixture with retained placement evidence reproduced this
amplification. Worker health now reads a scalar queue count under the short index
lock. Runtime reads numeric transaction observations and the last explicit
storage diagnostic, with its timestamp or `not_measured`; it never starts those
SQL counts or waits for the dispatch writer transaction. The explicit queue
endpoint also reuses one shallow selection for its historical scalar statistics.

Card polling renders the latest lifecycle message, state, target, error, and
small placement/materialization explanations. It does not render candidate
proofs or progress history. `GET /api/fleet/dispatch-jobs/{id}?view=status` projects
those fields without deep-copying the record. The authority proxy and MCP
`get_dispatch(view="status")` preserve that selection. The default `full` response
retains the existing detail contract. The card poller uses the status view.
Fleet's SSE diagnostic panel requests `/api/runtime?section=sse`, which assembles
only transport diagnostics. The reproducible fixture deliberately duplicates the
same placement subtree inside execution selection and the placement decision;
the benchmark reports its byte count. This is a measured synthetic example, not
a claim about exact duplication in the historical 190,849-character response.

Transcript ingestion previously compressed and fsynced later batch objects after
the first row write had already acquired SQLite's writer. Payload normalization
and compression now precede the Python writer lock; object publication precedes
`BEGIN IMMEDIATE`. The object publication/GC lock still protects the interval
between filesystem publication and reference commit. A failed transaction can
leave an unreferenced content-addressed file, which the existing collector can
remove. Event identity, sequence replacement, reference counts, and their rollback
remain one SQLite transaction. The separate metadata compatibility mirror retains
its existing post-commit behavior.

Replacement lookup now probes exact sequence and ID keys in chunks of at most
500 parameters. Sparse batches no longer select every row between their minimum
and maximum sequence. Existing unique indexes serve those probes; no speculative
index is added. Object reference deltas coalesce per digest and correctly account
for repeated sequences and event ID replacement within a batch.

## Measurement coverage

Runtime operations expose queue and runtime p50/p95/p99 over their last 256
observations. Instrumented Python locks expose wait/hold observations even for
operations without a deadline policy, including card/session work. Nested lock
holds can overlap; their sum is not an exclusive wall-clock breakdown. Lifetime
counters and maxima keep their existing meanings.

HTTP diagnostics use route templates, cap route cardinality at 128, count ASGI
response-body bytes, and retain 256 latency/size samples per route. SSE response
lifetimes are excluded from ordinary HTTP latency and counted separately.
Transcript numeric diagnostics expose preparation/publication time, input rows
and canonical bytes, lookup rows returned, attempted/committed row changes,
Python lock wait, SQLite writer acquisition, and transaction hold. Dispatch
transactions expose acquisition/hold observations too. SQLite BUSY/LOCKED failures
are counted when writer acquisition fails.

SQLite writer acquisition times include `BEGIN IMMEDIATE` execution and the
SQLite busy-handler wait. Python sqlite3 does not expose a separate successful
busy-wait callback; these numbers must not be relabeled as pure SQLite busy time.
Lookup rows are returned rows, not engine rows scanned. Exact scan counts and
per-statement timings across all PA databases remain outside this measurement
coverage. No query text, arguments, transcript bodies, or user identifiers are
retained in the new numeric diagnostics.

## Reproduction and validation

Run `uv run python scripts/benchmark_database_api.py`. It creates temporary
databases and object files, seeds 200 dispatches, measures 30 snapshots, and runs
50 iterations each of transcript batches, card writes, session writes, and reads
on four concurrent threads. It reports p50/p95/p99, JSON serialization, response
bytes, peak traced allocation, SQL statement verbs, and metadata write counts.
Use `PYTHONPATH` pointing at the base commit's extracted `src` to run exactly the
same script against the baseline. Tracemalloc is enabled for the snapshot
microbenchmarks; timings are comparisons within that fixture, not service SLOs.

Regression tests cover status fields and authority forwarding, writer-independent
runtime status, reader/writer overlap during delayed fsync, GC/publication
coordination, sparse replay, duplicate sequences and IDs, coalesced reference
writes, rollback, SQLite acquisition versus hold, SSE exclusion, route privacy,
and bounded percentile storage.

Measured on the same host against base `61e833fd`, with other local test/build
runs finished before the comparison:

| Fixture measurement | Baseline | Changed |
| --- | ---: | ---: |
| Runtime snapshot p95 | 163.984 ms | 0.624 ms |
| Runtime peak traced allocation | 1,557,184 bytes | 22,250 bytes |
| SQL statements across status samples | 310 SELECTs | 0 |
| Full/status dispatch JSON | 134,157 bytes | 1,901 bytes |
| Full/status JSON serialization p95 | 1.662 ms | 0.229 ms |
| Mixed transcript batch p95 | 661.923 ms | 23.736 ms |
| Mixed card write p95 | 104.796 ms | 3.200 ms |
| Mixed session write p95 | 125.077 ms | 3.988 ms |
| Mixed read p95 | 127.062 ms | 2.299 ms |
| Mixed SQL SELECT statements | 600 | 150 |

Both runs retained all 500 transcript events/references and compatibility rows,
passed transcript integrity with zero missing objects, and committed zero
dispatch mutations during status sampling. Both performed the same 500 transcript
mirror, 50 card, and 50 session metadata row changes. The normal unique-payload
workload does not reduce these durable writes; a separate repeated-payload test
checks that ten event rows share one reference-delta update. The new transcript
transaction hold p95 was 2.241 ms, measured separately from object publication
(10.791 ms p95) and SQLite writer acquisition (0.043 ms p95). The duplicate
placement subtree in this fixture was 64,248 bytes. Raw results are in
[`database-api-baseline.json`](benchmarks/database-api-baseline.json) and
[`database-api-after.json`](benchmarks/database-api-after.json).

Focused storage, queue, dispatch, API, metrics and schema checks passed, as did
isolated boot and wheel/source builds. A four-worker local suite was interrupted
after 910 passes and two timing-sensitive failures (browser retry count and
40 ms maximum event-loop lag); both failures passed when rerun alone. The PR's
full CI remains the merge gate. No service restart or deployment is included.

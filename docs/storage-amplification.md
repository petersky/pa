# Storage remediation, 2026-11-10

Scope: card fbb33d02, session08e45889, instance macbook/0c7d8ecb, repository
petersky/pa. Base706b75ba8295bf8d5bab80064708d3d871991fa0. Execution uses the
PA-leased card/session branch recorded by PR706 and its durable watch.

The bounded metadata walk (one lstat per file, no archive decoding, no service-data
reads) found43 registered Git worktrees totaling48,167,262,087 logical bytes.
[Per-root measurements and current durable owners](benchmarks/storage-worktrees-before.csv)
include legacy unleased checkouts; an unleased classification is not disposal
permission. Counts differ from PA lease counts because retained legacy Git
worktrees also appear in `git worktree list --porcelain`.

The inherited docs/benchmarks corpus was381 files/3,115,470,183 logical bytes.
[248 retired paths](benchmarks/retired-storage.csv) remove3,108,529,232 bytes
(99.78%). Compact JSON summaries, CSV comparisons, Markdown and synthetic code/tests
remain. Documentation references now link to the original revision. Historical
statements claiming every raw input is retained are superseded by the directory's
retention policy. No test consumes the deleted files.

Build reproduction:

```
uv run pytest tests/test_build_verified.py -q
uv run python scripts/build_verified.py --revision <exact-commit> --output .dev/wheels-<commit>
```

On source24a49a3a, the exported package/build inputs total8,829,796 bytes and the
wheel is2,422,066 bytes (SHA2561871c7a210aa3afd4f0dbfe8d17d073942be047f82b808b779ecec33c0ece8b0).
A 10ms metadata sampler measured11,253,936 bytes peak within the temporary
source/build-output tree and2,422,409 retained wheel+attestation bytes. The
temporary path was absent after exit; the additional measurement wheel was also
removed on exit. Only the original wheel and JSON attestation remain. Source replication peaks at one
selective tree, rather than a full tar plus full extracted checkout; the default
rejects source larger than64MiB before export. The source+wheel logical payload is
11,251,862 bytes; this is not a claim about total process peak disk use. Build
isolation/dependency metadata and uv's existing shared cache are additional.
Success and build failure regressions verify temporary source removal and exact
committed bytes despite dirty working files. They also verify unrelated retained
rollback wheels do not enter a new build attestation.

Existing cleanup remains owned by b17f2120/dispatchcf998fd2/session1b436b1d.
Its authoritative progress receipt at04:20:08Z reports removal of its own two build
duplicates (6,308,626,432 allocated bytes), separately from nine selected roots
(548,864 allocated bytes). This card neither repeats nor claims those effects.
Its remaining five cases stay with the lifecycle/custody owners:12ddbed6 lacks
legacy durable idle evidence;0bf67ce4/a99f42b0 have queued prompts and future
expiry;92d73d8c lease44 is unexpired;7938e21e has dirty/custody evidence plus a
newer unselected lease. The cleanup card remains waiting.

After merge, new materializations inherit the reduced tree. Existing workers must
finish or safely update through their owners and PA lifecycle; they retain old
tracked results until then. No other active branch was reset, cleaned or edited.
This source/tooling change does not activate a new running PA package. No runtime
restart is necessary to use the repository build helper. Git history and dependency
caches remain; no repack/GC/history rewrite or cache purge was performed. APFS
allocation/clone/snapshot effects prevent equating deleted logical bytes with
exclusive physical reclamation.

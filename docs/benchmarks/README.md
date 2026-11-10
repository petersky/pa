# Benchmark retention

The current checkout retains compact summaries, CSV comparisons, and small reference
inputs. On 2026-11-10, 248 historical compressed dumps, multipart manifests and
JSON snapshots larger than 128 KiB were retired (3,108,529,232 logical bytes).
[retired-storage.csv](retired-storage.csv) lists every removed path and byte count.
They remain recoverable at Git commit `706b75ba8295bf8d5bab80064708d3d871991fa0`.
No replacement archives, attachments, LFS objects or history rewrite were created.

Keep useful current summaries within 128 KiB per file and this directory within
10 MiB. Reproduction scripts and synthetic tests remain in scripts/ and tests/;
none of the deleted corpus was a test fixture. Generate raw observations only in a
bounded disposable directory, summarize the result, then remove the raw output.
Do not reconstruct retained-parent evidence chains in new worktrees. Historical
provenance verification requires the original revision; it does not describe current
runtime state or close any existing findings/cards.

For exact package builds use `scripts/build_verified.py` as documented in AGENTS.md.
It streams only the committed package/build inputs into a single temporary tree,
with no source tar on disk, reuses uv's cache and retains a wheel and attestation.
Normal `uv build` also uses selective wheel/sdist inputs from pyproject.toml.

Deleting tracked files reduces future checkouts and source exports after integration.
Existing active worktrees retain their old tracked files until their owners safely
update or finish. Do not reset, clean or edit another owner's tree. Git object
history, dependency caches, APFS clones and snapshots can retain blocks; logical or
allocated file totals are not proof of exclusive physical disk reclamation.

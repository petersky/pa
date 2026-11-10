# Selected post-collection fixture disposal

`workspace_collect_selected` accepts optional `residual_inventory_sha256` on
an exact lease target. It applies only to already-cleaned leases. Ordinary
Git collection retains all existing gates and does not infer disposal consent.

The digest is SHA256 of `json.dumps(rows, sort_keys=True).encode()`, with rows
sorted by relative POSIX path. `pa.repository.residual_cleanup.snapshot` defines
the inventory (including inode, mtime, kind, size and file SHA256). Inventory
reads do not open fixture databases as databases. Bound: 256 entries / 16 MiB.

The collector holds card admission and repository locks and requires every
lease under the selected card container cleaned, every durable owner closed
with explicit empty queue/no in-flight turn, no runtime/restart continuation,
and current Done or missing-card eligibility. It rejects links, Git metadata,
open files and changed inventories. Only empty directories, checkout-local
pytest cache, fixture `workspace_leases.db`, and `.dev/tmp` automation browser
fixture data are disposable. Other files require custody outside this operation.
No shared cache, dependency root, session store or user browser profile is touched.

The server saves an intent receipt on the selected lease before unlinking exact
inventoried files and removing empty directories. Read `workspace_leases` after
any interrupted response; do not resend an uncertain mutation. A `disposing`
receipt means partial effects are possible. Re-inventory actual disk state before
any new selection. Successful receipts record actual container absence, allocation
and sampled volume availability; these are not exclusive APFS reclamation claims.
Unexpected concurrent recreation can block empty-directory removal. Always verify
disk absence again after the receipt, and report any recreation instead of looping.

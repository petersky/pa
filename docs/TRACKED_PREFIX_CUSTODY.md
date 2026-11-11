# Bounded tracked-prefix custody

`workspace_tracked_prefix_custody` preserves truncated tracked files from an exact
terminal, expired, offline-idle selected worktree with an empty index. It does not
repair, materialize, relocate, remove or authorize collection of that worktree.

1. Call `prepare` with the observed lease/fence/HEAD/updated-at target and a stable
   idempotency key. Only changed regular HEAD prefixes (including empty files)
   are archived: at most 64 files and 64 MiB. Matching files and missing HEAD
   paths are not copied or recreated. Original index bytes and file ownership,
   mode, path, inode, timestamps and hashes are recorded in the ZIP manifest.
2. The ZIP is returned as at most three 24 MiB parts, each within the existing
   25 MiB attachment limit. Attach each part through the normal durable API.
   Existing card attachment count and total-byte limits remain in force. PA
   removes its temporary joined ZIP after fsyncing verified parts and receipt.
3. Call `verify` with the same key/target and ordered `attachments` entries
   containing `part`, `card_id`, `attachment_id`, `sha256`. PA verifies every
   retrieved part and the combined archive hash, seeks across parts without
   creating another joined copy, verifies preserved files/index, and rechecks
   attachment availability at the final boundary.
4. Use `status` with the original identity after an uncertain result. A `writing`
   receipt remains unknown; repeated prepare never overwrites it or replays work.

The service refreshes origin normally and requires HEAD to have no commits outside
those refs. Lease, acceptance, restart, native worktree and exact durable owner
idle/control guards apply before inspection and after the final source scan.
Links, non-prefix changes, untracked/ignored data, open changed files/index,
writers, over-budget input and changed evidence fail closed. Files outside HEAD
need their own supported custody. The API excludes concurrent provider admission
and rejects a live runtime. Original files, index, refs, prompts and owner controls
are unchanged, including when attachment verification fails.

The resulting receipt proves custody of the bounded changed files only. A dirty
or partial checkout still requires independently reviewed lifecycle handling and
fresh integration/acceptance/ownership guards before any collection. This tool
does not make an empty index or a completed card proof of disposable data.

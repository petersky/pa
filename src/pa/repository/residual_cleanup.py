"""Explicit, bounded disposal of fixture remnants after native Git collection.

The caller supplies SHA256 of the sorted JSON inventory returned by snapshot.
No recursive removal, Git operation, database opening, or session mutation occurs.
"""
from __future__ import annotations

import hashlib
import json
import shutil
import stat
from pathlib import Path

from pa.repository.workspace import WorkspaceProvisioningError
from pa.repository.untracked_preservation import _no_open_files


def snapshot(root: Path) -> tuple[list[dict], str]:
    rows = []
    pending = [root]
    total = 0
    while pending:
        p = pending.pop()
        info = p.lstat()
        relative = p.relative_to(root).as_posix()
        if stat.S_ISDIR(info.st_mode):
            kind, digest = 'directory', None
            pending.extend(p.iterdir())
        elif stat.S_ISREG(info.st_mode) and info.st_nlink == 1:
            total += info.st_size
            if total > 16 * 1024 * 1024:
                raise WorkspaceProvisioningError('Residual exceeds 16 MiB bound')
            kind, digest = 'file', hashlib.sha256(p.read_bytes()).hexdigest()
        else:
            raise WorkspaceProvisioningError('Residual contains a link or special file')
        if '.git' in p.relative_to(root).parts:
            raise WorkspaceProvisioningError('Residual contains Git metadata')
        rows.append(dict(path=relative, kind=kind, sha256=digest,
                         bytes=info.st_size if kind == 'file' else 0,
                         inode=info.st_ino, mtime_ns=info.st_mtime_ns))
        if len(rows) + len(pending) > 256:
            raise WorkspaceProvisioningError('Residual exceeds 256 entry bound')
    rows.sort(key=lambda row: row['path'])
    digest = hashlib.sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest()
    return rows, digest


def collect_residual(manager, lease, target, active: set[str]) -> dict:
    root = Path(lease.worktree_path).parent.parent
    if root.parent != manager.root / 'worktrees':
        raise WorkspaceProvisioningError('Residual is outside card-container layout')
    with manager.admission_guard(card_id=lease.card_id, session_id=lease.session_id,
                                 allow_concurrent=True):
        with manager._repository_lock(Path(lease.cache_path).name):
            def guard():
                manager.selected_cleanup_leases([target])
                siblings = [l for l in manager.list(card_id=lease.card_id)
                            if Path(l.worktree_path).parent.parent == root]
                lanes = manager.store.list_card_lanes()
                if lanes.get(lease.card_id) not in {None, 'done'}:
                    raise WorkspaceProvisioningError('Residual card is unfinished')
                if not manager._card_completion_eligible(lease.card_id):
                    raise WorkspaceProvisioningError('Residual acceptance is pending')
                for sibling in siblings:
                    owner = manager.store.get_session(sibling.session_id)
                    durable = (owner.config_json or {}).get('durable_runtime') if owner else None
                    if (sibling.state != 'cleaned' or sibling.session_id in active
                            or not owner or owner.status != 'closed'
                            or not isinstance(durable, dict)
                            or durable.get('in_flight', 'unknown') is not None
                            or durable.get('queued_prompts') != []
                            or manager.store.pending_restart_continuation(session_id=sibling.session_id)):
                        raise WorkspaceProvisioningError('Residual owner is not confirmed closed and idle')
            guard()
            manager._assert_managed_path(root)
            if not root.exists():
                return dict(state='residual_absent', container_removed=True)
            rows, digest = snapshot(root)
            if digest != target.residual_inventory_sha256:
                raise WorkspaceProvisioningError('Residual inventory changed')
            # Only explicitly inventoried pytest/automation fixtures are disposable.
            tree = Path(lease.worktree_path)
            for row in rows:
                p = root / row['path']
                if row['kind'] == 'file':
                    rel = p.relative_to(tree).as_posix() if tree in p.parents else ''
                    if not (rel == 'workspace_leases.db' or rel.startswith('.pytest_cache/')
                            or (rel.startswith('.dev/tmp/') and '/automation-session-home/' in rel)):
                        raise WorkspaceProvisioningError('Residual has unclassified useful data')
            for row in rows:
                if row['kind'] == 'directory':
                    _no_open_files(root / row['path'])
            guard()
            if snapshot(root)[1] != digest:
                raise WorkspaceProvisioningError('Residual changed before disposal')
            allocated = sum((root / r['path']).lstat().st_blocks * 512 for r in rows)
            free_before = shutil.disk_usage(root).free
            receipt = dict(inventory_sha256=digest, phase='disposing',
                           container_path=str(root), allocated_bytes_before=allocated,
                           filesystem_free_bytes_before=free_before)
            lease.cleanup_evidence = {**lease.cleanup_evidence, 'residual_cleanup': receipt}
            manager._save(lease)
            for row in sorted(rows, key=lambda r: len(Path(r['path']).parts), reverse=True):
                p = root / row['path']
                if row['kind'] == 'file':
                    info = p.lstat()
                    if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                            or info.st_ino != row['inode'] or info.st_mtime_ns != row['mtime_ns']
                            or hashlib.sha256(p.read_bytes()).hexdigest() != row['sha256']):
                        raise WorkspaceProvisioningError('Residual file changed during disposal')
                    p.unlink()
                else:
                    p.rmdir()
            receipt.update(phase='complete', removed_allocated_bytes=allocated,
                           filesystem_free_bytes_after=shutil.disk_usage(root.parent).free,
                           container_removed=not root.exists())
            manager._save(lease)
            return dict(state='residual_cleaned', container_removed=not root.exists(),
                        reclaimed_bytes=allocated, inventory_sha256=digest)

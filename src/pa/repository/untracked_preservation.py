"""Relocate an attachment-backed untracked file or flat directory intact."""
from __future__ import annotations

import hashlib
import json
import os
import stat
import subprocess
import zipfile
from datetime import UTC, datetime
from pathlib import Path

from pa.core.io import atomic_write_json
from pa.repository.materialization import digest, _sync_dir
from pa.repository.workspace import WorkspaceProvisioningError

MAX_FILES = 64
MAX_BYTES = 16 * 1024 * 1024


def _archive_manifest(archive: Path, expected_sha256: str, lease) -> dict:
    if archive.stat().st_size > MAX_BYTES or digest(archive) != expected_sha256:
        raise WorkspaceProvisioningError('Preservation attachment bytes changed or exceed capacity')
    with zipfile.ZipFile(archive) as z:
        names = z.namelist()
        if len(names) != len(set(names)) or len(names) > MAX_FILES + 1:
            raise WorkspaceProvisioningError('Preservation attachment has duplicate or excessive entries')
        if any(i.file_size > MAX_BYTES for i in z.infolist()) or sum(i.file_size for i in z.infolist()) > MAX_BYTES:
            raise WorkspaceProvisioningError('Preservation attachment exceeds decoded capacity')
        manifest = json.loads(z.read('preservation-manifest.json'))
        if (not isinstance(manifest, dict) or manifest.get('source_card_id') != lease.card_id
                or manifest.get('source_lease_id') != lease.id):
            raise WorkspaceProvisioningError('Preservation attachment belongs to another lease')
        files = manifest.get('files')
        if not isinstance(files, list) or not files or len(files) > MAX_FILES:
            raise WorkspaceProvisioningError('Preservation attachment has no bounded file manifest')
        expected = {}
        for row in files:
            if not isinstance(row, dict):
                raise WorkspaceProvisioningError('Preservation attachment file manifest is invalid')
            name = row['name']
            # Only one file or flat, explicitly named directory. No symlinks, recursion,
            # index restoration, ignored caches, or arbitrary archive extraction.
            if (not isinstance(name, str) or name.casefold() in {'.', '..', '.git'}
                    or Path(name).name != name):
                raise WorkspaceProvisioningError('Preservation attachment has an unsafe path')
            if name in expected:
                raise WorkspaceProvisioningError('Preservation attachment repeats a file')
            data = z.read(name)
            if len(data) != row['bytes'] or hashlib.sha256(data).hexdigest() != row['sha256']:
                raise WorkspaceProvisioningError('Preservation attachment file checksum mismatch')
            expected[name] = (len(data), row['sha256'])
        if set(names) != {*expected, 'preservation-manifest.json'}:
            raise WorkspaceProvisioningError('Preservation attachment has unmanifested entries')
    return expected


def _snapshot(directory: Path, expected: dict, *, single_file: bool = False) -> list[dict]:
    if single_file:
        if directory.is_symlink() or not directory.is_file():
            raise WorkspaceProvisioningError('Preservation requires a regular standalone file')
        children = [directory]
    else:
        if directory.is_symlink() or not directory.is_dir():
            raise WorkspaceProvisioningError('Preservation requires a regular directory')
        children = []
        for child in directory.iterdir():
            children.append(child)
            if len(children) > MAX_FILES:
                raise WorkspaceProvisioningError('Untracked directory exceeds preservation entry capacity')
    if {p.name for p in children} != set(expected):
        raise WorkspaceProvisioningError('Untracked directory entries changed or are not fully preserved')
    rows = []
    for p in sorted(children):
        info = p.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise WorkspaceProvisioningError('Preservation requires unlinked regular files')
        if info.st_size != expected[p.name][0] or digest(p) != expected[p.name][1]:
            raise WorkspaceProvisioningError('Untracked bytes differ from the durable attachment')
        rows.append({'name': p.name, 'bytes': info.st_size, 'sha256': expected[p.name][1],
                     'mode': info.st_mode, 'inode': info.st_ino, 'mtime_ns': info.st_mtime_ns})
    return rows


def _no_open_files(directory: Path) -> None:
    # A missing/denied/timed-out observer is not proof that files are unused.
    try:
        names = ['+d', str(directory)] if directory.is_dir() else [str(directory)]
        r = subprocess.run(['lsof', '-nP', '-Fpcfn', *names],
                           capture_output=True, timeout=5, check=False)
    except (OSError, subprocess.TimeoutExpired):
        raise WorkspaceProvisioningError('Open-file observer is unavailable; source retained') from None
    if r.returncode != 1 or r.stdout or r.stderr:
        raise WorkspaceProvisioningError('Open-file check is busy or unavailable; source retained')


class UntrackedPreservation:
    def __init__(self, manager):
        self.manager = manager

    def _tracked_dirty(self, tree: Path) -> bool:
        # Ordinary status may refresh the index; this preservation operation must
        # retain even its original bytes and must not call a filesystem monitor.
        output = self.manager._git('--no-optional-locks', '-c', 'core.fsmonitor=false',
            '-C', str(tree), 'status', '--porcelain=v1', '-z').stdout
        return any(p and not p.startswith('?? ') for p in output.split('\0'))

    def operate(self, *, target, relative_path: str, attachment_path: Path,
                attachment_sha256: str, principal: str, action: str) -> dict:
        target_identity = target.model_dump(mode='json')
        # Keep existing preservation operation identities byte-for-byte stable.
        for optional in ('expected_actual_branch', 'expected_native_id', 'expected_owner_updated_at',
                         'residual_inventory_sha256'):
            if target_identity.get(optional) is None:
                target_identity.pop(optional, None)
        fingerprint = {'target': target_identity, 'relative_path': relative_path,
                       'attachment_sha256': attachment_sha256, 'principal': principal}
        key = hashlib.sha256(json.dumps(fingerprint, sort_keys=True).encode()).hexdigest()
        root = self.manager.root / 'preserved-untracked' / key
        self.manager._assert_managed_path(root)
        receipt = root / 'receipt.json'
        if action == 'status':
            if not receipt.exists():
                if root.exists():
                    return {'operation_id': key, 'phase': 'incomplete', 'effect': 'unknown'}
                return {'operation_id': key, 'phase': 'not_started', 'effect': 'none'}
            saved = json.loads(receipt.read_text())
            if saved['fingerprint'] != fingerprint:
                raise WorkspaceProvisioningError('Preservation receipt identity changed')
            return saved['public']
        if ('\x00' in relative_path or Path(relative_path).name != relative_path
                or relative_path in {'', '.', '..', '.git'}):
            raise WorkspaceProvisioningError('Preserve one explicit top-level file or directory only')
        lease = self.manager.selected_cleanup_leases([target])[0]
        with self.manager.admission_guard(card_id=lease.card_id, session_id=lease.session_id):
            with self.manager._repository_lock(Path(lease.cache_path).name):
                if root.exists():
                    # Never replay a possibly completed rename after an interruption.
                    raise WorkspaceProvisioningError('Preservation already has a receipt; inspect status')
                lease = self.manager.selected_cleanup_leases([target])[0]
                tree = Path(lease.worktree_path)
                self.manager._assert_managed_path(tree)
                self.manager._assert_managed_path(Path(lease.cache_path))
                if tree.is_symlink():
                    raise WorkspaceProvisioningError('Selected preservation worktree is unsafe')
                self.manager._verify_selected_worktree(lease, target)
                index = Path(self.manager._git('-C', str(tree), 'rev-parse', '--path-format=absolute',
                                               '--git-path', 'index').stdout.strip())
                if index.is_symlink() or index.with_name('index.lock').exists():
                    raise WorkspaceProvisioningError('Index has a writer or unsafe path')
                head = self.manager._git('-C', str(tree), 'rev-parse', '--verify', 'HEAD').stdout.strip()
                if head != target.expected_head:
                    raise WorkspaceProvisioningError('Selected preservation HEAD changed')
                if (not lease.completed or not lease.merged or lease.expires_at > datetime.now(UTC)
                        or (lease.card_id and not self.manager._card_completion_eligible(lease.card_id))
                        or self.manager.store.pending_restart_continuation(session_id=lease.session_id)):
                    raise WorkspaceProvisioningError('Existing completion, expiry, acceptance or restart guard')
                directory = tree / relative_path
                self.manager._assert_managed_path(directory)
                if self.manager._git('-C', str(tree), 'ls-files', '-z', '--', ':(literal)' + relative_path).stdout:
                    raise WorkspaceProvisioningError('Preservation path contains tracked data')
                if self._tracked_dirty(tree):
                    raise WorkspaceProvisioningError('Tracked changes must be preserved by their owner')
                expected = _archive_manifest(attachment_path, attachment_sha256, lease)
                single_file = not directory.is_symlink() and directory.is_file()
                snapshot = _snapshot(directory, expected, single_file=single_file)
                _no_open_files(directory)
                # Recheck immediately before the sole worktree mutation.
                self.manager.selected_cleanup_leases([target])
                self.manager._verify_selected_worktree(lease, target)
                if _snapshot(directory, expected, single_file=single_file) != snapshot:
                    raise WorkspaceProvisioningError('Untracked directory changed during preservation')
                if (self.manager._git('-C', str(tree), 'rev-parse', 'HEAD').stdout.strip() != target.expected_head
                        or self._tracked_dirty(tree) or index.with_name('index.lock').exists()):
                    raise WorkspaceProvisioningError('Tracked state changed before preservation')
                if self.manager.store.pending_restart_continuation(session_id=lease.session_id):
                    raise WorkspaceProvisioningError('Restart continuation became pending')
                if lease.card_id and not self.manager._card_completion_eligible(lease.card_id):
                    raise WorkspaceProvisioningError('Card acceptance changed before preservation')
                root.mkdir(parents=True, mode=0o700)
                destination = root / 'files'
                if single_file:
                    destination.mkdir(mode=0o700)
                    destination = destination / directory.name
                    _sync_dir(root)
                _sync_dir(root.parent); _sync_dir(root.parent.parent)
                public = {'operation_id': key, 'phase': 'relocating', 'effect': 'unknown',
                          'attachment_sha256': attachment_sha256, 'file_count': len(snapshot),
                          'preserved_bytes': sum(r['bytes'] for r in snapshot),
                          'worktree_removed': False, 'reclaimed_bytes': 0}
                if single_file:
                    public['source_kind'] = 'file'
                atomic_write_json(receipt, {'fingerprint': fingerprint, 'snapshot': snapshot, 'public': public})
                # Atomic same-filesystem rename retains modes, timestamps and bytes.
                # No force/reset/clean, no branch/index mutation or ignored-file removal.
                os.rename(directory, destination)
                _sync_dir(tree); _sync_dir(destination.parent)
                if _snapshot(destination, expected, single_file=single_file) != snapshot:
                    raise WorkspaceProvisioningError('Relocated state requires manual reconciliation')
                public.update(phase='preserved', effect='complete')
                atomic_write_json(receipt, {'fingerprint': fingerprint, 'snapshot': snapshot, 'public': public})
                return public

"""Explicit, fenced recovery of an empty index; never discard an existing tree.

Preparation retains the complete worktree, original index and reachable HEAD
history outside the worktree. Apply only fills absent HEAD paths, after a fresh
snapshot comparison. Interrupted effects are observable and never auto-replayed.
"""
from __future__ import annotations

import hashlib
import json
import os
import stat
import subprocess
import tarfile
from pathlib import Path

from pa.core.io import atomic_write_json
from pa.repository.workspace import WorkspaceProvisioningError

MAX_SNAPSHOT_BYTES = 8 * 1024 * 1024 * 1024
MAX_SNAPSHOT_ENTRIES = 100000


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), 'rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(chunk)
    return h.hexdigest()


def inventory(root: Path) -> list[dict]:
    rows = []
    total = 0
    for directory, dirs, files in os.walk(root, followlinks=False):
        for name in sorted(dirs + files):
            path = Path(directory) / name
            info = path.lstat()
            row = {'path': path.relative_to(root).as_posix(),
                   'mode': stat.S_IMODE(info.st_mode), 'uid': info.st_uid,
                   'gid': info.st_gid, 'mtime_ns': info.st_mtime_ns}
            if stat.S_ISLNK(info.st_mode):
                row.update(kind='symlink', target=os.readlink(path))
            elif stat.S_ISDIR(info.st_mode):
                row.update(kind='directory')
            elif stat.S_ISREG(info.st_mode):
                total += info.st_size
                if total > MAX_SNAPSHOT_BYTES:
                    raise WorkspaceProvisioningError('Worktree exceeds bounded preservation capacity')
                row.update(kind='file', bytes=info.st_size, sha256=digest(path))
            else:
                raise WorkspaceProvisioningError('Special worktree files require manual preservation')
            rows.append(row)
            if len(rows) > MAX_SNAPSHOT_ENTRIES:
                raise WorkspaceProvisioningError('Worktree exceeds bounded preservation entry capacity')
    return sorted(rows, key=lambda row: row['path'])


def _sync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class MaterializationRepair:
    def __init__(self, manager):
        self.manager = manager

    def _lease(self, session_id, lease_id, card_id, fence):
        leases = [l for l in self.manager.list(card_id=card_id)
                  if l.id == lease_id and l.session_id == session_id]
        if len(leases) != 1 or leases[0].fencing_token != fence:
            raise WorkspaceProvisioningError('Exact lease or fencing token changed')
        lease = leases[0]
        if lease.state not in {'ready', 'failed'} or lease.completed or lease.merged:
            raise WorkspaceProvisioningError('Lease is not an unfinished materialization target')
        self.manager._verify_worktree(Path(lease.cache_path), Path(lease.worktree_path), lease)
        self.manager._assert_managed_path(Path(lease.worktree_path))
        return lease

    def _inspect(self, lease, expected_head):
        tree = Path(lease.worktree_path)
        git = lambda *args: self.manager._git('-C', str(tree), *args).stdout.strip()
        head = git('rev-parse', 'HEAD')
        if head != expected_head:
            raise WorkspaceProvisioningError('Worktree HEAD changed')
        if git('ls-files', '--stage', '-z'):
            raise WorkspaceProvisioningError('Only an empty index can be explicitly materialized')
        index = Path(git('rev-parse', '--path-format=absolute', '--git-path', 'index'))
        self.manager._assert_managed_path(index)
        if index.is_symlink() or index.with_name('index.lock').exists():
            raise WorkspaceProvisioningError('Index has an existing writer or unsafe path')
        entries = []
        raw = self.manager._git('-C', str(tree), 'ls-tree', '-rzl', '--full-tree', head).stdout
        for record in raw.split('\0'):
            if not record:
                continue
            metadata, name = record.split('\t', 1)
            mode, kind, oid, size = metadata.split()
            parts = Path(name).parts
            if (kind != 'blob' or mode not in {'100644', '100755', '120000'}
                    or not parts or Path(name).is_absolute() or '..' in parts
                    or any(p.casefold() == '.git' for p in parts)):
                raise WorkspaceProvisioningError('Unsupported tracked path; manual preservation required')
            for i in range(1, len(parts)):
                path = tree.joinpath(*parts[:i])
                if path.is_symlink():
                    raise WorkspaceProvisioningError('Tracked path has a retained symlink parent')
                if path.exists() and not path.is_dir():
                    raise WorkspaceProvisioningError('Tracked path collides with retained data')
            path = tree / name
            present = path.exists() or path.is_symlink()
            if present:
                info = path.lstat()
                if mode == '120000' and stat.S_ISLNK(info.st_mode):
                    blob = os.fsencode(os.readlink(path))
                    checksum = hashlib.new('sha1' if len(oid) == 40 else 'sha256')
                    checksum.update(b'blob ' + str(len(blob)).encode() + b'\0' + blob)
                elif mode in {'100644', '100755'} and stat.S_ISREG(info.st_mode):
                    if bool(info.st_mode & 0o111) != (mode == '100755'):
                        raise WorkspaceProvisioningError('Existing tracked mode differs from HEAD; retained for manual review')
                    checksum = hashlib.new('sha1' if len(oid) == 40 else 'sha256')
                    checksum.update(b'blob ' + str(info.st_size).encode() + b'\0')
                    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), 'rb') as stream:
                        for chunk in iter(lambda:stream.read(1024*1024), b''):checksum.update(chunk)
                else:
                    raise WorkspaceProvisioningError('Existing tracked path type differs from HEAD')
                if checksum.hexdigest() != oid:
                    raise WorkspaceProvisioningError('Existing tracked bytes differ from HEAD; retained for manual review')
            entries.append({'path': name, 'mode': mode, 'oid': oid, 'bytes': int(size), 'present':present})
        if not entries:
            raise WorkspaceProvisioningError('HEAD has no files to materialize')
        files = inventory(tree)
        index_hash = digest(index) if index.exists() else None
        state = {'head': head, 'index_sha256': index_hash,
                 'index_mode': stat.S_IMODE(index.stat().st_mode) if index.exists() else None,
                 'files': files, 'tracked': entries}
        revision = hashlib.sha256(json.dumps(state, sort_keys=True).encode()).hexdigest()
        return state, revision, index

    def operate(self, *, session_id, lease_id, card_id, expected_fence,
                expected_head, idempotency_key, principal, action, expected_revision=None,
                quarantine_untracked=False):
        fingerprint = dict(session_id=session_id, lease_id=lease_id, card_id=card_id,
                           expected_fence=expected_fence, expected_head=expected_head,
                           principal=principal, idempotency_key=idempotency_key,
                           quarantine_untracked=quarantine_untracked)
        key = hashlib.sha256(json.dumps([principal, idempotency_key]).encode()).hexdigest()
        root = self.manager.root / 'materialization-repairs' / key
        self.manager._assert_managed_path(root)
        receipt = root / 'receipt.json'
        if action == 'status':
            if not receipt.exists():
                return {'operation_id': key, 'phase': 'not_started', 'effect': 'none'}
            saved = json.loads(receipt.read_text())
            if saved['fingerprint'] != fingerprint:
                raise WorkspaceProvisioningError('Idempotency key belongs to a different exact repair')
            return saved['public']
        # Fence this card/lease, not the shared repository cache: streaming a
        # preservation archive must not delay another card's startup. Git reads
        # immutable objects using its own object-store concurrency guarantees;
        # the route fences the idle owner and apply owns its native index.lock.
        with self.manager.admission_guard(card_id=card_id, session_id=session_id):
            lease = self._lease(session_id, lease_id, card_id, expected_fence)
            with self.manager._repository_lock('materialization-' + lease.id):
                if receipt.exists():
                    saved = json.loads(receipt.read_text())
                    if saved['fingerprint'] != fingerprint:
                        raise WorkspaceProvisioningError('Idempotency key belongs to a different exact repair')
                    if action == 'status':
                        return saved['public']
                    if saved['phase'] == 'completed':
                        if action == 'apply' and expected_revision != saved['revision']:
                            raise WorkspaceProvisioningError('Preservation revision changed')
                        return saved['public']
                    if saved['phase'] != 'prepared':
                        raise WorkspaceProvisioningError('Preservation or repair outcome unknown; inspect this same operation')
                    if action == 'prepare':
                        return saved['public']
                elif action != 'prepare':
                    raise WorkspaceProvisioningError('No prepared preservation receipt for this exact operation')
                else:
                    state, revision, index = self._inspect(lease, expected_head)
                    needed = sum(r.get('bytes', 0) for r in state['files'])
                    import shutil
                    restore_bytes = sum(r['bytes'] for r in state['tracked'] if not r['present'])
                    if shutil.disk_usage(self.manager.root).free < needed * 2 + restore_bytes + 64 * 1024 * 1024:
                        raise WorkspaceProvisioningError('Insufficient preservation capacity; no worktree changes made')
                    root.mkdir(parents=True, mode=0o700)
                    saved = {'phase': 'preserving', 'revision': revision, 'fingerprint': fingerprint,
                             'public': {'operation_id': key, 'phase': 'preserving', 'effect': 'unknown'}}
                    atomic_write_json(receipt, saved)
                    if index.exists():
                        with (root / 'index.before').open('xb') as output:
                            output.write(index.read_bytes()); output.flush(); os.fsync(output.fileno())
                        os.chmod(root / 'index.before', state['index_mode'])
                    archive = root / 'worktree.before.tar'
                    with archive.open('xb') as output:
                        with tarfile.open(fileobj=output, mode='w') as tar:
                            for row in state['files']:
                                member = tarfile.TarInfo(row['path']); member.mode = row['mode']
                                member.uid = row['uid']; member.gid = row['gid']
                                member.pax_headers['mtime'] = f"{row['mtime_ns']//1000000000}.{row['mtime_ns']%1000000000:09d}"
                                if row['kind'] == 'file':
                                    member.size = row['bytes']
                                    with os.fdopen(os.open(Path(lease.worktree_path) / row['path'], os.O_RDONLY | os.O_NOFOLLOW), 'rb') as stream:
                                        tar.addfile(member, stream)
                                else:
                                    member.type = tarfile.SYMTYPE if row['kind'] == 'symlink' else tarfile.DIRTYPE
                                    if row['kind'] == 'symlink':member.linkname = row['target']
                                    tar.addfile(member)
                        output.flush(); os.fsync(output.fileno())
                    bundle = root / 'head.before.bundle'
                    # Preserve every commit not already reachable from the
                    # immutable base. Avoid copying gigabytes of shared history
                    # when this lease has no unique commits at all.
                    base_is_ancestor = self.manager._git('-C', lease.worktree_path, 'merge-base', '--is-ancestor', lease.base_sha, 'HEAD', check=False).returncode == 0
                    if expected_head == lease.base_sha:
                        bundle.write_text(json.dumps({'head': expected_head, 'base': lease.base_sha, 'unique_commits': 0}))
                    else:
                        revisions = ['HEAD', '^' + lease.base_sha] if base_is_ancestor else ['HEAD']
                        self.manager._git('-C', lease.worktree_path, 'bundle', 'create', str(bundle), *revisions)
                        self.manager._git('-C', lease.worktree_path, 'bundle', 'verify', str(bundle))
                    with bundle.open('rb') as stream:
                        os.fsync(stream.fileno())
                    after, again, _ = self._inspect(lease, expected_head)
                    if state != after or revision != again:
                        raise WorkspaceProvisioningError('Workspace changed during preservation; no repair performed')
                    if index.exists() and digest(root / 'index.before') != state['index_sha256']:
                        raise WorkspaceProvisioningError('Preserved index verification failed')
                    # Verify every archived file, including untracked/ignored
                    # evidence, rather than trusting successful tar creation.
                    with tarfile.open(archive) as tar:
                        for row, member in zip(state['files'], tar.getmembers(), strict=True):
                            if member.name != row['path'] or member.mode != row['mode']:
                                raise WorkspaceProvisioningError('Preservation metadata verification failed')
                            if row['kind'] == 'file':
                                h = hashlib.sha256()
                                with tar.extractfile(member) as stream:
                                    for chunk in iter(lambda: stream.read(1024 * 1024), b''):h.update(chunk)
                                if h.hexdigest() != row['sha256']:
                                    raise WorkspaceProvisioningError('Preserved file verification failed')
                            elif row['kind'] == 'symlink' and member.linkname != row['target']:
                                raise WorkspaceProvisioningError('Preserved symlink verification failed')
                    atomic_write_json(root / 'snapshot.json', state)
                    saved.update(phase='prepared', preservation={
                        'archive_sha256': digest(archive), 'bundle_sha256': digest(bundle),
                        'index_sha256': state['index_sha256'], 'index_mode': state['index_mode'], 'base_sha': lease.base_sha,
                        'history_scope': 'no_unique_commits' if expected_head == lease.base_sha else 'bundle_with_base_prerequisite' if base_is_ancestor else 'full_head_bundle'})
                    saved['public'] = {'operation_id': key, 'phase': 'prepared', 'effect': 'none',
                        'revision': revision, 'head': expected_head, 'lease_id': lease_id,
                        'fencing_token': expected_fence, 'retained_entries': len(state['files']),
                        'retained_file_bytes': needed,
                        'missing_tracked_paths': sum(not r['present'] for r in state['tracked']),
                        'matching_existing_tracked_paths': sum(r['present'] for r in state['tracked']),
                        'restore_file_bytes': restore_bytes,
                        'quarantine_untracked': quarantine_untracked,
                        'preservation': saved['preservation'],
                        'notice': ('Empty index is not proof of disposable changes. Explicit apply restores absent HEAD files and replaces the empty index; exact original state/history remains preserved. '
                                   + ('Only Git-visible untracked files will additionally be relocated intact into retained-worktree custody; matching HEAD files and ignored caches stay in place.' if quarantine_untracked else 'Existing untracked/ignored entries remain in place.'))}
                    atomic_write_json(receipt, saved); _sync_dir(root)
                    return saved['public']
                if action != 'apply' or expected_revision != saved['revision']:
                    raise WorkspaceProvisioningError('Exact preservation revision is required')
                state, revision, index = self._inspect(lease, expected_head)
                if revision != expected_revision:
                    raise WorkspaceProvisioningError('Index, worktree or HEAD changed after preservation')
                if (digest(root / 'worktree.before.tar') != saved['preservation']['archive_sha256']
                        or digest(root / 'head.before.bundle') != saved['preservation']['bundle_sha256']
                        or (state['index_sha256'] is not None and digest(root / 'index.before') != state['index_sha256'])):
                    raise WorkspaceProvisioningError('Preserved bytes changed; refusing repair')
                saved.update(phase='staging'); saved['public'].update(phase='staging', effect='none')
                atomic_write_json(receipt, saved)
                stage = root / 'staged'
                stage.mkdir()
                alternate = root / 'new.index'
                env = os.environ.copy(); env['GIT_INDEX_FILE'] = str(alternate); env['GIT_TERMINAL_PROMPT'] = '0'
                subprocess.run(['git', '-C', lease.worktree_path, 'read-tree', expected_head], env=env,
                               check=True, capture_output=True, timeout=300)
                missing = [r for r in state['tracked'] if not r['present']]
                if missing:
                    subprocess.run(['git','-C',lease.worktree_path,'checkout-index','-z','--stdin','--prefix='+str(stage)+'/'],
                                   input=b''.join(os.fsencode(r['path'])+b'\0' for r in missing), env=env,
                                   check=True, capture_output=True, timeout=300)
                quarantine = []
                if quarantine_untracked:
                    result = subprocess.run(['git','-C',lease.worktree_path,'ls-files','--others','--exclude-standard','-z'],
                                            env=env,check=True,capture_output=True,timeout=300)
                    quarantine = [os.fsdecode(p) for p in result.stdout.split(b'\0') if p]
                    if quarantine and any(Path(r['path']).name == '.gitignore' for r in missing):
                        raise WorkspaceProvisioningError('Missing ignore definitions require explicit quarantine review')
                # Native index.lock excludes cooperating Git writers. The final
                # full comparison also catches changes made while staging.
                lock = index.with_name('index.lock')
                fd = os.open(lock, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                applying = False
                try:
                    os.fchmod(fd, state['index_mode'] if state['index_mode'] is not None else 0o644)
                    # _inspect rejects our own lock; compare preserved content
                    # directly while this exclusive lock is held.
                    if inventory(Path(lease.worktree_path)) != state['files'] or (digest(index) if index.exists() else None) != state['index_sha256']:
                        raise WorkspaceProvisioningError('Workspace changed before apply')
                    if (stat.S_IMODE(index.stat().st_mode) if index.exists() else None) != state['index_mode']:
                        raise WorkspaceProvisioningError('Index mode changed before apply')
                    if self.manager._git('-C', lease.worktree_path, 'rev-parse', 'HEAD').stdout.strip() != expected_head:
                        raise WorkspaceProvisioningError('HEAD changed before apply')
                    saved.update(phase='applying'); saved['public'].update(phase='applying', effect='unknown')
                    atomic_write_json(receipt, saved); applying = True
                    if quarantine_untracked:
                        # Explicit preparation option, never inferred from an
                        # empty index. Move intact entries into retained custody,
                        # preserving original paths in addition to verified tar.
                        retained = root / 'retained-worktree'
                        retained.mkdir(mode=0o700)
                        tree = Path(lease.worktree_path)
                        retained_rows = {r['path'] for r in state['files'] if r['kind'] != 'directory'}
                        for name in quarantine:
                            if name not in retained_rows or name == '.git':
                                raise WorkspaceProvisioningError('Quarantine plan changed from preserved files')
                            entry = tree / name
                            destination = retained / name
                            destination.parent.mkdir(parents=True,exist_ok=True)
                            if destination.exists() or destination.is_symlink():
                                raise WorkspaceProvisioningError('Retained custody path already exists')
                            os.rename(entry, destination)
                            _sync_dir(entry.parent); _sync_dir(destination.parent)
                    sync_directories = {()}
                    for row in missing:
                        source = stage / row['path']; target = Path(lease.worktree_path) / row['path']
                        parent_fd = os.open(lease.worktree_path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
                        try:
                            parent_parts = Path(row['path']).parts[:-1]
                            for depth, component in enumerate(parent_parts):
                                try:os.mkdir(component, dir_fd=parent_fd)
                                except FileExistsError:pass
                                child_fd = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent_fd)
                                sync_directories.add(parent_parts[:depth + 1])
                                os.close(parent_fd); parent_fd = child_fd
                            if source.is_symlink():
                                os.symlink(os.readlink(source), target.name, dir_fd=parent_fd)
                            else:
                                with source.open('rb') as stream:os.fsync(stream.fileno())
                                os.link(source, target.name, dst_dir_fd=parent_fd, follow_symlinks=False)
                        finally:
                            os.close(parent_fd)
                    # Every file is synced before linking. Sync each changed
                    # directory once, children before parents, before committing
                    # the live index/receipt. A crash before this remains applying,
                    # with the complete original state already durable in custody.
                    for parts in sorted(sync_directories, key=len, reverse=True):
                        directory_fd = os.open(lease.worktree_path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
                        try:
                            for component in parts:
                                child_fd = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=directory_fd)
                                os.close(directory_fd); directory_fd = child_fd
                            os.fsync(directory_fd)
                        finally:
                            os.close(directory_fd)
                    with os.fdopen(fd, 'wb') as output:
                        fd = -1
                        output.write(alternate.read_bytes()); output.flush(); os.fsync(output.fileno())
                    os.replace(lock, index); _sync_dir(index.parent)
                    if self.manager._git('-C', lease.worktree_path, 'rev-parse', 'HEAD').stdout.strip() != expected_head:
                        raise WorkspaceProvisioningError('HEAD changed during materialization; inspect retained operation')
                    lease.dirty, lease.untracked = self.manager._status(Path(lease.worktree_path))
                    self.manager._save(lease)
                    saved.update(phase='completed'); saved['public'].update(
                        phase='completed', effect='complete', restored_paths=len(missing),
                        matching_existing_tracked_paths=sum(r['present'] for r in state['tracked']),
                        dirty=lease.dirty, untracked=lease.untracked,
                        retained_worktree_entries_relocated=quarantine_untracked,
                        notice='Original state/history retained; no target-tree cleanup, prompt retry or provider changes performed.')
                    atomic_write_json(receipt, saved)
                    return saved['public']
                finally:
                    if fd >= 0:os.close(fd)
                    # Once applying, retain every artifact on failure. No blind
                    # replay/rollback or removal of newly materialized files.
                    if lock.exists() and (not applying or saved['phase'] == 'completed'):
                        lock.unlink()

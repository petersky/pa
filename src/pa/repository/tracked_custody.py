"""Bounded attachment custody of truncated tracked files; never repair/remove a tree."""
from __future__ import annotations

import hashlib
import json
import os
import stat
import subprocess
import zipfile
from contextlib import ExitStack
from bisect import bisect_right
from datetime import UTC, datetime
from pathlib import Path

from pa.core.io import atomic_write_json
from pa.repository.materialization import digest, _sync_dir
from pa.repository.untracked_preservation import _no_open_files
from pa.repository.workspace import WorkspaceProvisioningError as Error

MAX_FILES = 64
MAX_BYTES = 64 * 1024 * 1024
MAX_INSPECT_BYTES = 256 * 1024 * 1024
MAX_PATHS = 10000
MAX_INDEX_BYTES = 2 * 1024 * 1024
MAX_PART_BYTES = 24 * 1024 * 1024
MAX_PARTS = 3


class JoinedArchive:
    """Seek over a few immutable attachment parts without a joined disk copy."""
    def __init__(self, streams):
        self.streams = streams
        self.offsets = [0]
        for stream in streams:
            self.offsets.append(self.offsets[-1] + os.fstat(stream.fileno()).st_size)
        self.position = 0

    def tell(self):
        return self.position

    def seekable(self):
        return True

    def seek(self, offset, whence=0):
        position = offset + (self.position if whence == 1 else self.offsets[-1] if whence == 2 else 0)
        if position < 0 or whence not in {0, 1, 2}:
            raise ValueError('Invalid multipart seek')
        self.position = position
        return position

    def read(self, size=-1):
        remaining = max(0, self.offsets[-1] - self.position)
        remaining = remaining if size < 0 else min(size, remaining)
        chunks = []
        while remaining:
            part = bisect_right(self.offsets, self.position) - 1
            stream = self.streams[part]
            stream.seek(self.position - self.offsets[part])
            chunk = stream.read(min(remaining, self.offsets[part+1] - self.position))
            if not chunk:
                raise Error('Durable custody part changed during retrieval')
            chunks.append(chunk)
            self.position += len(chunk)
            remaining -= len(chunk)
        return b''.join(chunks)


def verify_parts(paths, expected):
    if len(paths) != len(expected):
        raise Error('Exact durable custody parts required')
    whole = hashlib.sha256()
    for path, row in zip(paths, expected, strict=True):
        if path.is_symlink() or path.stat().st_size != row['bytes']:
            raise Error('Durable custody part size/type changed')
        h = hashlib.sha256()
        with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW), 'rb') as stream:
            for chunk in iter(lambda:stream.read(1024*1024), b''):
                h.update(chunk); whole.update(chunk)
        if h.hexdigest() != row['sha256']:
            raise Error('Durable attachment retrieval hash changed')
    return whole.hexdigest()


def source_file(root, relative):
    """Open every path component beneath the observed tree without following links."""
    parts = Path(relative).parts
    if not parts or Path(relative).is_absolute() or '..' in parts:
        raise Error('Unsafe custody path')
    fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for component in parts[:-1]:
            child = os.open(component, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd)
            os.close(fd)
            fd = child
        child = os.open(parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
        info = os.fstat(child)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            os.close(child)
            raise Error('Custody source is not an unlinked regular file')
        return os.fdopen(child, 'rb')
    finally:
        os.close(fd)


def metadata(path, limit=MAX_INSPECT_BYTES):
    s = path.lstat()
    if not stat.S_ISREG(s.st_mode) or s.st_nlink != 1 or s.st_size > limit:
        raise Error('Tracked custody requires unlinked regular files')
    h = hashlib.sha256()
    with os.fdopen(os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK), 'rb') as stream:
        remaining = s.st_size
        while remaining:
            chunk = stream.read(min(1024*1024, remaining))
            if not chunk:
                raise Error('Custody metadata source shrank')
            remaining -= len(chunk)
            h.update(chunk)
        if stream.read(1):
            raise Error('Custody metadata source grew')
    return dict(bytes=s.st_size, mode=stat.S_IMODE(s.st_mode), uid=s.st_uid,
                gid=s.st_gid, inode=s.st_ino, mtime_ns=s.st_mtime_ns,
                ctime_ns=s.st_ctime_ns, sha256=h.hexdigest())


class TrackedPrefixCustody:
    def __init__(self, manager):
        self.manager = manager

    def guard(self, target, principal):
        m = self.manager
        lease = m.selected_cleanup_leases([target])[0]
        owner = m.store.get_session(lease.session_id)
        durable = (owner.config_json or {}).get('durable_runtime') if owner else None
        binding = (owner.execution_binding or {}) if owner else {}
        if (not owner or owner.principal_id not in {None, principal}
                or owner.card_id != lease.card_id
                or owner.project_id != lease.project_id
                or owner.origin_instance_id != m.settings.instance_id
                or lease.id not in binding.get('lease_ids', [])
                or lease.repository_id not in binding.get('repository_ids', [])
                or lease.worktree_path not in binding.get('worktree_paths', [])
                or binding.get('branch') != lease.branch or binding.get('base_sha') != lease.base_sha
                or not isinstance(durable, dict) or durable.get('in_flight', 'unknown') is not None
                or durable.get('queued_prompts') != []
                or any(durable.get(k) for k in ('pending_permissions', 'pending_elicitations', 'pending_interaction'))):
            raise Error('Exact bound owner idle/control evidence is unavailable')
        if owner.external_session_id and m.store.native_history_session_ids(owner.external_session_id) != [lease.session_id]:
            raise Error('Exact native custody owner is ambiguous')
        if (lease.state not in {'ready', 'completed', 'cleanup_blocked'}
                or not lease.completed or not lease.merged or lease.expires_at > datetime.now(UTC)
                or not m._card_completion_eligible(lease.card_id)
                or m.store.pending_restart_continuation(session_id=lease.session_id)):
            raise Error('Existing completion, expiry, acceptance or restart guard')
        tree = m._assert_managed_path(Path(lease.worktree_path))
        if tree.is_symlink():
            raise Error('Unsafe custody worktree')
        m._verify_selected_worktree(lease, target)
        if self.git(lease, 'rev-parse', 'HEAD').stdout.strip() != target.expected_head:
            raise Error('Custody HEAD changed')
        if self.git(lease, 'rev-list', target.expected_head, '--not', '--remotes=origin').stdout.strip():
            raise Error('Custody HEAD has history outside refreshed origin refs')
        return lease, owner.updated_at

    def git(self, lease, *args):
        return self.manager._git('--no-optional-locks', '-c', 'core.fsmonitor=false',
            '-C', lease.worktree_path, *args, env_extra={'GIT_NO_LAZY_FETCH': '1'})

    def prefix(self, lease, path, oid):
        # Only stream the retained prefix, never export a large HEAD archive.
        proc = subprocess.Popen(['git', '-C', lease.worktree_path, 'cat-file', 'blob', oid],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            env={**os.environ, 'GIT_NO_LAZY_FETCH': '1', 'GIT_OPTIONAL_LOCKS': '0'})
        try:
            with source_file(lease.worktree_path, path.relative_to(lease.worktree_path)) as source:
                for chunk in iter(lambda: source.read(1024 * 1024), b''):
                    if proc.stdout.read(len(chunk)) != chunk:
                        raise Error('Changed tracked file is not an exact HEAD prefix')
                if not proc.stdout.read(1):
                    raise Error('Changed tracked file is not a strict HEAD prefix')
        finally:
            proc.stdout.close()
            if proc.poll() is None:
                proc.kill()
            proc.wait(timeout=5)

    def snapshot(self, lease, target):
        tree = Path(lease.worktree_path)
        if self.git(lease, 'ls-files', '--stage', '-z').stdout:
            raise Error('Tracked-prefix custody requires the original empty index')
        index = Path(self.git(lease, 'rev-parse', '--path-format=absolute', '--git-path', 'index').stdout.strip())
        self.manager._assert_managed_path(index)
        if index.is_symlink() or index.with_name('index.lock').exists():
            raise Error('Index has a writer or unsafe path')
        if index.exists() and index.stat().st_size > MAX_INDEX_BYTES:
            raise Error('Original index exceeds custody capacity')
        index_row = metadata(index, MAX_INDEX_BYTES) if index.exists() else None
        entries = {}
        for record in self.git(lease, 'ls-tree', '-rzl', '--full-tree', target.expected_head).stdout.split('\0'):
            if not record:
                continue
            fields, name = record.split('\t', 1)
            mode, kind, oid, size = fields.split()
            if len(entries) >= MAX_PATHS:
                raise Error('HEAD exceeds custody path capacity')
            entries[name] = (mode, kind, oid, int(size) if size != '-' else -1)
        rows, changed, total, retained, seen = [], [], 0, 0, 0
        for directory, dirs, files in os.walk(tree, followlinks=False):
            if Path(directory) == tree:
                dirs[:] = [d for d in dirs if d != '.git']
                files = [f for f in files if f != '.git']
            for name in sorted(dirs + files):
                seen += 1
                if seen > MAX_PATHS:
                    raise Error('Custody inventory path capacity exceeded')
                path = Path(directory) / name
                relative = path.relative_to(tree).as_posix()
                info = path.lstat()
                if stat.S_ISDIR(info.st_mode):
                    continue
                if relative not in entries:
                    raise Error('Non-HEAD data requires separate custody')
                total += info.st_size
                if total > MAX_INSPECT_BYTES or len(rows) >= MAX_PATHS:
                    raise Error('Custody inspection capacity exceeded')
                with source_file(tree, relative) as stream:
                    s = os.fstat(stream.fileno())
                    if not stat.S_ISREG(s.st_mode) or s.st_nlink != 1 or s.st_size != info.st_size:
                        raise Error('Custody source type/size changed')
                    checksum = hashlib.sha256()
                    remaining = s.st_size
                    while remaining:
                        chunk = stream.read(min(1024 * 1024, remaining))
                        if not chunk:
                            raise Error('Custody source shrank')
                        remaining -= len(chunk)
                        checksum.update(chunk)
                    if stream.read(1):
                        raise Error('Custody source grew')
                    row = dict(path=relative, bytes=s.st_size, mode=stat.S_IMODE(s.st_mode),
                        uid=s.st_uid, gid=s.st_gid, inode=s.st_ino, mtime_ns=s.st_mtime_ns,
                        ctime_ns=s.st_ctime_ns, sha256=checksum.hexdigest())
                mode, kind, oid, size = entries[relative]
                if kind != 'blob' or mode not in {'100644', '100755'} or bool(info.st_mode & 0o111) != (mode == '100755'):
                    raise Error('Tracked custody type/mode differs from HEAD')
                h = hashlib.new('sha1' if len(oid) == 40 else 'sha256')
                h.update(b'blob ' + str(info.st_size).encode() + b'\0')
                with source_file(tree, relative) as stream:
                    for chunk in iter(lambda: stream.read(1024 * 1024), b''):
                        h.update(chunk)
                if h.hexdigest() != oid:
                    retained += info.st_size
                    if len(changed) >= MAX_FILES or retained > MAX_BYTES or info.st_size >= size:
                        raise Error('Changed tracked custody exceeds prefix capacity')
                    self.prefix(lease, path, oid)
                    changed.append({**row, 'head_oid': oid, 'head_bytes': size})
                rows.append(row)
        if not changed:
            raise Error('No changed tracked prefixes to preserve')
        for row in changed:
            _no_open_files(tree / row['path'])
        if index_row:
            _no_open_files(index)
        state = {'files': sorted(rows, key=lambda r:r['path']), 'changed': sorted(changed, key=lambda r:r['path']), 'index': index_row}
        return state, index

    def operate(self, *, target, principal, idempotency_key, action,
                attachment_paths=None, attachment_ids=None,
                attachment_guard=None):
        fingerprint = {'target': target.model_dump(mode='json'), 'principal': principal}
        key = hashlib.sha256(json.dumps([principal, idempotency_key]).encode()).hexdigest()
        root = self.manager.root / 'tracked-prefix-custody' / key
        self.manager._assert_managed_path(root)
        receipt = root / 'receipt.json'
        saved = json.loads(receipt.read_text()) if receipt.exists() else None
        if saved and saved['fingerprint'] != fingerprint:
            raise Error('Custody idempotency identity changed')
        if action == 'status':
            return saved['public'] if saved else {'operation_id':key,
                'phase':'incomplete' if root.exists() else 'not_started',
                'effect':'unknown' if root.exists() else 'none'}
        if saved and action == 'prepare':
            return saved['public']
        lease = self.manager.selected_cleanup_leases([target])[0]
        with self.manager.admission_guard(card_id=lease.card_id, session_id=lease.session_id):
            with self.manager._repository_lock(Path(lease.cache_path).name):
                error = self.manager._refresh_origin_for_cleanup(Path(lease.cache_path))
                if error:
                    raise Error(error)
            lease, owner_revision = self.guard(target, principal)
            state, index = self.snapshot(lease, target)
            if action == 'prepare':
                if root.exists():
                    raise Error('Custody outcome unknown; inspect this operation')
                root.mkdir(parents=True, mode=0o700)
                public = {'operation_id':key, 'phase':'writing', 'effect':'unknown', 'worktree_removed':False, 'reclaimed_bytes':0}
                manifest = {'target':fingerprint['target'], 'source':{
                    'card_id':lease.card_id, 'session_id':lease.session_id,
                    'repository_id':lease.repository_id, 'branch':lease.branch,
                    'head':target.expected_head, 'owner_revision':str(owner_revision)}, **state}
                saved = {'fingerprint':fingerprint, 'state':state, 'manifest':manifest, 'public':public}
                atomic_write_json(receipt, saved)
                archive = root / 'tracked-prefixes.zip'
                with zipfile.ZipFile(archive, 'x', compression=zipfile.ZIP_STORED) as z:
                    z.writestr('preservation-manifest.json', json.dumps(manifest, sort_keys=True))
                    for row in state['changed']:
                        with source_file(lease.worktree_path, row['path']) as source:
                            with z.open('files/'+row['path'], 'w') as output:
                                h = hashlib.sha256()
                                remaining = row['bytes']
                                while remaining:
                                    chunk = source.read(min(1024*1024, remaining))
                                    if not chunk:
                                        raise Error('Custody source shrank while archiving')
                                    remaining -= len(chunk)
                                    h.update(chunk)
                                    output.write(chunk)
                                if source.read(1) or h.hexdigest() != row['sha256']:
                                    raise Error('Custody source changed while archiving')
                    if state['index']:
                        with os.fdopen(os.open(index, os.O_RDONLY | os.O_NOFOLLOW), 'rb') as source:
                            z.writestr('index.before', source.read(MAX_INDEX_BYTES + 1))
                with archive.open('rb') as stream:
                    os.fsync(stream.fileno())
                parts = []
                archive_hash = digest(archive)
                with archive.open('rb') as stream:
                    while data := stream.read(MAX_PART_BYTES):
                        if len(parts) == MAX_PARTS:
                            raise Error('Custody archive exceeds multipart capacity')
                        destination = root / ('part-%03d' % len(parts))
                        with destination.open('xb') as output:
                            output.write(data); output.flush(); os.fsync(output.fileno())
                        parts.append(dict(part=len(parts), source_path=str(destination), bytes=len(data),
                            sha256=hashlib.sha256(data).hexdigest()))
                if verify_parts([Path(p['source_path']) for p in parts], parts) != archive_hash:
                    raise Error('Prepared custody part verification failed')
                public.update(phase='prepared', effect='none', attachment_parts=parts, archive_sha256=archive_hash,
                    changed_files=len(state['changed']), preserved_bytes=sum(r['bytes'] for r in state['changed']))
            elif action == 'verify' and saved and saved['public']['phase'] in {'prepared', 'verified'}:
                public = saved['public']
                if verify_parts(attachment_paths, public['attachment_parts']) != public['archive_sha256']:
                    raise Error('Durable attachment retrieval hash changed')
                if state != saved['state']:
                    raise Error('Original tracked files/index changed after custody preparation')
                with ExitStack() as stack:
                    streams = [stack.enter_context(os.fdopen(os.open(p, os.O_RDONLY | os.O_NOFOLLOW), 'rb')) for p in attachment_paths]
                    z = stack.enter_context(zipfile.ZipFile(JoinedArchive(streams)))
                    if json.loads(z.read('preservation-manifest.json')) != saved['manifest']:
                        raise Error('Durable custody metadata changed')
                    expected = {'preservation-manifest.json', *('files/'+r['path'] for r in state['changed'])}
                    if state['index']:
                        expected.add('index.before')
                    if set(z.namelist()) != expected or len(z.namelist()) != len(expected):
                        raise Error('Durable custody archive entries changed')
                    for row in state['changed']:
                        h = hashlib.sha256()
                        with z.open('files/'+row['path']) as stream:
                            for chunk in iter(lambda:stream.read(1024*1024), b''):h.update(chunk)
                        if h.hexdigest() != row['sha256']:
                            raise Error('Durable custody file checksum changed')
                    if state['index'] and hashlib.sha256(z.read('index.before')).hexdigest() != state['index']['sha256']:
                        raise Error('Durable custody index checksum changed')
                public.update(phase='verified', effect='complete', attachment_ids=attachment_ids,
                    notice='Custody only; original files/index unchanged. This receipt does not authorize collection or materialization.')
            else:
                raise Error('Exact prepared custody receipt required')
            if action == 'verify':
                if attachment_guard is None:
                    raise Error('Durable attachment final-boundary guard required')
                if verify_parts(attachment_paths, public['attachment_parts']) != public['archive_sha256']:
                    raise Error('Durable custody blob changed at final boundary')
            if self.snapshot(lease, target)[0] != state:
                raise Error('Custody source changed at final boundary')
            if action == 'verify':
                attachment_guard()
            if self.guard(target, principal)[1] != owner_revision or index.with_name('index.lock').exists():
                raise Error('Custody owner or index writer changed at final boundary')
            atomic_write_json(receipt, saved)
            _sync_dir(root)
            if action == 'prepare':
                # The fsynced parts and receipt now retain the exact archive.
                # Remove only this verified temporary joined copy, never input.
                archive.unlink()
                _sync_dir(root)
            return public

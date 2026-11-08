"""Expiring, process-local, read-only content hash continuation.

This is observation, never preservation or apply authority. The complete tracked
metadata fence is bounded separately; trees beyond that fence fail explicitly.
No payload or serializable hash state crosses the API boundary.
"""
from __future__ import annotations

from collections import OrderedDict
from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import stat
import subprocess
import threading
import time
import uuid
import weakref

from pa.repository.workspace import WorkspaceProvisioningError

MAX_METADATA_BYTES = 4 * 1024 * 1024
MAX_INDEX_BYTES = 1024 * 1024
MAX_RECORD_BYTES = 16384
MAX_ROWS = 64
MAX_SCANS = 4
TTL_SECONDS = 300
_lock = threading.Lock()
_caches = weakref.WeakKeyDictionary()


def fail(message):
    raise WorkspaceProvisioningError('Paged provenance: ' + message)


def signature(info):
    return (info.st_dev, info.st_ino, info.st_mode, info.st_size,
            info.st_mtime_ns, info.st_ctime_ns)


@contextmanager
def parent_fd(tree, name):
    fd = os.open(tree, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        parts = Path(name).parts
        for part in parts[:-1]:
            child = os.open(part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                            dir_fd=fd)
            os.close(fd)
            fd = child
        yield fd, parts[-1]
    finally:
        os.close(fd)


def control_bytes(path, limit):
    """Bound control-file reads even if a path is replaced after inspection."""
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError:
        fail('unsafe metadata control file')
    with os.fdopen(fd, 'rb', buffering=0) as source:
        before = os.fstat(source.fileno())
        if not stat.S_ISREG(before.st_mode) or before.st_size > limit:
            fail('metadata control file exceeds capacity or type')
        data = source.read(limit + 1)
        if len(data) != before.st_size or signature(before) != signature(os.fstat(source.fileno())):
            fail('metadata control file changed during read')
    return data, signature(before)


def records(tree, head, *, index=False):
    # Stream NUL records rather than materializing an unbounded ls-tree result.
    env = dict(os.environ, GIT_TERMINAL_PROMPT='0', GIT_LFS_SKIP_SMUDGE='1')
    process = subprocess.Popen(
        ['git', '-c', 'credential.interactive=never', '-C', str(tree),
         *(['ls-files', '--stage', '-z'] if index else ['ls-tree', '-rzl', '--full-tree', head])], stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL, env=env)
    timer = threading.Timer(30, process.kill)
    timer.start()
    try:
        pending = b''
        while chunk := process.stdout.read(4096):
            pending += chunk
            while b'\0' in pending:
                row, pending = pending.split(b'\0', 1)
                if len(row) > MAX_RECORD_BYTES:
                    fail('tracked metadata record exceeds capacity')
                yield row
            if len(pending) > MAX_RECORD_BYTES:
                fail('tracked metadata record exceeds capacity')
        if pending or process.wait() != 0:
            fail('tracked metadata stream failed')
    finally:
        timer.cancel()
        process.stdout.close()
        if process.poll() is None:
            process.kill()
        process.wait()


def snapshot(manager, lease, head, path_limit, metadata_rows=None):
    tree = Path(lease.worktree_path)
    git = lambda *args: manager._git('-C', str(tree), *args).stdout.strip()
    link_bytes, link_sig = control_bytes(tree / '.git', 4096)
    link_text = os.fsdecode(link_bytes).strip()
    if not link_text.startswith('gitdir: ') or '\n' in link_text:
        fail('unsafe metadata control file')
    git_dir = Path(link_text[len('gitdir: '):])
    if not git_dir.is_absolute():
        git_dir = tree / git_dir
    manager._assert_managed_path(git_dir)
    head_path = git_dir / 'HEAD'
    head_bytes, head_sig = control_bytes(head_path, 4096)
    values = git('rev-parse', '--path-format=absolute', '--show-toplevel',
                 '--git-common-dir', '--git-path', 'HEAD', '--git-path', 'index', 'HEAD').splitlines()
    if len(values) != 5:
        fail('tracked metadata stream failed')
    root, common, head_file, index_file, actual_head = values
    if Path(root).resolve() != tree.resolve():
        raise WorkspaceProvisioningError('Worktree root does not match the leased path')
    if Path(common).resolve() != (Path(lease.cache_path) / '.git').resolve():
        raise WorkspaceProvisioningError('Worktree belongs to a different repository cache')
    if Path(head_file).resolve() != head_path.resolve():
        fail('unsafe metadata control file')
    if os.fsdecode(head_bytes).strip() != 'ref: refs/heads/' + lease.branch:
        raise WorkspaceProvisioningError('Leased worktree is detached or on the wrong branch')
    if actual_head != head:
        fail('HEAD changed')
    index = Path(index_file)
    manager._assert_managed_path(index)
    if index.is_symlink() or index.with_name('index.lock').exists():
        fail('index has a writer or unsafe path')
    # Empty-index observation is small, including extensions. Never hydrate an
    # unbounded index merely to determine whether it is eligible.
    if index.exists():
        index_bytes, index_sig = control_bytes(index, MAX_INDEX_BYTES)
        index_hash = hashlib.sha256(index_bytes).hexdigest()
    else:
        index_sig, index_hash = None, None
    index_stream = records(tree, head, index=True)
    try:
        if next(index_stream, None) is not None:
            fail('index is no longer empty')
    finally:
        index_stream.close()
    result = []
    identity_hash = hashlib.sha256(json.dumps(
        [head, link_bytes.hex(), link_sig, head_bytes.hex(), head_sig,
         signature(tree.lstat()), index_sig, index_hash]).encode())
    total = 0
    # The immutable HEAD manifest can be reused after verifying the same HEAD,
    # repository association and branch. Current tracked stats are always read.
    cached_metadata = [] if metadata_rows is None else metadata_rows
    stream = records(tree, head) if metadata_rows is None else iter(metadata_rows)
    try:
        for raw in stream:
            if len(result) >= path_limit:
                fail('tracked metadata exceeds 10000-path capacity')
            total += len(raw)
            if total > MAX_METADATA_BYTES:
                fail('tracked metadata exceeds byte capacity')
            metadata, raw_name = raw.split(b'\t', 1)
            if metadata_rows is None:
                cached_metadata.append(raw)
            mode, kind, oid, size = metadata.decode('ascii').split()
            name = os.fsdecode(raw_name)
            parts = Path(name).parts
            if (kind != 'blob' or mode not in {'100644', '100755', '120000'}
                    or not parts or Path(name).is_absolute() or '..' in parts
                    or any(x.casefold() == '.git' for x in parts)):
                fail('unsupported tracked metadata')
            parents = []
            missing = False
            for i in range(1, len(parts)):
                path = tree.joinpath(*parts[:i])
                try:
                    info = path.lstat()
                except FileNotFoundError:
                    missing = True
                    break
                if not stat.S_ISDIR(info.st_mode):
                    fail('tracked parent is not a retained directory')
                # Directory mtime/ctime changes also invalidate the fence.
                parents.append(signature(info))
            try:
                info = None if missing else (tree / name).lstat()
            except FileNotFoundError:
                info = None
            if info is not None:
                if mode == '120000':
                    if not stat.S_ISLNK(info.st_mode):
                        fail('tracked type changed')
                elif (not stat.S_ISREG(info.st_mode)
                      or bool(info.st_mode & 0o111) != (mode == '100755')):
                    fail('tracked type or executable mode changed')
            row = (name, mode, oid, int(size),
                   signature(info) if info is not None else None, parents)
            encoded = json.dumps(row, ensure_ascii=True, separators=(',', ':')).encode()
            total += len(encoded)
            if total > MAX_METADATA_BYTES:
                fail('tracked metadata fence exceeds byte capacity')
            identity_hash.update(encoded)
            result.append(row)
    finally:
        if hasattr(stream, 'close'):
            stream.close()
    if not result:
        fail('HEAD has no tracked paths')
    return result, identity_hash.hexdigest(), cached_metadata


def observe(manager, lease, fingerprint, cursor, *, byte_limit, path_limit):
    """Called under the existing exact-owner admission and per-lease lock."""
    scope = hashlib.sha256(json.dumps(fingerprint, sort_keys=True).encode()).hexdigest()
    with _lock:
        cache = _caches.setdefault(manager, OrderedDict())
        now = time.monotonic()
        for key in list(cache):
            if now - cache[key]['time'] > TTL_SECONDS:
                del cache[key]
        if cursor is None:
            key = scope
            state = cache.get(key)
        else:
            if not isinstance(cursor, str) or len(cursor) > 100:
                fail('invalid cursor')
            key = scope
            state = cache.get(key)
            if state is None or cursor not in {state['cursor'], state['previous']}:
                fail('cursor expired, stale or belongs to different inputs')
        if state is None:
            if len(cache) >= MAX_SCANS:
                # Never evict an in-flight scan to admit another one.
                fail('observation cache capacity reached; wait for expiry')
    rows, identity, metadata = snapshot(manager, lease, fingerprint['expected_head'], path_limit,
                                       state['metadata'] if state else None)
    if state is not None:
        if identity != state['identity']:
            with _lock:
                cache.pop(key, None)
            fail('tracked inputs or index changed; start a new observation')
        if cursor is None and state['previous'] is not None:
            fail('initial page already advanced; use its continuation cursor')
        if cursor == state['previous']:
            return json.loads(state['reply'])
    else:
        state = dict(identity=identity, position=0, offset=0, hasher=None,
                     completed=0, missing=0, differing=0, cursor=None,
                     previous=None, reply=None, time=now)
    position, offset = state['position'], state['offset']
    hasher = state['hasher'].copy() if state['hasher'] else None
    completed, missing, differing = state['completed'], state['missing'], state['differing']
    read = 0
    exported = []
    tree = Path(lease.worktree_path)
    while position < len(rows) and len(exported) < MAX_ROWS:
        name, mode, oid, expected_size, info, parents = rows[position]
        if info is None:
            exported.append(dict(path=name, expected_oid=oid, present=False))
            missing += 1
        else:
            size = info[3]
            if hasher is None:
                hasher = hashlib.new('sha1' if len(oid) == 40 else 'sha256')
                hasher.update(b'blob ' + str(size).encode() + b'\0')
            with parent_fd(tree, name) as (parent, leaf):
                if mode == '120000':
                    if size > byte_limit:
                        fail('symlink exceeds content capacity')
                    if size > byte_limit - read:
                        break
                    data = os.fsencode(os.readlink(leaf, dir_fd=parent))
                    if len(data) != size:
                        fail('tracked symlink changed during observation')
                    hasher.update(data)
                    read += len(data)
                    offset = size
                else:
                    fd = os.open(leaf, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent)
                    with os.fdopen(fd, 'rb', buffering=0) as source:
                        if signature(os.fstat(source.fileno())) != tuple(info):
                            fail('tracked file changed before read')
                        source.seek(offset)
                        while offset < size and read < byte_limit:
                            chunk = source.read(min(65536, size-offset, byte_limit-read))
                            if not chunk:
                                fail('tracked file changed during read')
                            hasher.update(chunk)
                            offset += len(chunk)
                            read += len(chunk)
                        if signature(os.fstat(source.fileno())) != tuple(info):
                            fail('tracked file changed during read')
            if offset < size:
                break
            actual = hasher.hexdigest()
            differing += actual != oid
            exported.append(dict(path=name, expected_oid=oid, actual_oid=actual,
                                 actual_bytes=size, present=True, differs=actual != oid))
        completed += 1
        position += 1
        offset, hasher = 0, None
        if read == byte_limit:
            break
    _, after, _ = snapshot(manager, lease, fingerprint['expected_head'], path_limit, metadata)
    if after != identity:
        with _lock:
            cache.pop(key, None)
        fail('tracked inputs or index changed during page')
    done = position == len(rows)
    next_cursor = None if done else uuid.uuid4().hex
    reply = dict(phase='diagnostic_page', effect='none', diagnostic=dict(
        head=fingerprint['expected_head'], input_identity=identity,
        tracked_paths=len(rows), completed_tracked_paths=completed,
        missing_tracked_paths=missing, differing_tracked_paths=differing,
        entries=exported, entries_truncated=False, complete=done,
        tracked_bytes_read=read, continued_file_bytes=offset,
        next_cursor=next_cursor, apply_eligible=False,
        notice='Read-only metadata, not preservation or apply authority. '
               'Process-local cursor expires; complete tracked metadata fence remains capped.'))
    updated = dict(identity=identity, metadata=metadata, position=position, offset=offset,
                   hasher=hasher, completed=completed, missing=missing,
                   differing=differing, cursor=next_cursor, previous=cursor,
                   reply=json.dumps(reply), time=time.monotonic())
    with _lock:
        # A completed scan retains only its final bounded replay for TTL.
        if key not in cache and len(cache) >= MAX_SCANS:
            fail('observation cache capacity reached; wait for expiry')
        cache[key] = updated
    return reply

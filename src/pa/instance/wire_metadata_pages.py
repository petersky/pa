"""Signed, content-free cursors over a fixed session's retained wire snapshot."""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
import secrets
import stat
from contextlib import contextmanager
from pathlib import Path

from pa.instance.wire_metadata import (
    ARTIFACTS, MAX_ARTIFACT_BYTES, MAX_LINE, MAX_OUTPUT, MAX_RECORDS,
    _identity, _metadata,
)


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def _digest(value):
    return hashlib.sha256(_json(value)).hexdigest()


def _fingerprint(info):
    return [info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns,
            info.st_ctime_ns, info.st_mode]


@contextmanager
def _scope(root: Path, session_id: str):
    fds = []
    try:
        flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_DIRECTORY
        fds.append(os.open(root, flags))
        for name in ('sessions', session_id):
            fds.append(os.open(name, flags, dir_fd=fds[-1]))
        yield fds[-1], [[os.fstat(fd).st_dev, os.fstat(fd).st_ino] for fd in fds]
    finally:
        for fd in reversed(fds):
            os.close(fd)


def _manifest(fd):
    files = []
    for name in ARTIFACTS:
        try:
            info = os.stat(name, dir_fd=fd, follow_symlinks=False)
            files.append({'name': name,
                          'state': 'readable' if stat.S_ISREG(info.st_mode) else 'unavailable',
                          'identity': _fingerprint(info)})
        except FileNotFoundError:
            files.append({'name': name, 'state': 'missing'})
        except OSError:
            # Permission changes cannot be silently accepted as a stable snapshot.
            raise ValueError('Wire snapshot scope unavailable') from None
    return files


def _encode(state, key):
    body = _json(state)
    encoded = base64.urlsafe_b64encode(body).decode().rstrip('=')
    return encoded + '.' + hmac.new(key, body, hashlib.sha256).hexdigest()


def _decode(token, key):
    if not isinstance(token, str) or len(token) > 16384:
        raise ValueError('Invalid wire cursor')
    try:
        body64, signature = token.split('.')
        body = base64.b64decode(body64 + '=' * (-len(body64) % 4), altchars=b'-_', validate=True)
        expected = hmac.new(key, body, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, signature):
            raise ValueError()
        result = json.loads(body)
        if result.get('v') != 1:
            raise ValueError()
        return result
    except (ValueError, TypeError, KeyError):
        raise ValueError('Invalid wire cursor') from None


def read_wire_metadata_page(root: Path, *, session_id: str, native_id: str,
                            prompt_id: str, attempt: int, scope: str,
                            secret: str, cursor: str | None = None,
                            selection: str = 'all', assess_unparsed: bool = False) -> dict:
    """Read one forward page; replaying a cursor replays the same immutable range.

    Cursor payloads contain only IDs, counters and filesystem identity metadata,
    never a partial record's bytes. Snapshot identity fences inode, size, mtime
    and ctime for every fixed artifact and the directory chain on every call.
    This is a local snapshot fence, not historical retention or provider proof.
    """
    from uuid import UUID
    if (str(UUID(session_id)) != session_id or not _identity(native_id)
            or not _identity(prompt_id) or not secret or selection not in {'all', 'outcomes'}):
        raise ValueError('Invalid wire snapshot binding')
    key = hmac.new(secret.encode(), b'pa:wire-metadata-pages:v1', hashlib.sha256).digest()
    binding = _digest([session_id, native_id, prompt_id, attempt, scope, selection])
    previous = _decode(cursor, key) if cursor else None
    if assess_unparsed and previous is None:
        raise ValueError('Gap assessment requires an existing signed snapshot cursor')
    if previous and previous['binding'] != binding:
        raise ValueError('Wire cursor belongs to another binding')
    with _scope(root, session_id) as (fd, directories):
        files = _manifest(fd)
        if previous:
            state = previous
            if state['files'] != files or state['directories'] != directories:
                raise ValueError('Wire snapshot changed; do not combine cursor generations')
        else:
            state = dict(v=1, binding=binding, directories=directories, files=files,
                         nonce=secrets.token_hex(16), index=0, offset=0, skipping=False,
                         scanned=0, examined=0, emitted=0, malformed=0, unparsed=0,
                         filtered=0, selection_filtered=0, links=[], conflicts=[], link_overflow=False,
                         effect='unknown', chain='0'*64)
        snapshot_id = _digest({k: state[k] for k in ('binding','directories','files','nonce')})
        start_index, start_offset = state['index'], state['offset']
        rows, ranges = [], []
        from pa.instance.wire_metadata_gaps import GapAssessment
        assessment = GapAssessment(native_id, prompt_id, attempt) if assess_unparsed else None
        read_bytes, lookahead = 0, 0
        page_examined = 0
        # Skip only manifest entries with no readable bytes. Missing/unavailable
        # entries remain explicitly reported, and historical coverage stays unknown.
        while state['index'] < len(files):
            entry = files[state['index']]
            if entry['state'] != 'readable' or state['offset'] >= entry['identity'][2]:
                state['index'] += 1
                state['offset'] = 0
                state['skipping'] = False
                continue
            name = entry['name']
            wire_fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd)
            try:
                if _fingerprint(os.fstat(wire_fd)) != entry['identity']:
                    raise ValueError('Wire snapshot changed')
                begin = state['offset']
                data = os.pread(wire_fd, min(MAX_ARTIFACT_BYTES, entry['identity'][2]-begin), begin)
                read_bytes = len(data)
                pos = 0
                while pos < len(data) and page_examined < MAX_RECORDS and len(rows) < MAX_OUTPUT:
                    end = data.find(b'\n', pos)
                    if state['skipping']:
                        if assessment:
                            assessment.segment(wire_fd, artifact=name, start=begin+pos,
                                end=begin+(len(data) if end < 0 else end+1),
                                size=entry['identity'][2], reason='oversized_continuation',
                                continued=True, terminated=end >= 0)
                        pos = len(data) if end < 0 else end+1
                        state['skipping'] = end < 0
                        continue
                    if end < 0:
                        if begin+len(data) == entry['identity'][2]:
                            # Incomplete final line is covered but not evidence.
                            state['unparsed'] += 1
                            if assessment:
                                assessment.segment(wire_fd, artifact=name, start=begin+pos,
                                    end=begin+len(data), size=entry['identity'][2], reason='incomplete_eof')
                            pos = len(data)
                        elif len(data)-pos > MAX_LINE:
                            state['unparsed'] += 1
                            if assessment:
                                assessment.segment(wire_fd, artifact=name, start=begin+pos,
                                    end=begin+len(data), size=entry['identity'][2], reason='oversized')
                            state['skipping'] = True
                            pos = len(data)
                        # Otherwise leave the boundary line for the next page;
                        # its bytes are lookahead, not committed scanned coverage.
                        break
                    raw = data[pos:end]
                    record_start = begin+pos
                    pos = end+1
                    page_examined += 1
                    state['examined'] += 1
                    if len(raw) > MAX_LINE:
                        state['unparsed'] += 1
                        if assessment:
                            assessment.segment(wire_fd, artifact=name, start=record_start,
                                end=begin+pos, size=entry['identity'][2], reason='oversized',
                                raw=raw, terminated=True)
                        continue
                    try:
                        record = json.loads(raw)
                    except (ValueError, UnicodeError, RecursionError):
                        state['malformed'] += 1
                        continue
                    row = _metadata(record, native_id)
                    if row is None:
                        state['filtered'] += 1
                        continue
                    exact = (row['native_binding'] == 'exact' and
                             row.get('queued_prompt_id') == prompt_id and row.get('attempt') == attempt)
                    message_id = row.get('message_id')
                    if (message_id and row['native_binding'] == 'exact' and (
                            ('queued_prompt_id' in row and row['queued_prompt_id'] != prompt_id)
                            or ('attempt' in row and row['attempt'] != attempt))):
                        if message_id in state['links']:
                            state['links'].remove(message_id)
                        if message_id not in state['conflicts']:
                            if len(state['conflicts']) < 64:
                                state['conflicts'].append(message_id)
                            else:
                                state['link_overflow'] = True
                                state['links'] = []
                        state['effect'] = 'unknown'
                    if message_id in state['conflicts']:
                        exact = False
                    if exact and message_id and message_id not in state['links']:
                        if len(state['links']) < 64:
                            state['links'].append(message_id)
                        else:
                            state['link_overflow'] = True
                    linked = (row['native_binding'] == 'exact' and message_id in state['links']
                              and row.get('attempt', attempt) == attempt)
                    row.update(artifact=name, byte_start=record_start, byte_end=begin+pos,
                               correlation='exact' if exact or linked else 'unknown')
                    if row['correlation'] == 'exact' and row['method'] == 'session/prompt' and row['direction'] == 'in':
                        if 'stop_reason' in row and 'error_code' not in row:
                            state['effect'] = 'completed'
                        elif row.get('acknowledged') and state['effect'] != 'completed':
                            state['effect'] = 'submitted'
                    # Outcomes selection omits only identity-free updates; it
                    # cannot discard a prompt/ack/terminal/correlation record.
                    if selection == 'outcomes' and row['method'] == 'session/update' and not any(
                        k in row for k in ('turn_id','message_id','request_id','queued_prompt_id','attempt','stop_reason','acknowledged','error_code')
                    ):
                        state['selection_filtered'] += 1
                        continue
                    rows.append(row)
                    state['emitted'] += 1
                if _fingerprint(os.fstat(wire_fd)) != entry['identity']:
                    raise ValueError('Wire snapshot changed during inspection')
            finally:
                os.close(wire_fd)
            if pos == 0 and data:
                raise ValueError('Wire page bounds cannot advance')
            state['offset'] = begin+pos
            state['scanned'] += pos
            lookahead = len(data)-pos
            covered = dict(artifact=name, byte_start=begin, byte_end=begin+pos,
                           sha256=hashlib.sha256(data[:pos]).hexdigest(), hash_scope='scanned_range')
            ranges.append(covered)
            state['chain'] = _digest([state['chain'], covered])
            break  # one artifact/range per call, even with sparse metadata
        # Normalize end position so the final data page reports exhaustion.
        while state['index'] < len(files):
            item = files[state['index']]
            if item['state'] == 'readable' and state['offset'] < item['identity'][2]:
                break
            state['index'] += 1
            state['offset'] = 0
            state['skipping'] = False
        # Reopen the directory chain as well as re-stat every fixed filename:
        # replacement, rotation, growth and truncation all invalidate the page.
        with _scope(root, session_id) as (after_fd, after_directories):
            if after_directories != directories or _manifest(after_fd) != files:
                raise ValueError('Wire snapshot changed during inspection')
        exhausted = state['index'] == len(files)
        result = dict(schema_version=2, snapshot_id=snapshot_id,
                    session_id=session_id, native_history_id=native_id, prompt_id=prompt_id,
                    attempt=attempt, selection=selection,
                    artifacts=[{'name':f['name'], 'state':f['state'],
                                'size_at_snapshot':f.get('identity',[None,None,0])[2]} for f in files],
                    records=rows, scanned_ranges=ranges, bytes_read=read_bytes,
                    lookahead_bytes=lookahead, records_examined=page_examined,
                    totals={k:state[k] for k in ('scanned','examined','emitted','malformed','unparsed','filtered','selection_filtered')},
                    position_before={'artifact_index':start_index,'byte_offset':start_offset},
                    position_after={'artifact_index':state['index'],'byte_offset':state['offset']},
                    coverage='snapshot_exhausted' if exhausted else 'partial',
                    snapshot_exhausted=exhausted, historical_coverage='unknown',
                    output_coverage='all_selected_metadata_in_scanned_ranges',
                    link_overflow=state['link_overflow'], conflicting_message_ids=len(state['conflicts']),
                    metadata_coverage='incomplete' if state['malformed'] or state['unparsed'] else 'complete_for_selection_in_scanned_ranges',
                    range_hash_chain=state['chain'],
                    effect='unknown' if state['conflicts'] or state['link_overflow'] else state['effect'], delivery_allowed=False,
                    native_provider_store='unavailable_through_this_reader',
                    next_cursor=None if exhausted else _encode(state,key))
        if assessment:
            result['unparsed_assessment'] = assessment.result()
        return result

"""Bounded, content-free inspection of one saved session's wire artifacts.

This is an observer, never a provider connection or a delivery decision. Missing
records cannot prove non-submission: wire logging is lossy and has no retention
manifest. Native provider stores outside PA's fixed session directory are not read.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from datetime import datetime
from pathlib import Path
from uuid import UUID

MAX_BYTES = 4 * 1024 * 1024
MAX_LINE = 64 * 1024
MAX_RECORDS = 20000
MAX_OUTPUT = 200
MAX_ARTIFACT_BYTES = 1024 * 1024
ARTIFACTS = ('wire.jsonl', 'wire.jsonl.1', 'wire.jsonl.2', 'wire.jsonl.3')
METHODS = {'initialize', 'session/prompt', 'session/load', 'session/resume', 'session/update'}
STOPS = {'end_turn', 'max_tokens', 'max_turn_requests', 'refusal', 'cancelled'}


def _identity(value):
    # Do not echo arbitrary strings masquerading as IDs, including credentials.
    if not isinstance(value, str) or len(value) > 128:
        return None
    if re.fullmatch(r'(?:restart-handoff:)?[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}', value):
        return value
    if re.fullmatch(r'(?:turn|msg|req|thread)_[A-Za-z0-9]{1,80}', value):
        return value
    return None


def _timestamp(value):
    try:
        parsed = datetime.fromisoformat(value)
        return parsed.isoformat() if parsed.tzinfo else None
    except (TypeError, ValueError):
        return None


def _metadata(record, native_id):
    if not isinstance(record, dict) or not isinstance(record.get('method'), str) or record['method'] not in METHODS:
        return None
    method, direction = record['method'], record.get('direction')
    if not isinstance(direction, str) or direction not in {'in', 'out'}:
        return None
    params = record.get('params')
    params = params if isinstance(params, dict) else {}
    result = record.get('result')
    result = result if isinstance(result, dict) else {}
    # IDs are accepted from fixed protocol fields only, never nested content.
    binding = params.get('session_id', record.get('session_id', result.get('session_id')))
    if binding is not None and binding != native_id:
        return None
    value = {'method': method, 'direction': direction,
             'timestamp': _timestamp(record.get('ts')),
             'native_binding': 'exact' if binding == native_id else 'unbound'}
    for field in ('message_id', 'request_id', 'turn_id', 'queued_prompt_id'):
        candidate = _identity(params.get(field, record.get(field, result.get(field))))
        if candidate:
            value[field] = candidate
    update = params.get('update')
    if method == 'session/update' and isinstance(update, dict):
        turn = _identity(update.get('turnId', update.get('turn_id')))
        if turn:
            value['turn_id'] = turn
        kind = update.get('sessionUpdate', update.get('session_update'))
        if isinstance(kind, str) and kind in {
            'tool_call', 'tool_call_update', 'agent_message_chunk',
            'user_message_chunk', 'usage_update', 'available_commands_update',
            'config_option_update', 'current_mode_update', 'plan',
        }:
            value['update_type'] = kind
    attempt = params.get('attempt', record.get('attempt'))
    if type(attempt) is int and 0 <= attempt <= 10000:
        value['attempt'] = attempt
    stop = result.get('stop_reason')
    if isinstance(stop, str) and stop in STOPS:
        value['stop_reason'] = stop
    if result.get('acknowledged') is True:
        value['acknowledged'] = True
    error = record.get('error')
    if isinstance(error, dict) and type(error.get('code')) is int:
        value['error_code'] = max(-2147483648, min(error['code'], 2147483647))
    return value


def read_wire_metadata(data_dir: Path, *, session_id: str, native_id: str,
                       prompt_id: str, attempt: int) -> dict:
    """Open only fixed names under server-selected sessions/<UUID>, without links.

    Hashes cover inspected tail ranges only. Snapshot coverage does not attest to
    historical retention, logging completeness, non-delivery or cancellation.
    """
    if str(UUID(session_id)) != session_id or not _identity(native_id) or not _identity(prompt_id):
        raise ValueError('Invalid saved session binding')
    result = dict(schema_version=1, session_id=session_id, native_history_id=native_id,
                  prompt_id=prompt_id, attempt=attempt, evidence_scope='retained_host_protocol_metadata', artifacts=[], records=[], effect='unknown',
                  historical_coverage='unknown', delivery_allowed=False,
                  native_provider_store='unavailable_through_this_reader',
                  coverage='missing', malformed_records=0, unparsed_records=0,
                  records_examined=0, output_truncated=False)
    descriptors = []
    flags = os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK
    try:
        fd = os.open(data_dir, flags | os.O_DIRECTORY)
        descriptors.append(fd)
        for name in ('sessions', session_id):
            fd = os.open(name, flags | os.O_DIRECTORY, dir_fd=fd)
            descriptors.append(fd)
        remaining = MAX_BYTES
        partial = False
        candidates = []
        for name in ARTIFACTS:
            info = {'name': name, 'state': 'missing'}
            result['artifacts'].append(info)
            try:
                wire_fd = os.open(name, flags, dir_fd=fd)
            except FileNotFoundError:
                continue
            except OSError:
                info['state'] = 'unavailable'
                partial = True
                continue
            with os.fdopen(wire_fd, 'rb') as stream:
                before = os.fstat(stream.fileno())
                if not stat.S_ISREG(before.st_mode):
                    info['state'] = 'unavailable'
                    partial = True
                    continue
                budget = min(remaining, MAX_ARTIFACT_BYTES)
                start = max(0, before.st_size - budget)
                stream.seek(start)
                data = stream.read(budget)
                remaining -= len(data)
                after = os.fstat(stream.fileno())
                truncated = start > 0 or len(data) < before.st_size
                changed = (before.st_size, before.st_mtime_ns, before.st_ino) != (after.st_size, after.st_mtime_ns, after.st_ino)
                info.update(state='read', size_at_open=before.st_size,
                            bytes_read=len(data), sha256=hashlib.sha256(data).hexdigest(),
                            hash_scope='inspected_range', byte_start=start, byte_end=start+len(data), truncated=truncated,
                            changed_during_read=changed)
                partial |= truncated or changed
                lines = data.split(b'\n')
                if start > 0:
                    # First line may start before the inspected range.
                    lines = lines[1:]
                    result['unparsed_records'] += 1
                if not lines:
                    continue
                if lines[-1]:
                    result['unparsed_records'] += 1
                    partial = True
                for line in lines[:-1]:
                    if result['records_examined'] >= MAX_RECORDS:
                        partial = True
                        break
                    result['records_examined'] += 1
                    if len(line) > MAX_LINE:
                        result['unparsed_records'] += 1
                        partial = True
                        continue
                    try:
                        record = json.loads(line)
                    except (ValueError, UnicodeError, RecursionError):
                        result['malformed_records'] += 1
                        partial = True
                        continue
                    safe = _metadata(record, native_id)
                    if safe is not None:
                        candidates.append(safe | {'artifact': name})
        # Only explicit exact native + prompt identity can establish a link.
        links = {r['message_id'] for r in candidates
                 if r.get('queued_prompt_id') == prompt_id
                 and r['native_binding'] == 'exact' and r.get('attempt') == attempt and r.get('message_id')}
        for r in candidates:
            exact = r['native_binding'] == 'exact' and (
                (r.get('queued_prompt_id') == prompt_id and r.get('attempt') == attempt)
                or (r.get('message_id') in links and r.get('attempt', attempt) == attempt))
            r['correlation'] = 'exact' if exact else 'unknown'
            if exact and r['method'] == 'session/prompt' and r['direction'] == 'in':
                if 'stop_reason' in r:
                    result['effect'] = 'completed'
                elif r.get('acknowledged') and result['effect'] != 'completed':
                    result['effect'] = 'submitted'
        # Put exact terminal evidence before unrelated diagnostics when bounded.
        candidates.sort(key=lambda r: (r['correlation'] != 'exact', 'stop_reason' not in r))
        result['records'] = candidates[:MAX_OUTPUT]
        result['output_truncated'] = len(candidates) > MAX_OUTPUT
        observed = any(a['state'] == 'read' for a in result['artifacts'])
        result['coverage'] = ('partial' if partial else 'observed_fixed_files_complete') if observed else ('unavailable' if partial else 'missing')
        result['correlation'] = 'exact' if any(r['correlation'] == 'exact' for r in candidates) else 'unknown'
        return result
    except FileNotFoundError:
        return result
    except OSError:
        result['coverage'] = 'unavailable'
        return result
    finally:
        for fd in reversed(descriptors):
            os.close(fd)

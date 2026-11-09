"""Optional bounded assessment of records the normal wire page did not parse.

Only complete JSON envelopes within the recovery cap are interpreted. Partial
prefixes, nested text and regex matches are never promoted to protocol evidence.
The caller owns the authenticated descriptor and immutable snapshot checks.
"""
from __future__ import annotations

import hashlib
import json
import os

from pa.instance.wire_metadata import _metadata

# Covers the remaining finite observed records (largest 1,221,317B including
# newline). A call covers <=1MiB ordinary page + <=2MiB extra read + <=2MiB
# decoded record input. Keep a fixed work ceiling; never accept a caller limit.
MAX_RECOVERY_LINE = 2 * 1024 * 1024
MAX_ASSESS_WORK = 6 * 1024 * 1024
MAX_GAPS = 32
MAX_DEPTH = 64


def _bounded_depth(raw):
    depth, string, escaped = 0, False, False
    for byte in raw:
        if string:
            if escaped:
                escaped = False
            elif byte == 92:
                escaped = True
            elif byte == 34:
                string = False
        elif byte == 34:
            string = True
        elif byte in (91, 123):
            depth += 1
            if depth > MAX_DEPTH:
                return False
        elif byte in (93, 125):
            depth -= 1
    return True  # json.loads validates syntax after this allocation guard


def _unique(pairs):
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError('Ambiguous JSON object')
        value[key] = item
    return value


class GapAssessment:
    def __init__(self, native_id, prompt_id, attempt):
        self.native_id, self.prompt_id, self.attempt = native_id, prompt_id, attempt
        self.gaps = []
        self.work = 0
        self.extra_read = 0

    def segment(self, fd, *, artifact, start, end, size, reason, raw=None,
                terminated=False, continued=False):
        if len(self.gaps) >= MAX_GAPS:
            # At most 17 oversized starts/continuations fit a 1MiB scanner page.
            # Fail rather than silently omit descriptors if limits ever change.
            raise ValueError('Gap assessment output bound exceeded')
        gap = dict(artifact=artifact, segment_start=start, segment_end=end,
                   record_start=None if continued else start,
                   record_end=end if terminated else None,
                   reason=reason, continued=continued,
                   recovery='continuation' if continued else 'unavailable')
        self.gaps.append(gap)
        if continued:
            return
        if reason == 'incomplete_eof':
            gap['recovery'] = 'incomplete_eof'
            return
        if raw is None:
            budget = min(MAX_RECOVERY_LINE + 1, MAX_ASSESS_WORK - self.work, size-start)
            if budget <= 0:
                gap['recovery'] = 'work_limit'
                return
            data = os.pread(fd, budget, start)
            self.work += len(data)
            self.extra_read += len(data)
            newline = data.find(b'\n')
            if newline < 0:
                gap['recovery'] = ('incomplete_eof' if start+len(data) == size
                                   else 'record_limit' if budget == MAX_RECOVERY_LINE+1 else 'work_limit')
                return
            gap['record_end'] = start+newline+1
            raw = data[:newline]
        if len(raw) > MAX_RECOVERY_LINE:
            gap['recovery'] = 'record_limit'
            return
        if self.work + len(raw) > MAX_ASSESS_WORK:
            gap['recovery'] = 'work_limit'
            return
        self.work += len(raw)
        gap['record_sha256'] = hashlib.sha256(raw).hexdigest()
        gap['hash_scope'] = 'complete_json_line_excluding_newline'
        if not _bounded_depth(raw):
            gap['recovery'] = 'depth_limit'
            return
        try:
            record = json.loads(raw, object_pairs_hook=_unique)
        except (ValueError, UnicodeError, RecursionError):
            gap['recovery'] = 'malformed_or_ambiguous_json'
            return
        row = _metadata(record, self.native_id)
        if row is None:
            gap['recovery'] = 'filtered_envelope'
            return
        exact = (row['native_binding'] == 'exact'
                 and row.get('queued_prompt_id') == self.prompt_id
                 and row.get('attempt') == self.attempt)
        row['correlation'] = 'exact' if exact else 'unknown'
        gap.update(recovery='metadata_recovered', metadata=row)

    def result(self):
        return dict(gaps=self.gaps, work_bytes=self.work,
                    extra_read_bytes=self.extra_read,
                    max_record_bytes=MAX_RECOVERY_LINE, max_work_bytes=MAX_ASSESS_WORK,
                    effect='unknown', delivery_allowed=False,
                    scope='only_unparsed_segments_in_this_original_page',
                    interpretation='complete_envelopes_only_no_cross_record_inference')

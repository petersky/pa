import json

import pytest

from pa.instance import wire_metadata_gaps as gaps, wire_metadata_pages as pages
from tests.test_wire_metadata import SID, NATIVE, PID, artifact, request, completed
from tests.test_wire_metadata_pages import read


def prepare(tmp_path, monkeypatch, tail):
    monkeypatch.setattr(pages, 'MAX_OUTPUT', 1)
    path = artifact(tmp_path, [request()])
    with path.open('ab') as stream:
        stream.write(tail)
    first = read(tmp_path)
    return path, first['next_cursor']


def oversized(**changes):
    value = request(queued_prompt_id=PID, attempt=1)
    value.update(changes)
    value['content'] = 'PRIVATE_SECRET' * 6000
    return json.dumps(value).encode() + b'\n'


def test_complete_oversized_metadata_recovered_with_exact_original_range(tmp_path, monkeypatch):
    tail = oversized()
    path, cursor = prepare(tmp_path, monkeypatch, tail)
    ordinary = read(tmp_path, cursor)
    assessed = read(tmp_path, cursor, assess_unparsed=True)
    detail = assessed.pop('unparsed_assessment')
    assert assessed == ordinary  # snapshot, cursor, rows, counters, hashes unchanged
    assert detail['effect'] == 'unknown' and not detail['delivery_allowed']
    gap, = detail['gaps']
    assert gap['recovery'] == 'metadata_recovered'
    assert gap['record_end'] == path.stat().st_size
    assert gap['record_end']-gap['record_start'] == len(tail)
    assert gap['metadata']['queued_prompt_id'] == PID
    assert gap['metadata']['correlation'] == 'exact'
    assert 'PRIVATE_SECRET' not in json.dumps(detail)


def test_cross_page_record_recovered_with_bounded_lookahead_and_continuation_ranges(tmp_path, monkeypatch):
    monkeypatch.setattr(pages, 'MAX_ARTIFACT_BYTES', 1024)
    monkeypatch.setattr(pages, 'MAX_LINE', 300)
    tail = oversized()
    path, cursor = prepare(tmp_path, monkeypatch, tail)
    page = read(tmp_path, cursor, assess_unparsed=True)
    detail = page['unparsed_assessment']; gap, = detail['gaps']
    assert gap['record_end'] == path.stat().st_size
    assert gap['segment_end'] < gap['record_end']
    assert gap['recovery'] == 'metadata_recovered'
    end = gap['segment_end']
    count = 1
    while page['next_cursor']:
        page = read(tmp_path, page['next_cursor'], assess_unparsed=True)
        piece, = page['unparsed_assessment']['gaps']
        assert piece['continued'] and piece['record_start'] is None
        assert piece['segment_start'] == end
        end = piece['segment_end']; count += 1
    assert end == gap['record_end'] and count > 2
    assert page['totals']['unparsed'] == 1


@pytest.mark.parametrize('kind', ['oversized', 'short'])
def test_incomplete_eof_never_promoted_even_if_json_prefix_is_valid(tmp_path, monkeypatch, kind):
    tail = oversized().rstrip(b'\n') if kind == 'oversized' else b'{"method":"session/prompt"}'
    path, cursor = prepare(tmp_path, monkeypatch, tail)
    page = read(tmp_path, cursor, assess_unparsed=True)
    gap, = page['unparsed_assessment']['gaps']
    assert gap['reason'] == 'incomplete_eof' and gap['recovery'] == 'incomplete_eof'
    assert gap['record_end'] is None and gap['segment_end'] == path.stat().st_size
    assert 'metadata' not in gap


def test_record_and_aggregate_caps_preserve_unknown_and_all_gap_descriptors(tmp_path, monkeypatch):
    monkeypatch.setattr(gaps, 'MAX_RECOVERY_LINE', 500)
    path, cursor = prepare(tmp_path, monkeypatch, oversized())
    page = read(tmp_path, cursor, assess_unparsed=True)
    assert page['unparsed_assessment']['gaps'][0]['recovery'] == 'record_limit'
    monkeypatch.setattr(gaps, 'MAX_RECOVERY_LINE', 1024*1024)
    monkeypatch.setattr(gaps, 'MAX_ASSESS_WORK', 100000)
    path, cursor = prepare(tmp_path, monkeypatch, oversized()+oversized())
    detail = read(tmp_path, cursor, assess_unparsed=True)['unparsed_assessment']
    assert len(detail['gaps']) == 2
    assert detail['work_bytes'] <= 100000
    assert detail['gaps'][1]['recovery'] == 'work_limit'


def test_auxiliary_read_budget_and_snapshot_change_are_fenced(tmp_path, monkeypatch):
    monkeypatch.setattr(pages, 'MAX_ARTIFACT_BYTES', 1024)
    monkeypatch.setattr(pages, 'MAX_LINE', 300)
    path, cursor = prepare(tmp_path, monkeypatch, oversized())
    monkeypatch.setattr(gaps, 'MAX_RECOVERY_LINE', 2000)
    detail = read(tmp_path, cursor, assess_unparsed=True)['unparsed_assessment']
    assert detail['extra_read_bytes'] == 2001
    assert detail['gaps'][0]['recovery'] == 'record_limit'
    real = gaps.os.pread
    def mutate(fd, size, offset):
        result = real(fd, size, offset)
        if size == 2001:
            with path.open('ab') as stream: stream.write(b'{}\n')
        return result
    monkeypatch.setattr(gaps.os, 'pread', mutate)
    with pytest.raises(ValueError): read(tmp_path, cursor, assess_unparsed=True)


@pytest.mark.parametrize('kind', ['duplicate', 'malformed', 'deep', 'wrong_native', 'nested_fake', 'unbound'])
def test_untrusted_or_unbound_envelopes_never_create_exact_correlation(tmp_path, monkeypatch, kind):
    value = request()
    if kind == 'wrong_native': value['params']['session_id'] = SID
    if kind == 'unbound': value['params'].pop('session_id')
    value['content'] = {'queued_prompt_id': PID, 'attempt': 1, 'session_id': NATIVE,
                        'text': 'PRIVATE_SECRET'*6000}
    tail = json.dumps(value).encode()+b'\n'
    if kind == 'duplicate': tail = tail[:-2]+b',"params":{}}\n'
    if kind == 'malformed': tail = tail[:-2]+b'broken\n'
    if kind == 'deep': tail = b'['*2000+b'"'+b'x'*70000+b'"'+b']'*2000+b'\n'
    _, cursor = prepare(tmp_path, monkeypatch, tail)
    detail = read(tmp_path, cursor, assess_unparsed=True)['unparsed_assessment']
    gap, = detail['gaps']
    assert gap.get('metadata', {}).get('correlation') != 'exact'
    if kind in {'duplicate','malformed'}:
        assert gap['recovery'] == 'malformed_or_ambiguous_json'
    if kind == 'deep': assert gap['recovery'] == 'depth_limit'
    assert 'PRIVATE_SECRET' not in json.dumps(detail)


def test_existing_cursor_mandatory_and_all_original_fences_still_apply(tmp_path, monkeypatch):
    path, cursor = prepare(tmp_path, monkeypatch, oversized())
    with pytest.raises(ValueError): read(tmp_path, assess_unparsed=True)
    with pytest.raises(ValueError): read(tmp_path, cursor+'bad', assess_unparsed=True)
    with pytest.raises(ValueError): read(tmp_path, cursor, assess_unparsed=True, selection='outcomes')
    with path.open('ab') as stream: stream.write(b'{}\n')
    with pytest.raises(ValueError): read(tmp_path, cursor, assess_unparsed=True)


def test_gap_terminal_does_not_join_legacy_ids_or_change_delivery_state(tmp_path, monkeypatch):
    value = completed(); value['private'] = 'PRIVATE_SECRET'*6000
    _, cursor = prepare(tmp_path, monkeypatch, json.dumps(value).encode()+b'\n')
    page = read(tmp_path, cursor, assess_unparsed=True)
    row = page['unparsed_assessment']['gaps'][0]['metadata']
    assert row['stop_reason'] == 'end_turn' and row['correlation'] == 'unknown'
    assert page['effect'] == 'unknown' and not page['delivery_allowed']

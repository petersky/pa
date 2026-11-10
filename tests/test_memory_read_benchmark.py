"""Bounds and independent complete public Memory/history reference contracts."""
import importlib.util
from pathlib import Path
from unittest.mock import patch
import pytest
from pa.domain.models import KnowledgeEntry

spec=importlib.util.spec_from_file_location('memory_read_benchmark',Path(__file__).parents[1]/'scripts/benchmark_memory_reads.py')
benchmark=importlib.util.module_from_spec(spec);spec.loader.exec_module(benchmark)


@pytest.mark.parametrize('kwargs',[{'iterations':0},{'iterations':7},{'population':119},{'population':241},{'payloads':()},{'payloads':(1,2,3)},{'payloads':(8193,)}])
def test_bounds_precede_fixture(kwargs):
    with patch.object(benchmark.tempfile,'TemporaryDirectory',side_effect=AssertionError('created fixture')):
        with pytest.raises(ValueError):benchmark.benchmark(**kwargs)


def test_independent_reference_retains_all_other_fields_on_redaction():
    entry=KnowledgeEntry(id='synthetic',summary='Synthetic private marker',owner='owner',sensitivity='restricted',source_url='https://synthetic.invalid',tags=['synthetic'],confidence=.6)
    other=benchmark.expected_memory([entry],{},'other')['records'][0]
    expected=entry.model_dump(mode='json');expected.update(summary='[redacted: restricted Memory]',tags=[],source_url=None,provenance=None)
    assert other==expected
    assert benchmark.expected_memory([entry],{},'owner')['records']==[entry.model_dump(mode='json')]


def test_compact_columns_preserve_zero_and_absent_sql_phases():
    result={'profiles':[{'capture':{'phase_ms':{'read':[2,1,2,2,2]},'sql':{'read':{'execute':[1,0,0,0]},'empty':{'fetchone':[1,0,.1,.1]}}},'runtime':{'routes':{'/actual':{'count':2,'max':3}}},'response_hashes':{'first':'abc','second':'abc'}}]}
    compact=benchmark.compact(result)['profiles'][0]
    assert compact['capture']['phase_names']==['read']
    kinds=compact['capture']['sql_kinds']
    assert compact['capture']['sql']['read'][kinds.index('execute')]==[1,0,0,0]
    assert compact['capture']['sql']['read'][kinds.index('fetchone')] is None
    assert compact['hash_values']==['abc'] and compact['response_hashes']=={'first':0,'second':0}


def test_targeted_history_and_authenticated_memory_reads_are_passive():
    result=benchmark.benchmark(iterations=1,population=120,payloads=(256,))
    assert not result.get('failed'),benchmark.json.dumps(result,separators=(',',':'))
    assert result['history']['case_counts_rows_total']['default']==[25,104]
    assert result['history']['case_counts_rows_total']['max']==[100,104]
    assert result['history']['case_counts_rows_total']['other_realm']==[25,26]
    profile=result['profiles'][0]
    assert profile['status_counts']['bad_cursor:422']==1
    assert profile['status_counts']['missing_audit:404']==1
    assert profile['status_counts']['unauthenticated:401']==1
    assert profile['durable_parity'] and result['temporary_fixture_removed']
    assert not any(profile['ge500ms'].values())

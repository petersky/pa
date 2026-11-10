"""Private actual UI read fixture bounds and independent context/HTML oracle."""
import importlib.util
from pathlib import Path
from unittest.mock import patch
import pytest

spec=importlib.util.spec_from_file_location('memory_ui_benchmark',Path(__file__).parents[1]/'scripts/benchmark_memory_ui.py')
benchmark=importlib.util.module_from_spec(spec);spec.loader.exec_module(benchmark)


@pytest.mark.parametrize('kwargs',[{'iterations':0},{'iterations':7},{'card_count':7},{'card_count':161},{'card_bytes':16385},{'note_count':119},{'note_count':241},{'note_bytes':4097}])
def test_resource_bounds_precede_new_fixture(kwargs):
    with patch.object(benchmark.tempfile,'TemporaryDirectory',side_effect=AssertionError('created fixture')):
        with pytest.raises(ValueError):benchmark.benchmark(**kwargs)


def test_actual_partial_context_html_auth_and_durable_parity():
    result=benchmark.benchmark(iterations=1,card_count=8,card_bytes=256,note_count=120,note_bytes=256)
    assert not result.get('failed'),benchmark.json.dumps(result,separators=(',',':'))
    profile=result['profiles'][0]
    assert profile['contexts']['default'][1:4]==[6,6,3]
    assert profile['contexts']['other_realm'][1:4]==[2,2,3]
    assert profile['contexts']['empty'][0]==0
    assert profile['status_counts']['bad_cursor:422']==profile['status_counts']['no_bearer_default:200']==1
    assert profile['contexts']['no_bearer_default']==profile['contexts']['default']
    assert profile['durable_parity'] and profile['auth_directory_unchanged'] and result['temporary_fixture_removed']


def test_diagnostic_reports_only_changed_paths_without_values():
    assert benchmark.difference_paths({'request':object(),'records':[{'summary':'synthetic-a'}]}, {'records':[{'summary':'synthetic-b'}]})==['/records/0/summary','/request']

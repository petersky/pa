"""Bounded private PR read fixture guards and independent filter/auth oracle."""
import importlib.util
from pathlib import Path
from unittest.mock import patch
import pytest
from pa.pr_supervisor.models import PRWatch,PRWatchStatus

spec=importlib.util.spec_from_file_location('pr_read_benchmark',Path(__file__).parents[1]/'scripts/benchmark_pr_reads.py')
benchmark=importlib.util.module_from_spec(spec);spec.loader.exec_module(benchmark)

@pytest.mark.parametrize('kwargs',[{'population':23},{'population':161},{'iterations':0},{'iterations':7},{'payloads':()},{'payloads':(1,1,1)},{'payloads':(32769,)},{'event_count':11},{'event_count':241}])
def test_bounds_reject_before_fixture(kwargs):
    with patch.object(benchmark.tempfile,'TemporaryDirectory',side_effect=AssertionError('created fixture')):
        with pytest.raises(ValueError):benchmark.benchmark(**kwargs)


def test_independent_reference_filters_before_paging():
    records=[PRWatch(id=f'w{i}',repository='synthetic/repo',pr_number=i+1,pr_url='https://synthetic.invalid',realm_id='other' if i%3==0 else 'default',card_id=f'card-{i%2}',status=PRWatchStatus.RETIRED if i%4==0 else PRWatchStatus.BLOCKED,last_error='synthetic' if i%2 else None) for i in range(12)]
    actual,total=benchmark.expected_watches(records,view='all',card='card-1',offset=1,limit=1)
    selected=sorted([r for r in records if r.realm_id=='default' and r.card_id=='card-1'],key=lambda r:(r.updated_at,r.id),reverse=True)
    assert total==len(selected) and actual==[selected[1].model_dump(mode='json')]
    error_rows,_=benchmark.expected_watches(records,view='errors')
    assert all(r['realm_id']=='default' and r['last_error'] and r['status'] in ['active','blocked'] for r in error_rows)


def test_actual_private_router_auth_full_payload_and_read_durability():
    result=benchmark.benchmark(population=24,iterations=1,payloads=(1024,),event_count=12)
    assert not result.get('failed'),benchmark.json.dumps(result,separators=(',',':'))
    profile=result['profiles'][0]
    assert profile['status_counts']['unauthenticated:401']==1
    assert profile['status_counts']['missing:404']==1
    assert profile['status_counts']['bad_boolean:422']==1
    assert profile['counts']['list:fastapi_serialize']==1
    assert profile['phase_ms']['fastapi_serialize:wall_ms'][0]>=8
    assert profile['durable_parity'] and not profile['worker_started']
    assert result['temporary_fixture_removed']

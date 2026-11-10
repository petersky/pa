"""Resource and independent selection guards for a private read-only benchmark."""
import importlib.util
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch
import pytest
from pa.execution.dispatch import DispatchRecord

spec=importlib.util.spec_from_file_location('dispatch_read_benchmark',Path(__file__).parents[1]/'scripts/benchmark_dispatch_reads.py')
benchmark=importlib.util.module_from_spec(spec);spec.loader.exec_module(benchmark)

@pytest.mark.parametrize('kwargs',[{'population':23},{'population':521},{'iterations':0},{'iterations':7},{'profiles':()},{'profiles':((1,1),)*3},{'profiles':((17,1),)},{'profiles':((1,4097),)}])
def test_bounds_fail_before_private_root(kwargs):
    with patch.object(benchmark.tempfile,'TemporaryDirectory',side_effect=AssertionError('created fixture')):
        with pytest.raises(ValueError):benchmark.benchmark(**kwargs)


def test_reference_selection_names_projection_and_limits():
    now=datetime.now(UTC)
    records=[DispatchRecord(dispatch_id=str(i),mutation_id=str(i),authority_instance_id='local',authority_instance_name='Old local',authority_url='http://synthetic.invalid',target_instance_id='target' if i%2 else 'other',target_instance_name='Old target',updated_at=now+timedelta(seconds=i)) for i in range(6)]
    rows=benchmark.expected_rows(records,{'local':'Current local','target':'Current target'},{'target_instance_id':'target','limit':0})
    assert [x['dispatch_id'] for x in rows]==['5']
    assert rows[0]['target_instance_name']=='Current target'
    assert rows[0]['target_instance_name_snapshot']=='Old target'
    assert rows[0]['target_instance_name_at_dispatch']=='Old target'
    assert rows[0]['view']=='status' and 'turn_end' not in rows[0]
    full=benchmark.expected_rows(records,{}, {'view':'full','limit':501})
    assert [x['dispatch_id'] for x in full]==list(map(str,reversed(range(6))))
    assert 'turn_end' in full[0] and full[0]['target_instance_name']=='Old target'
    assert benchmark.expected_rows(records,{}, {'target_instance_id':'absent'})==[]

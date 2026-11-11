"""Bounds and independent context reference; private measurement awaits reserve."""
import importlib.util
from pathlib import Path
from unittest.mock import patch
import pytest

spec=importlib.util.spec_from_file_location('session_audit_benchmark',Path(__file__).parents[1]/'scripts/benchmark_session_audit_reads.py')
benchmark=importlib.util.module_from_spec(spec);spec.loader.exec_module(benchmark)


@pytest.mark.parametrize('kwargs',[{'population':63},{'population':161},{'payloads':()},{'payloads':(1,2,3)},{'payloads':(16385,)},{'iterations':0},{'iterations':7}])
def test_resource_bounds_precede_storage(kwargs):
    with patch.object(benchmark.tempfile,'TemporaryDirectory',side_effect=AssertionError('fixture created')):
        with pytest.raises(ValueError):benchmark.benchmark(**kwargs)


def test_report_reserve_denial_precedes_fixture():
    report=benchmark.SimpleNamespace(stat=lambda:benchmark.SimpleNamespace(st_size=130616))
    with patch.object(benchmark.tempfile,'TemporaryDirectory',side_effect=AssertionError('fixture created')):
        with pytest.raises(RuntimeError,match='Reserve24KiB'):benchmark.benchmark(report_path=report)


def test_reference_cursor_splits_tied_timestamps_without_skips():
    """Pure in-memory oracle check; no store or disposable directory."""
    from datetime import UTC, datetime
    from urllib.parse import parse_qs, urlsplit
    stamp=datetime(2026,1,1,tzinfo=UTC)
    records=[benchmark.AgentSession(id=f'synthetic-{i:02}',agent_name='Synthetic',
        realm_id='default',updated_at=stamp) for i in range(27)]
    records.append(benchmark.AgentSession(id='other',agent_name='Synthetic',
        realm_id='other',updated_at=stamp))
    first=benchmark.expected_page(records,{},['default','other'])
    cursor=parse_qs(urlsplit(first['memory_next_url']).query)['cursor'][0]
    second=benchmark.expected_page(records,{'cursor':cursor},['default','other'])
    assert [r.id for r in first['audit_sessions']+second['audit_sessions']]==[
        f'synthetic-{i:02}' for i in range(26,-1,-1)]
    assert first['memory_has_more'] and not second['memory_has_more']
    assert second['memory_next_url'] is None
    assert benchmark.expected_page(records,{'realm':'absent'},[])['audit_sessions']==[]


def test_reference_public_context_keeps_full_session_fields():
    record=benchmark.AgentSession(agent_name='Synthetic',
        config_json={'synthetic':'<&>'},metrics_json={'sample':123})
    context=benchmark.expected_page([record],{},['default'])
    public=benchmark.public_page(context)
    assert public['audit_sessions']==[record.model_dump(mode='json')]
    assert public['audit_sessions'][0]['config_json']=={'synthetic':'<&>'}
    assert public['audit_sessions'][0]['metrics_json']=={'sample':123}

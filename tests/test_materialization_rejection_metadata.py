"""Repair guard diagnosis survives the bridge without exporting arbitrary data."""
import httpx
import pytest

from pa.mcp.local_api import _http_error
from pa.repository.materialization_errors import rejection_detail


@pytest.mark.parametrize('reason,code', [
    ('Durable exact-owner idle evidence is unavailable','owner_idle_unverified'),
    ('Exact lease or fencing token changed','lease_fence_changed'),
    ('Existing tracked bytes differ from HEAD; retained for manual review','tracked_bytes_differ'),
    ('Index has an existing writer or unsafe path','index_writer_or_unsafe'),
])
def test_guard_reason_survives_http_to_mcp_with_original_correlation(reason,code):
    response=httpx.Response(409,json={'detail':rejection_detail(reason)},
                            headers={'X-Request-ID':'original-correlation'})
    error=_http_error(response,method='POST',path='/api/workspaces/lease/materialization',
                      correlation_id='client-correlation',expected_instance_id='')
    assert error.code=='workspace_materialization_'+code
    assert reason in str(error) and error.correlation_id=='original-correlation'
    assert error.status==409
    assert error.detail['recovery_action']=='workspace_materialization_repair(action=status)'
    assert 'effect' not in error.detail and 'committed' not in error.detail


def test_unknown_failure_does_not_export_path_provider_text_or_imply_noncommit():
    private='/private/owner/path: native output and token=secret'
    detail=rejection_detail(private)
    error=_http_error(httpx.Response(409,json={'detail':detail}),method='POST',
                      path='/api/workspaces/lease/materialization',correlation_id='same',
                      expected_instance_id='')
    assert private not in str(error) and 'secret' not in str(detail)
    assert detail['code']=='workspace_materialization_precondition_failed'
    assert 'message' not in detail and 'recoverable' not in detail

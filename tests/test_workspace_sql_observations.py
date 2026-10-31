"""Observe the private workspace ledger without changing its native transactions."""
import json
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from pa.config import Settings
from pa.core.async_runtime import AsyncRuntime
from pa.core.performance import OperationObservation, current_observation
from pa.repository.workspace import WorkspaceManager


@pytest.fixture
def manager(tmp_path):
    return WorkspaceManager(
        Settings(data_dir=tmp_path / 'data', workspace_root=tmp_path / 'workspaces',
                 instance_id='private-test', instance_name='private'),
        SimpleNamespace(),
    )


def test_native_workspace_queries_include_lazy_rows_without_values(manager):
    observation = OperationObservation()
    token = current_observation.set(observation)
    try:
        with manager._connect() as conn:
            rows = conn.execute('SELECT ? AS secret UNION ALL SELECT ?',
                                ('private-proof', 'private-card')).fetchall()
        assert [row['secret'] for row in rows] == ['private-proof', 'private-card']
    finally:
        current_observation.reset(token)
    assert observation.sqlite_calls >= 4 and observation.sqlite_rows == 2
    assert observation.sqlite_statement_ms > 0
    assert observation.sqlite_statement_thread_cpu_ms > 0
    retained = json.dumps(asdict(observation))
    assert all(secret not in retained for secret in
               ['SELECT', 'private-proof', 'private-card', str(manager.db_path)])


def test_context_transaction_commit_and_rollback_survive_reconnect(manager):
    observation = OperationObservation()
    token = current_observation.set(observation)
    try:
        with manager._connect() as conn:
            conn.execute('CREATE TABLE observation_fixture (value TEXT)')
            conn.execute('INSERT INTO observation_fixture VALUES (?)', ('retained',))
        with pytest.raises(RuntimeError, match='rollback fixture'):
            with manager._connect() as conn:
                conn.execute('DELETE FROM observation_fixture')
                raise RuntimeError('rollback fixture')
        with manager._connect() as conn:
            assert [r[0] for r in conn.execute('SELECT value FROM observation_fixture')] == ['retained']
    finally:
        current_observation.reset(token)
    # Native C context-manager commit/rollback still bypass Python commit hooks.
    assert observation.sqlite_commit_ms == 0
    assert observation.sqlite_calls > 0


def test_unobserved_workspace_reads_do_not_add_observer_clocks(manager):
    with patch('pa.core.sqlite_metrics.time.perf_counter',
               side_effect=AssertionError('inactive observer clock')), patch(
                   'pa.core.sqlite_metrics.time.thread_time',
                   side_effect=AssertionError('inactive observer CPU clock')):
        assert manager.list() == []


@pytest.mark.asyncio
async def test_worker_includes_workspace_own_sql_observations(manager):
    runtime = AsyncRuntime(max_workers=1)
    try:
        assert await runtime.run_blocking('workspace.collect_garbage', manager.list) == []
        operation = runtime.snapshot()['operations']['workspace.collect_garbage']
        assert operation['completed'] == 1
        assert operation['failed'] == operation['timed_out'] == operation['rejected'] == 0
        assert operation['sqlite_calls']['max'] >= 4
        assert operation['sqlite_statement_ms']['max'] > 0
        assert operation['sqlite_statement_thread_cpu_ms']['max'] > 0
    finally:
        await runtime.close()

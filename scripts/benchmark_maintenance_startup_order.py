"""Compare automatic full-maintenance overlap with a settled-startup barrier.

Private databases only. The baseline deliberately overlaps real VACUUM with
startup writes; provider/network/Kernel timing and physical I/O are not measured.
"""
import argparse
import asyncio
from dataclasses import asdict
import json
import os
from pathlib import Path
import shutil
import sqlite3
import tempfile
import threading
import time
from unittest.mock import patch


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    options = parser.parse_args()
    assert not options.output.exists(), 'Preserve previous evidence'
    with tempfile.TemporaryDirectory(prefix='pa-startup-maintenance-') as directory:
        root = Path(directory)
        os.environ.update(PA_DATA_DIR=str(root/'data'), PA_WORKSPACE_ROOT=str(root/'workspaces'),
                          PA_AGENT_ENABLED='false')
        assert shutil.disk_usage(root).free > 2 * 1024**3
        from pa.config import Settings
        from pa.core.performance import OperationObservation, current_observation, distribution
        from pa.core.sqlite_metrics import ObservedConnection
        from pa.domain.projection import CardProjection
        from pa.instance.maintenance import InstanceMaintenanceService

        async def scenario(iteration, ordered):
            settings = Settings(data_dir=root/f'{iteration}-{ordered}',
                                workspace_root=root/'workspaces', agent_enabled=False)
            settings.data_dir.mkdir(parents=True)
            store = CardProjection(settings.db_path)
            with sqlite3.connect(settings.db_path) as conn:
                conn.execute('CREATE TABLE retained_fixture(id INTEGER PRIMARY KEY, body BLOB)')
                conn.executemany('INSERT INTO retained_fixture VALUES(?,?)',
                                 ((i, b'x'*4096) for i in range(32768)))
                conn.execute('CREATE TABLE startup_fixture(id INTEGER PRIMARY KEY, value INTEGER)')
                conn.execute('INSERT INTO startup_fixture VALUES(1,0)')
                conn.commit()
                conn.execute('DELETE FROM retained_fixture WHERE id<64')
                conn.commit()
            ready = asyncio.get_running_loop().create_future() if ordered else None
            service = InstanceMaintenanceService(settings, store, {}, startup_ready=ready)
            entered = threading.Event()
            execute = ObservedConnection.execute

            def measured_execute(conn, sql, *args, **kwargs):
                if sql == 'VACUUM':
                    entered.set()
                return execute(conn, sql, *args, **kwargs)

            def write_startup():
                samples = []
                with sqlite3.connect(settings.db_path, timeout=10, factory=ObservedConnection) as conn:
                    conn.execute('PRAGMA synchronous=FULL')
                    for _ in range(100):
                        observation = OperationObservation()
                        token = current_observation.set(observation)
                        started = time.perf_counter()
                        try:
                            conn.execute('UPDATE startup_fixture SET value=value+1 WHERE id=1')
                            conn.commit()
                        finally:
                            samples.append({'wall_ms': (time.perf_counter()-started)*1000,
                                            'observation': asdict(observation)})
                            current_observation.reset(token)
                return samples

            with patch.object(ObservedConnection, 'execute', measured_execute):
                started = time.perf_counter()
                service.start()
                try:
                    if not ordered:
                        assert await asyncio.to_thread(entered.wait, 10)
                    else:
                        await asyncio.sleep(0)
                        assert service.snapshot()['waiting_for_startup'] and not entered.is_set()
                    writes = await asyncio.to_thread(write_startup)
                    startup_ms = (time.perf_counter()-started)*1000
                    if ordered:
                        assert not entered.is_set(), 'automatic VACUUM crossed startup barrier'
                        ready.set_result(None)
                        assert await asyncio.to_thread(entered.wait, 10)
                    result = await asyncio.wait_for(asyncio.shield(service._active_run), 20)
                    total_ms = (time.perf_counter()-started)*1000
                finally:
                    await service.close()
            with sqlite3.connect(settings.db_path) as conn:
                assert conn.execute('PRAGMA quick_check').fetchall() == [('ok',)]
                assert conn.execute('SELECT value FROM startup_fixture').fetchone() == (100,)
                assert conn.execute('SELECT COUNT(*),SUM(length(body)) FROM retained_fixture').fetchone() == (32704,32704*4096)
            assert result['sqlite']['compacted'] and result['sqlite']['quick_check'] == 'ok'
            assert result['sqlite']['after']['freelist_count'] == 0
            assert result['sqlite']['after']['bytes'] < result['sqlite']['before']['bytes']
            assert result['sqlite']['status'] == 'ok' or result['sqlite'].get('reason') == 'busy'
            return {'iteration': iteration, 'ordered': ordered, 'startup_ms': startup_ms,
                    'pipeline_ms': total_ms, 'write_latency': distribution([x['wall_ms'] for x in writes]),
                    'writes': writes, 'full_maintenance': result,
                    'startup_write_500ms_violations': sum(x['wall_ms'] >= 500 for x in writes)}

        async def run():
            samples = []
            for i in range(3):
                for ordered in ((False, True) if i % 2 == 0 else (True, False)):
                    samples.append(await scenario(i, ordered))
            return samples

        samples = asyncio.run(run())
        output = {'contract': 'pa.private-maintenance-startup-order/v1', 'samples': samples,
                  'scope': __doc__, 'limits': 'Three alternating pairs, real full maintenance on151MB disposable projections and100FULL durable startup writes each. Does not measure real Kernel/provider/network/admission, native430MB or whole-card acceptance. Full sweep and any500ms violations remain explicit; no threshold/deadline/durability change.'}
        options.output.write_text(json.dumps(output, indent=2)+'\n')
        print(json.dumps([{k: s[k] for k in ['iteration', 'ordered', 'startup_ms', 'pipeline_ms',
                                            'write_latency', 'startup_write_500ms_violations']}
                          for s in samples]))


if __name__ == '__main__':
    main()

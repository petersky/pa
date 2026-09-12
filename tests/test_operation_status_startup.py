"""Auxiliary CLI boot cannot become a reconciliation owner."""
from __future__ import annotations

import asyncio
import hashlib
import os
import sqlite3
import subprocess
import sys

import pytest

from pa.config import Settings
from pa.core.kernel import Kernel
from pa.core.operation_status import OperationStatusService
from pa.core.writer_lock import DataDirAlreadyOwnedError, DataDirWriterLock


def settings_for(tmp_path):
    return Settings(data_dir=tmp_path / "data", workspace_root=tmp_path / "workspaces",
                    instance_id="startup-owner", agent_enabled=False, port=1)


@pytest.mark.parametrize("existing", [False, True])
def test_actual_cli_status_and_plugins_do_not_create_or_reset_live_repairs(tmp_path, existing):
    settings = settings_for(tmp_path)
    lock = DataDirWriterLock(settings.data_dir)
    lock.acquire()
    service = OperationStatusService(settings.data_dir) if existing else None
    conn = None
    path = settings.data_dir / "operation_reconciliation.db"
    try:
        if service:
            queued = service.admission("canonical", "default", "queued")
            running = service.admission("canonical", "default", "running")
            service._record(running["id"], "running")
            # A live owner can hold its transaction while CLI boot occurs.
            conn = sqlite3.connect(path)
            conn.execute("BEGIN IMMEDIATE")
        paths = [path, path.with_name(path.name + "-wal"), path.with_name(path.name + "-shm")]

        def snapshot():
            # SQLite's live mmap can flush SHM metadata asynchronously. Check
            # its bytes, while DB/WAL must preserve both bytes and timestamps.
            return {str(p): (None if p.name.endswith("-shm") else p.stat().st_mtime_ns,
                            hashlib.sha256(p.read_bytes()).hexdigest())
                    for p in paths if p.exists()}

        before = snapshot()
        env = {k: v for k, v in os.environ.items() if not k.startswith("PA_")}
        env.update(PA_DATA_DIR=str(settings.data_dir), PA_WORKSPACE_ROOT=str(settings.workspace_root),
                   PA_INSTANCE_ID="startup-owner", PA_AGENT_ENABLED="false", PA_PORT="1")
        guard = tmp_path / "guard"
        guard.mkdir()
        (guard / "sitecustomize.py").write_text('''
import sys
def audit(event, args):
    if event == 'sqlite3.connect' and str(args[0]).split('?')[0].endswith('operation_reconciliation.db'):
        raise AssertionError('CLI attempted reconciliation database access')
sys.addaudithook(audit)
''')
        env["PYTHONPATH"] = str(guard) + os.pathsep + env.get("PYTHONPATH", "")
        for arguments in (["status"], ["plugins", "list"]):
            result = subprocess.run([sys.executable, "-m", "pa", *arguments], env=env,
                                    capture_output=True, text=True, timeout=45)
            assert result.returncode == 0, result.stderr
            assert snapshot() == before
        if conn:
            assert dict(conn.execute("SELECT operation_key,state FROM reconciliation")) == {
                "queued": "queued", "running": "running",
            }
        else:
            assert not path.exists()
    finally:
        if conn:
            conn.rollback()
            conn.close()
        if service:
            asyncio.run(service.close())
        lock.release()


@pytest.mark.asyncio
async def test_real_lifespan_recovers_only_after_writer_claim_and_coalesces_once(tmp_path):
    settings = settings_for(tmp_path)
    prior_lock = DataDirWriterLock(settings.data_dir)
    prior_lock.acquire()
    prior = OperationStatusService(settings.data_dir)
    job = prior.admission("canonical", "default", "interrupted-key")
    prior._record(job["id"], "running")
    kernel = Kernel.boot(settings=settings, load_modules=False)
    app = kernel.build_app()
    assert "operation_status" not in kernel.ctx.services
    assert prior.read_job("canonical", "default", "interrupted-key")["state"] == "running"
    try:
        with pytest.raises(DataDirAlreadyOwnedError):
            async with app.router.lifespan_context(app):
                pytest.fail("competing server acquired writer ownership")
        assert "operation_status" not in kernel.ctx.services
        assert prior.read_job("canonical", "default", "interrupted-key")["state"] == "running"
    finally:
        await prior.close()
        prior_lock.release()

    async with app.router.lifespan_context(app):
        assert kernel.ctx.services["writer_lock"].held
        service = kernel.ctx.services["operation_status"]
        recovered = service.admission("canonical", "default", "interrupted-key")
        assert recovered["id"] == job["id"]
        assert recovered["state"] == "interrupted"
        effects = []

        async def repair(runtime):
            await runtime.run_blocking("test.repair_effect", effects.append, job["id"],
                                       wait_for_completion=True)
            return {"status": "succeeded"}

        for _ in range(5):
            service.schedule(service.admission("canonical", "default", "interrupted-key"), repair)
        await asyncio.gather(*service.tasks.values())
        assert effects == [job["id"]]
        assert service.read_job("canonical", "default", "interrupted-key")["state"] == "completed"
    assert not kernel.ctx.services["writer_lock"].held

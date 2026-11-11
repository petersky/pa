"""New private probes keep only aggregates and clean up failed runs, too."""
from pathlib import Path
import runpy

import pytest


@pytest.mark.parametrize("script", ["benchmark_transcript_health.py", "benchmark_maintenance_yield.py"])
@pytest.mark.parametrize("fail", [False, True])
def test_private_probe_cleanup(tmp_path, monkeypatch, script, fail):
    benchmark = runpy.run_path(str(Path(__file__).parents[1] / "scripts" / script))["benchmark"]
    namespace = benchmark.__globals__
    temporary = namespace["tempfile"].TemporaryDirectory
    created = []

    def directory(*args, **kwargs):
        result = temporary(*args, **kwargs, dir=tmp_path)
        created.append(Path(result.name))
        return result

    monkeypatch.setattr(namespace["tempfile"], "TemporaryDirectory", directory)
    sentinel = tmp_path / "preexisting"
    sentinel.write_text("preserve")
    if fail:
        if script == "benchmark_transcript_health.py":
            def reject(*args, **kwargs):
                raise RuntimeError("fixture read failure")
            monkeypatch.setattr(namespace["TranscriptStorage"], "referenced_hashes", reject)
            arguments = {"objects": 2, "pairs": 1}
        else:
            connect = namespace["sqlite3"].connect
            def reject(*args, **kwargs):
                connect(*args, **kwargs).close()  # Leave a newly-created file.
                raise RuntimeError("fixture seed failure")
            monkeypatch.setattr(namespace["sqlite3"], "connect", reject)
            arguments = {"pairs": 1, "rows": 16384}
        with pytest.raises(RuntimeError, match="fixture (seed|read) failure"):
            benchmark(**arguments)
    elif script == "benchmark_transcript_health.py":
        assert benchmark(objects=2, pairs=1)["temporary_fixture_removed"]
    else:
        # Exercise setup/failure cleanup without another large WAL measurement.
        monkeypatch.setattr(namespace["shutil"], "disk_usage", lambda _: type("Disk", (), {"free": 0})())
        with pytest.raises(RuntimeError, match="At least4GiB"):
            benchmark()
    assert len(created) == 1 and not created[0].exists()
    assert sentinel.read_text() == "preserve"

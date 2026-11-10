"""Private benchmark data must not accumulate, including on failure."""
from pathlib import Path
import runpy

import pytest


@pytest.mark.parametrize("fail", [False, True])
def test_benchmark_removes_only_its_temporary_fixture(tmp_path, monkeypatch, fail):
    benchmark = runpy.run_path(str(Path(__file__).parents[1] / "scripts/benchmark_transcript_gc.py"))["benchmark"]
    namespace = benchmark.__globals__
    temporary = namespace["tempfile"].TemporaryDirectory
    created = []
    def owned_directory(*args, **kwargs):
        directory = temporary(*args, **kwargs, dir=tmp_path)
        created.append(Path(directory.name))
        return directory
    monkeypatch.setattr(namespace["tempfile"], "TemporaryDirectory", owned_directory)
    sentinel = tmp_path / "preexisting"
    sentinel.write_bytes(b"preserve")
    if fail:
        def reject(self):
            raise RuntimeError("bounded benchmark failure")
        monkeypatch.setattr(namespace["TranscriptStorage"], "collect_unreferenced_objects", reject)
        with pytest.raises(RuntimeError, match="bounded benchmark failure"):
            benchmark(objects=2, pairs=1, orphans=1)
    else:
        result = benchmark(objects=2, pairs=1, orphans=1)
        assert result["temporary_fixture_removed"]
        assert result["objects_preserved"] == 2
        assert result["orphans_removed_per_call"] == 1
    assert len(created) == 1 and not created[0].exists()
    assert sentinel.read_bytes() == b"preserve"

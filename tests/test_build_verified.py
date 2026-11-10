"""Exercise the default export with large irrelevant inputs and both exit paths."""
import importlib.util
from pathlib import Path
import subprocess
import pytest

spec = importlib.util.spec_from_file_location('build_verified', Path(__file__).parents[1] / 'scripts/build_verified.py')
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


@pytest.mark.parametrize('fail', [False, True])
def test_selective_export_cleanup(tmp_path, fail):
    repo = tmp_path / 'repo'
    repo.mkdir()
    def git(*args):
        return subprocess.check_output(['git', '-C', str(repo), *args], text=True).strip()
    git('init')
    for name in ['src/pa/__init__.py', 'pyproject.toml', 'README.md', 'docs/benchmarks/old.raw.json.gz', 'tests/fixtures/large.bin']:
        p = repo / name
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_bytes(b'x' * (1024 * 1024 if 'large' in name or '.gz' in name else 10))
    git('add', '.')
    git('-c', 'user.name=Test', '-c', 'user.email=test@example.com', 'commit', '-m', 'fixture')
    expected = git('rev-parse', 'HEAD')
    (repo / 'src/pa/__init__.py').write_text('uncommitted')
    parent = tmp_path / 'temporary'
    parent.mkdir()
    try:
        with module.exported_source(repo, temporary_parent=parent) as (root, sha, size):
            assert sha == expected
            assert size == 30
            assert (root / 'src/pa/__init__.py').read_bytes() == b'x' * 10
            assert not (root / 'docs').exists()
            assert not (root / 'tests').exists()
            assert not list(parent.rglob('*.tar'))
            if fail:
                raise RuntimeError('build failed')
    except RuntimeError:
        assert fail
    assert list(parent.iterdir()) == []


def test_source_budget(tmp_path, monkeypatch):
    monkeypatch.setattr(module.subprocess, 'check_output', lambda args, **kw: 'a'*40 if 'rev-parse' in args else '100644 blob abc 67108865\tsrc/pa/huge\n')
    with pytest.raises(ValueError, match='exceeds'):
        with module.exported_source(tmp_path):
            pytest.fail('must reject before exporting')


def test_current_benchmark_retention_budget():
    root = Path(__file__).parents[1] / 'docs/benchmarks'
    files = [p for p in root.rglob('*') if p.is_file()]
    assert sum(p.stat().st_size for p in files) < 10 * 1024 * 1024
    assert all('.gz' not in p.name for p in files)
    assert all(p.stat().st_size <= 128 * 1024 for p in files if p.suffix == '.json')


@pytest.mark.parametrize('fail', [False, True])
def test_build_cleans_source_and_attests_only_its_wheel(tmp_path, monkeypatch, fail):
    from contextlib import contextmanager
    import json
    import hashlib
    roots = []
    @contextmanager
    def source(*args):
        import tempfile
        with tempfile.TemporaryDirectory(dir=tmp_path) as directory:
            roots.append(Path(directory))
            yield Path(directory), 'a' * 40, 123
    monkeypatch.setattr(module, 'exported_source', source)
    def build_command(command, *, cwd, check):
        directory = Path(command[-1])
        directory.mkdir()
        (directory / 'current.whl').write_bytes(b'wheel')
        if fail:
            raise subprocess.CalledProcessError(1, command)
    monkeypatch.setattr(module.subprocess, 'run', build_command)
    output = tmp_path / 'output'
    output.mkdir()
    (output / 'previous.whl').write_bytes(b'rollback')
    if fail:
        with pytest.raises(subprocess.CalledProcessError):
            module.build(tmp_path, 'HEAD', output)
        assert not (output / 'current.whl').exists()
    else:
        module.build(tmp_path, 'HEAD', output)
        manifest = json.loads((output / 'current.whl.attestation.json').read_text())
        assert manifest['source_commit'] == 'a' * 40
        assert manifest['wheels'] == {'current.whl': hashlib.sha256(b'wheel').hexdigest()}
    assert all(not root.exists() for root in roots)
    assert (output / 'previous.whl').read_bytes() == b'rollback'

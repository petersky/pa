#!/usr/bin/env python3
"""Build an exact commit from bounded package inputs, without a retained source tar."""
from contextlib import contextmanager
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tarfile
import tempfile

INPUTS = ('src/pa', 'pyproject.toml', 'hatch_build.py', 'README.md', 'uv.lock', '.python-version')
MAX_SOURCE_BYTES = 64 * 1024 * 1024


@contextmanager
def exported_source(repository, revision='HEAD', *, temporary_parent=None):
    repository = Path(repository).resolve()
    sha = subprocess.check_output(['git', '-C', str(repository), 'rev-parse', '--verify', revision + '^{commit}'], text=True).strip()
    listing = subprocess.check_output(['git', '-C', str(repository), 'ls-tree', '-rl', sha, '--', *INPUTS], text=True)
    size = sum(int(line.split()[3]) for line in listing.splitlines() if line.split()[1] == 'blob')
    if size > MAX_SOURCE_BYTES:
        raise ValueError(f'Package source exceeds {MAX_SOURCE_BYTES} bytes: {size}')
    paths = [line.split('\t', 1)[1] for line in listing.splitlines()]
    selected = [item for item in INPUTS if any(p == item or p.startswith(item + '/') for p in paths)]
    if not selected:
        raise ValueError('No package inputs found')
    with tempfile.TemporaryDirectory(prefix='pa-build-', dir=temporary_parent) as temporary:
        root = Path(temporary)
        process = subprocess.Popen(['git', '-C', str(repository), 'archive', sha, '--', *selected], stdout=subprocess.PIPE)
        try:
            with tarfile.open(fileobj=process.stdout, mode='r|') as archive:
                archive.extractall(root, filter='data')
            if process.wait() != 0:
                raise RuntimeError('git archive failed')
            yield root, sha, size
        finally:
            process.stdout.close()
            if process.poll() is None:
                process.terminate()
            process.wait()


def build(repository, revision, output):
    output = Path(output).resolve()
    output.mkdir(parents=True, exist_ok=True)
    with exported_source(repository, revision) as (root, sha, size):
        # uv uses its existing shared cache; the checkout and its environment are untouched.
        built = root / 'wheel-output'
        subprocess.run(['uv', 'build', '--wheel', '--out-dir', str(built)], cwd=root, check=True)
        wheels = list(built.glob('*.whl'))
        if len(wheels) != 1:
            raise RuntimeError('Expected exactly one built wheel')
        manifest = {'source_commit': sha, 'source_bytes': size, 'inputs': INPUTS,
                    'wheels': {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in wheels}}
        for wheel in wheels:
            destination = output / wheel.name
            if destination.exists():
                raise FileExistsError(destination)
            wheel.rename(destination)
        (output / (wheels[0].name + '.attestation.json')).write_text(json.dumps(manifest, indent=2) + '\n')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--revision', default='HEAD')
    parser.add_argument('--output', required=True, help='Dedicated wheel output directory; keep needed rollback wheels')
    args = parser.parse_args()
    build(Path(__file__).resolve().parents[1], args.revision, args.output)

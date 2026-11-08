"""Read-only verification of a retained-parent performance evidence closure."""
import argparse
import gzip
import hashlib
import io
import json
from pathlib import Path


def safe(root, name):
    path = (root / name).resolve()
    if (not path.is_relative_to(root)
            or path.relative_to(root).parts[:2] != ('docs', 'benchmarks')):
        raise ValueError('Evidence path escapes the repository')
    return path


def chunks(root, manifest, *, collect=False):
    if not 0 <= manifest['bytes'] <= 512 * 1024 * 1024:
        raise ValueError('Evidence root exceeds verification capacity')
    if sum(row['bytes'] for row in manifest['ordered_chunks']) != manifest['bytes']:
        raise ValueError('Evidence part sizes do not match root')
    digest = hashlib.sha256()
    total = 0
    payload = bytearray()
    for row in manifest['ordered_chunks']:
        if not 0 <= row['bytes'] <= 64 * 1024 * 1024:
            raise ValueError('Evidence part exceeds verification capacity')
        part = hashlib.sha256()
        size = 0
        with safe(root, row['path']).open('rb') as stream:
            while block := stream.read(1024 * 1024):
                size += len(block)
                if size > row['bytes']:
                    raise ValueError('Evidence part size changed')
                part.update(block)
                digest.update(block)
                if collect:
                    if len(payload) + len(block) > 64 * 1024 * 1024:
                        raise ValueError('Evidence delta exceeds verification capacity')
                    payload.extend(block)
        if size != row['bytes'] or part.hexdigest() != row['sha256']:
            raise ValueError('Evidence part hash mismatch')
        total += size
    if total != manifest['bytes'] or digest.hexdigest() != manifest['sha256']:
        raise ValueError('Evidence root hash mismatch')
    return bytes(payload) if collect else total


def manifest_json(path):
    with path.open('rb') as stream:
        raw = stream.read(1024 * 1024 + 1)
    if len(raw) > 1024 * 1024:
        raise ValueError('Evidence manifest exceeds capacity')
    return json.loads(raw)


def verify(root, name):
    manifest = manifest_json(safe(root, name))
    if manifest['contract'] != 'pa.lossless-retained-parent-delta/v2':
        raise ValueError('Expected retained-parent delta manifest')
    data = chunks(root, manifest, collect=True)
    with gzip.GzipFile(fileobj=io.BytesIO(data)) as stream:
        decoded = stream.read(64 * 1024 * 1024 + 1)
    if len(decoded) > 64 * 1024 * 1024:
        raise ValueError('Decoded evidence delta exceeds capacity')
    envelope = json.loads(decoded)
    parent = manifest['retained_parent']
    if envelope['retained_parent'] != parent:
        raise ValueError('Parent descriptor mismatch')
    old = manifest_json(safe(root, parent['manifest_path']))
    if old['bytes'] != parent['bytes'] or old['sha256'] != parent['sha256']:
        raise ValueError('Parent identity mismatch')
    retained = chunks(root, old)
    return dict(delta_bytes=len(data), delta_sha256=manifest['sha256'],
                retained_parent_bytes=retained, retained_parent_sha256=old['sha256'],
                complete_parent_verified=True, public_inputs=len(envelope['public_inputs']),
                local_hash_only_inputs=len(envelope['local_hash_only_inputs']))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('manifest')
    parser.add_argument('--repository', default='.')
    args = parser.parse_args()
    print(json.dumps(verify(Path(args.repository).resolve(), args.manifest)))

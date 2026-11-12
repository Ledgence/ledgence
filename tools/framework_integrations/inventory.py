"""Fingerprint installed SDK distributions without importing vendor code."""
import hashlib
import importlib.metadata
import json
from pathlib import Path
import re
import sys


def canonical(name): return re.sub(r'[-_.]+', '-', name).lower()


def inventory():
    root = Path(sys.prefix).resolve()
    packages, files = {}, {}
    for dist in importlib.metadata.distributions():
        name = canonical(dist.metadata['Name'])
        if name in ('pip', 'setuptools'): continue
        if name in packages or not dist.files:
            raise ValueError('ambiguous installed SDK inventory')
        packages[name] = dist.version
        for entry in dist.files:
            path = Path(dist.locate_file(entry)).resolve(strict=True)
            if not path.is_relative_to(root) or not path.is_file():
                raise ValueError('installed dependency escapes environment')
            key = path.relative_to(root).as_posix()
            digest = hashlib.sha256(path.read_bytes()).hexdigest()
            if key in files and files[key] != digest:
                raise ValueError('conflicting distribution file')
            files[key] = digest
    return {'packages': packages, 'files': files}


if __name__ == '__main__':
    print(json.dumps(inventory(), sort_keys=True, separators=(',', ':')))

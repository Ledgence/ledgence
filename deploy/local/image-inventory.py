"""Record exact runtime/source identities; this inventory is not legal approval."""
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import subprocess


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main():
    legal = Path('/opt/ledgence/legal')
    base = json.loads((legal / 'base-image.json').read_text())
    python_version = platform.python_version()
    if (python_version != base['python']['version']
            or os.environ.get('PYTHON_VERSION') != python_version
            or os.environ.get('PYTHON_SHA256') != base['python']['sha256']):
        raise SystemExit('pinned CPython identity differs from reviewed source metadata')
    python_license = Path('/usr/local/lib/python3.14/LICENSE.txt')
    if not python_license.is_file():
        raise SystemExit('official CPython image did not retain its full license')
    (legal / 'PYTHON-LICENSE.txt').write_bytes(python_license.read_bytes())
    rows = subprocess.check_output(['dpkg-query', '-W', '-f=${binary:Package}\t${Version}\t${Architecture}\t${source:Package}\t${source:Version}\t${db:Status-Status}\n'], text=True)
    packages = []
    for row in rows.splitlines():
        name, version, arch, source, source_version, status = row.split('\t')
        if status != 'installed':
            continue
        copyright_path = Path('/usr/share/doc') / name.split(':', 1)[0] / 'copyright'
        if not source or not source_version or not copyright_path.is_file():
            raise SystemExit(f'missing source identity or retained copyright for {name}')
        packages.append({'binary_name': name, 'binary_version': version,
                         'architecture': arch, 'source_name': source, 'source_version': source_version,
                         'copyright_path': str(copyright_path), 'copyright_sha256': sha(copyright_path)})
    if not packages:
        raise SystemExit('empty Debian runtime package inventory')
    distributions = []
    for distribution in importlib.metadata.distributions():
        notices = []
        for path in distribution.files or []:
            if any(word in path.name.lower() for word in ('license', 'copying', 'notice', 'copyright')):
                file = Path(distribution.locate_file(path))
                if file.is_file():
                    notices.append({'path': str(file), 'sha256': sha(file)})
        if not notices:
            raise SystemExit(f'missing retained notices for installed Python distribution {distribution.metadata["Name"]}')
        distributions.append({'name': distribution.metadata['Name'], 'version': distribution.version,
                              'notices': sorted(notices, key=lambda item: item['path'])})
    value = {'format': 1, 'base_image': base['base_image'], 'base_recipe': base['base_recipe'],
             'python': base['python'], 'machine': platform.machine(),
             'python_license_sha256': sha(python_license), 'os_release': platform.freedesktop_os_release(),
             'packages': sorted(packages, key=lambda item: item['binary_name']),
             'python_distributions': sorted(distributions, key=lambda item: item['name']),
             'system_notices': '/usr/share/doc/*/copyright and /usr/share/common-licenses (retained from base image)',
             'distribution': 'Inventory only. Public OCI redistribution also requires the complete corresponding-source archive and release-specific license review.'}
    (legal / 'image-runtime.json').write_text(json.dumps(value, indent=2) + '\n')


if __name__ == '__main__':
    main()

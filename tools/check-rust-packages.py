#!/usr/bin/env python3
"""Verify the publishable Rust source archives without uploading anything.

Cargo >=1.98 supports multi-package packaging, including a temporary registry
for unpublished workspace dependencies. Normal archive extraction/build checks
remain enabled; fresh extracts also run tests, doctests and documentation builds.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import subprocess
import tarfile
import tomllib

ROOT = Path(__file__).resolve().parents[1]
APIS = ('ledgence-worker-api', 'ledgence-orchestration-api')
REGISTRY = 'registry+https://github.com/rust-lang/crates.io-index'


def require(condition, message):
    if not condition:
        raise ValueError(message)


def digest(content):
    return hashlib.sha256(content).hexdigest()


def manifest(path):
    return tomllib.loads(path.read_text())


def publication_policy(root):
    workspace = manifest(root / 'Cargo.toml')['workspace']
    version = workspace['package']['version']
    require(re.fullmatch(r'\d+\.\d+\.\d+', version), 'expected a stable workspace version')
    packages = {}
    for member in workspace['members']:
        path = root / member / 'Cargo.toml'
        package = manifest(path)['package']
        name = package['name']
        require(name not in packages, 'duplicate workspace package')
        require(package['version'] == {'workspace': True}, 'package version must follow workspace')
        require(package.get('publish') == (['crates-io'] if name in APIS else False),
                f'{name}: unexpected publication policy')
        packages[name] = path
    require(len(packages) == 15 and set(APIS) <= packages.keys(), 'unexpected workspace package set')
    for name, dependency in workspace['dependencies'].items():
        if name.startswith('ledgence-'):
            require(dependency['version'] == version, f'{name}: mismatched internal dependency version')
    locked = {p['name']: p['version'] for p in manifest(root / 'Cargo.lock')['package'] if p['name'].startswith('ledgence-')}
    require(locked == dict.fromkeys(packages, version), 'workspace lock versions do not match')
    return version, packages


def dependency_tables(data):
    for kind in ('dependencies', 'dev-dependencies', 'build-dependencies'):
        yield from data.get(kind, {}).values()
    for target in data.get('target', {}).values():
        yield from dependency_tables(target)


def normalized_manifest(data, name, version):
    package = data['package']
    require(package['name'] == name and package['version'] == version, 'packaged name/version mismatch')
    require(package.get('publish') == ['crates-io'] and package.get('license') == 'MIT', 'packaged publish/license mismatch')
    require(package.get('readme') == 'README.md' and package.get('description') and
            package.get('repository') == 'https://github.com/Ledgence/ledgence' and
            package.get('documentation') == f'https://docs.rs/{name}' and
            package.get('rust-version') == '1.98', 'packaged public metadata missing or changed')
    require(not any(k in data for k in ('workspace', 'patch', 'replace')), 'packaged resolution override')
    require('workspace' not in package, 'packaged workspace pointer')
    for dependency in dependency_tables(data):
        require(isinstance(dependency, dict) and dependency.get('version'), 'unversioned dependency')
        require(not any(k in dependency for k in ('path', 'git', 'workspace', 'registry', 'registry-index')),
                'packaged dependency is not an independent crates.io dependency')
    if name == APIS[1]:
        require(data['dependencies'][APIS[0]] == {'version': version}, 'orchestration API worker dependency mismatch')


def archive_contents(archive, label):
    files = {}
    with tarfile.open(archive, 'r:gz') as tar:
        for entry in tar:
            path = PurePosixPath(entry.name)
            require(not path.is_absolute() and '..' not in path.parts and
                    path.parts[0] == label and len(path.parts) > 1, 'unsafe/wrong-root archive entry')
            require(entry.isfile(), 'source crate contains non-file entry')
            relative = str(PurePosixPath(*path.parts[1:]))
            require(relative not in files, 'duplicate archive member')
            files[relative] = tar.extractfile(entry).read()
    return files


def inspect_archive(archive, root, name, version, source_commit, allow_dirty):
    contents = archive_contents(archive, f'{name}-{version}')
    source = root / 'crates' / name
    expected = {str(p.relative_to(source)): p.read_bytes() for folder in ('src', 'tests')
                for p in (source / folder).rglob('*.rs')}
    expected.update({p: (source / p).read_bytes() for p in ('README.md', 'LICENSE')})
    expected['Cargo.toml.orig'] = (source / 'Cargo.toml').read_bytes()
    require(set(contents) == set(expected) | {'Cargo.toml', 'Cargo.lock', '.cargo_vcs_info.json'},
            f'{name}: source archive inventory differs')
    require(all(contents[p] == content for p, content in expected.items()), f'{name}: packaged source changed')
    require(contents['LICENSE'] == (root / 'LICENSE').read_bytes(), f'{name}: MIT notice differs')
    data = tomllib.loads(contents['Cargo.toml'].decode())
    normalized_manifest(data, name, version)
    vcs = json.loads(contents['.cargo_vcs_info.json'])
    require(vcs['git']['sha1'] == source_commit and vcs['path_in_vcs'] == f'crates/{name}', 'archive VCS identity differs')
    require(allow_dirty or not vcs['git'].get('dirty', False), 'archive recorded dirty source')
    lock = tomllib.loads(contents['Cargo.lock'].decode())
    for package in lock['package']:
        if package['name'] == name:
            require(package['version'] == version and 'source' not in package, 'packaged root lock identity')
        else:
            require(package.get('source') == REGISTRY and re.fullmatch(r'[a-f0-9]{64}', package.get('checksum', '')),
                    'packaged lock contains a non-registry dependency')
    return {'name': name, 'version': version, 'sha256': digest(archive.read_bytes()),
            'files': {p: digest(b) for p, b in sorted(contents.items())}, 'vcs': vcs,
            'manifest': data, 'lock': lock}


def internal_archive_link(packages):
    worker, orchestration = (packages[name] for name in APIS)
    dependencies = [p for p in orchestration['lock']['package'] if p['name'] == APIS[0]]
    require(len(dependencies) == 1 and dependencies[0]['version'] == worker['version'] and
            dependencies[0]['source'] == REGISTRY and dependencies[0]['checksum'] == worker['sha256'],
            'orchestration archive does not resolve the exact verified worker archive')


def source_snapshot(root, packages):
    paths = {root / 'Cargo.toml', root / 'Cargo.lock', root / 'LICENSE', root / 'rust-toolchain.toml', *packages.values()}
    for name in APIS:
        paths.update(p for p in (root / 'crates' / name).rglob('*') if p.is_file())
    return {str(p.relative_to(root)): digest(p.read_bytes()) for p in sorted(paths)}


def command_text(args, root):
    return subprocess.check_output(args, cwd=root, text=True).strip()


def index_path(name):
    if len(name) < 3:
        return str(len(name)) + '/' + name
    if len(name) == 3:
        return '3/' + name[0] + '/' + name
    return name[:2] + '/' + name[2:4] + '/' + name


def cached_index_entry(content, package):
    # Cargo sparse-cache records are NUL-delimited; a Git index is JSON lines.
    # Only the exact lockfile name/version/checksum entry may seed this fixture.
    for chunk in content.split(b'\0'):
        for line in chunk.splitlines():
            try:
                entry = json.loads(line)
            except (ValueError, UnicodeDecodeError):
                continue
            if (isinstance(entry, dict) and entry.get('name') == package['name'] and
                    entry.get('vers') == package['version'] and entry.get('cksum') == package['checksum']):
                return entry
    return None


def fixture_registry(output, target, packages):
    registry = output / 'fixture-registry'
    shutil.copytree(target / 'package' / 'tmp-registry', registry)
    cargo_home = Path(os.environ.get('CARGO_HOME', Path.home() / '.cargo')).resolve()
    dependencies = {}
    for package in packages.values():
        for dependency in package['lock']['package']:
            if dependency['name'] not in APIS:
                identity = (dependency['name'], dependency['version'])
                require(identity not in dependencies or dependencies[identity] == dependency,
                        'packaged dependency locks disagree')
                dependencies[identity] = dependency
    entries = {}
    for (name, version), dependency in sorted(dependencies.items()):
        filename = f'{name}-{version}.crate'
        cached = [p for p in sorted((cargo_home / 'registry/cache').glob('*/' + filename))
                  if digest(p.read_bytes()) == dependency['checksum']]
        require(cached, f'exact locked archive missing from Cargo cache: {filename}')
        shutil.copyfile(cached[0], registry / filename)
        relative = index_path(name)
        candidates = sorted((cargo_home / 'registry/index').glob('*/.cache/' + relative))
        candidates += sorted((cargo_home / 'registry/index').glob('*/' + relative))
        entry = next((found for p in candidates if (found := cached_index_entry(p.read_bytes(), dependency))), None)
        require(entry is not None, f'exact locked index entry missing from Cargo cache: {filename}')
        entries.setdefault(relative, []).append(entry)
    for relative, records in entries.items():
        path = registry / 'index' / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(''.join(json.dumps(entry, sort_keys=True) + '\n' for entry in records))
    config = output / 'standalone-registry.toml'
    config.write_text('[source.crates-io]\nreplace-with = "qualified-artifacts"\n'
                      '[source.qualified-artifacts]\nlocal-registry = ' + json.dumps(str(registry)) + '\n')
    inventory = {p.name: digest(p.read_bytes()) for p in sorted(registry.glob('*.crate'))}
    for name in APIS:
        require(inventory[f"{name}-{packages[name]['version']}.crate"] == packages[name]['sha256'],
                'fixture API archive differs from qualified bytes')
    return registry, config, inventory


def standalone_checks(output, target, packages, version, toolchain):
    registry, config, registry_archives = fixture_registry(output, target, packages)
    standalone = output / 'standalone'
    standalone.mkdir()
    result = {}
    for name in APIS:
        archive = target / 'package' / f'{name}-{version}.crate'
        contents = archive_contents(archive, f'{name}-{version}')
        directory = standalone / f'{name}-{version}'
        directory.mkdir()
        for relative, content in contents.items():
            path = directory / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)
        common = ['--locked', '--offline', '--all-features', '--manifest-path', str(directory / 'Cargo.toml'),
                  '--config', str(config)]
        metadata = json.loads(command_text(['cargo', '+' + toolchain, 'metadata', '--format-version', '1', *common], standalone))
        require(Path(metadata['workspace_root']).resolve() == directory.resolve(), 'extracted crate inherited a workspace')
        require(all(not Path(p['manifest_path']).resolve().is_relative_to(ROOT) for p in metadata['packages']),
                'standalone resolution reached product workspace source')
        for package in metadata['packages']:
            if package['name'] not in APIS:
                continue
            require(package['version'] == version, 'standalone API version differs')
            if package['name'] != name:
                # Cargo unpacks local-registry archives into its registry/src cache.
                # The source identity and every packaged byte are checked, not
                # an assumption about that cache's implementation-specific path.
                require(package['source'] == REGISTRY, 'internal API dependency is not a registry source')
            source = Path(package['manifest_path']).parent
            expected = packages[package['name']]['files']
            require(all((source / path).is_file() and digest((source / path).read_bytes()) == checksum
                        for path, checksum in expected.items()), 'standalone API source differs from exact archive')
        commands = {}
        for phase, extra in (('test', []), ('doc', ['--no-deps'])):
            phase_common = common if phase == 'test' else [value for value in common if value != '--all-features']
            command = ['cargo', '+' + toolchain, phase, *phase_common, '--target-dir', str(output / 'standalone-target'), '--color', 'never', *extra]
            log = output / f'{name}-archive-{phase}.log'
            environment = dict(os.environ)
            if phase == 'doc':
                environment['RUSTDOCFLAGS'] = '-Dwarnings'
            with log.open('x') as stream:
                subprocess.run(command, cwd=standalone, env=environment, stdout=stream, stderr=subprocess.STDOUT, check=True)
            text = log.read_text()
            if phase == 'test':
                require('Doc-tests ' + name.replace('-', '_') in text, 'packaged doctests not run')
                counts = re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;', text)
                require(counts and all(int(failed) == int(ignored) == 0 for _, failed, ignored in counts),
                        'packaged tests failed or were ignored')
                passed = sum(int(value) for value, _, _ in counts)
            commands[phase] = {'command': command, 'log_sha256': digest(log.read_bytes())}
        require(all((directory / path).read_bytes() == content for path, content in contents.items()),
                'packaged tests or docs changed source bytes')
        result[name] = {'passed': True, 'tests_and_doctests_passed': passed, 'workspace_root': metadata['workspace_root'],
                        'resolved_manifests': {p['name'] + '@' + p['version']: p['manifest_path'] for p in metadata['packages']},
                        'commands': commands, 'source_bytes_unchanged': True, 'documentation_features': 'default',
                        'declared_features': packages[name]['manifest'].get('features', {})}
    return {'packages': result, 'registry_archives': registry_archives,
            'scope': 'Prepublication fixture registry seeded from Cargo temporary-registry archives plus exact locked cached dependencies; not a claim of registry publication. No path patches or manifest edits.'}


def qualify(args):
    root = ROOT
    version, package_paths = publication_policy(root)
    commit = command_text(['git', 'rev-parse', 'HEAD'], root)
    dirty = bool(command_text(['git', 'status', '--porcelain', '--untracked-files=all'], root))
    require(args.allow_dirty or not dirty, 'package qualification requires a clean checkout (or explicit --allow-dirty)')
    snapshot = source_snapshot(root, package_paths)
    output = args.output.resolve()
    require(not output.exists(), 'evidence output must be a new directory')
    require(not output.is_relative_to(root), 'evidence output must be outside the repository for standalone tests')
    output.mkdir(parents=True)
    toolchain = manifest(root / 'rust-toolchain.toml')['toolchain']['channel']
    cargo_version = command_text(['cargo', '+' + toolchain, '--version'], root)
    rustc_version = command_text(['rustc', '+' + toolchain, '--version'], root)
    cargo_match = re.match(r'cargo (\d+)\.(\d+)\.', cargo_version)
    require(cargo_match and tuple(map(int, cargo_match.groups())) >= (1, 98), 'Cargo 1.98 or newer required')
    target = output / 'target'
    command = ['cargo', '+' + toolchain, 'package', '--locked', '--all-features', '--color', 'never',
               '-p', APIS[0], '-p', APIS[1], '--target-dir', str(target)]
    if args.allow_dirty:
        command.append('--allow-dirty')
    log = output / 'cargo-package.log'
    print('Packaging and verifying both API archives, then testing fresh standalone extracts; no publication.', flush=True)
    with log.open('x') as stream:
        subprocess.run(command, cwd=root, stdout=stream, stderr=subprocess.STDOUT, check=True)
    text = log.read_text()
    require(all(re.search(r'Verifying ' + re.escape(name) + r' v' + re.escape(version) + r'\b', text)
                for name in APIS), 'Cargo did not verify both packaged crates')
    packages = {}
    archives = {}
    for name in APIS:
        filename = f'{name}-{version}.crate'
        archive = target / 'package' / filename
        packages[name] = inspect_archive(archive, root, name, version, commit, args.allow_dirty)
        registry_copy = target / 'package' / 'tmp-registry' / filename
        require(archive.read_bytes() == registry_copy.read_bytes(), 'Cargo temporary-registry archive differs')
        archives[filename] = packages[name]['sha256']
    internal_archive_link(packages)
    standalone = standalone_checks(output, target, packages, version, toolchain)
    require(snapshot == source_snapshot(root, package_paths) and commit == command_text(['git', 'rev-parse', 'HEAD'], root),
            'package source changed during qualification')
    require(args.allow_dirty or not command_text(['git', 'status', '--porcelain', '--untracked-files=all'], root),
            'checkout became dirty during qualification')
    dist = output / 'dist'
    dist.mkdir()
    for filename, checksum in archives.items():
        shutil.copyfile(target / 'package' / filename, dist / filename)
        require(digest((dist / filename).read_bytes()) == checksum, 'exported archive changed')
    evidence = {'passed': True, 'version': version, 'source_commit': commit, 'source_dirty': dirty,
                'archives': archives, 'publishable_packages': list(APIS), 'nonpublishable_packages': sorted(set(package_paths) - set(APIS)),
                'toolchain': toolchain, 'cargo': cargo_version, 'rustc': rustc_version, 'command': command,
                'qualification_tool_sha256': digest(Path(__file__).read_bytes()),
                'sdkroot': os.environ.get('SDKROOT'),
                'log_sha256': digest(log.read_bytes()), 'source_sha256': snapshot, 'packages': packages,
                'verification': 'Cargo extracted and built each archive; orchestration resolved the worker from Cargo temporary-registry bytes, checked against its packaged lock checksum.',
                'standalone': standalone, 'publication_performed': False}
    (output / 'evidence.json').write_text(json.dumps(evidence, indent=2) + '\n')
    print(json.dumps({'passed': True, 'version': version, 'archives': archives, 'evidence': str(output / 'evidence.json')}, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path, help='new directory for dist/, log, evidence.json and Cargo verification files')
    parser.add_argument('--allow-dirty', action='store_true', help='development only; released artifacts must use a clean checkout')
    qualify(parser.parse_args())


if __name__ == '__main__':
    main()

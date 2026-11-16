// SPDX-License-Identifier: MIT
import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { readInstalledPackages, resolveInstalledPackages } from './license-inventory.mjs';
import { parentLicensePath } from './license-parent.mjs';

const metadata = (version, extra = {}) => ({ version, license: 'MIT', resolved: `https://registry.example/pkg-${version}.tgz`, integrity: `sha512-${version}`, ...extra });
const manifest = (name, version, extra = {}) => ({ name, version, license: 'MIT', ...extra });

function fixture(t) {
  const root = mkdtempSync(join(tmpdir(), 'ledgence-notices-'));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const put = (path, content) => {
    mkdirSync(join(root, path), { recursive: true });
    writeFileSync(join(root, path, 'package.json'), JSON.stringify(content));
  };
  return { root, put };
}

test('npm and pnpm hoists preserve both exact identities and native ownership', () => {
  const older = metadata('0.10.5', { optionalDependencies: { '@bruits/satteri-darwin-arm64': '0.10.5' } });
  const newer = metadata('0.11.7', { optionalDependencies: { '@bruits/satteri-darwin-arm64': '0.11.7' } });
  const locked = {
    'node_modules/satteri': older,
    'node_modules/@bruits/satteri-darwin-arm64': metadata('0.10.5', { optional: true }),
    'node_modules/@astrojs/markdown-satteri/node_modules/satteri': newer,
    'node_modules/@astrojs/markdown-satteri/node_modules/@bruits/satteri-darwin-arm64': metadata('0.11.7', { optional: true }),
  };
  const npm = Object.fromEntries(Object.entries(locked).map(([path, entry]) => [path, { ...entry, name: path.split('node_modules/').at(-1) }]));
  const pnpm = {
    'node_modules/satteri': { ...newer, name: 'satteri' },
    'node_modules/@bruits/satteri-darwin-arm64': manifest('@bruits/satteri-darwin-arm64', '0.11.7'),
    'node_modules/@astrojs/starlight/node_modules/satteri': { ...older, name: 'satteri' },
    'node_modules/@astrojs/starlight/node_modules/@bruits/satteri-darwin-arm64': manifest('@bruits/satteri-darwin-arm64', '0.10.5'),
  };
  for (const installed of [npm, pnpm]) {
    const result = resolveInstalledPackages(locked, installed);
    for (const [path, entry] of Object.entries(locked)) {
      const actual = result.installedPaths.get(path);
      assert.equal(installed[actual].version, entry.version);
      if (actual.includes('@bruits/')) {
        const parent = parentLicensePath(actual, installed[actual].name, entry, result.actualPackages, (candidate) => installed[candidate]);
        assert.equal(installed[parent].version, entry.version);
      }
    }
  }
});

test('duplicate identical locked identities may share one installed copy', () => {
  const locked = { 'node_modules/pkg': metadata('1'), 'node_modules/other/node_modules/pkg': metadata('1') };
  const result = resolveInstalledPackages(locked, { 'node_modules/pkg': manifest('pkg', '1') });
  assert.equal(result.installedPaths.get('node_modules/other/node_modules/pkg'), 'node_modules/pkg');
});

test('missing optional identity stays explicit, while a required identity cannot use another version', () => {
  const optional = { 'node_modules/pkg': metadata('1', { optional: true }) };
  assert.equal(resolveInstalledPackages(optional, {}).installedPaths.get('node_modules/pkg'), null);
  assert.throws(() => resolveInstalledPackages({ ...optional, 'node_modules/owner/node_modules/pkg': metadata('1') }, {}), /Required locked/);
  assert.throws(() => resolveInstalledPackages({ 'node_modules/pkg': metadata('1'), 'node_modules/other/node_modules/pkg': metadata('2', { optional: true }) }, { 'node_modules/other/node_modules/pkg': manifest('pkg', '2') }), /Required locked/);
});

test('different archives for one name/version/license are ambiguous even when absent', () => {
  for (const changed of [{ integrity: 'sha512-other' }, { resolved: 'https://elsewhere.example/pkg.tgz' }]) {
    assert.throws(() => resolveInstalledPackages({
      'node_modules/pkg': metadata('1', { optional: true }),
      'node_modules/other/node_modules/pkg': metadata('1', { optional: true, ...changed }),
    }, {}), /Ambiguous locked archive/);
  }
});

test('unlocked packages and changed installed names, versions or licenses fail', () => {
  const locked = { 'node_modules/pkg': metadata('1') };
  for (const changed of [{ name: 'other' }, { version: '2' }, { license: 'ISC' }]) {
    assert.throws(() => resolveInstalledPackages(locked, { 'node_modules/pkg': manifest('pkg', '1', changed) }), /location or name|not a locked/);
  }
  assert.throws(() => resolveInstalledPackages(locked, { 'node_modules/pkg': manifest('pkg', '1'), 'node_modules/extra': manifest('extra', '1') }), /not a locked/);
});

test('unsafe locations, linked lock entries and missing archive provenance fail', () => {
  for (const path of ['node_modules/../pkg', '../node_modules/pkg', 'node_modules/./pkg']) {
    assert.throws(() => resolveInstalledPackages({ [path]: metadata('1') }, {}), /Unsupported locked/);
  }
  assert.throws(() => resolveInstalledPackages({ 'node_modules/pkg': metadata('1', { link: true }) }, {}), /Unsupported locked/);
  assert.throws(() => resolveInstalledPackages({ 'node_modules/pkg': metadata('1', { integrity: undefined }) }, {}), /Missing locked archive/);
});

test('discovery traverses only actual hoisted dependencies, including scoped nested packages', (t) => {
  const { root, put } = fixture(t);
  put('node_modules/@scope/owner', manifest('@scope/owner', '1'));
  put('node_modules/@scope/owner/node_modules/pkg', manifest('pkg', '2'));
  put('node_modules/.ignored/old', manifest('old', '1'));
  assert.deepEqual(Object.keys(readInstalledPackages(root)), ['node_modules/@scope/owner', 'node_modules/@scope/owner/node_modules/pkg']);
});

test('discovery rejects linked package, scope and node_modules directories', (t) => {
  for (const target of ['node_modules/pkg', 'node_modules/@scope', 'node_modules']) {
    const { root, put } = fixture(t);
    put('external', manifest('pkg', '1'));
    mkdirSync(join(root, 'node_modules'), { recursive: true });
    if (target === 'node_modules') rmSync(join(root, target), { recursive: true });
    symlinkSync(join(root, 'external'), join(root, target), 'dir');
    assert.throws(() => readInstalledPackages(root), /Linked or non-directory/);
  }
});

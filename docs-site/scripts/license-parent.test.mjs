// SPDX-License-Identifier: MIT
import assert from 'node:assert/strict';
import test from 'node:test';
import { parentLicensePath } from './license-parent.mjs';

const name = '@bruits/satteri-darwin-arm64';
const metadata = { version: '0.11.7', license: 'MIT' };
const owner = { ...metadata, optionalDependencies: { [name]: metadata.version } };
const manifest = { name: 'satteri', ...owner };
const root = 'node_modules/satteri';
const nested = 'node_modules/astro/node_modules/satteri';
const native = `node_modules/astro/node_modules/${name}`;
const resolve = (path, locked, installed) => parentLicensePath(
  path, name, metadata, locked, (candidate) => installed[candidate],
);

test('uses a matching root native parent', () => {
  assert.equal(resolve(`node_modules/${name}`, { [root]: owner }, { [root]: manifest }), root);
});

test('uses the nested native parent while an older root parent exists', () => {
  const old = { ...owner, version: '0.10.5' };
  assert.equal(resolve(native, { [root]: old, [nested]: owner }, {
    [root]: { ...manifest, version: old.version }, [nested]: manifest,
  }), nested);
});

test('resolves a scoped parent at the nearest node_modules level', () => {
  const binary = '@astrojs/compiler-binding-darwin-arm64';
  const path = 'node_modules/astro/node_modules/@astrojs/compiler-binding';
  const entry = { version: '0.5.3', license: 'MIT', optionalDependencies: { [binary]: '0.5.3' } };
  assert.equal(parentLicensePath(`node_modules/astro/node_modules/${binary}`, binary,
    entry, { [path]: entry }, (candidate) => candidate === path
      ? { name: '@astrojs/compiler-binding', ...entry } : undefined), path);
});

test('a nearer mismatching parent cannot fall through to a matching root', async (t) => {
  for (const [label, change] of Object.entries({
    version: { version: '0.10.5' }, license: { license: 'ISC' },
    relation: { optionalDependencies: {} },
    selectedVersion: { optionalDependencies: { [name]: '0.10.5' } },
  })) {
    await t.test(label, () => {
      const mismatched = { ...owner, ...change };
      assert.throws(() => resolve(native, { [root]: owner, [nested]: mismatched }, {
        [root]: manifest, [nested]: { name: 'satteri', ...mismatched },
      }), /does not match locked and installed ownership/);
    });
  }
});

test('the installed parent must match the locked name, version, license and relationship', async (t) => {
  for (const [label, change] of Object.entries({
    name: { name: 'unrelated' }, version: { version: '0.10.5' },
    license: { license: 'ISC' }, relation: { optionalDependencies: {} },
  })) {
    await t.test(label, () => assert.throws(() => resolve(native,
      { [root]: owner, [nested]: owner },
      { [root]: manifest, [nested]: { ...manifest, ...change } }), /does not match/));
  }
});

test('a missing locked or installed nearest parent fails closed', () => {
  assert.throws(() => resolve(native, { [root]: owner, [nested]: owner },
    { [root]: manifest }), /does not match/);
  assert.throws(() => resolve(native, { [root]: owner },
    { [root]: manifest, [nested]: manifest }), /does not match/);
});

test('no parent and unknown native families produce no borrowed notices', () => {
  assert.equal(resolve(native, {}, {}), undefined);
  assert.equal(parentLicensePath('node_modules/unknown', 'unknown', metadata, {},
    () => { throw new Error('must not inspect unrelated packages'); }), undefined);
});

test('Pagefind UI uses its separately reviewed notice, not the native binary parent', () => {
  assert.equal(parentLicensePath('node_modules/@pagefind/default-ui', '@pagefind/default-ui',
    { version: '1.5.2', license: 'MIT' }, {}, () => {
      throw new Error('UI is not a native binary package');
    }), undefined);
});

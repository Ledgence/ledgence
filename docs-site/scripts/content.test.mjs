// SPDX-License-Identifier: MIT
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { pageMetadata } from './check-content.mjs';
import { checkStaticArtifact, resolveLocalLink } from './check-build.mjs';

test('published content requires useful metadata and rejects unfinished drafts', () => {
  assert.throws(() => pageMetadata('## Empty', 'draft.md'), /frontmatter/);
  assert.throws(() => pageMetadata('---\ntitle: Draft\n---\nText', 'draft.md'), /description/);
  assert.throws(() => pageMetadata('---\ntitle: Draft\ndescription: Test\n---\nTODO', 'draft.md'), /unfinished/);
  assert.throws(() => pageMetadata('---\ntitle: Draft\ndescription: Test\n---\n# Duplicate', 'draft.md'), /h1/);
});

test('clean documentation URLs and anchored links resolve to real static pages', () => {
  const directory = mkdtempSync(join(tmpdir(), 'ledgence-doc-links-'));
  try {
    mkdirSync(join(directory, 'tutorials'));
    writeFileSync(join(directory, 'index.html'), '<h1>Docs</h1>');
    writeFileSync(join(directory, 'tutorials/start.html'), '<h2 id="result">Result</h2>');
    assert.equal(resolveLocalLink(directory, 'index.html', '/tutorials/start#result'), join(directory, 'tutorials/start.html'));
    assert.equal(resolveLocalLink(directory, 'tutorials/start.html', '/'), join(directory, 'index.html'));
    assert.equal(resolveLocalLink(directory, 'index.html', 'https://ledgence.com/'), null);
    assert.throws(() => resolveLocalLink(directory, 'index.html', '/missing'), /broken link/);
    assert.throws(() => resolveLocalLink(directory, 'index.html', '/tutorials/start#typo'), /missing anchor/);
  } finally { rmSync(directory, { recursive: true, force: true }); }
});


test('static output rejects native tools and unreviewed WebAssembly', () => {
  const directory = mkdtempSync(join(tmpdir(), 'ledgence-doc-artifacts-'));
  try {
    const file = join(directory, 'tool');
    writeFileSync(file, Buffer.from('7f454c46', 'hex'));
    assert.throws(() => checkStaticArtifact(directory, file), /Native executable/);
    writeFileSync(file, Buffer.from('0061736d', 'hex'));
    assert.throws(() => checkStaticArtifact(directory, file), /Unreviewed WebAssembly/);
    mkdirSync(join(directory, 'pagefind'));
    const search = join(directory, 'pagefind/search.wasm');
    writeFileSync(search, Buffer.from('0061736d', 'hex'));
    assert.doesNotThrow(() => checkStaticArtifact(directory, search));
    assert.throws(() => checkStaticArtifact(directory, join(directory, 'binding.node')), /native artifact/);
  } finally { rmSync(directory, { recursive: true, force: true }); }
});

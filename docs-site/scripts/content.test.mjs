// SPDX-License-Identifier: MIT
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { pageMetadata, writeMarkdownExports } from './check-content.mjs';
import { checkMarkdownExports, checkStaticArtifact, resolveLocalLink } from './check-build.mjs';

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


test('Markdown exports survive index generation and retain all four sections', () => {
  const directory = mkdtempSync(join(tmpdir(), 'ledgence-doc-markdown-'));
  try {
    const pages = ['tutorials', 'how-to', 'reference', 'concepts'].map(section => ({
      path: `${section}/example.md`, slug: `${section}/example`, title: `${section} example`,
      description: 'An exported document.', body: '[Workflow context](/reference/workflow-context)\n',
    }));
    writeMarkdownExports(directory, pages, 'fixture-revision');
    assert.equal(checkMarkdownExports(directory), 4);
    for (const page of pages) {
      const exported = readFileSync(join(directory, 'markdown', page.path), 'utf8');
      assert.ok(exported.startsWith(`# ${page.title}\n`));
      assert.ok(exported.endsWith(page.body));
    }
    rmSync(join(directory, 'markdown/reference/example.md'));
    assert.throws(() => checkMarkdownExports(directory), /broken link/);
  } finally { rmSync(directory, { recursive: true, force: true }); }
});

test('Markdown index advertises only exported files and removes obsolete exports', () => {
  const directory = mkdtempSync(join(tmpdir(), 'ledgence-doc-markdown-'));
  try {
    const page = { path: 'reference/old.md', slug: 'reference/old', title: 'Old', description: 'Description.', body: 'Content.' };
    writeMarkdownExports(directory, [page], 'fixture-revision');
    writeMarkdownExports(directory, [
      { ...page, path: 'reference/new.md', slug: 'reference/new', title: 'New' },
      { ...page, path: 'reference/component.mdx', slug: 'reference/component', title: 'Component' },
    ], 'fixture-revision');
    assert.equal(checkMarkdownExports(directory), 1);
    assert.doesNotMatch(readFileSync(join(directory, 'llms.txt'), 'utf8'), /component\.mdx|old\.md/);
    assert.throws(() => readFileSync(join(directory, 'markdown/reference/old.md')), /ENOENT/);
  } finally { rmSync(directory, { recursive: true, force: true }); }
});

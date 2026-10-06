// SPDX-License-Identifier: MIT
import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { pageMetadata, releaseRevision, writeMarkdownExports } from './check-content.mjs';
import { checkFeatureGuides, checkMarkdownExports, checkStaticArtifact, resolveLocalLink } from './check-build.mjs';

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


test('release provenance omits unknown product SHA instead of substituting documentation HEAD', () => {
  const metadata = { sourceRef: 'v0.2.0' };
  assert.equal(releaseRevision(metadata, () => { throw new Error('tag not created'); }), undefined);
  assert.equal(releaseRevision(metadata, ref => {
    assert.equal(ref, 'v0.2.0');
    return 'verified-tag-commit';
  }), 'verified-tag-commit');
  assert.equal(releaseRevision({ ...metadata, sourceRevision: 'explicit-commit' }, () => {
    throw new Error('explicit provenance must not require a local tag');
  }), 'explicit-commit');
  const directory = mkdtempSync(join(tmpdir(), 'ledgence-doc-provenance-'));
  try {
    writeMarkdownExports(directory, [], 'documentation-checkout', null);
    const index = readFileSync(join(directory, 'llms.txt'), 'utf8');
    const currentRelease = JSON.parse(readFileSync(new URL('../release.json', import.meta.url), 'utf8'));
    assert.ok(index.includes(`Product source: ${currentRelease.sourceRef}. Documentation checkout: documentation-checkout.`));
    assert.match(index, /x86_64-unknown-linux-gnu, aarch64-apple-darwin/);
    assert.match(index, /Console is included/);
    const features = JSON.parse(readFileSync(new URL('../source-features.json', import.meta.url), 'utf8'));
    for (const feature of Object.values(features).filter(value => value.availability === 'development')) {
      assert.ok(index.includes(`Development-only: ${feature.title} (source ${feature.sourceRef}).`));
      assert.ok(index.includes(`https://docs.ledgence.com${feature.guide}`));
    }
    assert.doesNotMatch(index, /undefined|not included|source checkout;|v0\.1\.1/);
    writeMarkdownExports(directory, [], 'documentation-checkout', 'verified-product-commit');
    const resolved = readFileSync(join(directory, 'llms.txt'), 'utf8');
    assert.ok(resolved.includes(`Product source: ${currentRelease.sourceRef} (verified-product-commit). Documentation checkout: documentation-checkout.`));
  } finally { rmSync(directory, { recursive: true, force: true }); }
});


test('feature provenance validates guide pages and anchors before publication', () => {
  const directory = mkdtempSync(join(tmpdir(), 'ledgence-feature-guides-'));
  try {
    mkdirSync(join(directory, 'how-to'));
    writeFileSync(join(directory, 'how-to/install.html'), '<h2 id="one-line">Install</h2><span id="old-name"></span>');
    assert.doesNotThrow(() => checkFeatureGuides(directory, {
      installer: { guide: '/how-to/install#one-line' },
      legacy: { guide: '/how-to/install#old-name' },
    }));
    assert.throws(() => checkFeatureGuides(directory, { installer: { guide: '/how-to/install#missing' } }), /missing anchor/);
    assert.throws(() => checkFeatureGuides(directory, { installer: { guide: '/missing' } }), /broken link/);
    assert.throws(() => checkFeatureGuides(directory, { installer: { guide: 'https://example.com/install' } }), /local documentation path/);
    assert.throws(() => checkFeatureGuides(directory, { installer: { guide: '//example.com/install' } }), /local documentation path/);
    writeFileSync(join(directory, 'download.txt'), 'not a guide');
    assert.throws(() => checkFeatureGuides(directory, { installer: { guide: '/download.txt' } }), /documentation page/);
  } finally { rmSync(directory, { recursive: true, force: true }); }
});

test('Markdown index distinguishes released and development features with their own provenance', () => {
  const directory = mkdtempSync(join(tmpdir(), 'ledgence-feature-index-'));
  try {
    const features = {
      console: { availability: 'release', sourceRef: 'v1.2.0', verifiedOn: '2026-10-06', guide: '/tutorials/console' },
      installer: { title: 'CLI installation', availability: 'release', sourceRef: 'v1.2.0', verifiedOn: '2026-10-06', guide: '/how-to/install#one-line' },
      preview: { title: 'Preview integration', availability: 'development', sourceRef: 'develop', guide: '/how-to/preview' },
    };
    writeMarkdownExports(directory, [], 'docs-checkout', null, features);
    const index = readFileSync(join(directory, 'llms.txt'), 'utf8');
    assert.ok(index.includes('Released: CLI installation (source v1.2.0; verified 2026-10-06). See https://docs.ledgence.com/how-to/install#one-line.'));
    assert.ok(index.includes('Development-only: Preview integration (source develop).'));
    assert.ok(index.includes('https://docs.ledgence.com/how-to/preview'));
    assert.ok(index.includes('Console is included in source v1.2.0'));
    assert.doesNotMatch(index, /Development-only: CLI installation|Released: Preview integration|undefined/);
  } finally { rmSync(directory, { recursive: true, force: true }); }
});

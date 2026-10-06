// SPDX-License-Identifier: MIT
import { execFileSync } from 'node:child_process';
import { mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const source = join(root, 'src/content/docs');
const release = JSON.parse(readFileSync(join(root, 'release.json'), 'utf8'));
const sourceFeatures = JSON.parse(readFileSync(join(root, 'source-features.json'), 'utf8'));

export function releaseRevision(metadata = release, resolveTag = ref => execFileSync(
  'git', ['rev-parse', '--verify', `refs/tags/${ref}^{commit}`],
  { cwd: root, encoding: 'utf8', stdio: ['ignore', 'pipe', 'ignore'] },
).trim()) {
  if (metadata.sourceRevision) return metadata.sourceRevision;
  try { return resolveTag(metadata.sourceRef); }
  catch { return undefined; }
}

export function contentFiles(directory) {
  return readdirSync(directory, { withFileTypes: true }).flatMap(entry => {
    const path = join(directory, entry.name);
    if (entry.isSymbolicLink()) throw new Error(`Documentation cannot contain symlinks: ${path}`);
    return entry.isDirectory() ? contentFiles(path) : /\.mdx?$/.test(path) ? [path] : [];
  }).sort();
}

export function pageMetadata(text, path) {
  const match = text.match(/^---\n([\s\S]+?)\n---\n/);
  if (!match) throw new Error(`${path}: missing frontmatter`);
  const title = match[1].match(/^title:\s*(.+)$/m)?.[1];
  const description = match[1].match(/^description:\s*(.+)$/m)?.[1];
  if (!title || !description) throw new Error(`${path}: title and description are required`);
  const body = text.slice(match[0].length);
  if (/^# /m.test(body)) throw new Error(`${path}: the title already provides h1`);
  if (/\b(?:TODO|TBD|COMING SOON)\b/.test(body)) throw new Error(`${path}: unfinished content`);
  return { title: title.replace(/^['"]|['"]$/g, ''), description: description.replace(/^['"]|['"]$/g, ''), body };
}

export function writeMarkdownExports(directory, pages, revision, productRevision = releaseRevision(), features = sourceFeatures) {
  mkdirSync(directory, { recursive: true });
  rmSync(join(directory, 'markdown'), { recursive: true, force: true });
  const exported = pages.filter(page => page.path.endsWith('.md'));
  for (const page of exported) {
    const target = join(directory, 'markdown', page.path);
    mkdirSync(dirname(target), { recursive: true });
    writeFileSync(target, `# ${page.title}\n\n${page.description}\n\n${page.body}`);
  }
  const productIdentity = productRevision ? `${release.sourceRef} (${productRevision})` : release.sourceRef;
  const list = ['# Ledgence documentation', '', `> Documentation for the Ledgence ${release.series} release series: open-source agent and workflow orchestration.`, '', `Product source: ${productIdentity}. Documentation checkout: ${revision}.`, '', `Native bundles: ${release.nativeVersion} (${release.nativeTargets.join(', ')}). Python client: ${release.clientVersion}. Rust API crates: ${release.rustApiVersion}.`, `Console is included in source ${features.console.sourceRef} and Console-enabled native bundles; qualification date ${features.console.verifiedOn}. See https://docs.ledgence.com${features.console.guide}.`, '', 'Public APIs may change before 1.0. See the release reference for installation choices, host-library requirements, and supported scope.', ''];
  for (const [key, feature] of Object.entries(features)) {
    if (feature.availability === 'development') {
      list.push(`Development-only: ${feature.title} (source ${feature.sourceRef}). Requires a matching development build or a future qualified release. See https://docs.ledgence.com${feature.guide}.`, '');
    } else if (key !== 'console' && feature.availability === 'release') {
      list.push(`Released: ${feature.title} (source ${feature.sourceRef}; verified ${feature.verifiedOn}). See https://docs.ledgence.com${feature.guide}.`, '');
    }
  }
  for (const section of ['tutorials', 'how-to', 'reference', 'concepts']) {
    list.push(`## ${section === 'how-to' ? 'How-to guides' : section[0].toUpperCase() + section.slice(1)}`, '');
    for (const page of exported.filter(page => page.slug.startsWith(`${section}/`))) {
      list.push(`- [${page.title}](https://docs.ledgence.com/markdown/${page.path}): ${page.description}`);
    }
    list.push('');
  }
  writeFileSync(join(directory, 'llms.txt'), list.join('\n'));
}

export function prepareContent() {
  const files = contentFiles(source);
  const pages = files.map(file => {
    const path = file.slice(source.length + 1).replaceAll('\\', '/');
    return { path, slug: path.replace(/\.mdx?$/, '').replace(/^index$/, ''), ...pageMetadata(readFileSync(file, 'utf8'), path) };
  });
  for (const section of ['tutorials', 'how-to', 'reference', 'concepts']) {
    if (!pages.some(page => page.slug.startsWith(`${section}/`))) throw new Error(`Missing ${section} content`);
  }
  const revision = execFileSync('git', ['rev-parse', 'HEAD'], { cwd: root, encoding: 'utf8' }).trim();
  const productRevision = releaseRevision();
  const sourceInfo = {
    ...(productRevision ? { revision: productRevision } : {}),
    ref: release.sourceRef, documentationRevision: revision,
    channel: 'release', sourceFeatures, series: release.series, nativeVersion: release.nativeVersion,
    clientVersion: release.clientVersion, rustApiVersion: release.rustApiVersion,
    nativeTargets: release.nativeTargets, verifiedOn: release.verifiedOn,
    repository: 'https://github.com/Ledgence/ledgence',
  };
  mkdirSync(join(root, 'src/generated'), { recursive: true });
  mkdirSync(join(root, 'public'), { recursive: true });
  writeFileSync(join(root, 'src/generated/source.json'), JSON.stringify(sourceInfo, null, 2) + '\n');
  writeFileSync(join(root, 'public/source.json'), JSON.stringify(sourceInfo, null, 2) + '\n');
  writeMarkdownExports(join(root, 'public'), pages, revision, productRevision);
  console.log(`Validated ${pages.length} documentation pages; generated Markdown and source metadata.`);
  return pages;
}
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) prepareContent();

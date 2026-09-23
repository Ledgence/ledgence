// SPDX-License-Identifier: MIT
import { execFileSync } from 'node:child_process';
import { mkdirSync, readdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const source = join(root, 'src/content/docs');
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

export function writeMarkdownExports(directory, pages, revision) {
  mkdirSync(directory, { recursive: true });
  rmSync(join(directory, 'markdown'), { recursive: true, force: true });
  const exported = pages.filter(page => page.path.endsWith('.md'));
  for (const page of exported) {
    const target = join(directory, 'markdown', page.path);
    mkdirSync(dirname(target), { recursive: true });
    writeFileSync(target, `# ${page.title}\n\n${page.description}\n\n${page.body}`);
  }
  const list = ['# Ledgence documentation', '', '> Development documentation for Ledgence, an open-source agent and workflow orchestration platform.', '', `Product source revision: ${revision}. Public APIs are evolving.`, ''];
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
  const sourceInfo = { revision, channel: 'development', repository: 'https://github.com/Ledgence/ledgence' };
  mkdirSync(join(root, 'src/generated'), { recursive: true });
  mkdirSync(join(root, 'public'), { recursive: true });
  writeFileSync(join(root, 'src/generated/source.json'), JSON.stringify(sourceInfo, null, 2) + '\n');
  writeFileSync(join(root, 'public/source.json'), JSON.stringify(sourceInfo, null, 2) + '\n');
  writeMarkdownExports(join(root, 'public'), pages, revision);
  console.log(`Validated ${pages.length} documentation pages; generated Markdown and source metadata.`);
  return pages;
}
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) prepareContent();

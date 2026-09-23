// SPDX-License-Identifier: MIT
import { existsSync, readdirSync, readFileSync, statSync } from 'node:fs';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
export function filesUnder(directory) {
  return readdirSync(directory, { withFileTypes: true }).flatMap(entry => {
    const path = join(directory, entry.name);
    if (entry.isSymbolicLink()) throw new Error(`Build contains symlink: ${path}`);
    return entry.isDirectory() ? filesUnder(path) : [path];
  });
}
export function resolveLocalLink(directory, from, href) {
  const url = new URL(href.replaceAll('&amp;', '&'), `https://docs.ledgence.com/${from}`);
  if (url.origin !== 'https://docs.ledgence.com') return null;
  const pathname = decodeURIComponent(url.pathname);
  const raw = resolve(directory, '.' + pathname);
  if (raw !== resolve(directory) && !raw.startsWith(resolve(directory) + '/')) throw new Error(`Escaping link: ${href}`);
  const candidates = pathname === '/' ? [join(directory, 'index.html')] : [raw, raw + '.html', join(raw, 'index.html')];
  const target = candidates.find(candidate => existsSync(candidate) && statSync(candidate).isFile());
  if (!target) throw new Error(`${from}: broken link ${href}`);
  if (url.hash && target.endsWith('.html')) {
    const fragment = decodeURIComponent(url.hash.slice(1));
    const ids = [...readFileSync(target, 'utf8').matchAll(/\bid=["']([^"']+)["']/g)].map(match => match[1]);
    if (!ids.includes(fragment)) throw new Error(`${from}: missing anchor ${href}`);
  }
  return target;
}

export function checkStaticArtifact(directory, file) {
  const path = relative(directory, file).replaceAll('\\', '/');
  if (/(?:^|\/)(?:node_modules|\.git)(?:\/|$)/.test(path) || /\.(?:node|so(?:\.\d+)*|dylib|dll|exe|a|o)$/i.test(path)) {
    throw new Error(`Build tool or native artifact cannot be published: ${path}`);
  }
  const magic = readFileSync(file).subarray(0, 4).toString('hex');
  if (['7f454c46', 'feedface', 'feedfacf', 'cefaedfe', 'cffaedfe', 'cafebabe', 'bebafeca'].includes(magic) || magic.startsWith('4d5a')) {
    throw new Error(`Native executable cannot be published: ${path}`);
  }
  if (magic === '0061736d' && !(path.startsWith('pagefind/') && path.endsWith('.wasm'))) {
    throw new Error(`Unreviewed WebAssembly artifact: ${path}`);
  }
}

export function checkMarkdownExports(directory) {
  const index = readFileSync(join(directory, 'llms.txt'), 'utf8');
  const links = [...index.matchAll(/^- \[[^\]]+\]\(([^)]+)\):/gm)].map(match => match[1]);
  if (links.length === 0) throw new Error('Markdown index has no exported pages');
  for (const href of links) {
    const url = new URL(href);
    if (url.origin !== 'https://docs.ledgence.com' || !url.pathname.startsWith('/markdown/') || !url.pathname.endsWith('.md')) {
      throw new Error(`Unexpected Markdown index target: ${href}`);
    }
    resolveLocalLink(directory, 'llms.txt', href);
  }
  return links.length;
}

export function checkBuild(directory) {
  for (const file of ['index.html', '404.html', 'llms.txt', 'source.json', 'notices/LEDGENCE-LICENSE.txt', 'notices/manifest.json']) {
    if (!existsSync(join(directory, file))) throw new Error(`Missing build output: ${file}`);
  }
  const markdownPages = checkMarkdownExports(directory);
  const files = filesUnder(directory);
  for (const file of files) checkStaticArtifact(directory, file);
  const htmlFiles = files.filter(file => file.endsWith('.html'));
  for (const file of htmlFiles) {
    const html = readFileSync(file, 'utf8');
    const path = file.slice(directory.length + 1);
    for (const [, href] of html.matchAll(/\b(?:href|src)=["']([^"']+)["']/g)) {
      if (/^(?:mailto:|tel:|data:|javascript:)/.test(href)) continue;
      resolveLocalLink(directory, path, href);
    }
    if (!path.startsWith('notices/') && path !== '404.html') {
      if (!html.includes('rel="canonical"')) throw new Error(`${path}: missing canonical URL`);
      if (!html.includes('<h1')) throw new Error(`${path}: missing primary heading`);
    }
  }
  if (!files.some(path => path.includes('/pagefind/') && path.endsWith('.js'))) throw new Error('Missing static search index');
  console.log(`Checked ${htmlFiles.length} HTML pages, ${markdownPages} indexed Markdown exports, internal links, anchors, assets, notices, and search.`);
}
if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) checkBuild(join(root, 'dist'));

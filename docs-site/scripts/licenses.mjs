// SPDX-License-Identifier: MIT
// Keep legal files with the static release; this script has no npm dependencies.
import { createHash } from 'node:crypto';
import { existsSync, lstatSync, mkdirSync, readFileSync, readdirSync, renameSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, join, relative, resolve, sep } from 'node:path';

const root = process.cwd();
const checkOnly = process.argv.includes('--check');
const upstreamNotices = JSON.parse(readFileSync(join(root, 'scripts/legal-overrides/provenance.json'), 'utf8'));
if (process.argv.slice(2).some((argument) => argument !== '--check')) {
  throw new Error('Usage: node scripts/licenses.mjs [--check] (from the project root)');
}

// Deliberately exact: new SPDX expressions require review rather than guessing.
// OFL applies to the unmodified Inter font, not to Ledgence application code.
const allowed = new Set(['MIT', 'ISC', 'Apache-2.0', 'BSD-2-Clause', 'BSD-3-Clause', 'CC0-1.0', 'OFL-1.1', 'BlueOak-1.0.0']);

// These exact versions were reviewed for documentation tooling only. They are
// not imported by the browser application, linked to Ledgence, or redistributed
// as binaries. This is deliberately NOT a general MPL/LGPL/Python/0BSD allowance.
// Preserve AND expressions: every license applies; none is an alternative.
const reviewedToolLicenses = new Map(Object.entries({
  "@img/sharp-libvips-darwin-arm64@1.3.3": "LGPL-3.0-or-later",
  "@img/sharp-libvips-darwin-x64@1.3.3": "LGPL-3.0-or-later",
  "@img/sharp-libvips-linux-arm@1.3.3": "LGPL-3.0-or-later",
  "@img/sharp-libvips-linux-arm64@1.3.3": "LGPL-3.0-or-later",
  "@img/sharp-libvips-linux-ppc64@1.3.3": "LGPL-3.0-or-later",
  "@img/sharp-libvips-linux-riscv64@1.3.3": "LGPL-3.0-or-later",
  "@img/sharp-libvips-linux-s390x@1.3.3": "LGPL-3.0-or-later",
  "@img/sharp-libvips-linux-x64@1.3.3": "LGPL-3.0-or-later",
  "@img/sharp-libvips-linuxmusl-arm64@1.3.3": "LGPL-3.0-or-later",
  "@img/sharp-libvips-linuxmusl-x64@1.3.3": "LGPL-3.0-or-later",
  "@img/sharp-wasm32@0.35.4": "Apache-2.0 AND LGPL-3.0-or-later AND MIT",
  "@img/sharp-win32-arm64@0.35.4": "Apache-2.0 AND LGPL-3.0-or-later",
  "@img/sharp-win32-ia32@0.35.4": "Apache-2.0 AND LGPL-3.0-or-later",
  "@img/sharp-win32-x64@0.35.4": "Apache-2.0 AND LGPL-3.0-or-later",
  "argparse@2.0.1": "Python-2.0",
  "lightningcss@1.33.0": "MPL-2.0",
  "lightningcss-android-arm64@1.33.0": "MPL-2.0",
  "lightningcss-darwin-arm64@1.33.0": "MPL-2.0",
  "lightningcss-darwin-x64@1.33.0": "MPL-2.0",
  "lightningcss-freebsd-x64@1.33.0": "MPL-2.0",
  "lightningcss-linux-arm-gnueabihf@1.33.0": "MPL-2.0",
  "lightningcss-linux-arm64-gnu@1.33.0": "MPL-2.0",
  "lightningcss-linux-arm64-musl@1.33.0": "MPL-2.0",
  "lightningcss-linux-x64-gnu@1.33.0": "MPL-2.0",
  "lightningcss-linux-x64-musl@1.33.0": "MPL-2.0",
  "lightningcss-win32-arm64-msvc@1.33.0": "MPL-2.0",
  "lightningcss-win32-x64-msvc@1.33.0": "MPL-2.0",
  "tslib@2.8.1": "0BSD"
}));

// These archives omit standalone full license texts. Preserve their original
// declarations and attribution, without manufacturing missing legal text. This
// exception is limited to unmodified tools that are absent from the static site.
const declarationOnlyTools = new Map([
  ['am-i-vibing@0.4.0', 'MIT'],
  ['process-ancestry@0.1.0', 'MIT'],
  ['piccolore@0.1.3', 'ISC'],
  ['boolbase@1.0.0', 'ISC'],
]);

const project = JSON.parse(readFileSync(join(root, 'package.json'), 'utf8'));
const lock = JSON.parse(readFileSync(join(root, 'package-lock.json'), 'utf8'));
if (project.license !== 'MIT' || lock.packages?.['']?.license !== 'MIT') {
  throw new Error('Ledgence-owned code must retain its MIT license.');
}
if (lock.lockfileVersion !== 3) throw new Error('Review the license collector before changing lockfile format.');
const ownLicense = readFileSync(join(root, 'LICENSE'));
const digest = (value) => createHash('sha256').update(value).digest('hex');
const errors = [];
const packages = [];
const files = new Map([['LEDGENCE-LICENSE.txt', ownLicense]]);

function assertLicense(license, label) {
  if (!allowed.has(license) && reviewedToolLicenses.get(label) !== license) errors.push(`${label}: unreviewed or disallowed license ${JSON.stringify(license)}`);
}

function safePackagePath(path) {
  return /^node_modules\/(?:@[a-z0-9._-]+\/)?[a-z0-9._-]+(?:\/node_modules\/(?:@[a-z0-9._-]+\/)?[a-z0-9._-]+)*$/i.test(path)
    && path.split('/').every((part) => part !== '.' && part !== '..');
}

function legalFiles(directory) {
  const result = [];
  function walk(current, inLegalDirectory = false) {
    for (const entry of readdirSync(current, { withFileTypes: true }).sort((a, b) => a.name.localeCompare(b.name))) {
      if (entry.name === 'node_modules' || entry.name === '.git') continue;
      const path = join(current, entry.name);
      const legalName = /^(?:licen[cs]e|notice|copying|copyright|third[._-]party[._-](?:licen[cs]es?|notices?))(?:[._-][a-z0-9._-]+)?$/i.test(entry.name);
      if (entry.isSymbolicLink()) {
        if (legalName || inLegalDirectory) throw new Error(`Cannot collect a symlinked legal file: ${path}`);
        continue;
      }
      if (entry.isDirectory()) {
        walk(path, inLegalDirectory || /^(?:licen[cs]es?|notices?)$/i.test(entry.name));
      } else if (entry.isFile() && (legalName || inLegalDirectory)
        && !/\.(?:[cm]?js|[cm]?ts|map|json|wasm|node|png|jpe?g|svg|woff2?|ttf)$/i.test(entry.name)) {
        const bytes = readFileSync(path);
        if (bytes.includes(0) || bytes.length > 2_000_000) throw new Error(`Expected a readable license text: ${path}`);
        result.push({ path, relativePath: relative(directory, path).split(sep).join('/'), bytes });
      }
    }
  }
  walk(directory);
  return result;
}

// Some platform packages ship only a binary. Reuse their matching parent
// package's actual license, never a generic license fetched from the internet.
function parentLicensePath(name, metadata) {
  let parent;
  if (name.startsWith('@rolldown/binding-')) parent = 'rolldown';
  if (name.startsWith('@rollup/rollup-')) parent = 'rollup';
  if (name.startsWith('@esbuild/')) parent = 'esbuild';
  if (name.startsWith('@astrojs/compiler-binding-')) parent = '@astrojs/compiler-binding';
  if (name.startsWith('@bruits/satteri-')) parent = 'satteri';
  if (name.startsWith('@pagefind/')) parent = 'pagefind';
  if (name.startsWith('lightningcss-')) parent = 'lightningcss';
  if (!parent) return undefined;
  const path = `node_modules/${parent}`;
  const owner = lock.packages[path];
  if (owner?.version !== metadata.version || owner.license !== metadata.license
    || !(owner.optionalDependencies?.[name] || owner.dependencies?.[name])) return undefined;
  return path;
}

for (const [packagePath, metadata] of Object.entries(lock.packages).sort(([a], [b]) => a.localeCompare(b))) {
  if (!packagePath) continue;
  if (!safePackagePath(packagePath) || metadata.link) {
    errors.push(`Unsupported package location: ${packagePath}`);
    continue;
  }
  const name = packagePath.split('node_modules/').at(-1);
  const identity = `${name}@${metadata.version}`;
  const selectedLicense = metadata.license;
  const reviewedBuildOnly = reviewedToolLicenses.has(identity) || declarationOnlyTools.has(identity);
  assertLicense(selectedLicense, `${name}@${metadata.version}`);
  const location = resolve(root, packagePath);
  const installed = existsSync(join(location, 'package.json'));
  const record = {
    name, version: metadata.version, license: metadata.license, selectedLicense, packagePath,
    developmentOnly: metadata.dev === true, reviewedBuildOnly, optional: metadata.optional === true,
    installed, registryArchive: metadata.resolved ?? null, repository: null,
    integrity: metadata.integrity ?? null, noticeCoverage: installed ? 'bundled-legal-texts' : 'not-installed', notices: [],
  };
  packages.push(record);
  if (!installed) {
    if (!metadata.optional) errors.push(`${name}: required dependency is not installed; run npm ci first.`);
    continue;
  }
  try {
    if (lstatSync(location).isSymbolicLink()) throw new Error('Linked dependencies require an explicit license review.');
    const manifest = JSON.parse(readFileSync(join(location, 'package.json'), 'utf8'));
    record.repository = manifest.repository ?? null;
    if (manifest.name !== name || manifest.version !== metadata.version || manifest.license !== metadata.license) {
      throw new Error('Installed package name, version, or license differs from package-lock.json; run npm ci.');
    }
    let sourcePackage = packagePath;
    let notices = legalFiles(location);
    if (notices.length === 0) {
      const parent = parentLicensePath(name, metadata);
      if (parent && existsSync(join(root, parent, 'package.json'))) {
        sourcePackage = parent;
        notices = legalFiles(join(root, parent));
      }
    }
    for (const upstream of upstreamNotices.filter((entry) => entry.packages.includes(identity))) {
      if (upstream.license !== metadata.license || !/^[a-z0-9._-]+$/i.test(upstream.file)) {
        throw new Error('Upstream legal override does not match the reviewed license or safe file path.');
      }
      const path = join(root, 'scripts/legal-overrides', upstream.file);
      const bytes = readFileSync(path);
      if (digest(bytes) !== upstream.sha256) throw new Error('Upstream legal text changed; review its provenance.');
      notices.push({ path, relativePath: `UPSTREAM-${upstream.file}`, bytes, source: upstream.source });
    }
    const hasLicenseText = notices.some((file) => /^(?:licen[cs]e|copying|UPSTREAM-.*-LICENSE)/i.test(file.relativePath.split('/').at(-1)));
    const declaredOnly = declarationOnlyTools.get(identity) === metadata.license;
    const libvipsBundle = name.startsWith('@img/sharp-libvips-')
      && reviewedToolLicenses.get(identity) === 'LGPL-3.0-or-later';
    if (!hasLicenseText && !declaredOnly && !libvipsBundle) {
      throw new Error('No actual license text found; review the package before adding an exception.');
    }
    if (declaredOnly || libvipsBundle) {
      sourcePackage = packagePath;
      record.noticeCoverage = 'upstream-declarations-only; tooling-not-distributed';
      // README contains MIT attribution for the CLI helpers, and the complete
      // component license table for libvips. versions.json identifies that bundle.
      for (const relativePath of ['README.md', 'package.json', ...(libvipsBundle ? ['versions.json'] : [])]) {
        const path = join(location, relativePath);
        if (!existsSync(path)) throw new Error(`Reviewed upstream declaration is missing: ${relativePath}`);
        notices.push({ path, relativePath, bytes: readFileSync(path) });
      }
    }
    const folder = `${name.replaceAll('/', '__')}@${String(metadata.version).replace(/[^a-z0-9._-]/gi, '_')}-${digest(packagePath).slice(0, 8)}`;
    for (const notice of notices) {
      const destination = `packages/${folder}/${notice.relativePath}`;
      files.set(destination, notice.bytes);
      record.notices.push({ file: destination, source: notice.source ?? `${sourcePackage}/${notice.relativePath}`, sha256: digest(notice.bytes) });
    }
  } catch (error) {
    errors.push(`${name}: ${error.message}`);
  }
}

if (errors.length) throw new Error(`Dependency license review failed:\n${errors.map((error) => `- ${error}`).join('\n')}`);

const manifest = {
  description: 'Locked documentation dependency metadata and unchanged upstream notices. Some non-distributed tools provide declarations only, explicitly identified per package. Bundled components may have additional licenses within the preserved texts.',
  ledgenceLicense: 'LEDGENCE-LICENSE.txt',
  lockfileSha256: digest(readFileSync(join(root, 'package-lock.json'))),
  allowedPackageLicenses: [...allowed].sort(),
  reviewedToolLicenses: Object.fromEntries(reviewedToolLicenses), packages,
};
files.set('manifest.json', `${JSON.stringify(manifest, null, 2)}\n`);
const escape = (value) => String(value).replaceAll('&', '&amp;').replaceAll('<', '&lt;').replaceAll('>', '&gt;').replaceAll('"', '&quot;');
// Root-relative URLs also work when this page is served at /notices.
const href = (path) => `/notices/${path.split('/').map(encodeURIComponent).join('/')}`;
const rows = packages.map((item) => `<tr><td>${escape(item.name)}<br><small>${escape(item.version)}</small></td><td>${escape(item.license)}${item.selectedLicense !== item.license ? `<br><small>Used under ${escape(item.selectedLicense)}</small>` : ''}</td><td>${item.reviewedBuildOnly ? 'Build tooling only; not distributed' : 'Documentation dependency'}</td><td>${item.installed ? item.notices.map((notice) => `<a href="${escape(href(notice.file))}">${escape(notice.file.split('/').slice(2).join('/'))}</a>`).join('<br>') : 'Optional package; not installed in this build.'}</td></tr>`).join('\n');
files.set('index.html', `<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Ledgence — licenses and notices</title><style>body{font:16px/1.6 system-ui,sans-serif;max-width:1000px;margin:48px auto;padding:0 24px;color:#242424;background:#fafafa}h1{line-height:1.2}a{color:#245295}table{width:100%;border-collapse:collapse;font-size:14px}td,th{text-align:left;vertical-align:top;padding:16px 12px;border-bottom:1px solid #ddd;overflow-wrap:anywhere}th:first-child,td:first-child{padding-left:0}small{color:#666}nav{margin:24px 0}.table{overflow-x:auto}table{min-width:640px}</style></head><body>
<h1>Licenses and notices</h1><p>Ledgence-owned source code is licensed under MIT. Third-party software and fonts retain their original licenses; they are not relicensed under Ledgence's MIT license.</p>
<p>This collection preserves original legal texts and bundled notices from installed dependencies. A few tools that are not distributed in the static site supply declarations without complete license texts; their original README and package metadata are retained and identified in the manifest. Tooling is included for provenance, which does not mean its code is shipped in the website. Optional packages not installed here are listed from the lockfile.</p>
<p>Inter is supplied as an unmodified font under the SIL Open Font License 1.1. Astro, Starlight, Pagefind, and other browser components retain their respective licenses. Read the original files for complete terms and copyright notices.</p>
<nav><a href="/notices/LEDGENCE-LICENSE.txt">Ledgence MIT license</a> · <a href="/notices/manifest.json">Dependency manifest</a></nav>
<div class="table"><table><thead><tr><th>Package</th><th>Declared license</th><th>Use</th><th>Original legal files</th></tr></thead><tbody>${rows}</tbody></table></div></body></html>\n`);

if (!checkOnly) {
  // Validate everything before replacing previously generated notices.
  const output = join(root, 'public', 'notices');
  const staging = join(root, 'public', `.notices-${process.pid}`);
  try {
    mkdirSync(staging, { recursive: true });
    for (const [path, content] of files) {
      const destination = join(staging, path);
      mkdirSync(dirname(destination), { recursive: true });
      writeFileSync(destination, content);
    }
    rmSync(output, { recursive: true, force: true });
    renameSync(staging, output);
  } finally {
    rmSync(staging, { recursive: true, force: true });
  }
}
console.log(`Reviewed ${packages.length} locked packages; preserved ${files.size - 2} notice/provenance files${checkOnly ? ' (check only)' : ' in public/notices'}.`);

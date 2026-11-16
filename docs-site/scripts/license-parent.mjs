// SPDX-License-Identifier: MIT
// Native package notices must come from the parent that actually owns them.
export function parentLicensePath(packagePath, name, metadata, packages, readManifest) {
  let parent;
  if (name.startsWith('@rolldown/binding-')) parent = 'rolldown';
  if (name.startsWith('@rollup/rollup-')) parent = 'rollup';
  if (name.startsWith('@esbuild/')) parent = 'esbuild';
  if (name.startsWith('@astrojs/compiler-binding-')) parent = '@astrojs/compiler-binding';
  if (name.startsWith('@bruits/satteri-')) parent = 'satteri';
  if (/^@pagefind\/(?:darwin|freebsd|linux|windows)-(?:arm64|x64)$/.test(name)) parent = 'pagefind';
  if (name.startsWith('lightningcss-')) parent = 'lightningcss';
  if (!parent) return undefined;

  const parts = packagePath.split('/');
  const candidates = [`${packagePath}/node_modules/${parent}`];
  for (let index = parts.length - 1; index >= 0; index--) {
    if (parts[index] === 'node_modules') {
      candidates.push(`${parts.slice(0, index + 1).join('/')}/${parent}`);
    }
  }
  for (const path of candidates) {
    const owner = packages[path];
    const installed = readManifest(path);
    if (!owner && !installed) continue;
    // A nearer incompatible parent shadows farther copies. Never borrow their
    // legal text just because a different copy happens to have the right version.
    const ownsVersion = (entry) => entry?.version === metadata.version
      && entry.license === metadata.license
      && (entry.optionalDependencies?.[name] ?? entry.dependencies?.[name]) === metadata.version;
    if (!owner || !installed || installed.name !== parent
      || !ownsVersion(owner) || !ownsVersion(installed)) {
      throw new Error(`Native notice parent does not match locked and installed ownership: ${path}`);
    }
    return path;
  }
  return undefined;
}

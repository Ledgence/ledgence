// SPDX-License-Identifier: MIT
import { existsSync, lstatSync, readFileSync, readdirSync } from 'node:fs';
import { join } from 'node:path';

export function safePackagePath(path) {
  return /^node_modules\/(?:@[a-z0-9._-]+\/)?[a-z0-9._-]+(?:\/node_modules\/(?:@[a-z0-9._-]+\/)?[a-z0-9._-]+)*$/i.test(path)
    && path.split('/').every((part) => part !== '.' && part !== '..');
}

// Both supported package managers use a real, hoisted node_modules tree. Reject
// links rather than following external packages or virtual stores implicitly.
export function readInstalledPackages(root) {
  const installed = {};
  function directory(path) {
    if (!lstatSync(join(root, path)).isDirectory()) {
      throw new Error(`Linked or non-directory dependency location: ${path}`);
    }
  }
  function packageAt(path) {
    if (!safePackagePath(path)) throw new Error(`Unsupported package location: ${path}`);
    directory(path);
    const manifest = JSON.parse(readFileSync(join(root, path, 'package.json'), 'utf8'));
    if (manifest.name !== path.split('node_modules/').at(-1)) {
      throw new Error(`Installed package name differs from its location: ${path}`);
    }
    installed[path] = manifest;
    modulesAt(`${path}/node_modules`);
  }
  function modulesAt(path) {
    if (!existsSync(join(root, path))) return;
    directory(path);
    for (const name of readdirSync(join(root, path)).sort()) {
      if (name.startsWith('.')) continue;
      const child = `${path}/${name}`;
      if (name.startsWith('@')) {
        directory(child);
        for (const scoped of readdirSync(join(root, child)).sort()) packageAt(`${child}/${scoped}`);
      } else packageAt(child);
    }
  }
  modulesAt('node_modules');
  return installed;
}

const identity = (name, metadata) => JSON.stringify([name, metadata.version, metadata.license]);

export function resolveInstalledPackages(packages, installed) {
  const identities = new Map();
  const actualPackages = {};
  const installedPaths = new Map();
  for (const [path, metadata] of Object.entries(packages)) {
    if (!path) continue;
    if (!safePackagePath(path) || metadata.link) throw new Error(`Unsupported locked package location: ${path}`);
    const name = path.split('node_modules/').at(-1);
    const key = identity(name, metadata);
    const archive = JSON.stringify([metadata.resolved, metadata.integrity]);
    if (!metadata.resolved || !metadata.integrity) throw new Error(`Missing locked archive provenance: ${path}`);
    const existing = identities.get(key);
    if (existing && existing.archive !== archive) {
      throw new Error(`Ambiguous locked archive identity: ${name}@${metadata.version}`);
    }
    if (!existing) identities.set(key, { archive, metadata, paths: [] });
  }
  for (const [path, manifest] of Object.entries(installed).sort(([a], [b]) => a.localeCompare(b))) {
    if (!safePackagePath(path) || manifest.name !== path.split('node_modules/').at(-1)) {
      throw new Error(`Unsupported installed package location or name: ${path}`);
    }
    const match = identities.get(identity(manifest.name, manifest));
    if (!match) throw new Error(`Installed package is not a locked name/version/license identity: ${path}`);
    match.paths.push(path);
    actualPackages[path] = match.metadata;
  }
  for (const [path, metadata] of Object.entries(packages)) {
    if (!path) continue;
    const name = path.split('node_modules/').at(-1);
    const paths = identities.get(identity(name, metadata)).paths;
    if (!paths.length && !metadata.optional) throw new Error(`Required locked dependency is not installed: ${path}`);
    installedPaths.set(path, paths.includes(path) ? path : (paths[0] ?? null));
  }
  // Archive integrity remains lockfile provenance, verified by the package
  // manager during installation; this lookup does not hash installed payloads.
  return { installedPaths, actualPackages };
}

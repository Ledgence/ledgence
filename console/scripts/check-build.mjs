// SPDX-License-Identifier: MIT
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import {
  readFileSync,
  readdirSync,
  lstatSync,
  writeFileSync,
  existsSync,
} from "node:fs";
import { join, relative } from "node:path";
import { fileURLToPath } from "node:url";
const root = fileURLToPath(new URL("../", import.meta.url));
const output = join(root, "dist");
const digest = (data) => createHash("sha256").update(data).digest("hex");
const project = JSON.parse(readFileSync(join(root, "package.json"), "utf8"));
const inventory = JSON.parse(
  readFileSync(join(root, "third_party/inventory.json"), "utf8"),
);
for (const required of [
  "index.html",
  "notices/index.html",
  "notices/LEDGENCE-LICENSE.txt",
  "notices/inventory.json",
  "bundled-packages.json",
])
  if (!existsSync(join(output, required)))
    throw new Error(`Missing output: ${required}`);
const bundled = JSON.parse(
  readFileSync(join(output, "bundled-packages.json"), "utf8"),
);
const reviewed = new Set(
  inventory.packages.map((p) => `${p.name}@${p.version}`),
);
for (const id of bundled)
  if (!reviewed.has(id))
    throw new Error(`Unreviewed package in browser build: ${id}`);
const types = {
  html: "text/html; charset=utf-8",
  css: "text/css; charset=utf-8",
  js: "text/javascript; charset=utf-8",
  json: "application/json",
  txt: "text/plain; charset=utf-8",
  md: "text/plain; charset=utf-8",
  svg: "image/svg+xml",
  png: "image/png",
  ico: "image/x-icon",
};
const assets = [];
function walk(directory) {
  for (const entry of readdirSync(directory).sort()) {
    const path = join(directory, entry);
    const stat = lstatSync(path);
    if (stat.isSymbolicLink())
      throw new Error(`Symlink in static output: ${path}`);
    if (stat.isDirectory()) {
      walk(path);
      continue;
    }
    if (!stat.isFile())
      throw new Error(`Unexpected static output entry: ${path}`);
    const name = relative(output, path).split("\\").join("/");
    if (name === "console-manifest.json") continue;
    if (
      name.includes("..") ||
      name.startsWith("/") ||
      name.split("/").some((p) => !p)
    )
      throw new Error(`Invalid asset path: ${name}`);
    const data = readFileSync(path);
    const extension = name.split(".").at(-1);
    const contentType =
      types[extension] ??
      (name.startsWith("notices/") ? "text/plain; charset=utf-8" : null);
    if (!contentType) throw new Error(`Unknown asset content type: ${name}`);
    if (
      (name === "index.html" || name.startsWith("assets/")) &&
      /(?:window\.openai|Tweak|@vitest|test-fixture-only|sourceMappingURL=|https?:\/\/[^\s"']+\.(?:woff2?|css)(?:["']))/.test(
        data.toString(),
      )
    )
      throw new Error(`Development or remote runtime material in ${name}`);
    assets.push({
      path: name,
      sha256: digest(data),
      size_bytes: data.length,
      content_type: contentType,
    });
  }
}
walk(output);
const html = readFileSync(join(output, "index.html"), "utf8");
for (const match of html.matchAll(/(?:src|href)="([^"]+)"/g)) {
  if (
    !match[1].startsWith("/console/") ||
    !assets.some((a) => a.path === match[1].slice(9))
  )
    throw new Error(`Invalid entry asset reference: ${match[1]}`);
}
const overriddenRevision = process.env.LEDGENCE_SOURCE_REVISION;
const overriddenDirty = process.env.LEDGENCE_SOURCE_DIRTY;
if ((overriddenRevision === undefined) !== (overriddenDirty === undefined))
  throw new Error(
    "Supply both LEDGENCE_SOURCE_REVISION and LEDGENCE_SOURCE_DIRTY.",
  );
if (
  overriddenDirty !== undefined &&
  !["true", "false"].includes(overriddenDirty)
)
  throw new Error("LEDGENCE_SOURCE_DIRTY must be exactly true or false.");
const source_revision =
  overriddenRevision ??
  execFileSync("git", ["rev-parse", "HEAD"], {
    cwd: root,
    encoding: "utf8",
  }).trim();
const source_dirty =
  overriddenDirty === undefined
    ? execFileSync("git", ["status", "--porcelain"], {
        cwd: root,
        encoding: "utf8",
      }).trim().length > 0
    : overriddenDirty === "true";
if (!/^[a-f0-9]{40}$/.test(source_revision))
  throw new Error("Invalid source revision.");
if (
  assets.some((asset) => asset.size_bytes > 32 * 1024 * 1024) ||
  assets.reduce((sum, asset) => sum + asset.size_bytes, 0) > 64 * 1024 * 1024
)
  throw new Error("Console static output exceeds the server asset limits.");
const manifest = {
  schema_version: 1,
  console_version: project.version,
  console_contract_version: 4,
  source_revision,
  source_dirty,
  toolchain: { node: process.versions.node, pnpm: project.engines.pnpm },
  lockfile_sha256: digest(readFileSync(join(root, "pnpm-lock.yaml"))),
  assets,
};
writeFileSync(
  join(output, "console-manifest.json"),
  JSON.stringify(manifest, null, 2) + "\n",
);
console.log(
  `Verified ${assets.length} static files and ${bundled.length} bundled packages; manifest written.`,
);

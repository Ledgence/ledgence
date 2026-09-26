// SPDX-License-Identifier: MIT
import { createHash } from "node:crypto";
import {
  readFileSync,
  mkdirSync,
  writeFileSync,
  rmSync,
  cpSync,
  readdirSync,
} from "node:fs";
import { dirname, join } from "node:path";
const digest = (data) => createHash("sha256").update(data).digest("hex");
const read = (path) => readFileSync(new URL(`../${path}`, import.meta.url));
const root = new URL("../", import.meta.url);
const inventory = JSON.parse(read("third_party/inventory.json"));
const project = JSON.parse(read("package.json"));
if (project.license !== "MIT" || project.private !== true)
  throw new Error("Console must remain a private MIT package.");
if (digest(read("pnpm-lock.yaml")) !== inventory.lockfile_sha256)
  throw new Error("Review the changed dependency lockfile before building.");
if (digest(read("pnpm-workspace.yaml")) !== inventory.build_policy_sha256)
  throw new Error("Review the changed dependency build policy.");
for (const [field, review] of [
  ["dependencies", "direct_dependencies"],
  ["devDependencies", "direct_dev_dependencies"],
]) {
  if (JSON.stringify(project[field]) !== JSON.stringify(inventory[review]))
    throw new Error(`Unreviewed ${field}.`);
}
const policy = new Set([
  "MIT",
  "ISC",
  "Apache-2.0",
  "BSD-2-Clause",
  "BSD-3-Clause",
]);
const packages = new Map();
for (const item of inventory.packages) {
  const id = `${item.name}@${item.version}`;
  if (packages.has(id)) throw new Error(`Duplicate inventory entry: ${id}`);
  packages.set(id, item);
  if (
    !policy.has(item.declared_license) &&
    inventory.reviewed_special_licenses[id] !== item.declared_license
  )
    throw new Error(`Unreviewed license: ${id}`);
  if (
    !item.archive.startsWith("https://registry.npmjs.org/") ||
    !item.integrity.startsWith("sha512-")
  )
    throw new Error(`Unreviewed dependency source: ${id}`);
  if (!item.notices.length) throw new Error(`Missing legal material: ${id}`);
  for (const notice of item.notices) {
    if (
      !notice.file.startsWith("third_party/packages/") ||
      notice.file.split("/").some((p) => p === ".." || p === ".")
    )
      throw new Error("Unsafe notice path.");
    if (digest(read(notice.file)) !== notice.sha256)
      throw new Error(`Missing or changed notice: ${notice.file}`);
  }
}
const store = new URL("node_modules/.pnpm/", root);
for (const entry of readdirSync(store, { withFileTypes: true })) {
  if (!entry.isDirectory() || entry.name === "node_modules") continue;
  const modules = new URL(`${entry.name}/node_modules/`, store);
  for (const folder of readdirSync(modules, { withFileTypes: true })) {
    if (!folder.isDirectory()) continue;
    const paths = folder.name.startsWith("@")
      ? readdirSync(new URL(`${folder.name}/`, modules), {
          withFileTypes: true,
        })
          .filter((v) => v.isDirectory())
          .map((v) => `${folder.name}/${v.name}`)
      : [folder.name];
    for (const path of paths) {
      const data = JSON.parse(
        readFileSync(new URL(`${path}/package.json`, modules)),
      );
      const item = packages.get(`${data.name}@${data.version}`);
      if (!item || data.license !== item.declared_license)
        throw new Error(
          `Installed package differs from reviewed graph: ${data.name}@${data.version}`,
        );
    }
  }
}
const shadcn = JSON.parse(read("third_party/shadcn/provenance.json"));
if (digest(read("third_party/shadcn/LICENSE.md")) !== shadcn.license_sha256)
  throw new Error("shadcn license differs from reviewed source.");
const output = new URL("public/notices/", root);
rmSync(output, { recursive: true, force: true });
mkdirSync(output, { recursive: true });
const copied = new Set();
for (const item of inventory.packages)
  for (const notice of item.notices) {
    if (copied.has(notice.file)) continue;
    copied.add(notice.file);
    const destination = new URL(
      notice.file.replace("third_party/", ""),
      output,
    );
    mkdirSync(dirname(destination.pathname), { recursive: true });
    cpSync(new URL(notice.file, root), destination);
  }
cpSync(new URL("third_party/shadcn/", root), new URL("shadcn/", output), {
  recursive: true,
});
writeFileSync(new URL("LEDGENCE-LICENSE.txt", output), read("LICENSE"));
writeFileSync(
  new URL("inventory.json", output),
  JSON.stringify(inventory, null, 2) + "\n",
);
const escape = (v) =>
  String(v)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll('"', "&quot;");
const rows = inventory.packages
  .map(
    (p) =>
      `<li><strong>${escape(p.name)} ${escape(p.version)}</strong> — ${escape(p.declared_license)}<ul>${p.notices.map((n) => `<li><a href="/console/notices/${n.file.replace("third_party/", "").split("/").map(encodeURIComponent).join("/")}">${escape(n.file.split("/").at(-1))}</a></li>`).join("")}</ul></li>`,
  )
  .join("\n");
writeFileSync(
  new URL("index.html", output),
  `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Ledgence Console — Notices</title><link rel="stylesheet" href="/console/notices/style.css"></head><body><main><a href="/console/">Back to Console</a><h1>Licenses and notices</h1><p>Ledgence-owned code is MIT. Third-party components retain their own terms. This inventory includes development tools for provenance; listing a tool does not mean its code is included in the browser application. Browser packages actually used are recorded in the build's bundled-packages.json.</p><p><a href="/console/notices/LEDGENCE-LICENSE.txt">Ledgence MIT license</a> · <a href="/console/notices/shadcn/LICENSE.md">shadcn MIT license</a> · <a href="/console/notices/inventory.json">Reviewed inventory</a></p><p>Lucide includes ISC and Feather MIT notices. caniuse-lite is unmodified browser-support data under CC-BY-4.0, used only during builds; original attribution is retained in its supplied license material.</p><ul>${rows}</ul></main></body></html>`,
);
writeFileSync(
  new URL("style.css", output),
  "body{font:16px/1.6 system-ui,sans-serif;max-width:1000px;margin:40px auto;padding:0 24px;color:#20252e;background:#fff}a{color:#284a92;overflow-wrap:anywhere}li{margin:12px 0}main{min-width:0}",
);
console.log(
  `Reviewed ${inventory.packages.length} locked packages; retained ${copied.size} legal files plus shadcn notices.`,
);

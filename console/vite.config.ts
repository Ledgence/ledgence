import { defineConfig, type Plugin } from "vite";
import react from "@vitejs/plugin-react";
import { readFileSync } from "node:fs";
function bundledPackages(): Plugin {
  return {
    name: "ledgence-package-inventory",
    generateBundle(_options, bundle) {
      const packages = new Set<string>();
      for (const item of Object.values(bundle)) {
        if (item.type !== "chunk") continue;
        for (const moduleId of Object.keys(item.modules)) {
          const id = moduleId.replace(/^\0/, "");
          const marker = id.lastIndexOf("/node_modules/");
          if (marker < 0) continue;
          const suffix = id.slice(marker + 14);
          const parts = suffix.split("/");
          const name = parts[0]?.startsWith("@")
            ? parts.slice(0, 2).join("/")
            : parts[0];
          if (!name) throw new Error("Cannot identify bundled package.");
          const path = id.slice(0, marker + 14) + name + "/package.json";
          const metadata: unknown = JSON.parse(readFileSync(path, "utf8"));
          if (
            !metadata ||
            typeof metadata !== "object" ||
            !("name" in metadata) ||
            !("version" in metadata) ||
            typeof metadata.name !== "string" ||
            typeof metadata.version !== "string"
          )
            throw new Error("Invalid bundled package metadata.");
          packages.add(`${metadata.name}@${metadata.version}`);
        }
      }
      this.emitFile({
        type: "asset",
        fileName: "bundled-packages.json",
        source: JSON.stringify([...packages].sort(), null, 2) + "\n",
      });
    },
  };
}
export default defineConfig({
  base: "/console/",
  plugins: [react(), bundledPackages()],
  server: {
    host: "127.0.0.1",
    port: 5173,
    strictPort: true,
    proxy: { "/v1": { target: "http://127.0.0.1:8080" } },
  },
  build: { target: "es2022", sourcemap: false, assetsInlineLimit: 0 },
});

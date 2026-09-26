import { readFileSync } from "node:fs";
const project = JSON.parse(
  readFileSync(new URL("../package.json", import.meta.url), "utf8"),
);
if (process.versions.node !== project.engines.node)
  throw new Error(
    `Use Node ${project.engines.node}; found ${process.versions.node}.`,
  );
const expected = `pnpm/${project.engines.pnpm} `;
if (!process.env.npm_config_user_agent?.startsWith(expected))
  throw new Error(`Use pnpm ${project.engines.pnpm} for Console commands.`);

import { defineConfig, devices } from "@playwright/test";
const value = process.env.LEDGENCE_CONSOLE_URL;
if (!value)
  throw new Error(
    "LEDGENCE_CONSOLE_URL must identify the explicitly prepared acceptance Console.",
  );
const url = new URL(value);
if (
  !["http:", "https:"].includes(url.protocol) ||
  url.username ||
  url.password ||
  url.pathname !== "/console/" ||
  url.search ||
  url.hash
)
  throw new Error(
    "Use an absolute /console/ URL without credentials, query or fragment.",
  );
// Credential suites use independent disposable deployments and do not retain traces.
const selected = process.env.LEDGENCE_CONSOLE_LIVE_SUITE;
const protectedSuite =
  selected === "queue-policies" || selected === "run-budgets";
const output = protectedSuite ? selected : "console";
export default defineConfig({
  testMatch: protectedSuite
    ? selected === "queue-policies"
      ? ["**/queue-policies.spec.ts", "**/resource-policies.spec.ts"]
      : ["**/" + selected + ".spec.ts"]
    : ["**/*.spec.ts"],
  testIgnore: protectedSuite
    ? []
    : [
        "**/queue-policies.spec.ts",
        "**/resource-policies.spec.ts",
        "**/run-budgets.spec.ts",
      ],
  testDir: "./tests/live",
  workers: 1,
  fullyParallel: false,
  forbidOnly: true,
  retries: 0,
  timeout: 60000,
  expect: { timeout: 10000 },
  outputDir: "test-results/" + output + "-live",
  reporter: [
    ["list"],
    [
      "html",
      {
        open: "never",
        outputFolder: "playwright-report/" + output + "-live",
      },
    ],
  ],
  use: {
    baseURL: url.toString(),
    trace: protectedSuite ? "off" : "retain-on-failure",
  },
  projects: [
    { name: "chromium", use: { ...devices["Desktop Chrome"] } },
    { name: "webkit", use: { ...devices["Desktop Safari"] } },
  ],
});

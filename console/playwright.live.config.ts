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
// The queue-policy suite exercises a separate credential-bound disposable instance.
const queueSuite = process.env.LEDGENCE_CONSOLE_LIVE_SUITE === "queue-policies";
export default defineConfig({
  testMatch: queueSuite ? ["**/queue-policies.spec.ts"] : ["**/*.spec.ts"],
  testIgnore: queueSuite ? [] : ["**/queue-policies.spec.ts"],
  testDir: "./tests/live",
  workers: 1,
  fullyParallel: false,
  forbidOnly: true,
  retries: 0,
  timeout: 60000,
  expect: { timeout: 10000 },
  outputDir: queueSuite ? "test-results/queue-live" : "test-results/live",
  reporter: [
    ["list"],
    [
      "html",
      {
        open: "never",
        outputFolder: queueSuite
          ? "playwright-report/queue-live"
          : "playwright-report/live",
      },
    ],
  ],
  use: {
    baseURL: url.toString(),
    trace: queueSuite ? "off" : "retain-on-failure",
  },
  projects: [
    { name: "chromium", use: { ...devices["Desktop Chrome"] } },
    { name: "webkit", use: { ...devices["Desktop Safari"] } },
  ],
});

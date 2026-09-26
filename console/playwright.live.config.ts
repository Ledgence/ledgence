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
export default defineConfig({
  testDir: "./tests/live",
  workers: 1,
  fullyParallel: false,
  forbidOnly: true,
  retries: 0,
  timeout: 60000,
  expect: { timeout: 10000 },
  outputDir: "test-results/live",
  reporter: [
    ["list"],
    ["html", { open: "never", outputFolder: "playwright-report/live" }],
  ],
  use: { baseURL: url.toString(), trace: "retain-on-failure" },
  projects: [
    { name: "chromium", use: { ...devices["Desktop Chrome"] } },
    { name: "webkit", use: { ...devices["Desktop Safari"] } },
  ],
});

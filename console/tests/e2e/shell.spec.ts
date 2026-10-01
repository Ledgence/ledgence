import { readFileSync } from "node:fs";
import { decodeConfig } from "../../src/api/codecs";
import { parseUserJson, stringifyUserJson } from "../../src/api/json";
import { expect, test } from "@playwright/test";
for (const width of [320, 375, 768, 1280]) {
  test(`shell remains readable at ${width}px`, async ({ page }) => {
    await page.setViewportSize({ width, height: 900 });
    await page.route("**/v1/console/config", (route) =>
      route.fulfill({ status: 503, body: "Unavailable" }),
    );
    await page.goto("/console/");
    await expect(
      page.getByRole("navigation", { name: "Main navigation" }),
    ).toBeVisible();
    await expect(page.getByRole("alert")).toContainText("cannot connect");
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= window.innerWidth,
      ),
    ).toBe(true);
    await page
      .getByRole("combobox", { name: "Appearance" })
      .selectOption("dark");
    await expect(page.locator("html")).toHaveAttribute("data-theme", "dark");
    await page
      .getByRole("combobox", { name: "Appearance" })
      .selectOption("light");
    await expect(page.locator("html")).toHaveAttribute("data-theme", "light");
  });
}

test("sidebar preference survives reload and mobile navigation stays usable", async ({
  page,
}) => {
  await page.setViewportSize({ width: 1280, height: 900 });
  await page.route("**/v1/console/config", (route) =>
    route.fulfill({ status: 503, body: "Unavailable" }),
  );
  await page.goto("/console/executions");
  const main = page.getByRole("main");
  const expandedLeft = (await main.boundingBox())!.x;
  const sidebar = page.getByRole("complementary", { name: "Console sidebar" });
  const collapse = sidebar.getByRole("button", {
    name: "Collapse sidebar",
  });
  await expect(
    sidebar.getByRole("link", { name: "Ledgence Console home" }),
  ).toBeVisible();
  await expect(
    page.getByRole("banner").getByRole("button", { name: /sidebar/ }),
  ).toHaveCount(0);
  await expect(collapse).toHaveAttribute("title", "Collapse sidebar");
  expect((await collapse.boundingBox())!.y).toBeLessThan(80);
  await collapse.focus();
  await collapse.press("Enter");
  await expect(
    page.getByRole("button", { name: "Expand sidebar" }),
  ).toBeFocused();
  await expect(
    page.getByRole("button", { name: "Expand sidebar" }),
  ).toHaveAttribute("aria-expanded", "false");
  expect((await main.boundingBox())!.x).toBeLessThan(expandedLeft);
  await expect(
    sidebar.getByRole("link", { name: "Ledgence Console home" }),
  ).toHaveCount(0);
  await expect(
    sidebar
      .getByRole("button", { name: "Expand sidebar" })
      .locator(".brand-mark"),
  ).toBeVisible();
  await expect(page).toHaveURL(/\/executions$/);
  await expect(
    page.getByRole("link", { name: "Executions", exact: true }),
  ).toHaveAttribute("aria-current", "page");
  await expect(page.getByRole("link", { name: "Documentation" })).toBeVisible();
  await page.reload();
  await expect(
    page.getByRole("button", { name: "Expand sidebar" }),
  ).toBeVisible();

  await page.setViewportSize({ width: 320, height: 900 });
  await expect(
    page.getByRole("button", { name: "Expand sidebar" }),
  ).toBeHidden();
  const navigation = page.getByRole("navigation", { name: "Main navigation" });
  for (const name of ["Executions", "Programs", "Workers"]) {
    await expect(navigation.getByText(name, { exact: true })).toBeVisible();
  }
  await navigation.getByRole("link", { name: "Workers" }).click();
  await expect(page).toHaveURL(/\/workers$/);
  await expect(
    navigation.getByRole("link", { name: "Workers" }),
  ).toHaveAttribute("aria-current", "page");
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= window.innerWidth,
    ),
  ).toBe(true);

  await page.setViewportSize({ width: 1280, height: 900 });
  const expand = sidebar.getByRole("button", {
    name: "Expand sidebar",
  });
  await expect(expand).toHaveAttribute("title", "Expand sidebar");
  expect((await expand.boundingBox())!.y).toBeLessThan(80);
  await expand.focus();
  await expand.press("Enter");
  await expect(
    sidebar.getByRole("button", { name: "Collapse sidebar" }),
  ).toHaveAttribute("aria-expanded", "true");
  await expect(
    sidebar.getByRole("button", { name: "Collapse sidebar" }),
  ).toBeFocused();
  await expect(
    sidebar.getByRole("link", { name: "Ledgence Console home" }),
  ).toBeVisible();
  // Expanding the brand is a sidebar action, never a home navigation.
  await expect(page).toHaveURL(/\/workers$/);
  await page.setViewportSize({ width: 320, height: 900 });
  await expect(sidebar.locator(".shell-sidebar-header")).toBeHidden();
  await expect(
    sidebar.getByRole("button", { name: "Collapse sidebar" }),
  ).toBeHidden();
});

test("sidebar remains operable when preference storage is blocked", async ({
  page,
}) => {
  await page.setViewportSize({ width: 1280, height: 900 });
  await page.addInitScript(() => {
    Storage.prototype.getItem = () => {
      throw new DOMException("Storage is blocked", "SecurityError");
    };
    Storage.prototype.setItem = () => {
      throw new DOMException("Storage is blocked", "SecurityError");
    };
  });
  await page.route("**/v1/console/config", (route) =>
    route.fulfill({ status: 503, body: "Unavailable" }),
  );
  await page.goto("/console/");
  await page.getByRole("button", { name: "Collapse sidebar" }).click();
  await page.getByRole("link", { name: "Programs", exact: true }).click();
  await expect(page).toHaveURL(/\/programs$/);
  await page.getByRole("button", { name: "Expand sidebar" }).click();
  await expect(
    page.getByRole("button", { name: "Collapse sidebar" }),
  ).toBeVisible();
});

test("a long instance name stays bounded beside the header controls", async ({
  page,
}) => {
  const fixture = parseUserJson(
    readFileSync(
      new URL(
        "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v4.json",
        import.meta.url,
      ),
      "utf8",
    ),
    2 * 1024 * 1024,
  );
  if (!fixture || typeof fixture !== "object")
    throw Error("Missing Rust fixture");
  const config = decodeConfig(Reflect.get(fixture, "config"));
  config.instance_name =
    "Research and operations — " + "production-instance-".repeat(12);
  await page.route("**/v1/console/**", async (route) => {
    const resource = new URL(route.request().url()).pathname.replace(
      "/v1/console/",
      "",
    );
    if (resource !== "config" && resource !== "executions")
      throw Error(`Unexpected request ${resource}`);
    await route.fulfill({
      status: 200,
      headers: {
        "Content-Type": "application/json",
        "Ledgence-Console-Contract": "4",
        "Ledgence-Instance-Id": config.instance_id,
      },
      body: stringifyUserJson(
        resource === "config"
          ? config
          : { items: [], next_cursor: null, observed_at: 1790409600000 },
      ),
    });
  });
  await page.goto("/console/executions");
  const name = page.getByTitle(config.instance_name, { exact: true });
  for (const width of [320, 1280]) {
    await page.setViewportSize({ width, height: 900 });
    await expect(name).toBeVisible();
    await expect(page.locator(".shell-page-heading")).toBeHidden();
    const nameBox = (await name.boundingBox())!;
    const appearanceBox = (await page
      .getByRole("combobox", { name: "Appearance" })
      .boundingBox())!;
    expect(nameBox.x + nameBox.width).toBeLessThan(appearanceBox.x);
    expect(
      await name.evaluate(
        (element) => element.scrollWidth > element.clientWidth,
      ),
    ).toBe(true);
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
  }
});

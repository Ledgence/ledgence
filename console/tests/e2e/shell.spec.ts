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
  await page.getByRole("button", { name: "Collapse sidebar" }).click();
  await expect(
    page.getByRole("button", { name: "Expand sidebar" }),
  ).toHaveAttribute("aria-expanded", "false");
  expect((await main.boundingBox())!.x).toBeLessThan(expandedLeft);
  await expect(
    page.getByRole("link", { name: "Executions", exact: true }),
  ).toHaveAttribute("aria-current", "page");
  await expect(page.getByRole("link", { name: "Documentation" })).toBeVisible();
  await page.reload();
  await expect(
    page.getByRole("button", { name: "Expand sidebar" }),
  ).toBeVisible();

  await page.setViewportSize({ width: 320, height: 900 });
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
  await page.getByRole("button", { name: "Expand sidebar" }).click();
  await expect(
    page.getByRole("button", { name: "Collapse sidebar" }),
  ).toHaveAttribute("aria-expanded", "true");
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

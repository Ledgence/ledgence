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

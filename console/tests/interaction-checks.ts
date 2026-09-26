import { expect, type Page, type Locator } from "@playwright/test";
async function tabTo(page: Page, target: Locator) {
  // macOS WebKit follows Safari's native Option-Tab full-item navigation.
  // Plain Tab follows the host preference and can omit buttons and links.
  const tabKey =
    process.platform === "darwin" &&
    page.context().browser()?.browserType().name() === "webkit"
      ? "Alt+Tab"
      : "Tab";
  for (let index = 0; index < 40; index++) {
    await page.keyboard.press(tabKey);
    if (await target.evaluate((element) => element === document.activeElement))
      return tabKey;
  }
  throw new Error("Keyboard Tab did not reach the expected control.");
}
export async function keyboardDialog(page: Page) {
  const trigger = page.getByRole("button", {
    name: "Register agent",
    exact: true,
  });
  await expect(trigger).toBeVisible();
  const tabKey = await tabTo(page, trigger);
  await expect(trigger).toBeFocused();
  await page.keyboard.press("Enter");
  const dialog = page.getByRole("dialog");
  await expect(dialog).toBeVisible();
  await page.keyboard.press("Tab");
  await expect(
    page.getByRole("textbox", { name: "Exact version", exact: true }),
  ).toBeFocused();
  const visited = new Set<string>();
  for (let index = 0; index < 12; index++) {
    await page.keyboard.press(tabKey);
    expect(
      await dialog.evaluate((element) =>
        element.contains(document.activeElement),
      ),
    ).toBe(true);
    visited.add(
      await page.evaluate(() => {
        const active = document.activeElement;
        if (
          active instanceof HTMLInputElement ||
          active instanceof HTMLSelectElement ||
          active instanceof HTMLTextAreaElement
        )
          return `${active.tagName}:${active.labels?.[0]?.textContent?.trim() ?? ""}`;
        return active?.textContent?.trim() ?? "";
      }),
    );
  }
  expect(visited.size).toBeGreaterThanOrEqual(5);
  await page.keyboard.press(`Shift+${tabKey}`);
  expect(
    await dialog.evaluate((element) =>
      element.contains(document.activeElement),
    ),
  ).toBe(true);
  await page.keyboard.press("Escape");
  await expect(dialog).toHaveCount(0);
  await expect(trigger).toBeFocused();
}
export async function reducedMotionDialog(page: Page) {
  await page.emulateMedia({ reducedMotion: "reduce" });
  const trigger = page.getByRole("button", {
    name: "Register agent",
    exact: true,
  });
  await expect(trigger).toBeVisible();
  await trigger.hover();
  await trigger.click();
  await expect(page.getByRole("dialog")).toBeVisible();
  expect(
    await page.evaluate(
      () =>
        document
          .getAnimations()
          .filter((animation) => animation.playState === "running").length,
    ),
  ).toBe(0);
  await page.keyboard.press("Escape");
  await expect(trigger).toBeFocused();
  await page.getByRole("link", { name: "Workers", exact: true }).click();
  await expect(
    page.getByRole("heading", { name: "Workers", exact: true }),
  ).toBeVisible();
  expect(
    await page.evaluate(
      () =>
        document
          .getAnimations()
          .filter((animation) => animation.playState === "running").length,
    ),
  ).toBe(0);
}
export async function touchNavigation(page: Page) {
  await page.evaluate(() =>
    document.addEventListener("pointerdown", (event) => {
      document.documentElement.dataset.lastPointer = event.pointerType;
    }),
  );
  const workers = page.getByRole("link", { name: "Workers", exact: true });
  await workers.tap();
  await expect(
    page.getByRole("heading", { name: "Workers", exact: true }),
  ).toBeVisible();
  await page.getByRole("link", { name: "Agents", exact: true }).tap();
  const trigger = page.getByRole("button", {
    name: "Register agent",
    exact: true,
  });
  const box = await trigger.boundingBox();
  expect(box?.height).toBeGreaterThanOrEqual(44);
  await trigger.tap();
  await expect(page.getByRole("dialog")).toBeVisible();
  await page.getByRole("button", { name: "Close dialog" }).tap();
  await expect(page.getByRole("dialog")).toHaveCount(0);
  await expect(page.locator("html")).toHaveAttribute(
    "data-last-pointer",
    "touch",
  );
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
  ).toBe(true);
}
export async function doubledLayout(page: Page) {
  await page.setViewportSize({ width: 1280, height: 900 });
  await page.goto("/console/agents");
  const heading = page.getByRole("heading", { name: "Agents", exact: true });
  await expect(heading).toBeVisible();
  const original = await heading.boundingBox();
  // CSS zoom scales the actual rendered layout, unlike deviceScaleFactor. It is
  // not browser-chrome zoom; the constrained viewport separately checks reflow.
  await page.evaluate(() => {
    document.documentElement.style.zoom = "2";
  });
  const scaled = await heading.boundingBox();
  expect((scaled?.height ?? 0) / (original?.height ?? 1)).toBeCloseTo(2, 1);
  for (const title of ["Executions", "Workflows", "Agents", "Workers"]) {
    await page.getByRole("link", { name: title, exact: true }).click();
    await expect(
      page.getByRole("heading", { name: title, exact: true }),
    ).toBeVisible();
    await expect(page.locator(".loading-state")).toHaveCount(0);
    expect(
      await page.evaluate(
        () => document.documentElement.scrollWidth <= innerWidth,
      ),
    ).toBe(true);
  }
  await page.setViewportSize({ width: 640, height: 450 });
  await page.getByRole("link", { name: "Agents", exact: true }).click();
  await page
    .getByRole("button", { name: "Register agent", exact: true })
    .click();
  await expect(page.getByRole("dialog")).toBeVisible();
  await page
    .getByRole("textbox", { name: "Program ID", exact: true })
    .fill("zoom-check");
  await page.keyboard.press("Escape");
  await expect(
    page.getByRole("button", { name: "Register agent", exact: true }),
  ).toBeFocused();
  expect(
    await page.evaluate(
      () => document.documentElement.scrollWidth <= innerWidth,
    ),
  ).toBe(true);
}

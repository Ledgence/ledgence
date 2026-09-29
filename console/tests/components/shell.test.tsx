import { expect, it } from "vitest";
import { render } from "vitest-browser-react";
import { MemoryRouter } from "react-router";
import { AppShell } from "../../src/components/app-shell";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogTitle,
  DialogTrigger,
} from "../../src/components/ui/dialog";
import "../../src/styles/global.css";
it("navigates the console and renders instance names as text", async () => {
  const page = await render(
    <MemoryRouter initialEntries={["/executions"]}>
      <AppShell instanceName="<script>test</script>">
        <h1>Executions</h1>
      </AppShell>
    </MemoryRouter>,
  );
  await expect
    .element(page.getByRole("link", { name: "Executions", exact: true }))
    .toHaveAttribute("aria-current", "page");
  await page.getByRole("link", { name: "Workers", exact: true }).click();
  await expect
    .element(page.getByRole("link", { name: "Workers", exact: true }))
    .toHaveAttribute("aria-current", "page");
  await expect
    .element(page.getByRole("combobox", { name: "Appearance" }))
    .toBeVisible();
});
it("returns focus after a dialog closes", async () => {
  const page = await render(
    <Dialog>
      <DialogTrigger>Open details</DialogTrigger>
      <DialogContent>
        <DialogTitle>Details</DialogTitle>
        <DialogDescription>Inspect this operation.</DialogDescription>
      </DialogContent>
    </Dialog>,
  );
  await page.getByRole("button", { name: "Open details" }).click();
  await expect.element(page.getByRole("dialog")).toBeVisible();
  await page.getByRole("button", { name: "Close dialog" }).click();
  await expect
    .element(page.getByRole("button", { name: "Open details" }))
    .toHaveFocus();
});

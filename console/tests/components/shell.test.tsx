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
it("keeps Executions active inside a workflow and renders the server identity as text", async () => {
  const page = await render(
    <MemoryRouter initialEntries={["/workflows/wf-demo"]}>
      <AppShell instanceName="<script>test</script>" serverVersion="1.2.3">
        <h1>Executions</h1>
      </AppShell>
    </MemoryRouter>,
  );
  await expect
    .element(page.getByRole("link", { name: "Executions", exact: true }))
    .toHaveAttribute("aria-current", "page");
  await expect
    .element(page.getByText("<script>test</script>", { exact: true }))
    .toBeVisible();
  await expect
    .element(page.getByText("1.2.3", { exact: true }))
    .toBeInTheDocument();
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

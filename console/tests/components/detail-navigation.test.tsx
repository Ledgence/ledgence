// SPDX-License-Identifier: MIT
import { afterEach, expect, it } from "vitest";
import { render } from "vitest-browser-react";
import { userEvent } from "vitest/browser";
import { createMemoryRouter, RouterProvider, useLocation } from "react-router";
import { DetailPanels, DetailSections } from "../../src/features/detail-panels";
import type { DetailKind } from "../../src/features/detail-navigation";

const routers: ReturnType<typeof createMemoryRouter>[] = [];
afterEach(() => {
  for (const router of routers.splice(0)) router.dispose();
  localStorage.removeItem("ledgence-explorer-view-v3");
});
function Detail({ kind }: { kind: DetailKind }) {
  const location = useLocation();
  return (
    <>
      <p data-testid="url">{location.search}</p>
      <DetailPanels kind={kind}>
        {({ tab, section, openSection }) => (
          <>
            <p data-testid="tab">{tab}</p>
            <DetailSections
              current={section}
              onChange={openSection}
              sections={[
                {
                  id: "input",
                  title: "Input",
                  description: "Submitted data",
                  content: <p>Original input retained</p>,
                },
                {
                  id: "output",
                  title: "Output",
                  description: "Recorded result",
                  content: <p>Outcome retained</p>,
                },
              ]}
            />
          </>
        )}
      </DetailPanels>
    </>
  );
}
async function setup(kind: DetailKind, search: string) {
  const router = createMemoryRouter(
    [{ path: "*", element: <Detail kind={kind} /> }],
    {
      initialEntries: [
        {
          pathname: "/detail",
          search,
          state: { returnTo: "/executions?state=failed" },
        },
      ],
    },
  );
  routers.push(router);
  const view = await render(<RouterProvider router={router} />);
  return { router, view };
}
it("normalizes legacy content links in place, retains list navigation and opens only requested data", async () => {
  const { router, view } = await setup("task", "?tab=Result&cursor=opaque");
  await expect.element(view.getByText("Outcome retained")).toBeVisible();
  await expect
    .element(view.getByText("Original input retained"))
    .not.toBeInTheDocument();
  await expect
    .element(view.getByRole("button", { name: "General", exact: true }))
    .toHaveAttribute("aria-current", "page");
  await expect
    .element(view.getByTestId("url"))
    .toHaveTextContent("tab=General&cursor=opaque&section=output");
  expect(router.state.location.state.returnTo).toBe("/executions?state=failed");
  await view.getByRole("button", { name: "Input", exact: true }).click();
  await expect.element(view.getByText("Original input retained")).toBeVisible();
  await expect
    .element(view.getByText("Outcome retained"))
    .not.toBeInTheDocument();
  expect(router.state.location.search).not.toContain("cursor=");
  await router.navigate(-1);
  await expect.element(view.getByText("Outcome retained")).toBeVisible();
  await expect
    .element(view.getByRole("button", { name: "Graph", exact: true }))
    .not.toBeInTheDocument();
});
it("keeps all three workflow views explicit and preserves graph context through General", async () => {
  const { router, view } = await setup(
    "workflow",
    "?view=timeline&node=chosen&explorer_cursor=third",
  );
  await expect.element(view.getByTestId("tab")).toHaveTextContent("Trace");
  await view.getByRole("button", { name: "Graph", exact: true }).click();
  await expect.element(view.getByTestId("tab")).toHaveTextContent("Graph");
  await view.getByRole("button", { name: "General", exact: true }).click();
  await view.getByRole("button", { name: "Graph", exact: true }).click();
  expect(router.state.location.search).toContain("explorer_cursor=third");
  expect(router.state.location.search).toContain("node=chosen");
  expect(router.state.location.search).not.toContain("view=");
  expect(localStorage.getItem("ledgence-explorer-view-v3")).toBe("graph");
});
it("supports keyboard disclosure without mounting hidden data", async () => {
  const { view } = await setup("task", "?tab=General&section=input");
  const input = view.getByRole("button", { name: "Input", exact: true });
  await expect.element(input).toHaveAttribute("aria-expanded", "true");
  input.element().focus();
  await userEvent.keyboard("{Enter}");
  await expect.element(input).toHaveAttribute("aria-expanded", "false");
  await expect
    .element(view.getByText("Original input retained"))
    .not.toBeInTheDocument();
  await userEvent.keyboard(" ");
  await expect.element(input).toHaveAttribute("aria-expanded", "true");
});

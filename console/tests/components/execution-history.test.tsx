// SPDX-License-Identifier: MIT
import { afterEach, expect, it } from "vitest";
import { render } from "vitest-browser-react";
import {
  createMemoryRouter,
  RouterProvider,
  useSearchParams,
} from "react-router";
import { HistoryFilters } from "../../src/features/execution-history";
import "../../src/styles/global.css";
const routers: ReturnType<typeof createMemoryRouter>[] = [];
afterEach(() => {
  for (const router of routers.splice(0)) router.dispose();
});
function Filters() {
  const [params] = useSearchParams();
  return (
    <main className="main-content">
      <HistoryFilters key={params.toString()} />
      <output data-testid="query">{params.toString()}</output>
    </main>
  );
}
async function mount(query = "") {
  const router = createMemoryRouter(
    [{ path: "/executions", element: <Filters /> }],
    { initialEntries: [`/executions${query ? `?${query}` : ""}`] },
  );
  routers.push(router);
  const view = await render(<RouterProvider router={router} />);
  return { view, router };
}
it("applies inclusive UTC days with compatible type and status and resets paging", async () => {
  const { view, router } = await mount(
    "kind=task&state=active&cursor=page-2&limit=25",
  );
  await view.getByLabelText("Submitted from · UTC").fill("2024-02-29");
  await view.getByLabelText("Submitted through · UTC").fill("2024-02-29");
  await view
    .getByRole("combobox", { name: "Type", exact: true })
    .selectOptions("workflow");
  await expect
    .element(view.getByRole("combobox", { name: "Status", exact: true }))
    .toHaveValue("");
  await view
    .getByRole("combobox", { name: "Status", exact: true })
    .selectOptions("waiting");
  await view.getByRole("button", { name: "Apply filters" }).click();
  const params = new URLSearchParams(router.state.location.search);
  expect(params.get("submitted_from")).toBe(
    String(Date.parse("2024-02-29T00:00:00Z")),
  );
  expect(params.get("submitted_until")).toBe(
    String(Date.parse("2024-03-01T00:00:00Z")),
  );
  expect(params.get("kind")).toBe("workflow");
  expect(params.get("state")).toBe("waiting");
  expect(params.get("limit")).toBe("25");
  expect(params.has("cursor")).toBe(false);
});
it("keeps an inverted date range in the form without changing the query", async () => {
  const { view, router } = await mount("kind=workflow&state=running");
  await view.getByLabelText("Submitted from · UTC").fill("2026-09-30");
  await view.getByLabelText("Submitted through · UTC").fill("2026-09-29");
  await view.getByRole("button", { name: "Apply filters" }).click();
  await expect
    .element(view.getByRole("alert"))
    .toHaveTextContent("Submitted through must be on or after submitted from.");
  expect(router.state.location.search).toBe("?kind=workflow&state=running");
  await view.getByLabelText("Submitted through · UTC").fill("2026-09-30");
  await view.getByRole("button", { name: "Apply filters" }).click();
  await expect.element(view.getByRole("alert")).not.toBeInTheDocument();
});
it("keeps exact advanced searches discoverable and preserves empty correlation", async () => {
  const { view, router } = await mount(
    "program_id=agent%2Fresearch&correlation_key=&include_children=true",
  );
  await expect
    .element(view.getByLabelText("Exact program ID"))
    .toHaveValue("agent/research");
  await expect
    .element(view.getByLabelText("Include child executions"))
    .toBeDisabled();
  await expect
    .element(view.getByLabelText("Include child executions"))
    .toBeChecked();
  await view.getByRole("button", { name: "Apply filters" }).click();
  const params = new URLSearchParams(router.state.location.search);
  expect(params.get("program_id")).toBe("agent/research");
  expect(params.has("correlation_key")).toBe(true);
  expect(params.get("correlation_key")).toBe("");
  expect(params.get("include_children")).toBe("true");
});
it("allows opt-in children and clears all filters while retaining row size", async () => {
  const { view, router } = await mount("kind=task&limit=25");
  await view.getByText("More filters", { exact: true }).click();
  await view.getByLabelText("Include child executions").click();
  await view.getByRole("button", { name: "Apply filters" }).click();
  expect(
    new URLSearchParams(router.state.location.search).get("include_children"),
  ).toBe("true");
  await view.getByRole("button", { name: "Clear", exact: true }).click();
  expect(router.state.location.search).toBe("?limit=25");
  await expect
    .element(view.getByRole("combobox", { name: "Type", exact: true }))
    .toHaveValue("");
});
it("restores filters on browser Back without keeping a stale draft", async () => {
  const { view, router } = await mount("kind=workflow&state=waiting");
  await view
    .getByRole("combobox", { name: "Type", exact: true })
    .selectOptions("task");
  await view.getByRole("button", { name: "Apply filters" }).click();
  expect(new URLSearchParams(router.state.location.search).get("kind")).toBe(
    "task",
  );
  await router.navigate(-1);
  await expect
    .element(view.getByRole("combobox", { name: "Type", exact: true }))
    .toHaveValue("workflow");
  await expect
    .element(view.getByRole("combobox", { name: "Status", exact: true }))
    .toHaveValue("waiting");
});

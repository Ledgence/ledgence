import { afterEach, expect, it } from "vitest";
import { render } from "vitest-browser-react";
import {
  createMemoryRouter,
  RouterProvider,
  useLocation,
  useSearchParams,
} from "react-router";
import raw from "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v1.json?raw";
import { decodeConfig } from "../../src/api/codecs";
import { usePagination } from "../../src/api/hooks";
import { parseUserJson } from "../../src/api/json";
import { InstanceContext } from "../../src/app/instance";
import { BackLink, PageControls, Tabs } from "../../src/components/resource-ui";

const fixture = parseUserJson(raw, 2 * 1024 * 1024);
if (!fixture || typeof fixture !== "object" || !("config" in fixture))
  throw new Error("Missing Rust config.");
const config = decodeConfig(fixture.config);
const routers: ReturnType<typeof createMemoryRouter>[] = [];
afterEach(() => {
  for (const router of routers.splice(0)) router.dispose();
});

function Pager({ prefix = "", name = "Pages" }) {
  const pagination = usePagination(prefix);
  const page = Number(pagination.cursor?.replace("page", "") ?? "1");
  return (
    <fieldset>
      <legend>{name}</legend>
      <p data-testid={`${name} cursor`}>{pagination.cursor ?? "first"}</p>
      <PageControls
        pagination={pagination}
        nextCursor={`page${page + 1}`}
        observedAt={0}
        refresh={() => undefined}
        fetching={false}
      />
      <button onClick={pagination.reset}>Reset pages</button>
    </fieldset>
  );
}
function Navigation({ paired = false }) {
  const location = useLocation();
  const [params] = useSearchParams();
  return (
    <>
      <BackLink to="/executions">Back to executions</BackLink>
      <Tabs
        values={["Overview", "Input"]}
        current={params.get("tab") ?? "Overview"}
      />
      <p data-testid="Current URL">{location.pathname + location.search}</p>
      {paired ? (
        <>
          <Pager prefix="alpha_" name="Alpha" />
          <Pager prefix="beta_" name="Beta" />
        </>
      ) : (
        <Pager />
      )}
    </>
  );
}
async function setup(path: string, paired = false, state: unknown = null) {
  const url = new URL(path, "http://console.test");
  const router = createMemoryRouter(
    [{ path: "*", element: <Navigation paired={paired} /> }],
    {
      initialEntries: [
        {
          pathname: url.pathname,
          search: url.search,
          state,
        },
      ],
    },
  );
  routers.push(router);
  const view = await render(
    <InstanceContext.Provider value={config}>
      <RouterProvider router={router} />
    </InstanceContext.Provider>,
  );
  return { view, router };
}

it("resolves Previous from the current cursor after browser Back and Forward", async () => {
  const { view, router } = await setup("/mixed-history");
  const next = view.getByRole("button", { name: "Next", exact: true });
  for (let i = 0; i < 3; i++) await next.click();
  await router.navigate(-1);
  await router.navigate(-1);
  await expect
    .element(view.getByTestId("Pages cursor"))
    .toHaveTextContent("page2");
  await router.navigate(1);
  await expect
    .element(view.getByTestId("Pages cursor"))
    .toHaveTextContent("page3");
  await router.navigate(-1);
  await view.getByRole("button", { name: "Previous", exact: true }).click();
  await expect
    .element(view.getByTestId("Pages cursor"))
    .toHaveTextContent("first");
});

it("retains a cursor's predecessor when browser history revisits it after UI Previous", async () => {
  const { view, router } = await setup("/revisited-history");
  for (let i = 0; i < 3; i++)
    await view.getByRole("button", { name: "Next", exact: true }).click();
  await view.getByRole("button", { name: "Previous", exact: true }).click();
  await expect
    .element(view.getByTestId("Pages cursor"))
    .toHaveTextContent("page3");
  await router.navigate(-1);
  await expect
    .element(view.getByTestId("Pages cursor"))
    .toHaveTextContent("page4");
  await view.getByRole("button", { name: "Previous", exact: true }).click();
  await expect
    .element(view.getByTestId("Pages cursor"))
    .toHaveTextContent("page3");
});

it("retains pagination history when selecting an attempt on the current page", async () => {
  const { view, router } = await setup("/attempt-selection");
  for (let i = 0; i < 2; i++)
    await view.getByRole("button", { name: "Next", exact: true }).click();
  await router.navigate("/attempt-selection?cursor=page3&attempt=attempt42");
  await view.getByRole("button", { name: /^(Previous|First page)$/ }).click();
  await expect
    .element(view.getByTestId("Pages cursor"))
    .toHaveTextContent("page2");
  expect(router.state.location.search).toContain("attempt=attempt42");
});

it("uses First page for an unknown direct cursor and remembers only observed predecessors", async () => {
  const { view } = await setup("/direct-cursor?cursor=page40");
  await expect
    .element(view.getByRole("button", { name: "First page", exact: true }))
    .toBeEnabled();
  await view.getByRole("button", { name: "Next", exact: true }).click();
  await view.getByRole("button", { name: "Previous", exact: true }).click();
  await expect
    .element(view.getByTestId("Pages cursor"))
    .toHaveTextContent("page40");
  await view.getByRole("button", { name: "First page", exact: true }).click();
  await expect
    .element(view.getByTestId("Pages cursor"))
    .toHaveTextContent("first");
});

it("isolates filters and page sizes while keeping browser history usable after a reset", async () => {
  const { view, router } = await setup("/query-history?queue=billing");
  for (let i = 0; i < 2; i++)
    await view.getByRole("button", { name: "Next", exact: true }).click();
  await view.getByRole("combobox", { name: "Rows" }).selectOptions("50");
  await expect
    .element(view.getByTestId("Pages cursor"))
    .toHaveTextContent("first");
  await router.navigate(-1);
  await expect
    .element(view.getByTestId("Pages cursor"))
    .toHaveTextContent("page3");
  await view.getByRole("button", { name: "Previous", exact: true }).click();
  await expect
    .element(view.getByTestId("Pages cursor"))
    .toHaveTextContent("page2");
  await router.navigate("/query-history?queue=other&cursor=page3");
  await view.getByRole("button", { name: "First page", exact: true }).click();
  await expect
    .element(view.getByTestId("Current URL"))
    .toHaveTextContent("/query-history?queue=other");
  await view.getByRole("button", { name: "Next", exact: true }).click();
  await view.getByRole("button", { name: "Reset pages", exact: true }).click();
  await expect
    .element(view.getByTestId("Pages cursor"))
    .toHaveTextContent("first");
  await expect
    .element(view.getByRole("button", { name: "First page", exact: true }))
    .toBeDisabled();
});

it("keeps paired pagers independent when the other pager changes cursor or size", async () => {
  const { view, router } = await setup("/paired-history", true);
  const alpha = view.getByRole("group", { name: "Alpha", exact: true });
  const beta = view.getByRole("group", { name: "Beta", exact: true });
  for (let i = 0; i < 3; i++)
    await alpha.getByRole("button", { name: "Next", exact: true }).click();
  await beta.getByRole("button", { name: "Next", exact: true }).click();
  await beta.getByRole("button", { name: "Next", exact: true }).click();
  await router.navigate(-1);
  await expect
    .element(view.getByTestId("Beta cursor"))
    .toHaveTextContent("page2");
  await beta.getByRole("combobox", { name: "Rows" }).selectOptions("50");
  await alpha.getByRole("button", { name: "Previous", exact: true }).click();
  await expect
    .element(view.getByTestId("Alpha cursor"))
    .toHaveTextContent("page3");
  await expect
    .element(view.getByTestId("Beta cursor"))
    .toHaveTextContent("first");
});

it("bounds remembered predecessors and safely falls back after eviction", async () => {
  const { view, router } = await setup("/bounded-pages");
  for (let i = 0; i < 34; i++)
    await view.getByRole("button", { name: "Next", exact: true }).click();
  await router.navigate("/bounded-pages?cursor=page2");
  await view.getByRole("button", { name: "First page", exact: true }).click();
  await expect
    .element(view.getByTestId("Pages cursor"))
    .toHaveTextContent("first");
  for (let i = 0; i < 65; i++) {
    await router.navigate(`/bounded-queries?queue=${i}`);
    await view.getByRole("button", { name: "Next", exact: true }).click();
  }
  await router.navigate("/bounded-queries?queue=0&cursor=page2");
  await expect
    .element(view.getByRole("button", { name: "First page", exact: true }))
    .toBeEnabled();
}, 30000);

it("preserves filtered list return state through detail tabs and pagination", async () => {
  const returnTo = "/executions?queue=billing&state=queued&cursor=opaque";
  const state = { returnTo, extra: "retained" };
  const { view, router } = await setup(
    "/executions/task_navigation",
    false,
    state,
  );
  const back = view.getByRole("link", {
    name: "Back to executions",
    exact: true,
  });
  await expect.element(back).toHaveAttribute("href", returnTo);
  await view.getByRole("button", { name: "Input", exact: true }).click();
  await expect.element(back).toHaveAttribute("href", returnTo);
  await view.getByRole("button", { name: "Next", exact: true }).click();
  await expect.element(back).toHaveAttribute("href", returnTo);
  await view.getByRole("button", { name: "Previous", exact: true }).click();
  await view.getByRole("combobox", { name: "Rows" }).selectOptions("50");
  await view.getByRole("button", { name: "Reset pages", exact: true }).click();
  expect(router.state.location.state).toEqual(state);
  await back.click();
  await expect
    .element(view.getByTestId("Current URL"))
    .toHaveTextContent(returnTo);
});

import { afterEach, expect, it, vi } from "vitest";
import { render } from "vitest-browser-react";
import { MemoryRouter, Route, Routes, useLocation } from "react-router";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import fixtureSource from "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v2.json?raw";
import { decodeConfig } from "../../src/api/codecs";
import { parseUserJson, stringifyUserJson } from "../../src/api/json";
import * as dto from "../../src/api/resources";
import { InstanceContext } from "../../src/app/instance";
import {
  AgentsPage,
  AgentDetailPage,
  ProgramVersionPage,
} from "../../src/features/catalog";
import { WorkersPage, WorkerDetailPage } from "../../src/features/workers";
import "../../src/styles/global.css";

const fixture = parseUserJson(fixtureSource, 2 * 1024 * 1024);
function field(key: string): unknown {
  if (!fixture || typeof fixture !== "object" || !(key in fixture))
    throw new Error(`Missing Rust fixture: ${key}`);
  return Reflect.get(fixture, key);
}
const config = decodeConfig(field("config"));
function catalog() {
  const page = dto.programPage(field("programs"));
  return {
    ...page,
    items: page.items.map((program) => ({
      program,
      kinds: [program.metadata.kind ?? "unspecified"],
    })),
  };
}
const clients: QueryClient[] = [];
afterEach(() => {
  for (const client of clients) client.clear();
  clients.length = 0;
  vi.restoreAllMocks();
});
function response(value: unknown, status = 200) {
  return new Response(stringifyUserJson(value), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Ledgence-Console-Contract": "2",
      "Ledgence-Instance-Id": config.instance_id,
      "Request-Id": "req_console_test",
    },
  });
}
function Destination() {
  const location = useLocation();
  return <p>Draft form {location.search}</p>;
}
async function mount(route: string) {
  const client = new QueryClient({
    defaultOptions: {
      queries: { retry: false, refetchOnWindowFocus: false },
      mutations: { retry: false },
    },
  });
  clients.push(client);
  return render(
    <QueryClientProvider client={client}>
      <InstanceContext.Provider value={config}>
        <MemoryRouter initialEntries={[route]}>
          <Routes>
            <Route path="/agents" element={<AgentsPage />} />
            <Route path="/programs/:programId" element={<AgentDetailPage />} />
            <Route
              path="/programs/:programId/versions/:version"
              element={<ProgramVersionPage />}
            />
            <Route path="/agents/:programId" element={<AgentDetailPage />} />
            <Route
              path="/agents/:programId/versions/:version"
              element={<ProgramVersionPage />}
            />
            <Route path="/workers" element={<WorkersPage />} />
            <Route
              path="/workers/:workerSessionId"
              element={<WorkerDetailPage />}
            />
            <Route path="/executions/new" element={<Destination />} />
          </Routes>
        </MemoryRouter>
      </InstanceContext.Provider>
    </QueryClientProvider>,
  );
}

it("loads catalog summaries once and navigates exact opaque versions without submitting work", async () => {
  const paths: string[] = [];
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
    paths.push(String(input));
    expect(init?.method).toBe("GET");
    const path = new URL(String(input), location.origin).pathname;
    if (path === "/v1/console/programs/catalog") return response(catalog());
    if (path === "/v1/console/programs/versions")
      return response(field("program_versions"));
    if (path === "/v1/console/programs/inspect")
      return response(field("program_detail"));
    throw new Error(`Unexpected path ${path}`);
  });
  const page = await mount("/agents");
  await expect
    .element(page.getByText("1 registered version", { exact: true }))
    .toBeVisible();
  expect(paths).toHaveLength(1);
  await page.getByRole("link", { name: "View versions" }).click();
  await page.getByRole("link", { name: "release-a", exact: true }).click();
  await expect
    .element(page.getByText("4096 bytes", { exact: true }))
    .toBeVisible();
  await expect
    .element(page.getByText("Configured program store", { exact: true }))
    .toBeVisible();
  await page.getByRole("link", { name: "Use in an execution" }).click();
  await expect
    .element(
      page.getByText("Draft form ?program=invoice-issuer&version=release-a", {
        exact: true,
      }),
    )
    .toBeVisible();
  expect(paths).toHaveLength(3);
});

it("freezes an uncertain registration and retries exactly the same bytes", async () => {
  const bodies: string[] = [];
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
    if (
      new URL(String(input), location.origin).pathname ===
      "/v1/console/programs/register"
    ) {
      expect(init?.method).toBe("POST");
      bodies.push(String(init?.body));
      return bodies.length === 1
        ? response({}, 503)
        : response(field("program_receipt"));
    }
    return response(catalog());
  });
  const page = await mount("/agents");
  await page
    .getByRole("button", { name: "Register program", exact: true })
    .click();
  await page
    .getByRole("textbox", { name: "Program ID", exact: true })
    .fill("invoice-issuer");
  await page
    .getByRole("textbox", { name: "Exact version", exact: true })
    .fill("release-a");
  await page
    .getByRole("button", { name: "Register reference", exact: true })
    .click();
  await expect
    .element(page.getByText("The server returned HTTP 503.", { exact: true }))
    .toBeVisible();
  expect(bodies).toHaveLength(1);
  await expect
    .element(page.getByRole("textbox", { name: "Program ID", exact: true }))
    .toBeDisabled();
  await page.getByRole("button", { name: "Try again", exact: true }).click();
  await expect
    .element(page.getByRole("link", { name: "Inspect registered version" }))
    .toBeVisible();
  expect(bodies).toHaveLength(2);
  expect(bodies[1]).toBe(bodies[0]);
  expect(JSON.parse(bodies[0] ?? "{}")).toEqual({
    program: { id: "invoice-issuer", version: "release-a" },
    metadata: { display_name: null, description: null, kind: "unspecified" },
    update_metadata: false,
  });
});

it("does not turn an API error into an empty catalog", async () => {
  vi.spyOn(globalThis, "fetch").mockImplementation(async () =>
    response({}, 503),
  );
  const page = await mount("/agents");
  await expect.element(page.getByRole("alert")).toBeVisible();
  await expect
    .element(page.getByText("No registered agents", { exact: true }))
    .not.toBeInTheDocument();
  await expect
    .element(page.getByText("req_console_test", { exact: true }))
    .toBeVisible();
});

it("retains stale worker observations and separates consumers, slots and session expiry", async () => {
  const detail = dto.workerDetail(field("worker_detail"));
  detail.worker.freshness = "stale";
  detail.worker.session_expired = true;
  detail.worker.received_at = detail.slots.observed_at - 20000;
  let reads = 0;
  vi.spyOn(globalThis, "fetch").mockImplementation(async () =>
    ++reads === 1 ? response(detail) : response({}, 503),
  );
  const page = await mount("/workers/ws_demo");
  await expect
    .element(page.getByText("Session expired", { exact: true }))
    .toBeVisible();
  await expect
    .element(page.getByText("Active consumers", { exact: true }))
    .toBeVisible();
  await expect
    .element(page.getByText("Occupied process slots", { exact: true }))
    .toBeVisible();
  await expect
    .element(page.getByText("· 20 s before this observation", { exact: true }))
    .toBeVisible();
  await expect
    .element(
      page.getByRole("button", { name: "Inspect process slot 2: empty" }),
    )
    .toBeVisible();
  await page
    .getByRole("button", { name: "Inspect process slot 1: warm" })
    .click();
  await expect.element(page.getByRole("dialog")).toBeVisible();
  await expect
    .element(page.getByText("process_1", { exact: true }))
    .toBeVisible();
  await expect
    .element(page.getByRole("link", { name: "Open observed execution" }))
    .not.toBeInTheDocument();
  await page.getByRole("button", { name: "Close dialog" }).click();
  await expect
    .element(page.getByRole("button", { name: "Inspect process slot 1: warm" }))
    .toHaveFocus();
  await page.getByRole("button", { name: "Refresh", exact: true }).click();
  await expect
    .element(
      page.getByText("Showing the last successful observation", {
        exact: true,
      }),
    )
    .toBeVisible();
  await expect
    .element(page.getByRole("button", { name: "Inspect process slot 1: warm" }))
    .toBeVisible();
});

it("does not fabricate slots for unsupported or legacy reporting", async () => {
  const detail = dto.workerDetail(field("worker_detail"));
  detail.worker.detail_state = "unsupported_capacity";
  detail.worker.capacity = 1025;
  detail.slots.items = [];
  vi.spyOn(globalThis, "fetch").mockImplementation(async () =>
    response(detail),
  );
  const page = await mount("/workers/ws_demo");
  await expect
    .element(
      page.getByText("Detailed reporting is unsupported", { exact: false }),
    )
    .toBeVisible();
  await expect
    .element(
      page.getByRole("button", { name: "Inspect process slot", exact: false }),
    )
    .not.toBeInTheDocument();
});

it("suppresses unvalidated task links and preserves a selected slot through refresh", async () => {
  const detail = dto.workerDetail(field("worker_detail"));
  const slot = detail.slots.items[0];
  if (!slot) throw new Error("Missing fixture slot");
  slot.state = "executing";
  slot.task_id = "unverified_task";
  slot.attempt_id = "unverified_attempt";
  slot.link_diagnostic = "authority_mismatch";
  vi.spyOn(globalThis, "fetch").mockImplementation(async () =>
    response(detail),
  );
  const page = await mount("/workers/ws_demo");
  await page
    .getByRole("button", { name: "Inspect process slot 1: executing" })
    .click();
  await expect
    .element(
      page.getByText(
        "The reported execution association could not be validated.",
        { exact: false },
      ),
    )
    .toBeVisible();
  await expect
    .element(page.getByRole("link", { name: "Open observed execution" }))
    .not.toBeInTheDocument();
  await expect
    .element(page.getByRole("link", { name: "Inspect observed attempt" }))
    .not.toBeInTheDocument();
  // A new full report reuses the stable slot position, not the old process ID.
  slot.process_instance_id = "replacement_process";
  slot.process_id = 9001;
  detail.worker.snapshot_sequence = "9007199254740994";
  await clients.at(-1)?.invalidateQueries();
  await expect.element(page.getByRole("dialog")).toBeVisible();
  await expect
    .element(page.getByText("replacement_process", { exact: true }))
    .toBeVisible();
  await expect
    .element(page.getByText("process_1", { exact: true }))
    .not.toBeInTheDocument();
});

it("applies an exact worker queue and resets pagination before its single summary query", async () => {
  const paths: URL[] = [];
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input) => {
    paths.push(new URL(String(input), location.origin));
    return response(field("workers"));
  });
  const page = await mount(
    "/workers?cursor=previous-page&previous=start&limit=25",
  );
  await expect
    .element(
      page.getByRole("link", { name: "Local Python worker", exact: true }),
    )
    .toBeVisible();
  expect(paths).toHaveLength(1);
  await page
    .getByRole("combobox", { name: "Exact queue", exact: true })
    .fill("unlisted/queue");
  await page.getByRole("button", { name: "Apply filter", exact: true }).click();
  await expect.poll(() => paths.length).toBe(2);
  expect(paths[1]?.searchParams.get("queue")).toBe("unlisted/queue");
  expect(paths[1]?.searchParams.has("cursor")).toBe(false);
  expect(paths[1]?.searchParams.has("previous")).toBe(false);
  expect(paths[1]?.pathname).toBe("/v1/console/workers");
});

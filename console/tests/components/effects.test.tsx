// SPDX-License-Identifier: MIT
import { afterEach, expect, it, vi } from "vitest";
import { cleanup, render } from "vitest-browser-react";
import { createMemoryRouter, RouterProvider } from "react-router";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { providerFixture } from "../provider-fixture";
import source from "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v11.json?raw";
import { decodeConfig } from "../../src/api/codecs";
import { parseUserJson, stringifyUserJson } from "../../src/api/json";
import {
  accessController,
  type AccessCapability,
} from "../../src/app/access-state";
import { InstanceContext } from "../../src/app/instance";
import { EffectReviewPage, EffectsPage } from "../../src/features/effects";
import { effectDetailPath } from "../../src/api/effects";
import {
  effectAddress,
  effectScope,
  effectFixture,
  effectWire,
} from "../effect-fixture";
const fixtures = parseUserJson(source, 2 * 1024 * 1024) as Record<
  string,
  unknown
>;
const config = decodeConfig(fixtures.config);
config.polling.lists_ms = 60_000;
const clients: QueryClient[] = [];
const routers: ReturnType<typeof createMemoryRouter>[] = [];
afterEach(async () => {
  await cleanup();
  clients.splice(0).forEach((client) => client.clear());
  routers.splice(0).forEach((router) => router.dispose());
  accessController.begin("discard", true);
  vi.restoreAllMocks();
});
function reply(value: unknown, status = 200) {
  return new Response(stringifyUserJson(value), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Ledgence-Console-Contract": "11",
      "Ledgence-Instance-Id": config.instance_id,
    },
  });
}
function access(
  capabilities: AccessCapability[] = [
    "metadata_read",
    "sensitive_read",
    "effect_resolve",
  ],
  native = true,
) {
  accessController.accept(
    {
      mode: "credentials",
      actor_key: "operator",
      issuer: "local",
      subject: "Reader",
      class: "operator",
      instance_id: config.instance_id,
      scope: effectScope,
      capabilities,
      expires_at_ms: Date.now() + 60_000,
      operator_audit: true,
      native_effect_resolution: native,
    },
    accessController.begin("authenticate"),
    config.instance_id,
  );
}
function receipt(body: string) {
  const command = JSON.parse(body);
  return {
    occurrence: { scope: effectScope, ...command.occurrence },
    resolution_id: command.resolution_id,
    accepted_at: 25,
    accepted_revision: String(BigInt(command.expected_effect_revision) + 1n),
    basis_revision: command.expected_effect_revision,
    supersedes_resolution_id: command.supersedes_resolution_id,
    assessment_kind: command.assessment.kind,
    acceptance_event_id: "audit-resolution",
    already_accepted: true,
  };
}
async function mount(
  options: {
    capabilities?: AccessCapability[];
    native?: boolean;
    list?: boolean;
    resolve?: (body: string, index: number) => Response | Promise<Response>;
    auditFailure?: boolean;
    detail?: ReturnType<typeof effectFixture>;
  } = {},
) {
  access(options.capabilities, options.native);
  let detail = options.detail ?? effectFixture();
  const writes: string[] = [],
    reads: string[] = [],
    audits: unknown[] = [];
  vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
    const path = String(input);
    reads.push(path);
    if (path.endsWith("/approved-effects/inspect"))
      return reply(effectWire(detail));
    if (path.endsWith("/approved-effects/inbox"))
      return reply(effectWire({ items: [detail.summary], next_cursor: null }));
    if (path.endsWith("/approved-effects/resolve")) {
      const body = String(init?.body);
      writes.push(body);
      return options.resolve
        ? options.resolve(body, writes.length)
        : reply(receipt(body));
    }
    if (path.endsWith("/audit/events")) {
      audits.push(JSON.parse(String(init?.body)));
      if (options.auditFailure) return reply({}, 503);
      const accepted = receipt(writes.at(-1)!);
      return reply({
        items: [
          {
            event_id: accepted.acceptance_event_id,
            recorded_at: accepted.accepted_at,
            audience: {
              authority_id: "authority",
              instance_id: config.instance_id,
              scope: effectScope,
            },
            actor: {
              issuer: "local",
              subject: "Original assessor",
              class: "operator",
              credential_id: "credential-1",
            },
            operation: {
              kind: "effect_resolved",
              occurrence: accepted.occurrence,
              resolution_id: accepted.resolution_id,
              accepted_revision: accepted.accepted_revision,
              basis_revision: accepted.basis_revision,
              supersedes_resolution_id: accepted.supersedes_resolution_id,
              assessment_kind: accepted.assessment_kind,
            },
          },
        ],
        next_cursor: null,
      });
    }
    throw Error("Unexpected route " + path);
  });
  const client = new QueryClient({
    defaultOptions: {
      queries: { retry: false, refetchOnWindowFocus: false },
      mutations: { retry: false },
    },
  });
  clients.push(client);
  const router = createMemoryRouter(
    [
      { path: "/effects", element: <EffectsPage /> },
      { path: "/effects/review", element: <EffectReviewPage /> },
      { path: "/workflows/:id", element: <p>Original graph view</p> },
    ],
    {
      initialEntries: [
        {
          pathname: options.list ? "/effects" : "/effects/review",
          search: options.list
            ? "?filter=unconfirmed"
            : effectDetailPath(effectAddress).split("?")[1]!,
          state: {
            returnTo: "/workflows/wf-effect?tab=Graph&node=saved",
            returnNavigationKey: "original-graph",
          },
        },
      ],
    },
  );
  routers.push(router);
  const view = await render(
    <QueryClientProvider client={client}>
      <InstanceContext.Provider value={config}>
        <RouterProvider router={router} />
      </InstanceContext.Provider>
    </QueryClientProvider>,
  );
  return {
    view,
    router,
    writes,
    reads,
    audits,
    client,
    changeDetail(value: typeof detail) {
      detail = value;
    },
  };
}
async function prepare(view: Awaited<ReturnType<typeof mount>>["view"]) {
  await view
    .getByRole("textbox", { name: "Reason", exact: true })
    .fill("Matched the provider receipt");
  await view
    .getByRole("textbox", { name: "Evidence references", exact: true })
    .fill("receipt:1042");
  await view
    .getByRole("button", { name: "Review assessment", exact: true })
    .click();
}
async function close(view: Awaited<ReturnType<typeof mount>>["view"]) {
  await view
    .getByRole("dialog")
    .getByRole("button", { name: "Close", exact: true })
    .click();
}
it("records the reviewed lossless JSON and separately verifies exact acceptance attribution", async () => {
  const { view, writes, audits, client } = await mount();
  await view
    .getByRole("textbox", { name: "Result JSON" })
    .fill('{"id":18446744073709551615,"amount":1.0}');
  await prepare(view);
  expect(writes).toHaveLength(0);
  await view
    .getByRole("button", { name: "Record assessment", exact: true })
    .click();
  await expect.poll(() => writes.length).toBe(1);
  await close(view);
  await expect
    .element(view.getByRole("heading", { name: "Assessment recorded" }))
    .toBeVisible();
  await expect
    .element(view.getByText("Original assessor", { exact: true }))
    .toBeVisible();
  expect(writes[0]).toContain('"id":18446744073709551615');
  expect(writes[0]).toContain('"amount":1.0');
  expect(audits).toEqual([
    {
      filter: { kind: "event", event_id: "audit-resolution" },
      cursor: null,
      limit: 2,
    },
  ]);
  await client.invalidateQueries();
  expect(audits).toHaveLength(1);
});
it("retries exact bytes after a lost reply even if evidence changes during manual refresh", async () => {
  const { view, writes, changeDetail } = await mount({
    resolve: (body, index) =>
      index === 1 ? reply({}, 503) : reply(receipt(body)),
  });
  await prepare(view);
  await view
    .getByRole("button", { name: "Record assessment", exact: true })
    .click();
  await expect.poll(() => writes.length).toBe(1);
  await close(view);
  await expect
    .element(view.getByText(/Acceptance is unconfirmed/))
    .toBeVisible();
  const newer = effectFixture();
  newer.summary.effect_revision = "3";
  changeDetail(newer);
  await view
    .getByRole("region", { name: "Approved effect binding" })
    .getByRole("button", { name: "Refresh evidence" })
    .click();
  await view.getByRole("button", { name: "Retry same assessment" }).click();
  await expect
    .element(view.getByRole("heading", { name: "Assessment recorded" }))
    .toBeVisible();
  expect(writes).toHaveLength(2);
  expect(writes[1]).toBe(writes[0]);
  expect(JSON.parse(writes[1]!).expected_effect_revision).toBe("1");
});
it("requires a deliberate new review after conflict and preserves the old command until then", async () => {
  const { view, writes, changeDetail } = await mount({
    resolve: () => reply({}, 409),
  });
  await prepare(view);
  await view
    .getByRole("button", { name: "Record assessment", exact: true })
    .click();
  await expect.poll(() => writes.length).toBe(1);
  await close(view);
  await expect
    .element(view.getByText(/server rejected this assessment/))
    .toBeVisible();
  const newer = effectFixture();
  newer.summary.effect_revision = "4";
  changeDetail(newer);
  await view
    .getByRole("button", { name: "Start a new review", exact: true })
    .click();
  await view
    .getByRole("button", { name: "Refresh and start new review" })
    .click();
  await prepare(view);
  await view
    .getByRole("button", { name: "Record assessment", exact: true })
    .click();
  await expect.poll(() => writes.length).toBe(2);
  expect(JSON.parse(writes[1]!).expected_effect_revision).toBe("4");
  expect(JSON.parse(writes[1]!).resolution_id).not.toBe(
    JSON.parse(writes[0]!).resolution_id,
  );
});
it("restores an uncertain command after navigating away and same-actor reauthentication", async () => {
  const { view, writes, router } = await mount({
    resolve: () => reply({}, 503),
  });
  await prepare(view);
  await view
    .getByRole("button", { name: "Record assessment", exact: true })
    .click();
  await expect.poll(() => writes.length).toBe(1);
  await close(view);
  await router.navigate("/workflows/wf-effect");
  await expect.element(view.getByText("Original graph view")).toBeVisible();
  access();
  await router.navigate(effectDetailPath(effectAddress));
  await expect
    .element(view.getByText(/earlier assessment is retained/))
    .toBeVisible();
  await view.getByRole("button", { name: "Retry same assessment" }).click();
  await expect.poll(() => writes.length).toBe(2);
  expect(writes[1]).toBe(writes[0]);
});
it.each([
  {
    capabilities: ["metadata_read", "sensitive_read"] as AccessCapability[],
    native: true,
  },
  {
    capabilities: [
      "metadata_read",
      "sensitive_read",
      "effect_resolve",
    ] as AccessCapability[],
    native: false,
  },
])(
  "disables assessment creation without the required privilege and capability %j",
  async (options) => {
    const { view, writes } = await mount(options);
    await expect
      .element(
        view.getByRole("button", { name: "Review assessment", exact: true }),
      )
      .toBeDisabled();
    expect(writes).toHaveLength(0);
  },
);
it("metadata-only inbox shows no payload and deep inspection performs no request without sensitive_read", async () => {
  const { view, router, reads } = await mount({
    list: true,
    capabilities: ["metadata_read"],
  });
  await expect
    .element(view.getByRole("table", { name: "Effects", exact: true }))
    .toBeVisible();
  await view.getByRole("link", { name: "program:refund", exact: true }).click();
  await expect
    .element(view.getByText(/Sign in with sensitive_read/))
    .toBeVisible();
  expect(reads.filter((path) => path.endsWith("/inspect"))).toHaveLength(0);
  await view.getByRole("button", { name: "Back", exact: true }).click();
  expect(router.state.location.pathname).toBe("/effects");
  expect(router.state.location.search).toBe("?filter=unconfirmed");
});
it("reviews retained evidence without requesting collected execution records and restores its graph location", async () => {
  const { view, reads, router } = await mount();
  await expect
    .element(view.getByRole("heading", { name: "program:refund", exact: true }))
    .toBeVisible();
  expect(
    reads.every((path) => path.endsWith("/approved-effects/inspect")),
  ).toBe(true);
  await view.getByRole("button", { name: "Back", exact: true }).click();
  expect(router.state.location.pathname + router.state.location.search).toBe(
    "/workflows/wf-effect?tab=Graph&node=saved",
  );
  expect(router.state.location.state).toEqual({
    restoreNavigationKey: "original-graph",
  });
});
it("keeps a confirmed assessment distinct from a failed audit lookup", async () => {
  const { view, writes, audits, client } = await mount({ auditFailure: true });
  await prepare(view);
  await view
    .getByRole("button", { name: "Record assessment", exact: true })
    .click();
  await expect.poll(() => writes.length).toBe(1);
  await close(view);
  await expect
    .element(view.getByRole("heading", { name: "Assessment recorded" }))
    .toBeVisible();
  await expect
    .element(view.getByText(/Verified attribution is unavailable/))
    .toBeVisible();
  await client.invalidateQueries();
  expect(audits).toHaveLength(1);
});

it("downloads the same destination-bound command before dispatch and after an uncertain reply", async () => {
  const { view, writes } = await mount({ resolve: () => reply({}, 503) });
  const blobs: Blob[] = [];
  vi.spyOn(URL, "createObjectURL").mockImplementation((value) => {
    if (!(value instanceof Blob)) throw new Error("Expected an export Blob");
    blobs.push(value);
    return "blob:effect-command";
  });
  vi.spyOn(URL, "revokeObjectURL").mockImplementation(() => {});
  vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => {});
  await view
    .getByRole("textbox", { name: "Result JSON" })
    .fill('{"value":18446744073709551615,"float":1.0}');
  await prepare(view);
  await view
    .getByRole("dialog")
    .getByRole("button", { name: "Download saved command" })
    .click();
  expect(writes).toHaveLength(0);
  await view
    .getByRole("button", { name: "Record assessment", exact: true })
    .click();
  await expect.poll(() => writes.length).toBe(1);
  await close(view);
  await view.getByRole("button", { name: "Download saved command" }).click();
  expect(blobs).toHaveLength(2);
  const first = await blobs[0]!.text(),
    second = await blobs[1]!.text();
  expect(second).toBe(first);
  const saved = JSON.parse(first);
  expect(Object.keys(saved)).toEqual(["format", "base_url", "body"]);
  expect(saved.base_url).toBe(globalThis.location.origin);
  expect(saved.format).toBe("ledgence.effect-resolution.v1");
  expect(saved.body).toContain('"expected_effect_revision":1');
  expect(saved.body).toContain('"value":18446744073709551615');
  expect(saved.body).toContain('"float":1.0');
  expect(JSON.parse(saved.body).occurrence.scope).toEqual(effectScope);
  expect(JSON.parse(saved.body).resolution_id).toBe(
    JSON.parse(writes[0]!).resolution_id,
  );
});

it("shows the approved provider target, allocated recovery budget and retained report", async () => {
  const { view, writes } = await mount({ detail: providerFixture() });
  await expect
    .element(
      view.getByRole("heading", { name: "Provider operation", exact: true }),
    )
    .toBeVisible();
  await expect
    .element(
      view.getByRole("heading", {
        name: "Current provider report",
        exact: true,
      }),
    )
    .toBeVisible();
  await expect
    .element(view.getByText("No local journal result", { exact: true }))
    .toBeVisible();
  await expect
    .element(view.getByText("Export ready to download", { exact: true }))
    .toBeVisible();
  await expect.element(view.getByText("0 / 12", { exact: true })).toBeVisible();
  expect(writes).toHaveLength(0);
});

it("adds provider state filters without fetching sensitive detail from the inbox", async () => {
  const { view, reads, router } = await mount({
    list: true,
    detail: providerFixture(),
  });
  await view
    .getByRole("combobox", { name: "Provider operation", exact: true })
    .selectOptions("pending");
  await view
    .getByRole("button", { name: "Apply filters", exact: true })
    .click();
  await expect
    .poll(() => router.state.location.search)
    .toContain("provider_filter=pending");
  expect(router.state.location.search).not.toContain("filter=unconfirmed");
  expect(reads.some((path) => path.endsWith("/approved-effects/inspect"))).toBe(
    false,
  );
  await expect
    .element(view.getByText("No local journal result", { exact: true }))
    .toBeVisible();
});

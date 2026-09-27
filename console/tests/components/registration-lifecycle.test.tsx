import { afterEach, expect, it, vi } from "vitest";
import { render } from "vitest-browser-react";
import { MemoryRouter } from "react-router";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import fixtureSource from "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v1.json?raw";
import { decodeConfig } from "../../src/api/codecs";
import { parseUserJson, stringifyUserJson } from "../../src/api/json";
import * as dto from "../../src/api/resources";
import { InstanceContext } from "../../src/app/instance";
import { AgentsPage } from "../../src/features/catalog";

const fixture = parseUserJson(fixtureSource, 2 * 1024 * 1024);
function field(key: string): unknown {
  if (!fixture || typeof fixture !== "object" || !(key in fixture))
    throw new Error(`Missing fixture: ${key}`);
  return Reflect.get(fixture, key);
}
const config = decodeConfig(field("config"));
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
      "Ledgence-Console-Contract": "1",
      "Ledgence-Instance-Id": config.instance_id,
    },
  });
}
async function mount() {
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
        <MemoryRouter>
          <AgentsPage />
        </MemoryRouter>
      </InstanceContext.Provider>
    </QueryClientProvider>,
  );
}
for (const uncertainFirst of [false, true]) {
  it(`allows another registration after success${uncertainFirst ? " without clearing a closed uncertain command" : ""}`, async () => {
    const bodies: string[] = [];
    vi.spyOn(globalThis, "fetch").mockImplementation(async (input, init) => {
      if (
        new URL(String(input), location.origin).pathname.endsWith(
          "/programs/register",
        )
      ) {
        bodies.push(String(init?.body));
        if (uncertainFirst && bodies.length === 1) return response({}, 503);
        const command = JSON.parse(String(init?.body)) as {
          program: { id: string; version: string };
        };
        const receipt = dto.programReceipt(field("program_receipt"));
        receipt.version.descriptor.program = command.program;
        receipt.version.manifest.program = command.program;
        return response(receipt);
      }
      return response(field("programs"));
    });
    const view = await mount();
    const open = () =>
      view.getByRole("button", { name: "Register agent", exact: true }).click();
    const close = () =>
      view.getByRole("button", { name: "Close dialog", exact: true }).click();
    await open();
    await view
      .getByRole("textbox", { name: "Program ID", exact: true })
      .fill("invoice-issuer");
    await view
      .getByRole("textbox", { name: "Exact version", exact: true })
      .fill("release-a");
    await view
      .getByRole("button", { name: "Register reference", exact: true })
      .click();
    if (uncertainFirst) {
      await expect
        .element(
          view.getByText("The server returned HTTP 503.", { exact: true }),
        )
        .toBeVisible();
      await close();
      await open();
      await expect
        .element(
          view.getByRole("textbox", { name: "Exact version", exact: true }),
        )
        .toBeDisabled();
      await view
        .getByRole("button", { name: "Try again", exact: true })
        .click();
      await expect.poll(() => bodies.length).toBe(2);
      expect(bodies[1]).toBe(bodies[0]);
    }
    await expect
      .element(
        view.getByRole("link", {
          name: "Inspect registered version",
          exact: true,
        }),
      )
      .toBeVisible();
    await close();
    await open();
    await expect
      .element(
        view.getByRole("textbox", { name: "Exact version", exact: true }),
      )
      .toBeEnabled();
    await view
      .getByRole("textbox", { name: "Exact version", exact: true })
      .fill("release-b");
    await view
      .getByRole("button", { name: "Register reference", exact: true })
      .click();
    await expect
      .element(
        view.getByRole("link", {
          name: "Inspect registered version",
          exact: true,
        }),
      )
      .toHaveAttribute("href", "/agents/invoice-issuer/versions/release-b");
    expect(bodies).toHaveLength(uncertainFirst ? 3 : 2);
  });
}

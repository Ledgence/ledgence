import { afterEach, expect, it, vi } from "vitest";
import { render } from "vitest-browser-react";
import { focusManager, onlineManager } from "@tanstack/react-query";
import source from "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v16.json?raw";
import { parseUserJson } from "../../src/api/json";
import { decodeConfig } from "../../src/api/codecs";
import { InstanceContext } from "../../src/app/instance";
import { accessController } from "../../src/app/access-state";
import { ExecutionProgress } from "../../src/features/execution-progress";
import type { ProgressRequest } from "../../src/api/progress";
import { cursorPosition, progressPage, taskTarget } from "../progress-fixture";
import "../../src/styles/global.css";
const raw = parseUserJson(source, 2 * 1024 * 1024) as { config: unknown };
const config = decodeConfig(raw.config);
function response(value: unknown, status = 200, instance = config.instance_id) {
  return new Response(JSON.stringify(value), {
    status,
    headers: {
      "Content-Type": "application/json",
      "Ledgence-Console-Contract": "16",
      "Ledgence-Instance-Id": instance,
    },
  });
}
function pending(signal: AbortSignal | null | undefined): Promise<Response> {
  return new Promise((_, reject) => {
    signal?.addEventListener("abort", () => reject(signal.reason), {
      once: true,
    });
    if (signal?.aborted) reject(signal.reason);
  });
}
function tree(id = taskTarget.id, instance = config.instance_id) {
  return (
    <InstanceContext.Provider value={{ ...config, instance_id: instance }}>
      <ExecutionProgress target={{ kind: "task", id }}>
        <p>Paginated fallback</p>
      </ExecutionProgress>
    </InstanceContext.Provider>
  );
}
afterEach(() => {
  vi.restoreAllMocks();
  focusManager.setFocused(undefined);
  onlineManager.setOnline(true);
  const epoch = accessController.begin();
  accessController.accept(
    { mode: "operator_trusted" },
    epoch,
    config.instance_id,
  );
});
it("pauses only observation and resumes the accepted cursor, then reads late cleanup on demand", async () => {
  const requests: {
    query: ProgressRequest;
    signal: AbortSignal | null | undefined;
  }[] = [];
  let completed = false;
  vi.spyOn(globalThis, "fetch").mockImplementation(async (_input, init) => {
    const query = JSON.parse(String(init?.body)) as ProgressRequest;
    requests.push({ query, signal: init?.signal });
    if (!query.cursor) return response(progressPage(query));
    if (!completed) return pending(init?.signal);
    const reason =
      cursorPosition(query.cursor) === 1n ? "succeeded" : "cleanup_confirmed";
    return response(progressPage(query, [reason], "succeeded"));
  });
  const view = await render(tree());
  await expect
    .element(view.getByText("submitted", { exact: true }))
    .toBeVisible();
  await expect.poll(() => requests.length).toBe(2);
  await view.getByRole("button", { name: "Pause updates" }).click();
  expect(requests[1]!.signal!.aborted).toBe(true);
  const count = requests.length;
  await new Promise((resolve) => setTimeout(resolve, 350));
  expect(requests).toHaveLength(count);
  completed = true;
  await view.getByRole("button", { name: "Resume updates" }).click();
  await expect
    .element(view.getByText("succeeded", { exact: true }))
    .toBeVisible();
  expect(cursorPosition(requests.at(-1)!.query.cursor)).toBe(1n);
  await expect
    .element(view.getByText("Caught up · execution finished", { exact: true }))
    .toBeVisible();
  await view.getByRole("button", { name: "Check for updates" }).click();
  await expect
    .element(view.getByText("cleanup confirmed", { exact: true }))
    .toBeVisible();
  expect(cursorPosition(requests.at(-1)!.query.cursor)).toBe(2n);
  expect(document.querySelectorAll(".progress-events li")).toHaveLength(3);
});
for (const condition of ["hidden", "offline"] as const) {
  it(`aborts while ${condition} and resumes from its accepted event`, async () => {
    const requests: {
      query: ProgressRequest;
      signal: AbortSignal | null | undefined;
    }[] = [];
    vi.spyOn(globalThis, "fetch").mockImplementation(async (_input, init) => {
      const query = JSON.parse(String(init?.body)) as ProgressRequest;
      requests.push({ query, signal: init?.signal });
      return query.cursor
        ? pending(init?.signal)
        : response(progressPage(query));
    });
    const view = await render(tree());
    await expect
      .element(view.getByText("submitted", { exact: true }))
      .toBeVisible();
    await expect.poll(() => requests.length).toBe(2);
    if (condition === "hidden") focusManager.setFocused(false);
    else onlineManager.setOnline(false);
    await expect.poll(() => requests[1]!.signal!.aborted).toBe(true);
    await new Promise((resolve) => setTimeout(resolve, 350));
    expect(requests).toHaveLength(2);
    if (condition === "hidden") focusManager.setFocused(true);
    else onlineManager.setOnline(true);
    await expect.poll(() => requests.length).toBe(3);
    expect(cursorPosition(requests[2]!.query.cursor)).toBe(1n);
    await view.unmount();
    expect(requests[2]!.signal!.aborted).toBe(true);
  });
}
it("aborts and clears the previous execution or instance before new data is accepted", async () => {
  const requests: {
    query: ProgressRequest;
    signal: AbortSignal | null | undefined;
  }[] = [];
  let instance = config.instance_id;
  vi.spyOn(globalThis, "fetch").mockImplementation(async (_input, init) => {
    const query = JSON.parse(String(init?.body)) as ProgressRequest;
    requests.push({ query, signal: init?.signal });
    if (query.cursor) return pending(init?.signal);
    const page = progressPage(query);
    page.events[0]!.attempt_id = `att_${query.target.id}_${instance}`;
    return response(page, 200, instance);
  });
  const view = await render(tree("first"));
  await expect
    .element(view.getByText(`Attempt att_first_${instance}`))
    .toBeVisible();
  await expect.poll(() => requests.length).toBe(2);
  await view.rerender(tree("second"));
  await expect
    .element(view.getByText(`Attempt att_second_${instance}`))
    .toBeVisible();
  await expect
    .element(view.getByText(`Attempt att_first_${instance}`))
    .not.toBeInTheDocument();
  expect(requests[1]!.signal!.aborted).toBe(true);
  await expect.poll(() => requests.length).toBe(4);
  instance = "instance_other";
  await view.rerender(tree("second", instance));
  await expect
    .element(view.getByText(`Attempt att_second_${instance}`))
    .toBeVisible();
  await expect
    .element(view.getByText(`Attempt att_second_${config.instance_id}`))
    .not.toBeInTheDocument();
  expect(requests[3]!.signal!.aborted).toBe(true);
});
it("discards events and cursor after a verified actor change", async () => {
  const actor = (subject: string) => ({
    mode: "credentials" as const,
    actor_key: subject,
    issuer: "local",
    subject,
    class: "operator" as const,
    instance_id: config.instance_id,
    scope: { tenant_id: "acme", namespace: "demo" },
    capabilities: ["metadata_read" as const],
    expires_at_ms: Date.now() + 60000,
  });
  let epoch = accessController.begin();
  accessController.accept(actor("reader_first"), epoch, config.instance_id);
  const calls: {
    cursor: string | null;
    actor: string;
    signal: AbortSignal | null | undefined;
  }[] = [];
  vi.spyOn(globalThis, "fetch").mockImplementation(async (_input, init) => {
    const query = JSON.parse(String(init?.body)) as ProgressRequest;
    const identity = new Headers(init?.headers).get("X-Ledgence-Actor")!;
    calls.push({ cursor: query.cursor, actor: identity, signal: init?.signal });
    if (query.cursor) return pending(init?.signal);
    const page = progressPage(query);
    page.events[0]!.attempt_id = identity;
    return response(page);
  });
  const view = await render(tree());
  await expect.element(view.getByText("Attempt reader_first")).toBeVisible();
  await expect.poll(() => calls.length).toBe(2);
  epoch = accessController.begin();
  accessController.accept(actor("reader_second"), epoch, config.instance_id);
  await expect.element(view.getByText("Attempt reader_second")).toBeVisible();
  await expect
    .element(view.getByText("Attempt reader_first"))
    .not.toBeInTheDocument();
  expect(calls[1]!.signal!.aborted).toBe(true);
  expect(calls[2]).toMatchObject({ cursor: null, actor: "reader_second" });
});
it("shows retirement explicitly without clearing accepted history or silently resetting its cursor", async () => {
  const calls: ProgressRequest[] = [];
  vi.spyOn(globalThis, "fetch").mockImplementation(async (_input, init) => {
    const query = JSON.parse(String(init?.body)) as ProgressRequest;
    calls.push(query);
    return calls.length === 1
      ? response(progressPage(query))
      : response({}, 404);
  });
  const view = await render(tree());
  await expect
    .element(view.getByText("Retained history is unavailable", { exact: true }))
    .toBeVisible();
  await expect
    .element(view.getByText("submitted", { exact: true }))
    .toBeVisible();
  await expect
    .element(view.getByRole("button", { name: "Check for updates" }))
    .toBeDisabled();
  await new Promise((resolve) => setTimeout(resolve, 350));
  expect(calls).toHaveLength(2);
  expect(cursorPosition(calls[1]!.cursor)).toBe(1n);
  await view.getByRole("button", { name: "Browse all history" }).click();
  await expect
    .element(view.getByText("Paginated fallback", { exact: true }))
    .toBeVisible();
});

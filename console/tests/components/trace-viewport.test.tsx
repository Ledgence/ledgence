// SPDX-License-Identifier: MIT
import { afterEach, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-react";
import { explorerNode, type ExplorerNode } from "../../src/api/explorer";
import { Trace } from "../../src/features/explorer/trace";
import { entrypoint } from "../explorer-fixture";
import "../../src/styles/global.css";
import "../../src/styles/explorer.css";

afterEach(async () => {
  await page.viewport(1280, 900);
});

function records(count: number): ExplorerNode[] {
  return Array.from({ length: count }, (_, index) => ({
    ...entrypoint(`activation_${index}`, `step_${index}`),
    submitted_at: 1000 + index * 10,
    terminal_at: 1010 + index * 10,
  }));
}

function trace(nodes: ExplorerNode[], follow = false) {
  return (
    <div className="execution-explorer explorer-maximized">
      <Trace
        nodes={nodes}
        observedAt={10000}
        workflowActive={false}
        selectedId={null}
        select={() => undefined}
        state={{ start: "", end: "", search: "", follow }}
        updateState={() => undefined}
      />
    </div>
  );
}

function viewport() {
  const element = document.querySelector<HTMLDivElement>(".timeline-viewport");
  if (!element) throw new Error("Trace viewport is not mounted");
  return element;
}

async function expectFilledViewport() {
  const element = viewport();
  await expect
    .poll(() => {
      const bounds = element.getBoundingClientRect();
      const rendered = Array.from(element.querySelectorAll('[role="listitem"]'))
        .map((row) => row.getBoundingClientRect())
        .sort((a, b) => a.top - b.top);
      const visible = rendered.filter(
        (row) => row.bottom > bounds.top && row.top < bounds.bottom,
      );
      return {
        topCovered: (rendered[0]?.top ?? Infinity) <= bounds.top,
        bottomCovered: (rendered.at(-1)?.bottom ?? -Infinity) >= bounds.bottom,
        contiguous: rendered.every(
          (row, index) =>
            index === 0 || row.top === rendered[index - 1]!.bottom,
        ),
        bounded: rendered.length <= visible.length + 8 && rendered.length < 100,
      };
    })
    .toEqual({
      topCovered: true,
      bottomCovered: true,
      contiguous: true,
      bounded: true,
    });
}

it("fills a tall trace after empty data, scrolling, and viewport resizing with bounded overscan", async () => {
  await page.viewport(1440, 2400);
  const view = await render(trace([]));
  await expect
    .element(view.getByText("No retained work is available in this trace."))
    .toBeVisible();
  await view.rerender(trace(records(100)));
  expect(viewport().clientHeight).toBeGreaterThan(16 * 86);
  await expectFilledViewport();

  viewport().scrollTop = 40 * 86 + 43;
  await expectFilledViewport();

  await page.viewport(1440, 900);
  await expect.poll(() => viewport().clientHeight).toBeLessThan(900);
  await expectFilledViewport();
  expect(viewport().scrollTop).toBe(40 * 86 + 43);

  await page.viewport(1440, 2800);
  await expect.poll(() => viewport().clientHeight).toBeGreaterThan(2000);
  await expectFilledViewport();
  expect(viewport().scrollTop).toBe(40 * 86 + 43);
});

it("keeps the trace filled at the latest activity after appends and viewport resizing", async () => {
  await page.viewport(1440, 2400);
  const view = await render(trace(records(100), true));
  const expectFollowing = async () => {
    await expect
      .poll(
        () =>
          viewport().scrollHeight -
          viewport().clientHeight -
          viewport().scrollTop,
      )
      .toBe(0);
    await expectFilledViewport();
  };
  await expectFollowing();

  await view.rerender(trace(records(101), true));
  await expectFollowing();
  await expect
    .element(view.getByRole("button", { name: "step_100", exact: false }))
    .toBeVisible();

  await page.viewport(1440, 900);
  await expect.poll(() => viewport().clientHeight).toBeLessThan(900);
  await expectFollowing();

  await page.viewport(1440, 2800);
  await expect.poll(() => viewport().clientHeight).toBeGreaterThan(2000);
  await expectFollowing();
});

it.each([
  ["failed", "failure"],
  ["cancelled", "neutral"],
] as const)(
  "distinguishes the %s observed call from its accepted durable result",
  async (state, tone) => {
    const local = explorerNode({
      kind: "local",
      id: "local:lookup",
      activation_id: "activation_main",
      entrypoint: "main",
      revision: "1",
      relations: [],
      key: "lookup",
      callable: "app.lookup",
      accepted_at: 1200,
      accepting_attempt_id: "accepted_attempt",
      observation: {
        state,
        attempt_id: "observed_attempt",
        started_at: 1310,
        elapsed_us: "195000",
      },
    });
    const view = await render(trace([local]));
    await expect
      .element(view.getByText("accepted", { exact: true }))
      .toBeVisible();
    await expect
      .element(view.getByText(`Call ${state} · main`, { exact: true }))
      .toBeVisible();
    const row = viewport().querySelector<HTMLButtonElement>(".timeline-row")!;
    expect(row.dataset.tone).toBe(tone);
    expect(row.querySelector<HTMLElement>(".status")!.dataset.tone).toBe(
      "success",
    );
    const bar = row.querySelector<HTMLElement>(".timeline-bar")!;
    const normalized = (token: string) => {
      const sample = document.createElement("span");
      sample.style.color = `var(--${token})`;
      row.append(sample);
      const color = getComputedStyle(sample).color;
      sample.remove();
      return color;
    };
    expect(getComputedStyle(bar).backgroundColor).toBe(
      normalized(state === "failed" ? "failure" : "muted"),
    );
    expect(getComputedStyle(row.querySelector(".status")!).color).toBe(
      normalized("success"),
    );
    const header = document.querySelector(".timeline-window")!.textContent;
    expect(header).toContain("310");
    expect(header).toContain("505");
    await expect
      .element(view.getByText("195 ms", { exact: true }))
      .toBeVisible();
  },
);

it("shows one timestamp and zero elapsed time for a single retained milestone", async () => {
  const at = 253402300799999;
  const fork = explorerNode({
    kind: "fork",
    id: "fork:work",
    activation_id: "activation_main",
    entrypoint: "main",
    revision: "1",
    relations: [],
    key: "work",
    branch_keys: ["branch"],
    accepted_at: at,
    accepting_attempt_id: "attempt",
  });
  const view = await render(trace([fork]));
  await expect.element(view.getByText("From window start")).toBeVisible();
  expect(
    document
      .querySelector(".execution-timeline")!
      .getAttribute("data-timeline-end"),
  ).toBe(String(at));
  expect(
    document.querySelector(".timeline-window time")!.getAttribute("datetime"),
  ).toBe(new Date(at).toISOString());
  expect(document.querySelector(".timeline-scale")!.textContent).toBe("0 ms");
  expect(document.querySelector(".timeline-milestone")).not.toBeNull();
  expect(document.querySelector(".timeline-bar")).toBeNull();
});

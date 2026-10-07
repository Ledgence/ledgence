// SPDX-License-Identifier: MIT
import { afterEach, expect, it } from "vitest";
import { page } from "vitest/browser";
import { render } from "vitest-browser-react";
import type { ExplorerNode } from "../../src/api/explorer";
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

// SPDX-License-Identifier: MIT
import { expect, it } from "vitest";
import { render } from "vitest-browser-react";
import { ReactFlow, type Node, type NodeProps } from "@xyflow/react";
import raw from "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v5.json?raw";
import { parseUserJson } from "../../src/api/json";
import { workflowExplorer, type ExplorerNode } from "../../src/api/explorer";
import { WorkCard } from "../../src/features/explorer/node-card";
import "@xyflow/react/dist/style.css";
import "../../src/styles/global.css";
import "../../src/styles/explorer.css";

const fixture = parseUserJson(raw, 2 * 1024 * 1024);
if (!fixture || typeof fixture !== "object")
  throw Error("Missing Rust fixture");
const parent = workflowExplorer(Reflect.get(fixture, "explorer"));
const cases = Reflect.get(fixture, "explorer_cases");
if (!cases || typeof cases !== "object")
  throw Error("Missing Rust explorer cases");
const branch = workflowExplorer(Reflect.get(cases, "branch_security"));
const joins = parent.page.items.filter(
  (node) => node.kind === "child_wait" && node.member_keys.length === 1,
);
const firstJoin = joins[0],
  secondJoin = joins[1];
if (
  !firstJoin ||
  firstJoin.kind !== "child_wait" ||
  !secondJoin ||
  secondJoin.kind !== "child_wait"
)
  throw Error("Canonical one-result joins missing");
const local = branch.page.items.find((node) => node.kind === "local");
if (!local || local.kind !== "local" || !local.observation)
  throw Error("Canonical observed local step missing");

type CardNode = Node<{ record: ExplorerNode }, "card">;
function Card({ data }: NodeProps<CardNode>) {
  return <WorkCard node={data.record} />;
}
const nodeTypes = { card: Card };
function Gallery({ records }: { records: ExplorerNode[] }) {
  const nodes: CardNode[] = records.map((record, index) => ({
    id: record.id,
    type: "card",
    position: { x: 24, y: 24 + index * 112 },
    data: { record },
    width: record.kind === "local" ? 212 : 200,
    height: record.kind === "local" ? 84 : 48,
  }));
  return (
    <div style={{ width: 640, height: 360 }}>
      <ReactFlow nodes={nodes} nodeTypes={nodeTypes} />
    </div>
  );
}

it("shows distinct resume destinations for joins with the same result count", async () => {
  const view = await render(<Gallery records={[firstJoin, secondJoin]} />);
  expect(firstJoin.resume).not.toBe(secondJoin.resume);
  await expect
    .element(view.getByText(firstJoin.resume, { exact: true }))
    .toBeVisible();
  await expect
    .element(view.getByText(secondJoin.resume, { exact: true }))
    .toBeVisible();
  expect(
    [...document.querySelectorAll(".work-card-gate-caption")].map(
      (caption) => caption.textContent,
    ),
  ).toEqual(["Join · 1 result", "Join · 1 result"]);
});

it("changes the visible join status marker when a resume is scheduled", async () => {
  // Hold back the canonical resume evidence to isolate the presentation update.
  const view = await render(
    <Gallery records={[{ ...firstJoin, resumed_activation_id: null }]} />,
  );
  await expect
    .element(view.getByLabelText("wait registered", { exact: true }))
    .toBeVisible();
  expect(
    document.querySelector(".work-card-status-mark .lucide-clock-3"),
  ).not.toBeNull();
  expect(
    document.querySelector(".work-card")?.getAttribute("data-status"),
  ).toBe("waiting");
  await view.rerender(<Gallery records={[firstJoin]} />);
  await expect
    .element(view.getByLabelText("resume scheduled", { exact: true }))
    .toBeVisible();
  await expect
    .element(view.getByLabelText("wait registered", { exact: true }))
    .not.toBeInTheDocument();
  expect(
    document.querySelector(".work-card-status-mark .lucide-arrow-right"),
  ).not.toBeNull();
  expect(
    document.querySelector(".work-card-status-mark .lucide-clock-3"),
  ).toBeNull();
  await expect
    .element(view.getByText(firstJoin.resume, { exact: true }))
    .toBeVisible();
});

it("replaces callable duration with Replay when an accepted local result is replayed", async () => {
  const view = await render(<Gallery records={[local]} />);
  await expect.element(view.getByText("9 ms", { exact: true })).toBeVisible();
  // A replay may carry an observation interval, but the callable did not run.
  await view.rerender(
    <Gallery
      records={[
        { ...local, observation: { ...local.observation!, state: "replayed" } },
      ]}
    />,
  );
  await expect.element(view.getByText("Replay", { exact: true })).toBeVisible();
  await expect
    .element(view.getByText("9 ms", { exact: true }))
    .not.toBeInTheDocument();
  await expect
    .element(view.getByText("accepted", { exact: true }))
    .toBeVisible();
  expect(
    document.querySelector(".work-card-duration")?.getAttribute("title"),
  ).toBe("Replay observation; the callable did not run");
});

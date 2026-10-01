// SPDX-License-Identifier: MIT
import type { ExplorerNode } from "../../api/explorer";
export type Point = { x: number; y: number };
export type Box = Point & { width: number; height: number };
export type NodeDimensions = Record<string, { width: number; height: number }>;
export type GraphPresentation = {
  scope: string;
  positions: Record<string, Point>;
  viewport?: { x: number; y: number; zoom: number };
};
export const nodeSize = { width: 236, height: 106 };
export function graphNodeSize(record: Pick<ExplorerNode, "kind">) {
  switch (record.kind) {
    case "child":
      return { width: 224, height: 100 };
    case "local":
      return { width: 212, height: 84 };
    case "entrypoint":
      return { width: 232, height: 64 };
    case "fork":
    case "child_wait":
      return { width: 200, height: 48 };
    case "external_wait":
      return { width: 224, height: 64 };
  }
}
export function intersects(a: Box, b: Box, gap = 0) {
  return (
    a.x < b.x + b.width + gap &&
    a.x + a.width + gap > b.x &&
    a.y < b.y + b.height + gap &&
    a.y + a.height + gap > b.y
  );
}

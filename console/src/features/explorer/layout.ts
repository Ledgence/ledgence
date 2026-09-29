// SPDX-License-Identifier: MIT
export type Point = { x: number; y: number };
export type Box = Point & { width: number; height: number };
export type GraphPresentation = {
  scope: string;
  positions: Record<string, Point>;
  viewport?: { x: number; y: number; zoom: number };
};
export const nodeSize = { width: 236, height: 106 };
export function intersects(a: Box, b: Box, gap = 0) {
  return (
    a.x < b.x + b.width + gap &&
    a.x + a.width + gap > b.x &&
    a.y < b.y + b.height + gap &&
    a.y + a.height + gap > b.y
  );
}

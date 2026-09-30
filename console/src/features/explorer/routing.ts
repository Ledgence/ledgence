// SPDX-License-Identifier: MIT
import { nodeSize, type Box, type NodeDimensions, type Point } from "./layout";

export function crossesBox(a: Point, b: Point, box: Box, padding = 6) {
  const left = box.x - padding,
    right = box.x + box.width + padding;
  const top = box.y - padding,
    bottom = box.y + box.height + padding;
  if (a.x === b.x)
    return (
      a.x > left &&
      a.x < right &&
      Math.max(a.y, b.y) > top &&
      Math.min(a.y, b.y) < bottom
    );
  if (a.y === b.y)
    return (
      a.y > top &&
      a.y < bottom &&
      Math.max(a.x, b.x) > left &&
      Math.min(a.x, b.x) < right
    );
  return true; // The router only emits orthogonal segments.
}
const length = (points: Point[]) =>
  points.reduce((sum, point, index) => {
    const before = points[index - 1];
    return (
      sum +
      (before ? Math.abs(before.x - point.x) + Math.abs(before.y - point.y) : 0)
    );
  }, 0);
function compact(points: Point[]) {
  const result: Point[] = [];
  for (const point of points) {
    const before = result.at(-1),
      prior = result.at(-2);
    if (before?.x === point.x && before.y === point.y) continue;
    if (
      prior &&
      before &&
      ((prior.x === before.x && before.x === point.x) ||
        (prior.y === before.y && before.y === point.y))
    )
      result.pop();
    result.push(point);
  }
  return result;
}
export function routeEdge(
  from: string,
  to: string,
  positions: Record<string, Point>,
  lane = 0,
  sizes?: NodeDimensions,
) {
  const source = positions[from],
    target = positions[to];
  if (!source || !target) return null;
  const sourceSize = sizes?.[from] ?? nodeSize,
    targetSize = sizes?.[to] ?? nodeSize;
  const start = {
    x: source.x + sourceSize.width / 2,
    y: source.y + sourceSize.height,
  };
  const end = { x: target.x + targetSize.width / 2, y: target.y };
  const gap = end.y - start.y;
  const lead = Math.min(18 + (lane % 4) * 6, gap > 12 ? gap / 2 : Infinity);
  const a = { x: start.x, y: start.y + lead },
    b = { x: end.x, y: end.y - lead };
  const boxes = Object.entries(positions).map(([id, p]) => ({
    ...p,
    ...(sizes?.[id] ?? nodeSize),
  }));
  const clear = (points: Point[]) =>
    points.every(
      (p, i) => !i || !boxes.some((box) => crossesBox(points[i - 1]!, p, box)),
    );
  const middle = (a.y + b.y) / 2;
  // Each primary path has the Manhattan lower-bound length. The first clear
  // one is therefore already shortest, with the same tie order as a stable
  // sort of every candidate. Avoid testing all obstacle corridors in this case.
  const primary: Point[][] = [
    [a, { x: a.x, y: middle }, { x: b.x, y: middle }, b],
    [a, { x: b.x, y: a.y }, b],
    [a, { x: a.x, y: b.y }, b],
  ];
  const direct = primary.find(clear);
  if (direct)
    return { points: compact([start, ...direct, end]), obstructed: false };
  const candidates = boxes
    .flatMap((box) => [
      box.x - 22 - (lane % 3) * 6,
      box.x + box.width + 22 + (lane % 3) * 6,
    ])
    .map((x) => [a, { x, y: a.y }, { x, y: b.y }, b]);
  const routed =
    candidates.filter(clear).sort((x, y) => length(x) - length(y))[0] ??
    findRoute(a, b, boxes);
  return {
    points: compact([start, ...(routed ?? [a, { x: a.x, y: b.y }, b]), end]),
    obstructed: !routed,
  };
}

// Fallback visibility grid for reconverging edges and manually moved nodes.
// Coordinates lie on obstacle corridors, not on a per-pixel canvas grid.
function findRoute(start: Point, end: Point, boxes: Box[]): Point[] | null {
  const xs = [
    ...new Set([
      start.x,
      end.x,
      ...boxes.flatMap((b) => [b.x - 18, b.x + b.width + 18]),
    ]),
  ].sort((a, b) => a - b);
  const ys = [
    ...new Set([
      start.y,
      end.y,
      ...boxes.flatMap((b) => [b.y - 18, b.y + b.height + 18]),
    ]),
  ].sort((a, b) => a - b);
  const width = xs.length;
  const index = (point: Point) =>
    ys.indexOf(point.y) * width + xs.indexOf(point.x);
  const point = (id: number): Point => ({
    x: xs[id % width]!,
    y: ys[Math.floor(id / width)]!,
  });
  const first = index(start),
    last = index(end);
  const distances = new Map<number, number>([[first, 0]]),
    previous = new Map<number, number>();
  const heap: { id: number; score: number }[] = [];
  const push = (id: number, score: number) => {
    heap.push({ id, score });
    let i = heap.length - 1;
    while (i > 0) {
      const parent = (i - 1) >> 1;
      if (heap[parent]!.score <= score) break;
      [heap[i], heap[parent]] = [heap[parent]!, heap[i]!];
      i = parent;
    }
  };
  const pop = () => {
    const first = heap[0]!;
    const tail = heap.pop()!;
    if (heap.length) {
      heap[0] = tail;
      let i = 0;
      while (true) {
        let child = i * 2 + 1;
        if (child >= heap.length) break;
        if (
          child + 1 < heap.length &&
          heap[child + 1]!.score < heap[child]!.score
        )
          child++;
        if (heap[i]!.score <= heap[child]!.score) break;
        [heap[i], heap[child]] = [heap[child]!, heap[i]!];
        i = child;
      }
    }
    return first.id;
  };
  const visited = new Set<number>();
  push(first, 0);
  while (heap.length) {
    const id = pop();
    if (visited.has(id)) continue;
    if (id === last) {
      const route = [point(id)];
      let cursor = id;
      while (previous.has(cursor)) {
        cursor = previous.get(cursor)!;
        route.push(point(cursor));
      }
      return route.reverse();
    }
    visited.add(id);
    const p = point(id),
      column = id % width,
      row = Math.floor(id / width);
    const next = [
      column > 0 ? id - 1 : -1,
      column < width - 1 ? id + 1 : -1,
      row > 0 ? id - width : -1,
      row < ys.length - 1 ? id + width : -1,
    ];
    for (const candidate of next) {
      if (candidate < 0 || visited.has(candidate)) continue;
      const q = point(candidate);
      if (boxes.some((box) => crossesBox(p, q, box))) continue;
      const distance =
        distances.get(id)! + Math.abs(p.x - q.x) + Math.abs(p.y - q.y);
      if (distance >= (distances.get(candidate) ?? Infinity)) continue;
      distances.set(candidate, distance);
      previous.set(candidate, id);
      push(candidate, distance + Math.abs(q.x - end.x) + Math.abs(q.y - end.y));
    }
  }
  return null;
}
export const edgePath = (points: Point[]) =>
  points.map((p, i) => `${i ? "L" : "M"}${p.x},${p.y}`).join(" ");

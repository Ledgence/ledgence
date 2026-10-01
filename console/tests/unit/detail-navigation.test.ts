// SPDX-License-Identifier: MIT
import { describe, expect, it } from "vitest";
import {
  detailDestination,
  resolveDetailNavigation,
  type DetailKind,
} from "../../src/features/detail-navigation";

describe("execution detail URL compatibility", () => {
  it.each([
    ["task", "Overview", "General", "identity"],
    ["task", "Input", "General", "input"],
    ["task", "Output", "General", "output"],
    ["task", "Result", "General", "output"],
    ["task", "Resources", "General", "resources"],
    ["task", "Attempts", "Trace", "attempts"],
    ["task", "History", "Trace", "history"],
    ["workflow", "Overview", "General", "identity"],
    ["workflow", "Context", "General", "identity"],
    ["workflow", "Input", "General", "input"],
    ["workflow", "Output", "General", "output"],
    ["workflow", "Result", "General", "output"],
    ["workflow", "Resources", "General", "resources"],
    ["workflow", "Advanced", "General", "history"],
    ["workflow", "History", "General", "history"],
    ["workflow", "Recorded work", "General", "work"],
    ["workflow", "Local steps", "General", "local"],
    ["workflow", "Waits", "General", "waits"],
  ])("maps %s %s to its preserved content", (kind, legacy, tab, section) => {
    const params = new URLSearchParams({
      tab: legacy,
      cursor: "opaque",
      attempt: "attempt-4",
      node: "node-3",
    });
    const result = resolveDetailNavigation(kind as DetailKind, params);
    expect([result.tab, result.section]).toEqual([tab, section]);
    expect(result.canonical.get("cursor")).toBe("opaque");
    expect(result.canonical.get("attempt")).toBe("attempt-4");
    expect(result.canonical.get("node")).toBe("node-3");
    expect(params.get("tab")).toBe(legacy);
    expect(
      resolveDetailNavigation(
        kind as DetailKind,
        result.canonical,
      ).canonical.toString(),
    ).toBe(result.canonical.toString());
  });

  it("keeps explicit Graph/Trace above stale view aliases and never offers a task graph", () => {
    expect(
      resolveDetailNavigation(
        "workflow",
        new URLSearchParams("tab=Graph&view=timeline"),
      ).tab,
    ).toBe("Graph");
    expect(
      resolveDetailNavigation(
        "workflow",
        new URLSearchParams("tab=Execution&view=timeline"),
      ).tab,
    ).toBe("Trace");
    expect(
      resolveDetailNavigation(
        "workflow",
        new URLSearchParams("view=trace"),
      ).canonical.get("view"),
    ).toBeNull();
    expect(
      resolveDetailNavigation("workflow", new URLSearchParams(), "Trace").tab,
    ).toBe("Trace");
    expect(
      resolveDetailNavigation("task", new URLSearchParams("tab=Graph")),
    ).toMatchObject({ tab: "Trace", section: "attempts" });
  });

  it.each(["constructor", "__proto__", "toString", "unknown"])(
    "handles unknown tab %s as a default view",
    (tab) => {
      expect(
        resolveDetailNavigation("task", new URLSearchParams({ tab })),
      ).toMatchObject({ tab: "Trace", section: "attempts" });
      expect(
        resolveDetailNavigation("workflow", new URLSearchParams({ tab })),
      ).toMatchObject({ tab: "Graph", section: null });
    },
  );

  it("isolates panel cursors while retaining the Explorer page and selection across General", () => {
    const params = new URLSearchParams(
      "tab=Trace&explorer_cursor=page3&node=selected&cursor=old&child_cursor=old-child&activation_previous=old-prev",
    );
    const general = detailDestination(params, "General", "resources");
    expect([...general.entries()]).toEqual([
      ["tab", "General"],
      ["explorer_cursor", "page3"],
      ["node", "selected"],
      ["section", "resources"],
    ]);
    const graph = detailDestination(general, "Graph");
    expect(graph.get("explorer_cursor")).toBe("page3");
    expect(graph.get("node")).toBe("selected");
    expect(graph.get("section")).toBeNull();
  });
});

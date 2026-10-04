// SPDX-License-Identifier: MIT
export type DetailKind = "task" | "workflow";
export type DetailTab = "Graph" | "Trace" | "General";

const taskAliases: Record<string, [DetailTab, string]> = {
  overview: ["General", "identity"],
  input: ["General", "input"],
  output: ["General", "output"],
  result: ["General", "output"],
  resources: ["General", "resources"],
  attempts: ["Trace", "attempts"],
  history: ["Trace", "history"],
};
const workflowAliases: Record<string, [DetailTab, string]> = {
  overview: ["General", "identity"],
  context: ["General", "identity"],
  input: ["General", "input"],
  output: ["General", "output"],
  result: ["General", "output"],
  resources: ["General", "resources"],
  advanced: ["General", "history"],
  "recorded work": ["General", "work"],
  waits: ["General", "waits"],
  approvals: ["General", "approvals"],
  "local steps": ["General", "local"],
  history: ["General", "history"],
};

/** Canonicalize old detail URLs without losing their cursor or selected item. */
export function resolveDetailNavigation(
  kind: DetailKind,
  params: URLSearchParams,
  workflowDefault: "Graph" | "Trace" = "Graph",
) {
  const requested = params.get("tab")?.toLowerCase();
  const aliases = kind === "task" ? taskAliases : workflowAliases;
  const alias =
    requested && Object.hasOwn(aliases, requested)
      ? aliases[requested]
      : undefined;
  let tab: DetailTab = kind === "task" ? "Trace" : workflowDefault;
  let section = params.get("section");
  if (alias) [tab, section] = alias;
  else if (requested === "general") tab = "General";
  else if (requested === "trace") tab = "Trace";
  else if (requested === "graph" && kind === "workflow") tab = "Graph";
  else if (kind === "workflow" && (!requested || requested === "execution")) {
    const view = params.get("view");
    if (view === "timeline" || view === "trace") tab = "Trace";
    else if (view === "graph") tab = "Graph";
  }
  const available =
    tab === "General"
      ? kind === "task"
        ? ["identity", "input", "output", "resources"]
        : [
            "identity",
            "input",
            "output",
            "resources",
            "approvals",
            "waits",
            "work",
            "local",
            "history",
          ]
      : kind === "task"
        ? ["attempts", "history"]
        : [];
  section =
    (available.length > 0 && section === "none") ||
    (section && available.includes(section))
      ? section
      : (available[0] ?? null);
  const canonical = new URLSearchParams(params);
  canonical.set("tab", tab);
  canonical.delete("view");
  if (section) canonical.set("section", section);
  else canonical.delete("section");
  return { tab, section, canonical };
}

/** Paging from a different panel must never become a cursor for another API. */
export function detailDestination(
  params: URLSearchParams,
  tab: DetailTab,
  section?: string | null,
) {
  const next = new URLSearchParams(params);
  next.set("tab", tab);
  next.delete("view");
  if (section) next.set("section", section);
  else next.delete("section");
  for (const key of [...next.keys()]) {
    // Graph and Trace share one Explorer page. General must not discard it.
    if (
      !key.startsWith("explorer_") &&
      (key.endsWith("cursor") || key.endsWith("previous"))
    )
      next.delete(key);
  }
  return next;
}

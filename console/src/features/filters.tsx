import { useSearchParams } from "react-router";
import { useInstance } from "../app/instance";
import { Button } from "../components/ui/button";
export function Filters({ kind }: { kind: "tasks" | "workflows" }) {
  const [params, set] = useSearchParams();
  const config = useInstance();
  const states =
    kind === "tasks"
      ? ["queued", "active", "succeeded", "failed", "cancelled"]
      : [
          "running",
          "waiting",
          "failing",
          "cancelling",
          "succeeded",
          "failed",
          "cancelled",
        ];
  function apply(form: HTMLFormElement) {
    const input = new FormData(form);
    const next = new URLSearchParams();
    if (kind === "tasks" && params.get("state"))
      next.set("state", params.get("state") ?? "");
    for (const key of [
      "state",
      "queue",
      "submitted_from",
      "submitted_until",
      "parent_workflow_id",
    ]) {
      const value = input.get(key);
      if (typeof value === "string" && value) {
        if (key.startsWith("submitted_")) {
          const at = Date.parse(value);
          if (Number.isFinite(at)) next.set(key, String(at));
        } else next.set(key, value);
      }
    }
    if (input.get("correlation_enabled"))
      next.set("correlation_key", String(input.get("correlation_key") ?? ""));
    if (input.get("root_only")) next.set("root_only", "true");
    next.set(
      "limit",
      params.get("limit") ?? String(config.limits.default_page_size),
    );
    set(next);
  }
  const dateValue = (key: string) => {
    const value = params.get(key);
    if (value === null) return "";
    const n = Number(value);
    if (!Number.isSafeInteger(n) || n < 0 || n > 253402300799999) return "";
    const date = new Date(n);
    return new Date(n - date.getTimezoneOffset() * 60000)
      .toISOString()
      .slice(0, 16);
  };
  return (
    <form
      key={params.toString()}
      className="filters"
      onSubmit={(e) => {
        e.preventDefault();
        apply(e.currentTarget);
      }}
    >
      {kind === "workflows" && (
        <label>
          Status
          <select name="state" defaultValue={params.get("state") ?? ""}>
            <option value="">All statuses</option>
            {states.map((s) => (
              <option key={s}>{s}</option>
            ))}
          </select>
        </label>
      )}
      {kind === "tasks" && (
        <label>
          Queue
          <input
            name="queue"
            defaultValue={params.get("queue") ?? ""}
            list="queue-suggestions"
          />
          <datalist id="queue-suggestions">
            {config.suggested_queues.map((q) => (
              <option key={q}>{q}</option>
            ))}
          </datalist>
        </label>
      )}
      <div>
        <label>
          Exact correlation
          <input
            name="correlation_key"
            defaultValue={params.get("correlation_key") ?? ""}
          />
        </label>
        <label className="check-label">
          <input
            type="checkbox"
            name="correlation_enabled"
            defaultChecked={params.has("correlation_key")}
          />
          Apply correlation, including empty
        </label>
      </div>
      <details>
        <summary>More filters</summary>
        <div className="filter-extra">
          <label>
            Submitted from
            <input
              name="submitted_from"
              type="datetime-local"
              defaultValue={dateValue("submitted_from")}
            />
          </label>
          <label>
            Submitted until (exclusive)
            <input
              name="submitted_until"
              type="datetime-local"
              defaultValue={dateValue("submitted_until")}
            />
          </label>
          {kind === "workflows" && (
            <>
              <label>
                Parent workflow ID
                <input
                  name="parent_workflow_id"
                  defaultValue={params.get("parent_workflow_id") ?? ""}
                />
              </label>
              <label className="check-label">
                <input
                  type="checkbox"
                  name="root_only"
                  defaultChecked={params.get("root_only") === "true"}
                />
                Root workflows only
              </label>
            </>
          )}
        </div>
      </details>
      <Button type="submit" variant="outline">
        Apply filters
      </Button>
      <Button
        type="button"
        variant="ghost"
        onClick={() => set(new URLSearchParams())}
      >
        Clear
      </Button>
    </form>
  );
}

export function ExecutionStatusTabs() {
  const [params, set] = useSearchParams();
  const current = params.get("state") ?? "";
  const states = [
    ["", "All"],
    ["active", "Active"],
    ["queued", "Queued"],
    ["failed", "Failed"],
    ["succeeded", "Succeeded"],
    ["cancelled", "Cancelled"],
  ] as const;
  return (
    <nav className="tabs status-tabs" aria-label="Execution status">
      {states.map(([value, label]) => (
        <Button
          key={value}
          variant="ghost"
          aria-pressed={current === value}
          onClick={() => {
            const next = new URLSearchParams(params);
            if (value) next.set("state", value);
            else next.delete("state");
            next.delete("cursor");
            next.delete("previous");
            set(next, { preventScrollReset: true });
          }}
        >
          {label}
        </Button>
      ))}
    </nav>
  );
}

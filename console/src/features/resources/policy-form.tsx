// SPDX-License-Identifier: MIT
import { useState } from "react";
import { useInstance } from "../../app/instance";
import {
  resourceKey,
  resourcePut,
  resourceReceipt,
  type ResourceSnapshot,
} from "../../api/resource-sharing";
import type { QueueScope } from "../../api/queue-policies";
import { useQueueCommand } from "../queues/use-queue-command";
import { PendingRequest } from "../queues/command";
import { Button } from "../../components/ui/button";

export function ResourcePolicyForm({
  name,
  scope,
  snapshot,
  onSaved,
  onClose,
}: {
  name: string;
  scope: QueueScope;
  snapshot: ResourceSnapshot | null;
  onSaved: (revision: number) => void;
  onClose: (reconcile: boolean) => void;
}) {
  const config = useInstance();
  // A background refresh must never replace the revision the operator opened.
  const [initial] = useState(snapshot?.resource ?? null);
  const policy = initial?.policy;
  const [rate, setRate] = useState(!!policy?.rate);
  const [weighted, setWeighted] = useState(!!policy?.fair_sharing);
  const [groups, setGroups] = useState(() =>
    (
      policy?.fair_sharing?.groups ?? [
        { name: "default", weight: 1, queues: [] },
      ]
    ).map((g) => ({
      id: crypto.randomUUID(),
      name: g.name,
      weight: String(g.weight),
      queues: g.queues.join("\n"),
    })),
  );
  const [defaultGroup, setDefaultGroup] = useState(
    policy?.fair_sharing?.default_group ?? "default",
  );
  const [error, setError] = useState("");
  const command = useQueueCommand(
    "resources/put",
    resourceKey(config.instance_id, name),
    resourcePut,
    (raw, cmd) => resourceReceipt(raw, scope, cmd),
    (r) => onSaved(r.resource.revision),
  );
  if (command.command)
    return (
      <PendingRequest
        identity={command.command.identity}
        body={command.command.body}
        pending={command.mutation.isPending}
        error={command.mutation.error}
        retry={command.retry}
        reset={() => {
          command.reset();
          onClose(true);
        }}
      />
    );
  function update(
    id: string,
    key: "name" | "weight" | "queues",
    value: string,
  ) {
    setGroups((rows) =>
      rows.map((g) => (g.id === id ? { ...g, [key]: value } : g)),
    );
  }
  return (
    <section className="card">
      <h2>{initial ? "Edit resource policy" : "Create resource policy"}</h2>
      <form
        className="form-grid"
        onSubmit={(e) => {
          e.preventDefault();
          setError("");
          try {
            const data = new FormData(e.currentTarget);
            const n = (key: string) => Number(data.get(key));
            const fair = weighted
              ? {
                  groups: groups.map((g) => ({
                    name: g.name,
                    weight: Number(g.weight),
                    queues: g.queues.split("\n").filter((q) => q.length),
                  })),
                  default_group: defaultGroup,
                }
              : undefined;
            command.send(
              resourcePut({
                name,
                request_id: crypto.randomUUID(),
                expected_revision: initial?.revision ?? null,
                policy: {
                  enabled: data.get("enabled") === "on",
                  max_occupancy: n("capacity"),
                  rate: rate
                    ? {
                        numerator: n("numerator"),
                        period_ms: n("period"),
                        burst: n("burst"),
                      }
                    : null,
                  offer_timeout_ms: n("offer"),
                  start_timeout_ms: n("start"),
                  ...(fair && { fair_sharing: fair }),
                },
                reason: String(data.get("reason") ?? "").trim(),
              }),
            );
          } catch (err) {
            setError(
              err instanceof Error ? err.message : "Invalid resource policy.",
            );
          }
        }}
      >
        <label>
          <input
            name="enabled"
            type="checkbox"
            defaultChecked={policy?.enabled ?? true}
          />{" "}
          Enable new offers
        </label>
        <label>
          Maximum occupancy
          <input
            name="capacity"
            type="number"
            min="1"
            max="1000000"
            required
            defaultValue={policy?.max_occupancy ?? 1}
          />
        </label>
        <label>
          Offer timeout (ms)
          <input
            name="offer"
            type="number"
            min="1000"
            max="300000"
            required
            defaultValue={policy?.offer_timeout_ms ?? 30000}
          />
        </label>
        <label>
          Start timeout (ms)
          <input
            name="start"
            type="number"
            min="100"
            max="30000"
            required
            defaultValue={policy?.start_timeout_ms ?? 5000}
          />
        </label>
        <label>
          <input
            type="checkbox"
            checked={rate}
            onChange={(e) => setRate(e.target.checked)}
          />{" "}
          Limit start rate
        </label>
        {rate && (
          <fieldset>
            <legend>Rate limit</legend>
            <label>
              Starts per period
              <input
                name="numerator"
                type="number"
                min="1"
                max="1000000"
                required
                defaultValue={policy?.rate?.numerator ?? 1}
              />
            </label>
            <label>
              Period (ms)
              <input
                name="period"
                type="number"
                min="1"
                max="86400000"
                required
                defaultValue={policy?.rate?.period_ms ?? 1000}
              />
            </label>
            <label>
              Burst
              <input
                name="burst"
                type="number"
                min="1"
                max="1000000"
                required
                defaultValue={policy?.rate?.burst ?? 1}
              />
            </label>
          </fieldset>
        )}
        {config.capabilities.resource_fair_sharing && (
          <label>
            <input
              type="checkbox"
              checked={weighted}
              onChange={(e) => setWeighted(e.target.checked)}
            />{" "}
            Use weighted queue groups
          </label>
        )}
        {weighted && (
          <fieldset>
            <legend>Weighted offer order</legend>
            <p>
              Groups take turns in the order shown. Each queue belongs to one
              group. Unmapped queues use the default group.
            </p>
            {groups.map((group, i) => (
              <fieldset key={group.id}>
                <legend>Group {i + 1}</legend>
                <label>
                  Group {i + 1} name
                  <input
                    required
                    value={group.name}
                    onChange={(e) => {
                      if (defaultGroup === group.name)
                        setDefaultGroup(e.target.value);
                      update(group.id, "name", e.target.value);
                    }}
                  />
                </label>
                <label>
                  Group {i + 1} weight
                  <input
                    type="number"
                    required
                    min="1"
                    max="1000"
                    value={group.weight}
                    onChange={(e) => update(group.id, "weight", e.target.value)}
                  />
                </label>
                <label>
                  Group {i + 1} queues (one per line)
                  <textarea
                    value={group.queues}
                    onChange={(e) => update(group.id, "queues", e.target.value)}
                  />
                </label>
                {i > 0 && (
                  <Button
                    type="button"
                    variant="outline"
                    onClick={() =>
                      setGroups((rows) => [
                        ...rows.slice(0, i - 1),
                        group,
                        rows[i - 1]!,
                        ...rows.slice(i + 1),
                      ])
                    }
                  >
                    Move group earlier
                  </Button>
                )}
                <Button
                  variant="outline"
                  type="button"
                  disabled={groups.length === 1}
                  onClick={() => {
                    const next = groups.filter((g) => g.id !== group.id);
                    setGroups(next);
                    if (group.name === defaultGroup)
                      setDefaultGroup(next[0]!.name);
                  }}
                >
                  Remove group
                </Button>
              </fieldset>
            ))}
            <Button
              type="button"
              variant="outline"
              disabled={groups.length >= 16}
              onClick={() =>
                setGroups((rows) => [
                  ...rows,
                  {
                    id: crypto.randomUUID(),
                    name: "",
                    weight: "1",
                    queues: "",
                  },
                ])
              }
            >
              Add group
            </Button>
            <label>
              Default group
              <select
                required
                value={defaultGroup}
                onChange={(e) => setDefaultGroup(e.target.value)}
              >
                {groups.map((g) => (
                  <option key={g.id} value={g.name}>
                    {g.name}
                  </option>
                ))}
              </select>
            </label>
          </fieldset>
        )}
        <label>
          Reason for this change
          <input name="reason" required maxLength={1024} />
        </label>
        {error && <p role="alert">{error}</p>}
        <div className="actions">
          <Button type="submit">Save policy</Button>
          <Button
            type="button"
            variant="outline"
            onClick={() => onClose(false)}
          >
            Cancel
          </Button>
        </div>
      </form>
    </section>
  );
}

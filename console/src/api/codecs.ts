import { isLosslessNumber } from "lossless-json";
import { consoleContractVersion, type ConsoleConfig } from "./contracts";
export class ContractError extends Error {
  constructor(
    message: string,
    public requestId: string | null = null,
  ) {
    super(message);
    this.name = "ContractError";
  }
}
export function record(
  value: unknown,
  fields: readonly string[],
): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value))
    throw new ContractError("Expected an object.");
  const object = value as Record<string, unknown>;
  const keys = Object.keys(object);
  if (
    keys.length !== fields.length ||
    fields.some((key) => !Object.hasOwn(object, key))
  )
    throw new ContractError("Response fields do not match Console contract 1.");
  return object;
}
function text(value: unknown): string {
  if (
    typeof value !== "string" ||
    !value.length ||
    new TextEncoder().encode(value).length > 1024
  )
    throw new ContractError("Invalid response text.");
  return value;
}
function integer(value: unknown, min = 0, max = 4294967295): number {
  if (isLosslessNumber(value)) value = Number(value.value);
  if (
    typeof value !== "number" ||
    !Number.isSafeInteger(value) ||
    value < min ||
    value > max
  )
    throw new ContractError("Invalid response integer.");
  return value;
}
function flag(value: unknown): boolean {
  if (typeof value !== "boolean")
    throw new ContractError("Invalid capability.");
  return value;
}
export function decodeConfig(value: unknown): ConsoleConfig {
  const v = record(value, [
    "contract_version",
    "server_version",
    "instance_id",
    "instance_name",
    "capabilities",
    "suggested_queues",
    "limits",
    "polling",
  ]);
  if (integer(v.contract_version) !== consoleContractVersion)
    throw new ContractError(
      "This Console build is incompatible with the server contract.",
    );
  const c = record(v.capabilities, [
    "executions",
    "workflows",
    "programs",
    "workers",
  ]);
  const l = record(v.limits, [
    "default_page_size",
    "max_page_size",
    "metadata_max_bytes",
    "submission_max_bytes",
    "input_max_bytes",
    "max_visible_workflow_nodes",
    "max_detailed_worker_slots",
  ]);
  const p = record(v.polling, [
    "lists_ms",
    "active_task_ms",
    "waiting_workflow_ms",
    "workers_ms",
    "catalog_stale_ms",
    "worker_fresh_ms",
    "worker_recent_ms",
  ]);
  if (!Array.isArray(v.suggested_queues) || v.suggested_queues.length > 100)
    throw new ContractError("Invalid queue suggestions.");
  const config: ConsoleConfig = {
    contract_version: consoleContractVersion,
    server_version: text(v.server_version),
    instance_id: text(v.instance_id),
    instance_name: text(v.instance_name),
    capabilities: {
      executions: flag(c.executions),
      workflows: flag(c.workflows),
      programs: flag(c.programs),
      workers: flag(c.workers),
    },
    suggested_queues: v.suggested_queues.map(text),
    limits: {
      default_page_size: integer(l.default_page_size, 1, 100),
      max_page_size: integer(l.max_page_size, 1, 100),
      metadata_max_bytes: integer(l.metadata_max_bytes, 1, 2097152),
      submission_max_bytes: integer(l.submission_max_bytes, 1),
      input_max_bytes: integer(l.input_max_bytes, 1),
      max_visible_workflow_nodes: integer(l.max_visible_workflow_nodes, 1, 100),
      max_detailed_worker_slots: integer(l.max_detailed_worker_slots, 1, 1024),
    },
    polling: {
      lists_ms: integer(p.lists_ms, 1000, 300000),
      active_task_ms: integer(p.active_task_ms, 1000, 300000),
      waiting_workflow_ms: integer(p.waiting_workflow_ms, 1000, 300000),
      workers_ms: integer(p.workers_ms, 1000, 300000),
      catalog_stale_ms: integer(p.catalog_stale_ms, 1000, 3600000),
      worker_fresh_ms: integer(p.worker_fresh_ms, 1000, 300000),
      worker_recent_ms: integer(p.worker_recent_ms, 1000, 3600000),
    },
  };
  if (
    config.limits.default_page_size > config.limits.max_page_size ||
    config.limits.input_max_bytes > config.limits.submission_max_bytes ||
    config.polling.worker_fresh_ms > config.polling.worker_recent_ms
  )
    throw new ContractError("Inconsistent Console configuration.");
  return config;
}

// Mirrors ConsoleConfig in ledgence-orchestration-api/src/console.rs.
export const consoleContractVersion = 3;
export interface ConsoleConfig {
  contract_version: number;
  server_version: string;
  instance_id: string;
  instance_name: string;
  capabilities: {
    executions: boolean;
    workflows: boolean;
    programs: boolean;
    workers: boolean;
  };
  suggested_queues: string[];
  limits: {
    default_page_size: number;
    max_page_size: number;
    metadata_max_bytes: number;
    submission_max_bytes: number;
    input_max_bytes: number;
    max_visible_workflow_nodes: number;
    max_detailed_worker_slots: number;
  };
  polling: {
    lists_ms: number;
    active_task_ms: number;
    waiting_workflow_ms: number;
    workers_ms: number;
    catalog_stale_ms: number;
    worker_fresh_ms: number;
    worker_recent_ms: number;
  };
}

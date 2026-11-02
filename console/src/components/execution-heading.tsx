// SPDX-License-Identifier: MIT
import type { ReactNode } from "react";
import { Box, Workflow } from "lucide-react";
import { ExecutionContext } from "./execution-context";
import "../styles/execution-heading.css";

export function ExecutionHeading({
  kind,
  id,
  title,
  context,
  metadata,
  actions,
}: {
  kind: "task" | "workflow";
  id: string;
  title: string;
  context?: { label: string; value: string; version?: string } | undefined;
  metadata?: ReactNode;
  actions?: ReactNode;
}) {
  const Icon = kind === "workflow" ? Workflow : Box;
  return (
    <>
      <header className="shell-header">
        <ExecutionContext kind={kind} id={id} title={title} />
      </header>
      <div className="execution-summary">
        <div className="execution-summary-identity">
          <span className="execution-kind-icon" aria-hidden="true">
            <Icon />
          </span>
          <div>
            <p className="execution-kind">
              {context?.label ?? (kind === "workflow" ? "Workflow" : "Task")}
            </p>
            {context && (
              <p className="execution-program-description">
                <strong>{context.value}</strong>
                {context.version && (
                  <code title="Program version">{context.version}</code>
                )}
              </p>
            )}
          </div>
        </div>
        <div className="execution-summary-metadata">{metadata}</div>
        {actions && <div className="actions">{actions}</div>}
      </div>
    </>
  );
}

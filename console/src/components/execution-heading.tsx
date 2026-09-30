// SPDX-License-Identifier: MIT
import type { ReactNode } from "react";
import { ShellHeaderContent } from "../app/header-slot";
import { ExecutionContext } from "./execution-context";
import "../styles/execution-heading.css";

export function ExecutionHeading({
  kind,
  id,
  title,
  description,
  metadata,
  actions,
}: {
  kind: "task" | "workflow";
  id: string;
  title: string;
  description?: string;
  metadata?: ReactNode;
  actions?: ReactNode;
}) {
  return (
    <>
      <ShellHeaderContent>
        <ExecutionContext kind={kind} id={id} title={title} />
      </ShellHeaderContent>
      <div className="execution-summary">
        <div className="execution-summary-metadata">
          {description && (
            <span className="execution-program-description">{description}</span>
          )}
          {metadata}
        </div>
        {actions && <div className="actions">{actions}</div>}
      </div>
    </>
  );
}

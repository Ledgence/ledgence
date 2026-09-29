import { useState } from "react";
import { BrowserRouter, Routes, Route, Navigate } from "react-router";
import { QueryClientProvider, useQuery } from "@tanstack/react-query";
import { AppShell } from "../components/app-shell";
import { ErrorState, LoadingState } from "../components/async-state";
import { createQueryClient } from "../app/query-client";
import { getConfig } from "../api/client";
import { consoleContractVersion } from "../api/contracts";
import { ApiError } from "../api/errors";
import { NavigationMemory } from "./navigation";
import { InstanceContext } from "./instance";
import { ExecutionDetailPage } from "../features/executions";
import {
  ExecutionsPage,
  LegacyWorkflowsRedirect,
} from "../features/execution-history";
import { NewExecutionPage } from "../features/new-execution";
import { WorkflowDetailPage } from "../features/workflows";
import {
  AgentsPage,
  AgentDetailPage,
  ProgramVersionPage,
} from "../features/catalog";
import { WorkersPage, WorkerDetailPage } from "../features/workers";
function Console() {
  const config = useQuery({
    queryKey: [location.origin, consoleContractVersion, "config"],
    queryFn: ({ signal }) => getConfig(signal),
    staleTime: Infinity,
  });
  return (
    <AppShell instanceName={config.data?.instance_name ?? "Ledgence"}>
      {config.isPending ? (
        <LoadingState label="Connecting to your instance" />
      ) : config.isError ? (
        <>
          <ErrorState
            title="Console cannot connect"
            message={config.error.message}
            onRetry={() => {
              void config.refetch();
            }}
          />
          {config.error instanceof ApiError && config.error.requestId && (
            <p className="wrap">
              Request ID: <code>{config.error.requestId}</code>
            </p>
          )}
        </>
      ) : (
        <InstanceContext.Provider value={config.data}>
          <NavigationMemory />
          <Routes>
            <Route index element={<Navigate to="/executions" replace />} />
            <Route path="executions" element={<ExecutionsPage />} />
            <Route path="executions/new" element={<NewExecutionPage />} />
            <Route
              path="executions/:taskId"
              element={<ExecutionDetailPage />}
            />
            <Route path="workflows" element={<LegacyWorkflowsRedirect />} />
            <Route
              path="workflows/:workflowId"
              element={<WorkflowDetailPage />}
            />
            <Route path="programs" element={<AgentsPage />} />
            <Route path="programs/:programId" element={<AgentDetailPage />} />
            <Route
              path="programs/:programId/versions/:version"
              element={<ProgramVersionPage />}
            />
            <Route path="agents" element={<AgentsPage />} />
            <Route path="agents/:programId" element={<AgentDetailPage />} />
            <Route
              path="agents/:programId/versions/:version"
              element={<ProgramVersionPage />}
            />
            <Route path="workers" element={<WorkersPage />} />
            <Route
              path="workers/:workerSessionId"
              element={<WorkerDetailPage />}
            />
            <Route
              path="*"
              element={
                <ErrorState
                  title="Page not found"
                  message="Use the navigation to open a Console section."
                />
              }
            />
          </Routes>
        </InstanceContext.Provider>
      )}
    </AppShell>
  );
}
export function App() {
  const [client] = useState(createQueryClient);
  return (
    <QueryClientProvider client={client}>
      <BrowserRouter basename="/console">
        <Console />
      </BrowserRouter>
    </QueryClientProvider>
  );
}

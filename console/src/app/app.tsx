import { useEffect, useState } from "react";
import { AccessBoundary } from "./access";
import { accessController } from "./access-state";
import { BrowserRouter, Routes, Route, Navigate } from "react-router";
import { QueryClientProvider, useQuery } from "@tanstack/react-query";
import { AppShell } from "../components/app-shell";
import { ErrorState, LoadingState } from "../components/async-state";
import { createQueryClient } from "../app/query-client";
import { getConfig } from "../api/client";
import { consoleContractVersion } from "../api/contracts";
import { ApiError } from "../api/errors";
import { NavigationHistoryProvider, NavigationMemory } from "./navigation";
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
import {
  ApprovalInboxPage,
  ApprovalReviewPage,
} from "../features/approval-inbox";

import { EffectsPage, EffectReviewPage } from "../features/effects";

function Console() {
  const config = useQuery({
    queryKey: [location.origin, consoleContractVersion, "config"],
    queryFn: ({ signal }) => getConfig(signal),
    staleTime: Infinity,
  });
  return (
    <AppShell
      instanceName={config.data?.instance_name ?? "Ledgence"}
      serverVersion={config.data?.server_version}
    >
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
            <Route path="effects" element={<EffectsPage />} />
            <Route path="effects/review" element={<EffectReviewPage />} />
            <Route path="approvals" element={<ApprovalInboxPage />} />
            <Route path="approvals/review" element={<ApprovalReviewPage />} />
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
function AuthenticatedConsole() {
  const [client] = useState(createQueryClient);
  useEffect(() => {
    const view = accessController.getSnapshot().view;
    const unsubscribe = accessController.subscribe(() => {
      const state = accessController.getSnapshot();
      if (state.view !== view || state.phase === "locked") client.clear();
      else if (state.phase === "checking") void client.cancelQueries();
      else void client.invalidateQueries();
    });
    return () => {
      unsubscribe();
      void client.cancelQueries();
      client.clear();
    };
  }, [client]);
  return (
    <QueryClientProvider client={client}>
      <NavigationHistoryProvider>
        <Console />
      </NavigationHistoryProvider>
    </QueryClientProvider>
  );
}
export function App() {
  return (
    <BrowserRouter basename="/console">
      <AccessBoundary>
        <AuthenticatedConsole />
      </AccessBoundary>
    </BrowserRouter>
  );
}

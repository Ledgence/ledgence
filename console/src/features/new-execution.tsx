import { useLayoutEffect, useRef, useState } from "react";
import { useNavigate, useSearchParams } from "react-router";
import { useQueryClient } from "@tanstack/react-query";
import { useInstance } from "../app/instance";
import { useResource } from "../api/hooks";
import { useCommand, freezeCommand } from "../api/commands";
import { parseUserJson, stringifyUserJson, formatUserJson } from "../api/json";
import { ApiError } from "../api/errors";
import {
  taskDetail,
  observedWorkflow,
  programDetail,
  submission,
} from "../api/resources";
import { variant, object, enumeration, type Decoded } from "../api/schema";
import {
  PageHeading,
  BackLink,
  QueryError,
  CopyText,
  Empty,
} from "../components/resource-ui";
import { LoadingState } from "../components/async-state";
import { Button } from "../components/ui/button";
import { CommandFeedback } from "../components/command-feedback";
// Keep request/response variants explicit even though the two routes differ.
const response = variant({
  task: object({ kind: enumeration("task"), value: taskDetail }),
  workflow: object({ kind: enumeration("workflow"), value: observedWorkflow }),
});
type Submission = Decoded<typeof submission>;
export function NewExecutionPage() {
  const [params] = useSearchParams();
  const source = params.get("source_task");
  const query = useResource("tasks/inspect", { task_id: source }, taskDetail, {
    enabled: !!source,
    maximumBytes: 3 * 1024 * 1024,
    staleTime: Infinity,
  });
  return (
    <>
      <BackLink to="/executions">Executions</BackLink>
      <PageHeading
        title={source ? "Run again" : "New execution"}
        description={
          source
            ? "Create a new submission with a new identity. Existing work and effects remain unchanged."
            : "Run an exact program version on this instance."
        }
      />
      {source ? (
        query.isPending ? (
          <LoadingState label="Loading the original submission" />
        ) : query.error ? (
          <QueryError error={query.error} retry={() => void query.refetch()} />
        ) : (
          query.data && (
            <ExecutionForm key={source} initial={query.data.input} />
          )
        )
      ) : (
        <ExecutionForm
          key={`${params.get("program") ?? ""}:${params.get("version") ?? ""}`}
        />
      )}
    </>
  );
}
function ExecutionForm({ initial }: { initial?: Submission }) {
  const config = useInstance();
  const navigate = useNavigate();
  const mounted = useRef(false);
  useLayoutEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);
  const client = useQueryClient();
  const [params] = useSearchParams();
  const [program, setProgram] = useState(
    initial?.program.id ?? params.get("program") ?? "",
  );
  const [version, setVersion] = useState(
    initial?.program.version ?? params.get("version") ?? "",
  );
  const [queue, setQueue] = useState(
    initial?.queue ?? config.suggested_queues[0] ?? "",
  );
  const [correlation, setCorrelation] = useState(
    initial?.correlation_key ?? "",
  );
  const [hasCorrelation, setHasCorrelation] = useState(
    initial ? initial.correlation_key !== null : false,
  );
  const [input, setInput] = useState(
    initial ? stringifyUserJson(initial.data, 2) : "{}",
  );
  const [maxAttempts, setMaxAttempts] = useState(
    initial?.retry_policy.max_attempts ?? 3,
  );
  const [retryDelay, setRetryDelay] = useState(
    initial?.retry_policy.retry_delay_ms ?? 5000,
  );
  const [timeout, setTimeoutValue] = useState(
    initial?.attempt_timeout_ms ?? 300000,
  );
  const [intent, setIntent] = useState<"task" | "workflow">(
    config.capabilities.executions ? "task" : "workflow",
  );
  const [verified, setVerified] = useState<{
    id: string;
    version: string;
  } | null>(null);
  const [validation, setValidation] = useState("");
  const catalogue = useResource(
    "programs/inspect",
    { program_id: verified?.id, version: verified?.version },
    programDetail,
    {
      enabled: config.capabilities.programs && !!verified,
      staleTime: config.polling.catalog_stale_ms,
    },
  );
  const referenceMatches =
    verified?.id === program && verified.version === version;
  const catalogMatches =
    referenceMatches &&
    !catalogue.error &&
    catalogue.data?.version.descriptor.program.id === program &&
    catalogue.data.version.descriptor.program.version === version;
  const kind = catalogMatches
    ? catalogue.data?.version.metadata.kind
    : undefined;
  const actualIntent = kind === "task" || kind === "workflow" ? kind : intent;
  const command = useCommand(
    actualIntent === "task" ? "tasks" : "workflows",
    (value) => response({ kind: actualIntent, value }),
    (value) => {
      void client.invalidateQueries();
      // Acceptance remains durable after leaving this form. A late reply must
      // refresh the cache without taking over the user's current navigation.
      if (!mounted.current) return;
      navigate(
        value.kind === "task"
          ? `/executions/${encodeURIComponent(value.value.task_id)}`
          : `/workflows/${encodeURIComponent(value.value.workflow.workflow_id)}`,
      );
    },
  );
  const locked = command.command !== null;
  const unknownPackage =
    referenceMatches &&
    catalogue.error instanceof ApiError &&
    catalogue.error.status === 404;
  const ready = config.capabilities.programs
    ? referenceMatches &&
      !catalogue.isFetching &&
      (catalogMatches || unknownPackage)
    : !!program && !!version;
  function submit() {
    try {
      if (!ready)
        throw new Error(
          "Verify the exact program and version before submitting.",
        );
      if (hasCorrelation && new TextEncoder().encode(correlation).length > 512)
        throw new Error("Correlation key must be at most 512 UTF-8 bytes.");
      const data = parseUserJson(input, config.limits.input_max_bytes);
      const body = submission({
        program: { id: program, version },
        queue,
        correlation_key: hasCorrelation ? correlation : null,
        data,
        retry_policy: { max_attempts: maxAttempts, retry_delay_ms: retryDelay },
        attempt_timeout_ms: timeout,
      });
      if (timeout < 60000)
        throw new Error("Attempt timeout must be at least 60000 ms.");
      const id = crypto.randomUUID();
      const operation = freezeCommand({ idempotency_key: id, input: body }, id);
      if (
        new TextEncoder().encode(operation.body).length >
        config.limits.submission_max_bytes
      )
        throw new Error("The submission exceeds the instance byte limit.");
      setValidation("");
      command.send(operation);
    } catch (error) {
      setValidation(
        error instanceof Error ? error.message : "Invalid submission.",
      );
    }
  }
  if (!config.capabilities.executions && !config.capabilities.workflows)
    return <Empty>Submission is unavailable on this instance.</Empty>;
  return (
    <form
      className="card execution-form"
      onSubmit={(e) => {
        e.preventDefault();
        submit();
      }}
    >
      <p>
        Instance <strong>{config.instance_name}</strong>
      </p>
      <fieldset disabled={locked}>
        <legend className="sr-only">Execution configuration</legend>
        <div className="form-grid">
          <label>
            Program ID
            <input
              required
              value={program}
              onChange={(e) => setProgram(e.target.value)}
              maxLength={128}
            />
          </label>
          <label>
            Exact version
            <input
              required
              value={version}
              onChange={(e) => setVersion(e.target.value)}
              maxLength={128}
            />
          </label>
        </div>
        {config.capabilities.programs && (
          <Button
            variant="outline"
            type="button"
            disabled={!program || !version || catalogue.isFetching}
            onClick={() => {
              setVerified({ id: program, version });
              if (referenceMatches) void catalogue.refetch();
            }}
          >
            Verify reference
          </Button>
        )}
        {config.capabilities.programs &&
          referenceMatches &&
          catalogue.isPending && (
            <p role="status">Checking registered metadata…</p>
          )}
        {catalogMatches && catalogue.data && (
          <div className="notice">
            <p>Registered {catalogue.data.version.metadata.kind} package</p>
            <CopyText
              value={catalogue.data.version.descriptor.digest}
              label="Copy expected digest"
            />
          </div>
        )}
        {referenceMatches &&
          catalogue.error &&
          (unknownPackage ? (
            <p className="notice">
              This reference is not registered in the catalog. Choose its
              intended use explicitly. Submission will resolve it against the
              configured program store.
            </p>
          ) : (
            <QueryError
              error={catalogue.error}
              retry={() => void catalogue.refetch()}
            />
          ))}
        {!config.capabilities.programs && (
          <p className="notice">
            The program catalog is unavailable on this instance. Choose the
            intended use explicitly. Submission resolves the exact reference
            against the configured program store.
          </p>
        )}
        {referenceMatches &&
          catalogue.data &&
          !catalogue.error &&
          !catalogMatches && (
            <p role="alert" className="notice error-notice">
              The catalog returned a different program reference. Verify this
              reference again.
            </p>
          )}
        {(!config.capabilities.programs ||
          kind === "unspecified" ||
          unknownPackage) && (
          <label>
            Execution kind
            <select
              value={intent}
              onChange={(e) =>
                setIntent(e.target.value === "workflow" ? "workflow" : "task")
              }
            >
              <option value="task" disabled={!config.capabilities.executions}>
                Task
              </option>
              <option
                value="workflow"
                disabled={!config.capabilities.workflows}
              >
                Workflow controller
              </option>
            </select>
            <span className="muted">
              Only use Workflow controller for a program that implements the
              workflow protocol.
            </span>
          </label>
        )}
        <div className="form-grid">
          <label>
            Queue
            <input
              required
              list="new-queue-suggestions"
              value={queue}
              onChange={(e) => setQueue(e.target.value)}
              maxLength={128}
            />
            <datalist id="new-queue-suggestions">
              {config.suggested_queues.map((q) => (
                <option key={q}>{q}</option>
              ))}
            </datalist>
          </label>
          <div>
            <label>
              Correlation key
              <input
                value={correlation}
                onChange={(e) => setCorrelation(e.target.value)}
                maxLength={512}
              />
            </label>
            <label className="check-label">
              <input
                type="checkbox"
                checked={hasCorrelation}
                onChange={(e) => setHasCorrelation(e.target.checked)}
              />
              Include correlation, including empty
            </label>
          </div>
        </div>
        <label>
          JSON input
          <textarea
            className="json-editor"
            required
            spellCheck={false}
            value={input}
            onChange={(e) => setInput(e.target.value)}
            rows={12}
          />
        </label>
        <Button
          type="button"
          variant="ghost"
          onClick={() => {
            try {
              setInput(formatUserJson(input, config.limits.input_max_bytes));
              setValidation("");
            } catch (error) {
              setValidation(
                error instanceof Error ? error.message : "Invalid JSON.",
              );
            }
          }}
        >
          Format JSON
        </Button>
        <details>
          <summary>Advanced execution policy</summary>
          <div className="form-grid">
            <label>
              Maximum attempts
              <input
                type="number"
                min={1}
                max={1000}
                required
                value={maxAttempts}
                onChange={(e) => setMaxAttempts(Number(e.target.value))}
              />
            </label>
            <label>
              Retry delay (ms)
              <input
                type="number"
                min={0}
                max={86400000}
                required
                value={retryDelay}
                onChange={(e) => setRetryDelay(Number(e.target.value))}
              />
            </label>
            <label>
              Attempt timeout (ms)
              <input
                type="number"
                min={60000}
                max={86400000}
                required
                value={timeout}
                onChange={(e) => setTimeoutValue(Number(e.target.value))}
              />
            </label>
          </div>
        </details>
      </fieldset>
      {validation && (
        <p role="alert" className="notice error-notice">
          {validation}
        </p>
      )}
      {command.mutation.error && command.command ? (
        <CommandFeedback
          error={command.mutation.error}
          identity={command.command.identity}
          retry={command.retry}
          reset={command.reset}
        />
      ) : (
        <Button
          type="submit"
          disabled={
            locked ||
            !ready ||
            (actualIntent === "workflow"
              ? !config.capabilities.workflows
              : !config.capabilities.executions)
          }
        >
          {command.mutation.isPending
            ? "Submitting…"
            : actualIntent === "workflow"
              ? "Start workflow"
              : "Submit execution"}
        </Button>
      )}
    </form>
  );
}

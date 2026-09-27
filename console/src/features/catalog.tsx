import { useState } from "react";
import { Link, useParams } from "react-router";
import { useQueryClient } from "@tanstack/react-query";
import { ArrowRight, Package, Plus } from "lucide-react";
import { useInstance } from "../app/instance";
import { usePagination, useResource } from "../api/hooks";
import { freezeCommand, useCommand } from "../api/commands";
import { ApiError } from "../api/errors";
import { ContractError } from "../api/codecs";
import * as dto from "../api/resources";
import { LoadingState } from "../components/async-state";
import { CommandFeedback } from "../components/command-feedback";
import {
  BackLink,
  CopyText,
  Empty,
  Field,
  Fields,
  PageControls,
  PageHeading,
  QueryError,
  Status,
  When,
} from "../components/resource-ui";
import { Button } from "../components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogTitle,
  DialogTrigger,
} from "../components/ui/dialog";

const validReference = (value: string) =>
  /^[a-z0-9._-]{1,128}$/.test(value) && value !== "." && value !== "..";
const versionPath = (id: string, version: string) =>
  `/agents/${encodeURIComponent(id)}/versions/${encodeURIComponent(version)}`;

export function AgentsPage() {
  const config = useInstance();
  const pagination = usePagination();
  const query = useResource(
    "programs",
    { limit: pagination.limit, cursor: pagination.cursor },
    dto.programPage,
    {
      enabled: config.capabilities.programs,
      staleTime: config.polling.catalog_stale_ms,
    },
  );
  return (
    <>
      <PageHeading
        title="Agents"
        description="Programs registered in this instance, ready to use by exact version."
        actions={config.capabilities.programs && <RegisterProgram />}
      />
      {!config.capabilities.programs ? (
        <Empty>The program catalog is unavailable on this server.</Empty>
      ) : (
        <>
          {query.isPending && (
            <LoadingState label="Loading registered agents" />
          )}
          {query.error && (
            <QueryError
              error={query.error}
              retry={() => void query.refetch()}
              stale={!!query.data}
            />
          )}
          {query.data && (
            <>
              {query.data.items.length ? (
                <div className="card-grid">
                  {query.data.items.map((item) => (
                    <article className="card" key={item.program_id}>
                      <div className="summary-line">
                        <Package aria-hidden="true" />
                        <Status value={item.metadata.kind} />
                      </div>
                      <h2>
                        <Link
                          to={`/agents/${encodeURIComponent(item.program_id)}`}
                        >
                          {item.metadata.display_name ?? item.program_id}
                        </Link>
                      </h2>
                      {item.metadata.display_name && (
                        <p className="muted wrap">{item.program_id}</p>
                      )}
                      {item.metadata.description && (
                        <p className="wrap">{item.metadata.description}</p>
                      )}
                      <p>
                        {item.registered_versions} registered{" "}
                        {item.registered_versions === "1"
                          ? "version"
                          : "versions"}
                      </p>
                      <p className="muted">
                        Last registration{" "}
                        <When value={item.last_registered_at} />
                      </p>
                      <Link
                        className="back-link"
                        to={`/agents/${encodeURIComponent(item.program_id)}`}
                      >
                        View versions <ArrowRight aria-hidden="true" />
                      </Link>
                    </article>
                  ))}
                </div>
              ) : (
                <section className="empty-state">
                  <h2>No registered agents</h2>
                  <p>
                    Publish a package to the configured program store, then
                    register its exact program ID and version here. Existing
                    executions can still use unregistered programs.
                  </p>
                </section>
              )}
              <PageControls
                pagination={pagination}
                nextCursor={query.data.next_cursor}
                observedAt={query.data.observed_at}
                refresh={() => void query.refetch()}
                fetching={query.isFetching}
              />
            </>
          )}
        </>
      )}
    </>
  );
}

export function AgentDetailPage() {
  const { programId = "" } = useParams();
  const config = useInstance();
  const pagination = usePagination();
  const valid = validReference(programId);
  const query = useResource(
    "programs/versions",
    {
      program_id: programId,
      limit: pagination.limit,
      cursor: pagination.cursor,
    },
    (value) => {
      const page = dto.programVersions(value);
      if (page.items.some((item) => item.descriptor.program.id !== programId))
        throw new ContractError(
          "The returned versions belong to another program.",
        );
      return page;
    },
    {
      enabled: valid && config.capabilities.programs,
      staleTime: config.polling.catalog_stale_ms,
    },
  );
  return (
    <>
      <BackLink to="/agents">Agents</BackLink>
      <PageHeading
        title={valid ? programId : "Invalid program reference"}
        description="Registered versions, ordered by registration time. Choose an exact version."
        actions={
          valid &&
          config.capabilities.programs && (
            <RegisterProgram key={programId} programId={programId} />
          )
        }
      />
      {!valid ? (
        <Empty>This link does not contain a valid program ID.</Empty>
      ) : !config.capabilities.programs ? (
        <Empty>The program catalog is unavailable on this server.</Empty>
      ) : (
        <>
          {query.isPending && (
            <LoadingState label="Loading registered versions" />
          )}
          {query.error && (
            <CatalogError
              error={query.error}
              stale={!!query.data}
              retry={() => void query.refetch()}
            />
          )}
          {query.data && (
            <>
              {query.data.items.length ? (
                <div className="card-grid">
                  {query.data.items.map((item) => (
                    <article
                      className="card"
                      key={item.descriptor.program.version}
                    >
                      <div className="summary-line">
                        <h2>
                          <Link
                            to={versionPath(
                              programId,
                              item.descriptor.program.version,
                            )}
                          >
                            {item.descriptor.program.version}
                          </Link>
                        </h2>
                        <Status value={item.metadata.kind} />
                      </div>
                      {item.metadata.description && (
                        <p className="wrap">{item.metadata.description}</p>
                      )}
                      <p className="muted">
                        Python {item.manifest.runtime.python} ·{" "}
                        {item.manifest.platform.os}{" "}
                        {item.manifest.platform.arch}
                      </p>
                      <CopyText
                        value={item.descriptor.digest}
                        label="Copy version digest"
                      />
                      <p>
                        Registered <When value={item.registered_at} />
                      </p>
                      <Link
                        className="back-link"
                        to={versionPath(
                          programId,
                          item.descriptor.program.version,
                        )}
                      >
                        Inspect version <ArrowRight aria-hidden="true" />
                      </Link>
                    </article>
                  ))}
                </div>
              ) : (
                <Empty>
                  No versions are registered on this page. Register an exact
                  reference or return to the first page.
                </Empty>
              )}
              <PageControls
                pagination={pagination}
                nextCursor={query.data.next_cursor}
                observedAt={query.data.observed_at}
                refresh={() => void query.refetch()}
                fetching={query.isFetching}
              />
            </>
          )}
        </>
      )}
    </>
  );
}

export function ProgramVersionPage() {
  const { programId = "", version = "" } = useParams();
  const config = useInstance();
  const valid = validReference(programId) && validReference(version);
  const query = useResource(
    "programs/inspect",
    { program_id: programId, version },
    (value) => {
      const detail = dto.programDetail(value);
      if (
        detail.version.descriptor.program.id !== programId ||
        detail.version.descriptor.program.version !== version
      )
        throw new ContractError(
          "The server returned a different program reference.",
        );
      return detail;
    },
    {
      enabled: valid && config.capabilities.programs,
      staleTime: config.polling.catalog_stale_ms,
    },
  );
  const item = query.data?.version;
  const canUse =
    item &&
    (item.metadata.kind === "workflow"
      ? config.capabilities.workflows
      : item.metadata.kind === "task"
        ? config.capabilities.executions
        : config.capabilities.executions || config.capabilities.workflows);
  return (
    <>
      <BackLink
        to={valid ? `/agents/${encodeURIComponent(programId)}` : "/agents"}
      >
        Registered versions
      </BackLink>
      <PageHeading
        title={item?.metadata.display_name ?? "Program version"}
        description={
          valid ? `${programId} / ${version}` : "Invalid program reference"
        }
        actions={
          canUse && (
            <Link
              className="button button-primary"
              to={`/executions/new?program=${encodeURIComponent(programId)}&version=${encodeURIComponent(version)}`}
            >
              Use in an execution
            </Link>
          )
        }
      />
      {!valid ? (
        <Empty>
          This link does not contain a valid exact program reference.
        </Empty>
      ) : !config.capabilities.programs ? (
        <Empty>The program catalog is unavailable on this server.</Empty>
      ) : (
        <>
          {query.isPending && <LoadingState label="Loading program version" />}
          {query.error && (
            <CatalogError
              error={query.error}
              stale={!!item}
              retry={() => void query.refetch()}
            />
          )}
          {item && (
            <section className="card">
              {item.metadata.description && (
                <p className="wrap">{item.metadata.description}</p>
              )}
              <Fields>
                <Field label="Program ID">
                  <CopyText value={item.descriptor.program.id} />
                </Field>
                <Field label="Exact version">
                  <CopyText value={item.descriptor.program.version} />
                </Field>
                <Field label="Declared use">
                  <Status value={item.metadata.kind} />
                </Field>
                <Field label="Digest">
                  <CopyText
                    value={item.descriptor.digest}
                    label="Copy artifact digest"
                  />
                </Field>
                <Field label="Artifact size">
                  {item.descriptor.size} bytes
                </Field>
                <Field label="Python">{item.manifest.runtime.python}</Field>
                <Field label="Operating system">
                  {item.manifest.platform.os}
                </Field>
                <Field label="Architecture">
                  {item.manifest.platform.arch}
                </Field>
                <Field label="Protocol">{item.manifest.runtime.protocol}</Field>
                <Field label="Handler">
                  <code className="wrap">{item.manifest.handler}</code>
                </Field>
                <Field label="Registered">
                  <When value={item.registered_at} />
                </Field>
                <Field label="Verification">Configured program store</Field>
              </Fields>
              <p className="notice">
                This is a registered artifact reference. Execution resolves it
                against the instance’s configured program store. The submission
                form lets you review inputs before any work starts.
              </p>
              {item.metadata.kind === "workflow" && (
                <p>
                  This package is declared as a workflow controller. The form
                  will offer Start workflow.
                </p>
              )}
              {item.metadata.kind === "unspecified" && (
                <p>
                  No execution kind is declared. Choose task or workflow
                  explicitly in the submission form.
                </p>
              )}
              <div className="summary-line">
                <span className="muted">
                  Observed <When value={query.data?.observed_at ?? null} />
                </span>
                <Button
                  variant="outline"
                  disabled={query.isFetching}
                  onClick={() => void query.refetch()}
                >
                  Refresh
                </Button>
                <RegisterProgram
                  key={JSON.stringify([programId, version])}
                  programId={programId}
                  version={version}
                />
              </div>
            </section>
          )}
        </>
      )}
    </>
  );
}

function CatalogError({
  error,
  retry,
  stale,
}: {
  error: unknown;
  retry: () => void;
  stale: boolean;
}) {
  return (
    <>
      {!stale && error instanceof ApiError && error.status === 404 && (
        <section className="notice">
          <h2>Reference not registered</h2>
          <p>
            This catalog entry is unavailable. An existing execution’s
            descriptor remains authoritative for that execution.
          </p>
        </section>
      )}
      <QueryError error={error} retry={retry} stale={stale} />
    </>
  );
}

function RegisterProgram({
  programId = "",
  version = "",
}: {
  programId?: string;
  version?: string;
}) {
  const [open, setOpen] = useState(false);
  const [id, setId] = useState(programId);
  const [exactVersion, setVersion] = useState(version);
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [kind, setKind] = useState<"task" | "workflow" | "unspecified">(
    "unspecified",
  );
  const [updateMetadata, setUpdateMetadata] = useState(false);
  const [error, setError] = useState("");
  const config = useInstance();
  const client = useQueryClient();
  const command = useCommand(
    "programs/register",
    (value) => {
      const receipt = dto.programReceipt(value);
      if (
        receipt.version.descriptor.program.id !== id ||
        receipt.version.descriptor.program.version !== exactVersion
      )
        throw new ContractError(
          "The registration receipt has a different program reference.",
        );
      return receipt;
    },
    () => {
      void client.invalidateQueries({
        predicate: (query) =>
          query.queryKey.some(
            (key) =>
              typeof key === "string" && key.startsWith("/v1/console/programs"),
          ),
      });
    },
  );
  const locked = command.command !== null;
  function submit() {
    if (!validReference(id) || !validReference(exactVersion)) {
      setError(
        "Program ID and exact version must use 1–128 lowercase letters, digits, dots, underscores or hyphens. A dot or double dot alone is invalid.",
      );
      return;
    }
    if (
      new TextEncoder().encode(name).length > 128 ||
      new TextEncoder().encode(description).length > 4096
    ) {
      setError(
        "Display name must fit 128 UTF-8 bytes and description must fit 4096 UTF-8 bytes.",
      );
      return;
    }
    setError("");
    command.send(
      freezeCommand(
        {
          program: { id, version: exactVersion },
          metadata: {
            display_name: name || null,
            description: description || null,
            kind,
          },
          update_metadata: updateMetadata,
        },
        `${id}@${exactVersion}`,
      ),
    );
  }
  return (
    <Dialog
      open={open}
      onOpenChange={(next) => {
        if (command.mutation.isPending) return;
        if (next && command.mutation.isSuccess) command.reset();
        setOpen(next);
      }}
    >
      <DialogTrigger asChild>
        <Button variant="outline">
          <Plus aria-hidden="true" />
          {version ? "Register / update metadata" : "Register agent"}
        </Button>
      </DialogTrigger>
      <DialogContent>
        <DialogTitle>Register an exact program reference</DialogTitle>
        <DialogDescription>
          Publish the package first. Ledgence verifies the reference against
          this instance’s configured program store; registration does not upload
          or run code.
        </DialogDescription>
        <form
          onSubmit={(e) => {
            e.preventDefault();
            submit();
          }}
        >
          <p>
            Instance <strong>{config.instance_name}</strong>
          </p>
          <fieldset disabled={locked}>
            <legend className="sr-only">Program registration</legend>
            <div className="form-grid">
              <label>
                Program ID
                <input
                  required
                  value={id}
                  onChange={(e) => setId(e.target.value)}
                  maxLength={128}
                />
              </label>
              <label>
                Exact version
                <input
                  required
                  value={exactVersion}
                  onChange={(e) => setVersion(e.target.value)}
                  maxLength={128}
                />
              </label>
              <label>
                Display name (optional)
                <input
                  value={name}
                  onChange={(e) => setName(e.target.value)}
                  maxLength={128}
                />
              </label>
              <label>
                Declared use
                <select
                  value={kind}
                  onChange={(e) =>
                    setKind(
                      e.target.value === "task"
                        ? "task"
                        : e.target.value === "workflow"
                          ? "workflow"
                          : "unspecified",
                    )
                  }
                >
                  <option value="unspecified">Unspecified</option>
                  <option value="task">Task</option>
                  <option value="workflow">Workflow controller</option>
                </select>
              </label>
            </div>
            <label>
              Description (optional)
              <textarea
                value={description}
                onChange={(e) => setDescription(e.target.value)}
                rows={3}
                maxLength={4096}
              />
            </label>
            <label className="check-label">
              <input
                type="checkbox"
                checked={updateMetadata}
                onChange={(e) => setUpdateMetadata(e.target.checked)}
              />
              Replace descriptive metadata for an existing registration
            </label>
            <p className="muted">
              An existing version’s digest stays immutable. Replacement applies
              the display fields above, including clearing omitted fields.
            </p>
          </fieldset>
          {error && (
            <p role="alert" className="notice error-notice">
              {error}
            </p>
          )}
          {command.mutation.error instanceof ApiError &&
            command.mutation.error.status === 409 && (
              <p className="notice error-notice">
                Registration conflicts with the existing immutable reference or
                its descriptive metadata. Verify the program store and use
                explicit metadata replacement when appropriate; an existing
                digest cannot be replaced.
              </p>
            )}
          {command.mutation.error && command.command && (
            <CommandFeedback
              error={command.mutation.error}
              identity={command.command.identity}
              retry={command.retry}
              reset={command.reset}
            />
          )}
          {command.mutation.data ? (
            <div className="notice" role="status">
              <p>
                {command.mutation.data.already_registered
                  ? "Reference already registered."
                  : "Reference registered."}{" "}
                {command.mutation.data.metadata_updated &&
                  "Descriptive metadata updated."}
              </p>
              <Link
                to={versionPath(
                  command.mutation.data.version.descriptor.program.id,
                  command.mutation.data.version.descriptor.program.version,
                )}
                onClick={() => setOpen(false)}
              >
                Inspect registered version
              </Link>
              <CopyText
                value={command.mutation.data.version.descriptor.digest}
                label="Copy registered digest"
              />
            </div>
          ) : (
            !command.mutation.error && (
              <Button type="submit" disabled={locked}>
                {command.mutation.isPending
                  ? "Verifying and registering…"
                  : "Register reference"}
              </Button>
            )
          )}
        </form>
      </DialogContent>
    </Dialog>
  );
}

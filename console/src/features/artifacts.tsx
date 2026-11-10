// SPDX-License-Identifier: MIT
import { useEffect, useId, useRef, useState } from "react";
import { Link, useLocation } from "react-router";
import { Download, File } from "lucide-react";
import { useInstance } from "../app/instance";
import { usePermission } from "../app/access-session";
import { usePagination } from "../api/hooks";
import { useTerminalRefresh } from "../api/terminal-refresh";
import {
  useArtifacts,
  type ArtifactManifest,
  type ArtifactTarget,
} from "../api/artifacts";
import { downloadArtifact } from "../api/artifact-download";
import { Button } from "../components/ui/button";
import { Table } from "../components/ui/table";
import { LoadingState } from "../components/async-state";
import {
  CopyText,
  Empty,
  Field,
  Fields,
  QueryError,
  When,
} from "../components/resource-ui";

export function ArtifactsPanel({
  target,
  active,
}: {
  target: ArtifactTarget;
  active: boolean;
}) {
  const config = useInstance();
  const location = useLocation();
  const metadata = usePermission("metadata_read");
  const page = usePagination("artifacts_");
  const heldDownload = useRef<AbortController | null>(null);
  const [downloading, setDownloading] = useState(false);
  const downloads: DownloadAdmission = {
    busy: downloading,
    acquire() {
      if (heldDownload.current) return null;
      const controller = new AbortController();
      heldDownload.current = controller;
      setDownloading(true);
      return controller;
    },
    release(controller) {
      if (heldDownload.current !== controller) return;
      heldDownload.current = null;
      setDownloading(false);
    },
  };
  useEffect(
    () => () => heldDownload.current?.abort(),
    [location.key, config.instance_id],
  );
  const query = useArtifacts(target, page.limit, page.cursor, active);
  useTerminalRefresh(
    active,
    [config.instance_id, target.kind, target.id, page.cursor],
    query,
    metadata,
  );
  if (!metadata)
    return (
      <p className="notice">
        Artifact metadata is not available with your current access.
      </p>
    );
  return (
    <section className="card artifact-panel" aria-label="Files in this run">
      <p className="muted">
        Files in this run are shared by its root execution and descendants.
        Producer details identify the execution that published each file.
      </p>
      {query.isPending && <LoadingState />}
      {query.error && (
        <QueryError
          error={query.error}
          retry={() => void query.refetch()}
          stale={!!query.data}
        />
      )}
      {query.data && (
        <>
          {query.data.items.length === 0 ? (
            <Empty>
              No artifacts yet. Files published by this run will appear here
              after their contents are verified.
            </Empty>
          ) : (
            <Table>
              <thead>
                <tr>
                  <th scope="col">File</th>
                  <th scope="col">Size</th>
                  <th scope="col">Producer</th>
                  <th scope="col">Available since</th>
                  <th scope="col">Download</th>
                </tr>
              </thead>
              <tbody>
                {query.data.items.map((artifact) => (
                  <ArtifactRow
                    key={artifact.reference.id}
                    artifact={artifact}
                    downloads={downloads}
                  />
                ))}
              </tbody>
            </Table>
          )}
          <div className="page-controls">
            <Button
              variant="outline"
              disabled={query.isFetching}
              onClick={() => void query.refetch()}
            >
              Refresh files
            </Button>
            <Button
              variant="outline"
              disabled={!page.cursor || query.isFetching}
              onClick={page.back}
            >
              Previous
            </Button>
            <Button
              variant="outline"
              disabled={!query.data.next_cursor || query.isFetching}
              onClick={() => {
                if (query.data.next_cursor) page.next(query.data.next_cursor);
              }}
            >
              Next
            </Button>
          </div>
        </>
      )}
    </section>
  );
}
type DownloadAdmission = {
  busy: boolean;
  acquire(): AbortController | null;
  release(controller: AbortController): void;
};
function ArtifactRow({
  artifact,
  downloads,
}: {
  artifact: ArtifactManifest;
  downloads: DownloadAdmission;
}) {
  const config = useInstance();
  const permitted = usePermission("sensitive_read");
  const transfer = useRef<AbortController | null>(null);
  const [pending, setPending] = useState(false);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  useEffect(
    () => () => transfer.current?.abort(),
    [artifact.reference.id, config.instance_id],
  );
  const root = artifact.root;
  const [expanded, setExpanded] = useState(false);
  const detailsId = useId();
  async function download() {
    if (transfer.current) return;
    const controller = downloads.acquire();
    if (!controller) return;
    transfer.current = controller;
    setPending(true);
    setMessage("");
    setError("");
    try {
      await downloadArtifact(artifact, config.instance_id, controller.signal);
      if (!controller.signal.aborted)
        setMessage("Verified and sent to your browser.");
    } catch (failure) {
      if (!controller.signal.aborted)
        setError(
          failure instanceof Error
            ? failure.message
            : "The file could not be downloaded.",
        );
    } finally {
      if (transfer.current === controller) {
        transfer.current = null;
        setPending(false);
        downloads.release(controller);
      }
    }
  }
  return (
    <>
      <tr>
        <td>
          <div className="artifact-name">
            <File aria-hidden="true" />
            <strong>{artifact.filename ?? artifact.key}</strong>
          </div>
          <p className="muted">{artifact.reference.media_type}</p>
          <Button
            variant="ghost"
            aria-expanded={expanded}
            aria-controls={detailsId}
            onClick={() => setExpanded(!expanded)}
          >
            File details
          </Button>
        </td>
        <td>{formatArtifactSize(artifact.reference.size)}</td>
        <td>
          <ArtifactProducer artifact={artifact} />
        </td>
        <td>
          <When value={artifact.accepted_at} />
        </td>
        <td>
          {permitted ? (
            <div className="artifact-download" aria-busy={pending}>
              <Button
                variant="outline"
                disabled={downloads.busy}
                onClick={() => void download()}
                aria-label={`Download ${artifact.filename ?? artifact.key}`}
              >
                <Download aria-hidden="true" />
                {pending ? "Verifying…" : "Download"}
              </Button>
              {pending && (
                <Button
                  variant="ghost"
                  onClick={() => {
                    transfer.current?.abort();
                    setMessage("Download cancelled.");
                  }}
                >
                  Cancel download
                </Button>
              )}
              {message && (
                <p role="status" className="muted">
                  {message}
                </p>
              )}
              {error && <p role="alert">{error}</p>}
            </div>
          ) : (
            <span className="muted">Sensitive-read access required</span>
          )}
        </td>
      </tr>
      {expanded && (
        <tr className="artifact-detail-row">
          <td colSpan={5} id={detailsId}>
            <Fields>
              <Field label="Artifact ID">
                <CopyText value={artifact.reference.id} />
              </Field>
              <Field label="Logical key">{artifact.key}</Field>
              <Field label="SHA-256">
                <CopyText value={artifact.reference.digest} />
              </Field>
              <Field label="Root execution">
                <Link
                  to={`/${root.kind === "workflow" ? "workflows" : "executions"}/${encodeURIComponent(root.id)}`}
                >
                  {root.kind === "workflow" ? "Workflow" : "Task"}: {root.id}
                </Link>
              </Field>
              {artifact.first_attempt_id && (
                <Field label="First attempt">
                  <CopyText value={artifact.first_attempt_id} />
                </Field>
              )}
              <Field label="Created">
                <When value={artifact.created_at} />
              </Field>
            </Fields>
          </td>
        </tr>
      )}
    </>
  );
}
function ArtifactProducer({ artifact }: { artifact: ArtifactManifest }) {
  const producer = artifact.producer;
  switch (producer.kind) {
    case "task":
      return (
        <Link to={`/executions/${encodeURIComponent(producer.task_id)}`}>
          Task: {producer.task_id}
        </Link>
      );
    case "workflow_activation":
      return (
        <>
          <Link to={`/workflows/${encodeURIComponent(producer.workflow_id)}`}>
            Workflow: {producer.workflow_id}
          </Link>
          <br />
          <Link
            to={`/executions/${encodeURIComponent(producer.activation_id)}`}
          >
            Activation: {producer.activation_id}
          </Link>
        </>
      );
    case "verified_actor":
      return (
        <span title={producer.identity.issuer}>
          Published by {producer.identity.subject}
        </span>
      );
    case "trusted_instance":
      return <span>Instance: {producer.instance_id}</span>;
  }
}
function formatArtifactSize(size: number) {
  if (size < 1024) return `${size.toLocaleString()} B`;
  const unit = size >= 1024 * 1024 ? "MiB" : "KiB";
  return `${new Intl.NumberFormat(undefined, { maximumFractionDigits: 1 }).format(size / (unit === "MiB" ? 1024 * 1024 : 1024))} ${unit}`;
}

import { useState, type ReactNode } from "react";
import { Link, useLocation, useSearchParams } from "react-router";
import { Check, Copy, RefreshCw, Circle, ArrowLeft } from "lucide-react";
import { ApiError } from "../api/errors";
import { ContractError } from "../api/codecs";
import { stringifyUserJson } from "../api/json";
import { Button } from "./ui/button";
import type { usePagination } from "../api/hooks";
export function CopyText({
  value,
  label = "Copy",
}: {
  value: string;
  label?: string;
}) {
  const [feedback, setFeedback] = useState("");
  return (
    <span className="copy-value">
      <code title={value}>{value}</code>
      <Button
        variant="ghost"
        aria-label={label}
        onClick={() =>
          void navigator.clipboard.writeText(value).then(
            () => setFeedback("Copied"),
            () => setFeedback("Select and copy the text manually."),
          )
        }
      >
        {feedback === "Copied" ? (
          <Check aria-hidden="true" />
        ) : (
          <Copy aria-hidden="true" />
        )}
      </Button>
      <span role="status" className="sr-only">
        {feedback}
      </span>
    </span>
  );
}
export function Status({ value }: { value: string }) {
  return (
    <span className={`status status-${value}`}>
      <Circle aria-hidden="true" />
      {value.replaceAll("_", " ")}
    </span>
  );
}
export function When({ value }: { value: number | null }) {
  return value === null ? (
    <span className="muted">Not recorded</span>
  ) : (
    <time dateTime={new Date(value).toISOString()}>
      {new Date(value).toLocaleString()}
    </time>
  );
}
export function Elapsed({ start, end }: { start: number; end: number }) {
  const ms = Math.max(0, end - start);
  const seconds = Math.floor(ms / 1000);
  const value =
    ms < 1000
      ? `${ms} ms`
      : seconds < 60
        ? `${seconds}s`
        : seconds < 3600
          ? `${Math.floor(seconds / 60)}m ${seconds % 60}s`
          : `${Math.floor(seconds / 3600)}h ${Math.floor((seconds % 3600) / 60)}m`;
  return (
    <span
      className="elapsed"
      title={`${ms.toLocaleString()} ms at the last observation`}
    >
      Elapsed {value}
    </span>
  );
}
export function PageHeading({
  title,
  description,
  actions,
  metadata,
}: {
  title: string;
  description?: string;
  actions?: ReactNode;
  metadata?: ReactNode;
}) {
  return (
    <header className="page-heading">
      <div>
        <h1>{title}</h1>
        {description && <p className="muted">{description}</p>}
        {metadata && (
          <div className="heading-metadata summary-line">{metadata}</div>
        )}
      </div>
      {actions && <div className="actions">{actions}</div>}
    </header>
  );
}
export function BackLink({
  to,
  children,
}: {
  to: string;
  children: ReactNode;
}) {
  const location = useLocation();
  const state: unknown = location.state;
  const returnTo =
    state &&
    typeof state === "object" &&
    "returnTo" in state &&
    typeof state.returnTo === "string" &&
    (state.returnTo === to || state.returnTo.startsWith(`${to}?`))
      ? state.returnTo
      : to;
  return (
    <Link className="back-link" to={returnTo}>
      <ArrowLeft aria-hidden="true" />
      {children}
    </Link>
  );
}
export function Empty({
  filtered = false,
  children,
}: {
  filtered?: boolean;
  children: ReactNode;
}) {
  return (
    <section className="empty-state">
      <h2>{filtered ? "No matching records" : "Nothing here yet"}</h2>
      <p>{children}</p>
    </section>
  );
}
export function QueryError({
  error,
  retry,
  stale = false,
}: {
  error: unknown;
  retry: () => void;
  stale?: boolean;
}) {
  const id =
    error instanceof ApiError || error instanceof ContractError
      ? error.requestId
      : null;
  return (
    <section className="notice error-notice" role="alert">
      <strong>
        {stale
          ? "Showing the last successful observation"
          : error instanceof ApiError && error.status === 404
            ? "Resource not found or retired"
            : "Unable to load this resource"}
      </strong>
      <p>
        {error instanceof Error
          ? error.message
          : "An unexpected error occurred."}
      </p>
      {id && (
        <p>
          Request ID <CopyText value={id} label="Copy request ID" />
        </p>
      )}
      <Button variant="outline" onClick={retry}>
        Try again
      </Button>
    </section>
  );
}
export function PageControls({
  pagination,
  nextCursor,
  observedAt,
  refresh,
  fetching,
  navigationOnly = false,
}: {
  pagination: ReturnType<typeof usePagination>;
  nextCursor: string | null;
  observedAt: number;
  refresh: () => void;
  fetching: boolean;
  navigationOnly?: boolean;
}) {
  return (
    <div className="page-controls">
      <span className="muted">
        Observed <When value={observedAt} />
      </span>
      {!navigationOnly && (
        <>
          <label>
            Rows{" "}
            <select
              value={pagination.limit}
              onChange={(e) => pagination.size(Number(e.target.value))}
            >
              {[25, 50, 100].map((n) => (
                <option key={n}>{n}</option>
              ))}
            </select>
          </label>
          <Button variant="outline" onClick={refresh} disabled={fetching}>
            <RefreshCw aria-hidden="true" />
            Refresh
          </Button>
        </>
      )}
      {(pagination.cursor || nextCursor) && (
        <>
          <Button
            variant="outline"
            disabled={!pagination.cursor}
            onClick={pagination.back}
          >
            {pagination.previous.length ? "Previous" : "First page"}
          </Button>
          <Button
            variant="outline"
            disabled={!nextCursor}
            onClick={() => {
              if (nextCursor) pagination.next(nextCursor);
            }}
          >
            Next
          </Button>
        </>
      )}
    </div>
  );
}
export function JsonView({
  value,
  label = "JSON",
}: {
  value: unknown;
  label?: string;
}) {
  const text = stringifyUserJson(value, 2);
  return (
    <div className="json-view">
      <div className="json-toolbar">
        <strong>{label}</strong>
      </div>
      <pre tabIndex={0} aria-label={label}>
        {text}
      </pre>
      <CopyJson text={text} />
    </div>
  );
}
export function Tabs({
  values,
  current,
}: {
  values: readonly string[];
  current: string;
}) {
  const [params, set] = useSearchParams();
  const location = useLocation();
  return (
    <nav className="tabs" aria-label="Detail views">
      {values.map((value) => (
        <Button
          key={value}
          variant="ghost"
          aria-current={value === current ? "page" : undefined}
          onClick={() => {
            const p = new URLSearchParams(params);
            p.set("tab", value);
            for (const key of [...p.keys()])
              if (key.endsWith("cursor") || key.endsWith("previous"))
                p.delete(key);
            set(p, { preventScrollReset: true, state: location.state });
          }}
        >
          {value}
        </Button>
      ))}
    </nav>
  );
}
export function Fields({ children }: { children: ReactNode }) {
  return <dl className="fields">{children}</dl>;
}
export function Field({
  label,
  children,
}: {
  label: string;
  children: ReactNode;
}) {
  return (
    <div>
      <dt>{label}</dt>
      <dd>{children}</dd>
    </div>
  );
}

function CopyJson({ text }: { text: string }) {
  const [message, setMessage] = useState("");
  return (
    <div className="actions">
      <Button
        variant="outline"
        onClick={() =>
          void navigator.clipboard.writeText(text).then(
            () => setMessage("Copied"),
            () => setMessage("Select and copy the JSON manually."),
          )
        }
      >
        Copy JSON
      </Button>
      <span role="status">{message}</span>
    </div>
  );
}

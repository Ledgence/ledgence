import { permitsRequest } from "./access-policy";
import {
  accessController,
  AccessChanged,
  type AccessLease,
} from "../app/access-state";
import { consoleContractVersion, type ConsoleConfig } from "./contracts";
import { ContractError, decodeConfig } from "./codecs";
import { parseUserJson } from "./json";
import { ApiError } from "./errors";
export const metadataMaximumBytes = 2 * 1024 * 1024;
export async function readBoundedText(
  response: Response,
  maximumBytes: number,
): Promise<string> {
  const size = response.headers.get("Content-Length");
  if (size && (!/^\d+$/.test(size) || Number(size) > maximumBytes)) {
    await response.body?.cancel();
    throw new ContractError("Server response exceeds the Console limit.");
  }
  if (!response.body) throw new ContractError("Server response is empty.");
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let length = 0;
  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      length += value.length;
      if (length > maximumBytes) {
        await reader.cancel();
        throw new ContractError("Server response exceeds the Console limit.");
      }
      chunks.push(value);
    }
  } finally {
    reader.releaseLock();
  }
  const bytes = new Uint8Array(length);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.length;
  }
  try {
    return new TextDecoder("utf-8", { fatal: true }).decode(bytes);
  } catch {
    throw new ContractError("The server returned invalid UTF-8.");
  }
}
export async function request<T>(
  path: string,
  decode: (value: unknown) => T,
  signal: AbortSignal,
  expectedInstance?: string,
  command?: { body: string; maximumBytes: number },
  maximumBytes = metadataMaximumBytes,
  access: AccessLease | null | undefined = undefined,
  actorAssertion?: string,
): Promise<{ value: T; headers: Headers }> {
  if (!/^\/v1\/console\/[a-z/-]+(?:\?[^#]*)?$/.test(path))
    throw new ContractError("Invalid Console API route.");
  if (
    command &&
    new TextEncoder().encode(command.body).length > command.maximumBytes
  )
    throw new ContractError("Command exceeds the configured request limit.");
  const lease =
    access === null
      ? undefined
      : (access ?? (await accessController.ready(signal)));
  if (lease && !lease.owner.current(lease)) throw new AccessChanged();
  if (
    lease &&
    !permitsRequest(path, !!command, (capability) =>
      lease.owner.permits(capability),
    )
  )
    throw new ApiError(
      "Your credential does not permit this operation. Contact your instance administrator.",
      403,
    );
  const controller = new AbortController();
  const timer = setTimeout(
    () => controller.abort(new Error("Request deadline exceeded.")),
    10000,
  );
  const abort = () => controller.abort(signal.reason);
  signal.addEventListener("abort", abort, { once: true });
  if (signal.aborted) abort();
  let requestId: string | null = null;
  try {
    const response = await fetch(path, {
      signal: controller.signal,
      method: command ? "POST" : "GET",
      headers: {
        Accept: "application/json",
        "X-Ledgence-Console-Origin": globalThis.location.origin,
        ...((lease?.actor ?? actorAssertion)
          ? { "X-Ledgence-Actor": (lease?.actor ?? actorAssertion)! }
          : {}),
        ...(command ? { "Content-Type": "application/json" } : {}),
      },
      ...(command ? { body: command.body } : {}),
      cache: "no-store",
      redirect: "error",
      credentials: "same-origin",
    });
    requestId = response.headers.get("Request-Id");
    if (lease && !lease.owner.current(lease)) {
      await response.body?.cancel();
      throw new AccessChanged();
    }
    if (!response.ok) {
      await response.body?.cancel();
      if (lease && [401, 403].includes(response.status))
        lease.owner.lock(
          response.status === 401
            ? "Sign in to continue."
            : "Your access changed or this operation is not permitted.",
          lease.epoch,
        );
      const raw = response.headers.get("Retry-After");
      let retry = raw && /^\d+$/.test(raw) ? Number(raw) * 1000 : null;
      if (raw && retry === null) {
        const at = Date.parse(raw);
        if (Number.isFinite(at)) retry = Math.max(0, at - Date.now());
      }
      throw new ApiError(
        `The server returned HTTP ${response.status}.`,
        response.status,
        requestId,
        retry,
      );
    }
    if (
      response.headers.get("Ledgence-Console-Contract") !==
      String(consoleContractVersion)
    ) {
      await response.body?.cancel();
      throw new ContractError(
        "The response has an incompatible Console contract.",
      );
    }
    if (
      expectedInstance &&
      response.headers.get("Ledgence-Instance-Id") !== expectedInstance
    ) {
      await response.body?.cancel();
      throw new ContractError(
        "The response belongs to a different Ledgence instance.",
      );
    }
    if (
      response.headers
        .get("Content-Type")
        ?.split(";")[0]
        ?.trim()
        .toLowerCase() !== "application/json"
    ) {
      await response.body?.cancel();
      throw new ContractError(
        "Expected a JSON response from the Ledgence API.",
      );
    }
    const body = await readBoundedText(response, maximumBytes);
    let value: unknown;
    try {
      value = parseUserJson(body, maximumBytes);
    } catch {
      throw new ContractError(
        "The server returned invalid or duplicate-field JSON.",
      );
    }
    const decoded = decode(value);
    if (lease && !lease.owner.current(lease)) throw new AccessChanged();
    return { value: decoded, headers: response.headers };
  } catch (error) {
    if (signal.aborted) throw error;
    if (error instanceof ContractError) {
      error.requestId = requestId;
      throw error;
    }
    if (error instanceof ApiError) throw error;
    throw new ApiError(
      controller.signal.aborted
        ? "The request deadline expired. Its result may be unknown."
        : "The connection ended before a complete response was received.",
      null,
      requestId,
    );
  } finally {
    clearTimeout(timer);
    signal.removeEventListener("abort", abort);
  }
}
export const readMetadata = request;
export async function getConfig(signal: AbortSignal): Promise<ConsoleConfig> {
  const { value, headers } = await request(
    "/v1/console/config",
    decodeConfig,
    signal,
  );
  if (headers.get("Ledgence-Instance-Id") !== value.instance_id)
    throw new ContractError(
      "The instance identity does not match its configuration.",
      headers.get("Request-Id"),
    );
  return value;
}
export function apiPath(
  resource: string,
  parameters: Record<string, string | number | null | undefined> = {},
): string {
  if (!/^[a-z/-]+$/.test(resource))
    throw new ContractError("Invalid resource.");
  const query = new URLSearchParams();
  for (const [key, value] of Object.entries(parameters))
    if (value !== undefined && value !== null) query.set(key, String(value));
  const suffix = query.toString();
  return `/v1/console/${resource}${suffix ? `?${suffix}` : ""}`;
}

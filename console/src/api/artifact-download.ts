// SPDX-License-Identifier: MIT
import { accessController, AccessChanged } from "../app/access-state";
import { consoleContractVersion } from "./contracts";
import { ContractError } from "./codecs";
import { ApiError } from "./errors";
import { parseUserJson } from "./json";
import {
  artifactManifest,
  sameArtifactRef,
  type ArtifactManifest,
} from "./artifacts";

function manifestHeader(response: Response, expected: ArtifactManifest) {
  const encoded = response.headers.get("Ledgence-Artifact-Manifest");
  if (
    !encoded ||
    encoded.length > 8192 ||
    !/^[A-Za-z0-9+/]*={0,2}$/.test(encoded)
  )
    throw new ContractError("Missing or invalid artifact manifest.");
  let decoded: unknown;
  try {
    const bytes = Uint8Array.from(atob(encoded), (character) =>
      character.charCodeAt(0),
    );
    decoded = parseUserJson(
      new TextDecoder("utf-8", { fatal: true }).decode(bytes),
      4096,
    );
  } catch {
    throw new ContractError("Invalid artifact manifest encoding.");
  }
  const manifest = artifactManifest(decoded);
  if (
    !sameArtifactRef(manifest.reference, expected.reference) ||
    JSON.stringify(manifest) !== JSON.stringify(expected)
  )
    throw new ContractError(
      "The downloaded file does not match the selected artifact.",
    );
  return manifest;
}

/** A user-requested binary transfer has its own total deadline. No preview,
 * query cache, presigned URL or automatic retry is involved. */
export async function downloadArtifact(
  expected: ArtifactManifest,
  instanceId: string,
  signal: AbortSignal,
  save: (blob: Blob, filename: string) => void = saveArtifact,
): Promise<void> {
  artifactManifest(expected);
  const access = await accessController.ready(signal);
  const assertCurrent = () => {
    signal.throwIfAborted();
    if (access && !access.owner.current(access)) throw new AccessChanged();
  };
  assertCurrent();
  if (access && !access.owner.permits("sensitive_read"))
    throw new ApiError(
      "Downloading files requires sensitive-read access.",
      403,
    );
  const controller = new AbortController();
  const abort = () => controller.abort(signal.reason);
  signal.addEventListener("abort", abort, { once: true });
  if (signal.aborted) abort();
  const unsubscribe = access?.owner.subscribe(() => {
    if (!access.owner.current(access)) controller.abort(new AccessChanged());
  });
  const deadline = setTimeout(
    () => controller.abort(new Error("Artifact download deadline exceeded.")),
    120000,
  );
  let requestId: string | null = null;
  let body: ReadableStreamDefaultReader<Uint8Array> | undefined;
  try {
    const response = await fetch("/v1/console/artifacts/download", {
      method: "POST",
      signal: controller.signal,
      cache: "no-store",
      redirect: "error",
      credentials: "same-origin",
      headers: {
        Accept: "application/octet-stream",
        "Content-Type": "application/json",
        "X-Ledgence-Console-Origin": globalThis.location.origin,
        ...(access?.actor ? { "X-Ledgence-Actor": access.actor } : {}),
      },
      body: JSON.stringify({ reference: expected.reference }),
    });
    requestId = response.headers.get("Request-Id");
    if (!response.ok) {
      await response.body?.cancel();
      if (access && [401, 403].includes(response.status))
        access.owner.lock(
          "Your access changed or this download is not permitted.",
          access.epoch,
        );
      throw new ApiError(
        `The server returned HTTP ${response.status}.`,
        response.status,
        requestId,
      );
    }
    try {
      assertCurrent();
      if (
        response.headers.get("Ledgence-Console-Contract") !==
          String(consoleContractVersion) ||
        response.headers.get("Ledgence-Instance-Id") !== instanceId
      )
        throw new ContractError(
          "The download belongs to a different Console instance or contract.",
        );
      if (
        response.headers
          .get("Content-Type")
          ?.split(";")[0]
          ?.trim()
          .toLowerCase() !== "application/octet-stream" ||
        !/^attachment(?:;|$)/i.test(
          response.headers.get("Content-Disposition") ?? "",
        )
      )
        throw new ContractError("Expected an attachment download.");
      if (
        response.headers.get("Content-Length") !==
        String(expected.reference.size)
      )
        throw new ContractError(
          "The download length does not match its artifact.",
        );
      manifestHeader(response, expected);
      if (!response.body)
        throw new ContractError("The artifact body is missing.");
    } catch (error) {
      await response.body?.cancel();
      throw error;
    }
    body = response.body!.getReader();
    // The declared, validated 64 MiB cap bounds this browser buffer. WebCrypto
    // verifies the entire file before any save action; it has no streaming SHA.
    const bytes = new Uint8Array(expected.reference.size);
    let offset = 0;
    for (;;) {
      const { done, value } = await body.read();
      controller.signal.throwIfAborted();
      assertCurrent();
      if (done) break;
      if (offset + value.length > bytes.length)
        throw new ContractError(
          "The artifact body exceeds its declared length.",
        );
      bytes.set(value, offset);
      offset += value.length;
    }
    if (offset !== bytes.length)
      throw new ContractError("The artifact download was truncated.");
    const hash = await crypto.subtle.digest("SHA-256", bytes);
    controller.signal.throwIfAborted();
    assertCurrent();
    const digest =
      "sha256:" +
      [...new Uint8Array(hash)]
        .map((byte) => byte.toString(16).padStart(2, "0"))
        .join("");
    if (digest !== expected.reference.digest)
      throw new ContractError(
        "Artifact integrity verification failed. The file was not saved.",
      );
    // Always save as an attachment; untrusted media types are never rendered.
    save(
      new Blob([bytes], { type: "application/octet-stream" }),
      expected.filename ?? `${expected.reference.id}.bin`,
    );
  } catch (error) {
    if (access && !access.owner.current(access)) throw new AccessChanged();
    if (error instanceof ContractError) {
      error.requestId = requestId;
      throw error;
    }
    if (error instanceof ApiError || signal.aborted) throw error;
    throw new ApiError(
      controller.signal.aborted
        ? "The file download deadline expired. Retry the download when ready."
        : "The file download did not complete.",
      null,
      requestId,
    );
  } finally {
    await body?.cancel().catch(() => {});
    body?.releaseLock();
    clearTimeout(deadline);
    unsubscribe?.();
    signal.removeEventListener("abort", abort);
  }
}
export function saveArtifact(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  try {
    link.href = url;
    link.download = filename;
    link.rel = "noopener";
    document.body.append(link);
    link.click();
  } finally {
    link.remove();
    // Let the browser acquire the attachment, then promptly revoke its private
    // object URL. The URL never enters app state, history, logs or the API.
    setTimeout(() => URL.revokeObjectURL(url), 0);
  }
}

// SPDX-License-Identifier: MIT
import { readFileSync } from "node:fs";
import { beforeEach, afterEach, expect, it, vi } from "vitest";
import {
  artifactManifest,
  artifactPage,
  artifactPageFor,
} from "../../src/api/artifacts";
import { downloadArtifact } from "../../src/api/artifact-download";
import { requiredCapabilities } from "../../src/api/access-policy";
import { accessController } from "../../src/app/access-state";
const fixture = JSON.parse(
  readFileSync(
    new URL(
      "../../../crates/ledgence-orchestration-api/tests/fixtures/console-v16.json",
      import.meta.url,
    ),
    "utf8",
  ),
);
const manifest = artifactManifest(fixture.artifact_manifest);
function trusted() {
  const epoch = accessController.begin("test", true);
  accessController.accept({ mode: "operator_trusted" }, epoch, "instance_demo");
}
function response(content = "hello", overrides: HeadersInit = {}) {
  return new Response(content, {
    headers: {
      "Content-Type": "application/octet-stream",
      "Content-Length": "5",
      "Content-Disposition": "attachment; filename=invoice-report.txt",
      "Ledgence-Console-Contract": "16",
      "Ledgence-Instance-Id": "instance_demo",
      "Request-Id": "req-download",
      "Ledgence-Artifact-Manifest": btoa(JSON.stringify(manifest)),
      ...overrides,
    },
  });
}
beforeEach(() => {
  vi.stubGlobal("location", { origin: "http://console.test" });
  trusted();
});
afterEach(() => {
  vi.useRealTimers();
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
  trusted();
});
it("decodes the Rust projection without accepting private scope or storage locations", () => {
  expect(artifactPage(fixture.artifact_page).items[0]).toEqual(manifest);
  // Match the public contract's bounded ASCII value containing a slash;
  // the transport still returns application/octet-stream, never this label.
  expect(
    artifactManifest({
      ...manifest,
      reference: { ...manifest.reference, media_type: "/" },
    }).reference.media_type,
  ).toBe("/");
  expect(() =>
    artifactManifest({
      ...manifest,
      scope: { tenant_id: "x", namespace: "y" },
    }),
  ).toThrow();
  expect(() =>
    artifactManifest({ ...manifest, url: "https://object-store/private" }),
  ).toThrow();
  expect(() =>
    artifactPage({ items: [manifest, manifest], next_cursor: null }),
  ).toThrow();
  expect(() =>
    artifactManifest({ ...manifest, filename: "../escape" }),
  ).toThrow();
  expect(() =>
    artifactManifest({
      ...manifest,
      reference: { ...manifest.reference, size: 67108865 },
    }),
  ).toThrow();
});
it("rejects zero creation time and oversized producer issuers", () => {
  expect(() => artifactManifest({ ...manifest, created_at: 0 })).toThrow();
  expect(() =>
    artifactManifest({
      ...manifest,
      producer: {
        kind: "verified_actor",
        identity: { issuer: "x".repeat(257), subject: "alice" },
      },
    }),
  ).toThrow();
});
it("uses independent metadata and file-content permissions even for POST reads", () => {
  expect(requiredCapabilities("/v1/console/artifacts/list", true)).toEqual([
    "metadata_read",
  ]);
  expect(requiredCapabilities("/v1/console/artifacts/inspect", true)).toEqual([
    "metadata_read",
  ]);
  expect(requiredCapabilities("/v1/console/artifacts/download", true)).toEqual([
    "sensitive_read",
  ]);
});
it("verifies an explicit attachment transfer before passing bytes to the browser", async () => {
  const fetch = vi.fn().mockResolvedValue(response());
  vi.stubGlobal("fetch", fetch);
  const save = vi.fn();
  await downloadArtifact(
    manifest,
    "instance_demo",
    new AbortController().signal,
    save,
  );
  expect(fetch).toHaveBeenCalledOnce();
  expect(fetch.mock.calls[0]?.[1]).toMatchObject({
    method: "POST",
    redirect: "error",
    cache: "no-store",
    credentials: "same-origin",
    headers: { "X-Ledgence-Console-Origin": "http://console.test" },
    body: JSON.stringify({ reference: manifest.reference }),
  });
  expect(save).toHaveBeenCalledOnce();
  expect(save.mock.calls[0]?.[1]).toBe("invoice-report.txt");
  expect(await (save.mock.calls[0]?.[0] as Blob).text()).toBe("hello");
});
it.each(["HELLO", "hell", "hello!"])(
  "does not save corrupted, truncated or oversized content %s",
  async (body) => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(response(body)));
    const save = vi.fn();
    await expect(
      downloadArtifact(
        manifest,
        "instance_demo",
        new AbortController().signal,
        save,
      ),
    ).rejects.toMatchObject({ requestId: "req-download" });
    expect(save).not.toHaveBeenCalled();
  },
);
it.each([
  { "Ledgence-Instance-Id": "another" },
  { "Ledgence-Console-Contract": "15" },
  { "Content-Type": "text/html" },
  { "Content-Disposition": "inline" },
  { "Content-Length": "6" },
  { "Ledgence-Artifact-Manifest": "bad base64" },
  {
    "Ledgence-Artifact-Manifest": btoa(
      JSON.stringify({
        ...manifest,
        reference: { ...manifest.reference, id: "another-artifact" },
      }),
    ),
  },
])("rejects incorrect attachment headers before saving %#", async (headers) => {
  vi.stubGlobal("fetch", vi.fn().mockResolvedValue(response("hello", headers)));
  const save = vi.fn();
  await expect(
    downloadArtifact(
      manifest,
      "instance_demo",
      new AbortController().signal,
      save,
    ),
  ).rejects.toThrow();
  expect(save).not.toHaveBeenCalled();
});
it("metadata-only credentials cannot initiate a file download", async () => {
  const epoch = accessController.begin("restricted", true);
  accessController.accept(
    {
      mode: "credentials",
      actor_key: "actor",
      issuer: "issuer",
      subject: "subject",
      class: "operator",
      instance_id: "instance_demo",
      scope: { tenant_id: "acme", namespace: "billing" },
      capabilities: ["metadata_read"],
      expires_at_ms: Date.now() + 60000,
    },
    epoch,
    "instance_demo",
  );
  const fetch = vi.fn();
  vi.stubGlobal("fetch", fetch);
  await expect(
    downloadArtifact(
      manifest,
      "instance_demo",
      new AbortController().signal,
      vi.fn(),
    ),
  ).rejects.toMatchObject({ status: 403 });
  expect(fetch).not.toHaveBeenCalled();
});
function stalledFetch() {
  const fetch = vi.fn(
    (_url, init: RequestInit) =>
      new Promise<Response>((_resolve, reject) => {
        init.signal?.addEventListener(
          "abort",
          () => reject(init.signal?.reason),
          { once: true },
        );
      }),
  );
  vi.stubGlobal("fetch", fetch);
  return fetch;
}
it("identity changes abort an active transfer instead of saving under another actor", async () => {
  const fetch = stalledFetch();
  const save = vi.fn();
  const result = downloadArtifact(
    manifest,
    "instance_demo",
    new AbortController().signal,
    save,
  ).catch((error: unknown) => error);
  await vi.waitFor(() => expect(fetch).toHaveBeenCalledOnce());
  accessController.begin("different identity", true);
  expect(await result).toMatchObject({ status: 403 });
  expect(fetch.mock.calls[0]?.[1].signal?.aborted).toBe(true);
  expect(save).not.toHaveBeenCalled();
});
it("navigation aborts and the total binary deadline is independent of the JSON deadline", async () => {
  vi.useFakeTimers();
  const fetch = stalledFetch();
  const save = vi.fn();
  const result = downloadArtifact(
    manifest,
    "instance_demo",
    new AbortController().signal,
    save,
  ).catch((error: unknown) => error);
  await vi.advanceTimersByTimeAsync(10001);
  expect(fetch.mock.calls[0]?.[1].signal?.aborted).toBe(false);
  await vi.advanceTimersByTimeAsync(110000);
  expect(await result).toMatchObject({
    message: expect.stringContaining("deadline expired"),
  });
  expect(save).not.toHaveBeenCalled();
  const controller = new AbortController();
  const cancelled = downloadArtifact(
    manifest,
    "instance_demo",
    controller.signal,
    save,
  ).catch((error: unknown) => error);
  await vi.advanceTimersByTimeAsync(1);
  controller.abort();
  await cancelled;
  expect(fetch.mock.calls[1]?.[1].signal?.aborted).toBe(true);
});

it("rejects nonadvancing and over-limit artifact pages", () => {
  expect(() =>
    artifactPageFor(25, "same")({ items: [manifest], next_cursor: "same" }),
  ).toThrow();
  expect(() =>
    artifactPageFor(25, null)({ items: [], next_cursor: "next" }),
  ).toThrow();
  expect(() =>
    artifactPageFor(0, null)({ items: [manifest], next_cursor: null }),
  ).toThrow();
});

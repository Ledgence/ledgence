import { afterEach, describe, it, expect, vi } from "vitest";
import { request } from "../../src/api/client";
import { object, string } from "../../src/api/schema";
import { freezeCommand } from "../../src/api/commands";
import { parseUserJson } from "../../src/api/json";
const decode = object({ id: string });
function reply(body: string, extra: HeadersInit = {}) {
  return new Response(body, {
    headers: {
      "Content-Type": "application/json",
      "Ledgence-Console-Contract": "1",
      "Ledgence-Instance-Id": "instance_demo",
      "Request-Id": "req-test",
      ...extra,
    },
  });
}
afterEach(() => vi.unstubAllGlobals());
describe("Console transport boundaries", () => {
  it("rejects cross-instance responses and preserves request ID", async () => {
    vi.stubGlobal(
      "fetch",
      vi
        .fn()
        .mockResolvedValue(
          reply('{"id":"a"}', { "Ledgence-Instance-Id": "other" }),
        ),
    );
    await expect(
      request(
        "/v1/console/tasks",
        decode,
        new AbortController().signal,
        "instance_demo",
      ),
    ).rejects.toMatchObject({ requestId: "req-test" });
  });
  it("rejects duplicate fields before decoding", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(reply('{"id":"a","id":"b"}')),
    );
    await expect(
      request(
        "/v1/console/tasks",
        decode,
        new AbortController().signal,
        "instance_demo",
      ),
    ).rejects.toThrow("duplicate");
  });
  it("retains Retry-After and request identity on rate limits", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(
        new Response("", {
          status: 429,
          headers: { "Retry-After": "3", "Request-Id": "req-limit" },
        }),
      ),
    );
    await expect(
      request(
        "/v1/console/tasks",
        decode,
        new AbortController().signal,
        "instance_demo",
      ),
    ).rejects.toMatchObject({
      status: 429,
      retryAfterMs: 3000,
      requestId: "req-limit",
    });
  });
  it("rejects oversized responses and malformed content types", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(reply('{"id":"a"}')));
    await expect(
      request(
        "/v1/console/tasks",
        decode,
        new AbortController().signal,
        "instance_demo",
        undefined,
        2,
      ),
    ).rejects.toThrow("limit");
    vi.stubGlobal(
      "fetch",
      vi
        .fn()
        .mockResolvedValue(
          reply('{"id":"a"}', { "Content-Type": "application/jsonoops" }),
        ),
    );
    await expect(
      request(
        "/v1/console/tasks",
        decode,
        new AbortController().signal,
        "instance_demo",
      ),
    ).rejects.toThrow("Expected a JSON");
  });
  it("propagates cancellation to the underlying request", async () => {
    const c = new AbortController();
    c.abort();
    const mock = vi.fn((_path: unknown, init: RequestInit) => {
      expect(init.signal?.aborted).toBe(true);
      return Promise.reject(new DOMException("Aborted", "AbortError"));
    });
    vi.stubGlobal("fetch", mock);
    await expect(
      request("/v1/console/tasks", decode, c.signal, "instance_demo"),
    ).rejects.toThrow("Aborted");
  });
  it("freezes exact lossless request bytes for retry", () => {
    const command = freezeCommand(
      {
        idempotency_key: "one",
        input: parseUserJson(
          '{"large":9007199254740993,"float":1.0,"zero":-0.0}',
          1000,
        ),
      },
      "one",
    );
    expect(Object.isFrozen(command)).toBe(true);
    expect(command.body).toContain("9007199254740993");
    expect(command.body).toContain("1.0");
    expect(command.body).toContain("-0.0");
  });
});

it("treats invalid UTF-8 as a permanent protocol failure with request ID", async () => {
  vi.stubGlobal(
    "fetch",
    vi.fn().mockResolvedValue(
      new Response(new Uint8Array([0xc3, 0x28]), {
        headers: {
          "Content-Type": "application/json",
          "Ledgence-Console-Contract": "1",
          "Ledgence-Instance-Id": "instance_demo",
          "Request-Id": "req-encoding",
        },
      }),
    ),
  );
  await expect(
    request(
      "/v1/console/tasks",
      decode,
      new AbortController().signal,
      "instance_demo",
    ),
  ).rejects.toMatchObject({
    name: "ContractError",
    requestId: "req-encoding",
    message: "The server returned invalid UTF-8.",
  });
});

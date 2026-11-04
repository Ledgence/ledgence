// SPDX-License-Identifier: MIT
import { readFileSync } from "node:fs";
import type { BrowserContext } from "@playwright/test";

export async function login(
  context: BrowserContext,
  origin: string,
  key: string,
) {
  const file = process.env[key];
  if (!file)
    throw new Error("Provide a disposable local " + key + " credential file.");
  const credential = JSON.parse(readFileSync(file, "utf8")) as {
    server: string;
    token: string;
  };
  if (new URL(credential.server).origin !== origin || !credential.token)
    throw new Error(
      "The test credential must be bound to the disposable server.",
    );
  // Use the real login endpoint outside Playwright instrumentation so raw credentials do not reach test traces.
  const response = await fetch(origin + "/v1/console/access/login", {
    method: "POST",
    redirect: "error",
    headers: {
      "Content-Type": "application/json",
      Origin: origin,
      "X-Ledgence-Console-Origin": origin,
    },
    body: JSON.stringify({ credential: credential.token }),
    signal: AbortSignal.timeout(10000),
  });
  if (!response.ok)
    throw new Error("Local Console login failed with HTTP " + response.status);
  const header = response.headers.get("set-cookie");
  if (!header || !header.toLowerCase().includes("httponly"))
    throw new Error("An HttpOnly Console session cookie was required.");
  const pair = header.slice(0, header.indexOf(";"));
  const eq = pair.indexOf("=");
  if (eq <= 0) throw new Error("Invalid session cookie.");
  await context.addCookies([
    {
      name: pair.slice(0, eq),
      value: pair.slice(eq + 1),
      domain: new URL(origin).hostname,
      path: "/",
      httpOnly: true,
      secure: new URL(origin).protocol === "https:",
      sameSite: "Strict",
    },
  ]);
}

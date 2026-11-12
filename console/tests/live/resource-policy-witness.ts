// SPDX-License-Identifier: MIT
import type { Page, Response } from "@playwright/test";
import type { QueueScope } from "../../src/api/queue-policies";
import {
  inspectResource,
  resourcePut,
  resourceReceipt,
  type ResourcePut,
  type ResourceSnapshot,
} from "../../src/api/resource-sharing";

export interface ResourceAuthority {
  actor: string;
  scope: QueueScope;
}
type WireSnapshot = Omit<ResourceSnapshot, "resource"> & {
  resource: Omit<ResourceSnapshot["resource"], "policy"> & {
    policy: ResourcePut["policy"];
  };
};

// Chromium can discard a completed browser response body. Verify the committed
// policy through an independent read using the edit's original authenticated
// browser session; never repeat the mutation to recover its response body.
export async function inspectCommittedResource(
  page: Page,
  response: Response,
  authority: ResourceAuthority,
  name: string,
): Promise<WireSnapshot> {
  const origin = new URL(page.url()).origin;
  const endpoint = new URL(response.url());
  const request = response.request();
  if (
    response.status() !== 200 ||
    endpoint.origin !== origin ||
    endpoint.pathname !== "/v1/console/resources/put" ||
    request.method() !== "POST" ||
    !authority.actor ||
    request.headers()["x-ledgence-actor"] !== authority.actor
  )
    throw new Error("The accepted edit must belong to the operator session.");
  const command = resourcePut(request.postDataJSON());
  if (command.name !== name)
    throw new Error("The accepted edit must identify the selected resource.");
  const snapshot = await page.request.post(
    origin + "/v1/console/resources/inspect",
    {
      headers: {
        Origin: origin,
        "X-Ledgence-Console-Origin": origin,
        "X-Ledgence-Actor": authority.actor,
      },
      data: { name },
      maxRedirects: 0,
      maxRetries: 0,
    },
  );
  if (snapshot.status() !== 200)
    throw new Error("The operator could not inspect the committed resource.");
  const raw: unknown = await snapshot.json();
  inspectResource(authority.scope, name)(raw);
  // Match the complete submitted policy and next revision, not merely a name or
  // a successful GET. Keep the raw shape so wire-level omission checks survive.
  const saved = raw as WireSnapshot;
  resourceReceipt(
    { resource: saved.resource, already_accepted: false },
    authority.scope,
    command,
  );
  return saved;
}

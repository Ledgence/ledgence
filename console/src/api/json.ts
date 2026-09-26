import { parse, stringify } from "lossless-json";
// User JSON is never converted through native JSON.parse/JSON.stringify.
export function parseUserJson(source: string, maximumBytes: number): unknown {
  if (new TextEncoder().encode(source).byteLength > maximumBytes)
    throw new Error(`JSON exceeds the ${maximumBytes}-byte limit.`);
  return parse(source) as unknown;
}
export function stringifyUserJson(value: unknown, indent?: number): string {
  const result = stringify(value, undefined, indent);
  if (typeof result !== "string") throw new Error("The value is not JSON.");
  return result;
}
export function formatUserJson(source: string, maximumBytes: number): string {
  return stringifyUserJson(parseUserJson(source, maximumBytes), 2);
}

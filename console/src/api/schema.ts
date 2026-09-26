import { isLosslessNumber } from "lossless-json";
import { ContractError, record } from "./codecs";
export type Decoder<T> = (value: unknown) => T;
export type Decoded<D> = D extends Decoder<infer T> ? T : never;
export function object<S extends Record<string, Decoder<unknown>>>(
  shape: S,
): Decoder<{ [K in keyof S]: Decoded<S[K]> }> {
  return (value) => {
    const input = record(value, Object.keys(shape));
    const output: Record<string, unknown> = {};
    for (const [key, decode] of Object.entries(shape))
      output[key] = decode(input[key]);
    // Every property has just been checked by its corresponding decoder.
    return output as { [K in keyof S]: Decoded<S[K]> };
  };
}
export function string(value: unknown): string {
  if (typeof value !== "string")
    throw new ContractError("Expected response text.");
  return value;
}
export function id(value: unknown): string {
  const result = string(value);
  if (
    !result ||
    new TextEncoder().encode(result).length > 512 ||
    [...result].some((c) => c.charCodeAt(0) < 32 || c.charCodeAt(0) === 127)
  )
    throw new ContractError("Invalid response identifier.");
  return result;
}
export function boolean(value: unknown): boolean {
  if (typeof value !== "boolean")
    throw new ContractError("Expected a boolean.");
  return value;
}
export function integer(min = 0, max = 4294967295): Decoder<number> {
  return (value) => {
    if (isLosslessNumber(value) && !/^(0|[1-9][0-9]*)$/.test(value.value))
      throw new ContractError("Expected a nonnegative integer token.");
    const n = isLosslessNumber(value) ? Number(value.value) : value;
    if (typeof n !== "number" || !Number.isSafeInteger(n) || n < min || n > max)
      throw new ContractError("Invalid response integer.");
    return n;
  };
}
export const timestamp = integer(0, 253402300799999);
export function decimal(value: unknown): string {
  const v = string(value);
  if (!/^(0|[1-9][0-9]{0,19})$/.test(v) || BigInt(v) > 18446744073709551615n)
    throw new ContractError("Expected a canonical u64 decimal string.");
  return v;
}
export function enumeration<const T extends readonly string[]>(
  ...values: T
): Decoder<T[number]> {
  return (value) => {
    const v = string(value);
    if (!values.includes(v))
      throw new ContractError("Unknown response variant.");
    return v as T[number];
  };
}
export function nullable<T>(decode: Decoder<T>): Decoder<T | null> {
  return (value) => (value === null ? null : decode(value));
}
export function array<T>(decode: Decoder<T>, max = 100): Decoder<T[]> {
  return (value) => {
    if (!Array.isArray(value) || value.length > max)
      throw new ContractError("Invalid response array.");
    return value.map(decode);
  };
}
export const payload: Decoder<unknown> = (value) => value;
export function variant<S extends Record<string, Decoder<unknown>>>(
  shape: S,
): Decoder<Decoded<S[keyof S]>> {
  return (value) => {
    if (
      value === null ||
      typeof value !== "object" ||
      !("kind" in value) ||
      typeof value.kind !== "string" ||
      !Object.hasOwn(shape, value.kind)
    )
      throw new ContractError("Unknown result variant.");
    const decode = shape[value.kind];
    if (!decode) throw new ContractError("Missing result decoder.");
    return decode(value) as Decoded<S[keyof S]>;
  };
}
export function page<T>(decode: Decoder<T>) {
  return object({
    items: array(decode),
    next_cursor: nullable(string),
    observed_at: timestamp,
  });
}

export function refine<T>(
  decode: Decoder<T>,
  valid: (value: T) => boolean,
  message: string,
): Decoder<T> {
  return (value) => {
    const result = decode(value);
    if (!valid(result)) throw new ContractError(message);
    return result;
  };
}

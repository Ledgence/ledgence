import { describe, expect, it } from "vitest";
import {
  formatUserJson,
  parseUserJson,
  stringifyUserJson,
} from "../../src/api/json";
describe("lossless user JSON", () => {
  for (const value of [
    "9007199254740993",
    "18446744073709551615",
    "-0",
    "0.1234567890123456789012345",
    "1.00000000000000001e+100",
    "null",
  ]) {
    it(`preserves ${value}`, () =>
      expect(stringifyUserJson(parseUserJson(`{"value":${value}}`, 1024))).toBe(
        `{"value":${value}}`,
      ));
  }
  it("rejects duplicate keys", () =>
    expect(() => parseUserJson('{"id":1,"id":2}', 1024)).toThrow(/Duplicate/));
  it("bounds UTF-8 bytes", () =>
    expect(() => parseUserJson('"é"', 3)).toThrow(/limit/));
  it("formats without rounding", () =>
    expect(formatUserJson('{"id":9007199254740993}', 1024)).toContain(
      "9007199254740993",
    ));
});

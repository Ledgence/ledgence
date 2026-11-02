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
  it.each([
    '{"__proto__":2}',
    '{"__proto__":{"isLosslessNumber":true},"constructor":"user"}',
    '[{"toString":null,"isLosslessNumber":true,"value":"19"}]',
  ])("preserves user object keys in %s", (source) => {
    const value = parseUserJson(source, 1024);
    expect(stringifyUserJson(value)).toBe(source);
    expect(
      stringifyUserJson(parseUserJson(formatUserJson(source, 1024), 1024)),
    ).toBe(source);
  });
  it.each([
    '{"id":1,"id":2}',
    '{"id":1,"id":1}',
    '{"a":[{"id":1,"\\u0069d":1}]}',
    '{"__proto__":{},"__proto__":{}}',
  ])("rejects duplicate keys in %s", (source) => {
    expect(() => parseUserJson(source, 1024)).toThrow(/Duplicate/);
  });
  it("bounds UTF-8 bytes", () =>
    expect(() => parseUserJson('"é"', 3)).toThrow(/limit/));
  it("formats without rounding", () =>
    expect(formatUserJson('{"id":9007199254740993}', 1024)).toContain(
      "9007199254740993",
    ));
});

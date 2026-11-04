// SPDX-License-Identifier: MIT
import * as s from "./schema";

function validPlatformText(value: string, limit: number) {
  return (
    value.length > 0 &&
    new TextEncoder().encode(value).length <= limit &&
    [...value].every((character) => {
      const code = character.codePointAt(0)!;
      return (
        code >= 32 &&
        !(code >= 127 && code <= 159) &&
        !(code >= 0xfdd0 && code <= 0xfdef) &&
        (code & 0xfffe) !== 0xfffe
      );
    })
  );
}
export const effectText = (limit: number) =>
  s.refine(
    s.string,
    (value) => validPlatformText(value, limit),
    "Effect identifier exceeds its limit.",
  );
export const effectId = effectText(128);
export const effectScope = s.object({
  tenant_id: effectId,
  namespace: effectId,
});
export const effectOccurrence = s.object({
  scope: effectScope,
  workflow_id: effectId,
  activation_id: effectId,
  approval_key: effectId,
});
export type EffectOccurrence = s.Decoded<typeof effectOccurrence>;

"""Strict bounded JSON matching the Rust wire profile; no user coercion hooks."""
from __future__ import annotations

import json
import math
import unicodedata

from .errors import InputError, ProtocolError

DATA_LIMIT = 1024 * 1024
CONTROL_LIMIT = 2 * 1024 * 1024
RESPONSE_LIMIT = 16 * 1024 * 1024
STATUS_LIMIT = 16 * 1024
MAX_DEPTH = 64


def text(value, name: str, maximum: int = 128, *, empty: bool = False,
         noncharacters: bool = False) -> str:
    if type(value) is not str:
        raise InputError(f"{name} must be a string")
    try:
        raw = value.encode("utf-8")
    except UnicodeError as exc:
        raise InputError(f"{name} must contain Unicode scalar values") from exc
    if (not empty and not raw) or len(raw) > maximum or any(
        unicodedata.category(c) == "Cc" or (
            not noncharacters and (0xFDD0 <= ord(c) <= 0xFDEF or ord(c) & 0xFFFE == 0xFFFE)
        ) for c in value
    ):
        raise InputError(f"invalid {name}")
    return value


def integer(value, name: str, low: int = 0, high: int = (1 << 64) - 1) -> int:
    if type(value) is not int or not low <= value <= high:
        raise InputError(f"{name} must be an integer from {low} through {high}")
    return value


def duration(value, name: str, maximum: float | None = None) -> float:
    if type(value) not in (int, float):
        raise InputError(f"{name} must be a positive finite number")
    try:
        result = float(value)
    except OverflowError as exc:
        raise InputError(f"{name} is too large") from exc
    if not math.isfinite(result) or result <= 0 or (maximum is not None and result > maximum):
        raise InputError(f"invalid {name}")
    return result


def validate(value, limit: int, *, max_depth: int = MAX_DEPTH) -> None:
    """Bound traversal as well as the eventual bytes, accepting plain JSON types."""
    remaining = limit
    ancestors: set[int] = set()

    def visit(item, depth):
        nonlocal remaining
        remaining -= 1
        if remaining < 0:
            raise InputError("JSON value exceeds its byte limit")
        kind = type(item)
        if item is None or kind is bool:
            return
        if kind is str:
            if len(item) > remaining:
                raise InputError("JSON string exceeds its byte limit")
            try:
                remaining -= len(item.encode("utf-8"))
            except UnicodeError as exc:
                raise InputError("JSON strings must contain Unicode scalar values") from exc
        elif kind is int:
            integer(item, "JSON integer", -(1 << 63), (1 << 64) - 1)
        elif kind is float:
            if not math.isfinite(item):
                raise InputError("JSON floating-point numbers must be finite")
        elif kind in (dict, list, tuple):
            if depth >= max_depth:
                raise InputError("JSON value exceeds its container depth limit")
            identity = id(item)
            if identity in ancestors:
                raise InputError("JSON value contains a cycle")
            ancestors.add(identity)
            try:
                if kind is dict:
                    for key, child in item.items():
                        if type(key) is not str:
                            raise InputError("JSON object keys must be strings")
                        visit(key, depth + 1)
                        visit(child, depth + 1)
                else:
                    for child in item:
                        visit(child, depth + 1)
            finally:
                ancestors.remove(identity)
        else:
            raise InputError("value must contain only plain JSON-compatible types")
        if remaining < 0:
            raise InputError("JSON value exceeds its byte limit")

    visit(value, 0)


def encode(value, limit: int = CONTROL_LIMIT, *, max_depth: int = MAX_DEPTH + 8) -> bytes:
    validate(value, limit, max_depth=max_depth)
    chunks: list[bytes] = []
    length = 0
    encoder = json.JSONEncoder(ensure_ascii=False, allow_nan=False, separators=(",", ":"))
    for part in encoder.iterencode(value):
        chunk = part.encode("utf-8")
        length += len(chunk)
        if length > limit:
            raise InputError("encoded JSON exceeds its byte limit")
        chunks.append(chunk)
    return b"".join(chunks)


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON object key")
        result[key] = value
    return result


def _int(token):
    if token == "-0":
        return -0.0
    if len(token) > 20:
        raise ValueError("integer token exceeds i64/u64")
    result = int(token)
    if not -(1 << 63) <= result <= (1 << 64) - 1:
        raise ValueError("integer token exceeds i64/u64")
    return result


def _float(token):
    result = float(token)
    if not math.isfinite(result):
        raise ValueError("nonfinite JSON number")
    return result


def _constant(_):
    raise ValueError("nonfinite JSON number")


def decode(raw: bytes, limit: int = RESPONSE_LIMIT):
    if len(raw) > limit:
        raise ProtocolError("response exceeds its byte limit")
    try:
        value = json.loads(raw.decode("utf-8", errors="strict"), object_pairs_hook=_pairs,
                           parse_int=_int, parse_float=_float, parse_constant=_constant)
        validate(value, limit, max_depth=MAX_DEPTH + 12)
        return value
    except (ValueError, UnicodeError, RecursionError) as exc:
        raise ProtocolError("response is not valid Ledgence JSON") from exc


def fields(value, required: set[str], optional: set[str] = frozenset()):
    if type(value) is not dict or set(value) - required - optional or required - set(value):
        raise InputError("invalid object fields")
    return value

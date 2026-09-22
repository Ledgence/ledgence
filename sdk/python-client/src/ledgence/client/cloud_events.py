"""Portable CloudEvent validation for external workflow events (MIT)."""
import calendar
import ipaddress
import re
import unicodedata

from . import codec
from .errors import InputError

EVENT_LIMIT = 64 * 1024
EVENT_COMMAND_LIMIT = 70 * 1024


def _validate_event_uri(value, *, absolute=False):
    """Validate RFC 3986 URI references without rewriting their identity."""
    atom = r"(?:[A-Za-z0-9._~!$&'()*+,;=\-]|%[0-9A-Fa-f]{2})"
    pchar = rf"(?:{atom}|[:@])"
    path_query, fragment_mark, fragment = value.partition("#")
    if absolute and fragment_mark:
        raise InputError("dataschema must be an absolute URI without a fragment")
    path, query_mark, query = path_query.partition("?")
    if not re.fullmatch(rf"(?:{pchar}|[/?])*", fragment) or not re.fullmatch(rf"(?:{pchar}|[/?])*", query):
        raise InputError("invalid CloudEvent URI query or fragment")
    scheme = re.match(r"[A-Za-z][A-Za-z0-9+.-]*:", path)
    if absolute and scheme is None:
        raise InputError("dataschema must be an absolute URI")
    if scheme is not None:
        path = path[scheme.end():]
    authority = path.startswith("//")
    if authority:
        host, slash, rest = path[2:].partition("/")
        path = slash + rest
        if "@" in host:
            user, _, host = host.partition("@")
            if not re.fullmatch(rf"(?:{atom}|:)*", user):
                raise InputError("invalid CloudEvent URI user information")
        if host.startswith("["):
            address, close, port = host[1:].partition("]")
            if not close or (port and not re.fullmatch(r":[0-9]*", port)):
                raise InputError("invalid CloudEvent URI host")
            if re.fullmatch(r"v[0-9A-Fa-f]+\.[A-Za-z0-9._~!$&'()*+,;=:\-]+", address, re.IGNORECASE) is None:
                try:
                    if "%" in address:
                        raise ValueError("zone identifier is not an RFC3986 address")
                    ipaddress.IPv6Address(address)
                except ValueError as exc:
                    raise InputError("invalid CloudEvent URI address") from exc
        else:
            name, colon, port = host.partition(":")
            if not re.fullmatch(rf"{atom}*", name) or (colon and not re.fullmatch(r"[0-9]*", port)):
                raise InputError("invalid CloudEvent URI host or port")
    if not re.fullmatch(rf"(?:{pchar}|/)*", path):
        raise InputError("invalid CloudEvent URI path")
    if scheme is None and not authority and ":" in path.partition("/")[0]:
        raise InputError("relative CloudEvent URI first segment cannot contain a colon")


def _validate_event_trace(event):
    trace = event.get("traceparent")
    if trace is not None and (type(trace) is not str
            or not re.fullmatch(r"00-[0-9a-f]{32}-[0-9a-f]{16}-[0-9a-f]{2}", trace)
            or trace[3:35] == "0" * 32 or trace[36:52] == "0" * 16):
        raise InputError("invalid CloudEvent traceparent")
    if "tracestate" not in event:
        return
    state = event["tracestate"]
    if trace is None or type(state) is not str or not state.isascii() or len(state) > 512:
        raise InputError("invalid CloudEvent tracestate")
    members = state.split(",")
    if len(members) > 32:
        raise InputError("too many CloudEvent tracestate members")
    seen = set()
    for member in members:
        member = member.strip(" ")
        if not member:
            continue
        key, equal, item = member.partition("=")
        if (not equal or not item or len(item) > 256 or key in seen
                or not re.fullmatch(r"(?:[a-z][a-z0-9_*/-]{0,255}|[a-z0-9][a-z0-9_*/-]{0,240}@[a-z][a-z0-9_*/-]{0,13})", key)
                or any(not 0x20 <= ord(char) <= 0x7e or char in ",=" for char in item)):
            raise InputError("invalid CloudEvent tracestate member")
        seen.add(key)


def _valid_event_leap_second(year, month, day, hour, minute, offset_minutes):
    # Match time's final-UTC-second-of-a-month stand-in. Integer/calendar
    # arithmetic preserves year0000 and rollover into UTC year-1, unlike datetime.
    day_change, utc_minute = divmod(hour * 60 + minute - offset_minutes, 24 * 60)
    if utc_minute != 23 * 60 + 59:
        return False
    day += day_change
    if day < 1:
        month -= 1
        if month == 0:
            year, month = year - 1, 12
        day += calendar.monthrange(year, month)[1]
    elif day > calendar.monthrange(year, month)[1]:
        day -= calendar.monthrange(year, month)[1]
        month += 1
        if month == 13:
            year, month = year + 1, 1
    return -9999 <= year <= 9999 and day == calendar.monthrange(year, month)[1]


def _validate_event_context(value):
    # Keep this portable common profile aligned with worker-api's borrowed
    # CloudEvent validator. Execution identity requirements do not apply here.
    if type(value) is not dict:
        raise InputError("CloudEvent must be an object")
    for name, item in value.items():
        if name == "data":
            continue
        if re.fullmatch(r"[a-z0-9]+", name) is None:
            raise InputError("invalid CloudEvent context name")
        if type(item) is str:
            if any(unicodedata.category(c) == "Cc" or 0xFDD0 <= ord(c) <= 0xFDEF
                   or ord(c) & 0xFFFE == 0xFFFE for c in item):
                raise InputError("invalid CloudEvent context string")
        elif type(item) is not bool and (type(item) is not int or not -(1 << 31) <= item < (1 << 31)):
            raise InputError("invalid CloudEvent context value")
    if value.get("specversion") != "1.0":
        raise InputError("CloudEvent requires version1.0")
    for name in ("id", "source", "type"):
        if type(value.get(name)) is not str or not value[name]:
            raise InputError(f"CloudEvent requires nonempty {name}")
    _validate_event_uri(value["source"])
    for name in ("subject", "dataschema", "time"):
        if name in value and (type(value[name]) is not str or not value[name]):
            raise InputError(f"CloudEvent {name} must be a nonempty string")
    if "dataschema" in value:
        _validate_event_uri(value["dataschema"], absolute=True)
    if "time" in value:
        match = re.fullmatch(r"([0-9]{4})-([0-9]{2})-([0-9]{2})[Tt]([0-9]{2}):([0-9]{2}):([0-9]{2})(?:\.[0-9]+)?(?:[Zz]|([+-])([0-9]{2}):([0-9]{2}))", value["time"])
        if match is None:
            raise InputError("CloudEvent time must be RFC3339")
        year, month, day, hour, minute, second = map(int, match.groups()[:6])
        if (not 1 <= month <= 12 or not 1 <= day <= calendar.monthrange(year, month)[1]
                or hour > 23 or minute > 59 or second > 60
                or (match[7] is not None and (int(match[8]) > 23 or int(match[9]) > 59))):
            raise InputError("invalid CloudEvent RFC3339 timestamp")
        offset = 0 if match[7] is None else (int(match[8]) * 60 + int(match[9]))
        if match[7] == "-":
            offset = -offset
        if second == 60 and not _valid_event_leap_second(year, month, day, hour, minute, offset):
            raise InputError("CloudEvent leap second must be the final UTC second of a month")
    _validate_event_trace(value)
    return value


def validate_event(value):
    codec.encode(value, EVENT_LIMIT, max_depth=96)
    _validate_event_context(value)
    if value.get("datacontenttype") != "application/json" or "data" not in value:
        raise InputError("CloudEvent requires JSON data")
    if len(value["id"].encode("utf-8")) > 128 or len(value["source"].encode("utf-8")) > 2048:
        raise InputError("CloudEvent id/source exceed their UTF-8 byte limits")
    codec.encode(value["data"], EVENT_LIMIT, max_depth=64)
    return value

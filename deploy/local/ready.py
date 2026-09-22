"""Bounded HTTP readiness probe; uses the supplied host CPython standard library."""
import sys
import urllib.request
with urllib.request.urlopen(sys.argv[1], timeout=2) as response:
    if response.status != 200:
        raise SystemExit(1)

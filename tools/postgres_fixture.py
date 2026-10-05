"""Preserve connection settings while selecting an explicitly owned fixture DB."""
import re
import urllib.parse


def owned_database_url(parent, database):
    """Return a PostgreSQL URL whose database cannot be overridden by its query.

    psql and SQLx accept query parameters after the URI path, including dbname.
    Reject database/service overrides before any resource creation, and retain
    other settings exactly (including SSL settings and Unix-socket hosts).
    Errors intentionally exclude caller-supplied connection strings.
    """
    if not isinstance(database, str) or not re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", database):
        raise ValueError("invalid owned database name")
    if not isinstance(parent, str) or any(ord(char) <= 0x20 or ord(char) == 0x7f for char in parent):
        raise ValueError("database parent must be a PostgreSQL URI")
    try:
        parsed = urllib.parse.urlsplit(parent)
        # Accessing port also validates malformed/out-of-range authority ports.
        parsed.port
        if parsed.scheme not in ("postgres", "postgresql") or parsed.fragment:
            raise ValueError
        query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
        if not parsed.hostname and not any(key == "host" and value for key, value in query):
            raise ValueError
    except ValueError:
        raise ValueError("database parent must be a PostgreSQL URI") from None
    if any(key.lower() in ("dbname", "database", "service") for key, _ in query):
        raise ValueError("database URI must not override its database through query parameters")
    # urlunsplit omits // for an empty authority with the postgres scheme.
    # Retain URI syntax for explicit Unix-socket connections via ?host=/path.
    return f"{parsed.scheme}://{parsed.netloc}/{database}" + ("?" + parsed.query if parsed.query else "")


def create_owned_database(admin, database):
    """Create a uniquely named fixture DB, cleaning up an uncertain CREATE.

    Callers allocate an unpredictable name and retain normal cleanup after this
    succeeds. A timeout or interruption can arrive after CREATE committed, so
    failure must still attempt cleanup on that same server and exact name.
    """
    if not isinstance(database, str) or not re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", database):
        raise ValueError("invalid owned database name")
    try:
        admin(f'CREATE DATABASE "{database}"')
    except BaseException:
        admin(f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)')
        raise

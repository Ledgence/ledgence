"""Check query metadata and ignored database tests against disposable PostgreSQL.

Requires LEDGENCE_POSTGRES_URL, LEDGENCE_POSTGRES_CONTAINER, psql, Docker, and
Cargo. The tests restart the disposable container. Scratch schema setup here is
deliberately not a production migration tool.
"""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tempfile


ADAPTER = "ledgence-adapter-postgres"

# The complete preflight and migration set run in one transaction. A populated
# target fails instead of being cleared or silently treated as a scratch DB.
EMPTY_DATABASE_CHECK = """
DO $ledgence_query_check$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_namespace
        WHERE nspname NOT IN ('public', 'information_schema')
          AND nspname NOT LIKE 'pg\\_%' ESCAPE '\\'
    ) OR EXISTS (
        SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public'
    ) OR EXISTS (
        SELECT 1 FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace
        WHERE n.nspname = 'public'
    ) OR EXISTS (
        SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'public'
    ) THEN
        RAISE EXCEPTION 'Query checking requires an empty disposable database';
    END IF;
END;
$ledgence_query_check$;
"""


def migration_sql(directory):
    migrations = []
    for path in directory.glob("*.sql"):
        if path.name.endswith(".down.sql"):
            continue
        match = re.fullmatch(r"([0-9]+)_.+\.sql", path.name)
        if not match:
            raise ValueError(f"Unsupported migration filename: {path.name}")
        migrations.append((int(match.group(1)), path))
    migrations.sort()
    if not migrations or len({version for version, _ in migrations}) != len(migrations):
        raise ValueError("Expected a nonempty migration set with unique versions")
    return EMPTY_DATABASE_CHECK + "\n" + "\n".join(
        path.read_text(encoding="utf-8") for _, path in migrations
    )


def metadata(directory):
    files = sorted(directory.glob("query-*.json"))
    if not files:
        raise ValueError(f"No SQLx query metadata in {directory}")
    return {path.name: json.loads(path.read_text(encoding="utf-8")) for path in files}


def compare_metadata(expected_directory, generated_directory):
    expected = metadata(expected_directory)
    generated = metadata(generated_directory)
    missing = sorted(generated.keys() - expected.keys())
    stale = sorted(expected.keys() - generated.keys())
    changed = sorted(name for name in expected.keys() & generated.keys()
                     if expected[name] != generated[name])
    if missing or stale or changed:
        descriptions = [f"{kind}: {', '.join(names)}"
                        for kind, names in [("missing", missing), ("stale", stale), ("changed", changed)]
                        if names]
        raise ValueError("Committed SQLx metadata differs; regenerate and review .sqlx files ("
                         + "; ".join(descriptions) + ")")
    return len(expected)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--psql", default="psql", help="PostgreSQL psql executable or wrapper")
    args = parser.parse_args()
    url = os.environ.get("LEDGENCE_POSTGRES_URL")
    if not url:
        parser.error("LEDGENCE_POSTGRES_URL must name a dedicated disposable PostgreSQL database")
    if not os.environ.get("LEDGENCE_POSTGRES_CONTAINER"):
        parser.error("LEDGENCE_POSTGRES_CONTAINER must name the disposable container the tests restart")
    root = Path(__file__).resolve().parents[1]
    environment = dict(os.environ, DATABASE_URL=url)
    try:
        print("Creating the query-checking schema in the empty disposable database", flush=True)
        subprocess.run(
            [args.psql, "--dbname", url, "-X", "--quiet", "--set", "ON_ERROR_STOP=1",
             "--single-transaction", "--file", "-"],
            input=migration_sql(root / "crates" / ADAPTER / "migrations"),
            text=True, env=environment, cwd=root, check=True,
        )
        with tempfile.TemporaryDirectory(prefix="ledgence-sqlx-check-") as temporary:
            generated = Path(temporary)
            online = dict(environment, SQLX_OFFLINE="false", SQLX_OFFLINE_DIR=str(generated))
            print("Checking SQL against PostgreSQL and regenerating temporary query metadata", flush=True)
            # SQLx only writes metadata when a query macro expands. Remove this
            # package's cached build so an earlier build cannot produce a false pass.
            subprocess.run(["cargo", "clean", "-p", ADAPTER], cwd=root, env=online, check=True)
            subprocess.run(
                ["cargo", "check", "-p", ADAPTER, "--all-targets", "--all-features", "--locked"],
                cwd=root, env=online, check=True,
            )
            count = compare_metadata(root / ".sqlx", generated)
            print(f"Verified {count} committed SQLx query descriptions", flush=True)
        offline = dict(environment, SQLX_OFFLINE="true")
        offline.pop("SQLX_OFFLINE_DIR", None)
        print("Running the explicitly selected PostgreSQL integration tests", flush=True)
        test_command = [
            "cargo", "test", "-p", ADAPTER, "--all-targets", "--all-features", "--locked",
        ]
        listing = subprocess.run(
            test_command + ["--", "--ignored", "--list"],
            cwd=root, env=offline, check=True, text=True, stdout=subprocess.PIPE,
        )
        if not any(line.endswith(": test") for line in listing.stdout.splitlines()):
            raise ValueError("The PostgreSQL gate selected no ignored database tests")
        subprocess.run(
            test_command + ["--", "--ignored", "--test-threads=1"],
            cwd=root, env=offline, check=True,
        )
    except subprocess.CalledProcessError as error:
        # psql's connection URI may contain credentials; do not render argv.
        print(f"PostgreSQL verification command exited with status {error.returncode}", file=sys.stderr)
        return 1
    except (OSError, ValueError) as error:
        print(f"PostgreSQL verification failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

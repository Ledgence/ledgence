"""Check production crate dependencies preserve Ledgence's hexagonal boundaries."""

import json
import pathlib
import subprocess
import sys


def violations(graph):
    """Check manifest declarations, including renamed and target dependencies."""
    allowed = {
        "ledgence-worker-api": set(),
        "ledgence-adapter-otel": {"ledgence-worker-api"},
        "ledgence-orchestration-api": {"ledgence-worker-api"},
        "ledgence-orchestration-core": {"ledgence-orchestration-api", "ledgence-worker-api"},
        "ledgence-orchestration-service": {
            "ledgence-orchestration-api", "ledgence-orchestration-core", "ledgence-worker-api",
        },
        "ledgence-adapter-postgres": {
            "ledgence-orchestration-api", "ledgence-orchestration-core", "ledgence-worker-api",
        },
        "ledgence-worker-core": {"ledgence-worker-api"},
        "ledgence-worker-delivery": {
            "ledgence-worker-api", "ledgence-worker-core",
            "ledgence-orchestration-api", "ledgence-orchestration-core",
        },
        "ledgence-adapter-artifact": {"ledgence-worker-api"},
        "ledgence-adapter-subprocess": {"ledgence-worker-api"},
        "ledgence-adapter-http": {"ledgence-orchestration-api", "ledgence-worker-api"},
        "ledgence-orchestrator": {
            "ledgence-adapter-otel",
            "ledgence-adapter-http", "ledgence-adapter-artifact", "ledgence-adapter-postgres",
            "ledgence-orchestration-api", "ledgence-orchestration-service", "ledgence-worker-api",
        },
        "ledgence-cli": {"ledgence-adapter-otel","ledgence-adapter-http", "ledgence-orchestration-api", "ledgence-worker-api"},
        "ledgence-worker": {
            "ledgence-adapter-otel",
            "ledgence-worker-api", "ledgence-worker-core",
            "ledgence-adapter-artifact", "ledgence-adapter-subprocess",
            "ledgence-adapter-http", "ledgence-worker-delivery", "ledgence-orchestration-api",
        },
    }
    members = set(graph["workspace_members"])
    packages = {p["name"]: p for p in graph["packages"] if p["id"] in members}
    errors = []
    for name, package in packages.items():
        if name not in allowed:
            errors.append(f"{name}: declare its architectural boundary in this check")
            continue
        for dependency in package["dependencies"]:
            target = dependency["name"]
            if target == "sqlx" or target.startswith("sqlx-"):
                if name != "ledgence-adapter-postgres":
                    errors.append(f"{name}: SQLx dependencies belong only in ledgence-adapter-postgres")
                elif target != "sqlx":
                    errors.append(f"{name}: depend on the public sqlx crate, not {target}")
                else:
                    allowed_features = {
                        "postgres", "runtime-tokio", "tls-rustls-ring-native-roots",
                        "migrate", "macros",
                    }
                    if dependency["uses_default_features"] or set(dependency["features"]) - allowed_features:
                        errors.append(f"{name}: SQLx must use only the reviewed PostgreSQL features")
            if (target.startswith("opentelemetry") or target == "tracing-opentelemetry") and name != "ledgence-adapter-otel" and dependency["kind"] != "dev":
                errors.append(f"{name}: OpenTelemetry SDK dependencies belong only in ledgence-adapter-otel")
            if dependency["kind"] == "dev":
                continue
            if target in packages and target not in allowed[name]:
                errors.append(f"{name} must not depend on {target} ({dependency['kind'] or 'normal'})")
    return errors


def main():
    root = pathlib.Path(__file__).resolve().parents[1]
    graph = json.loads(subprocess.check_output(
        ["cargo", "metadata", "--locked", "--format-version", "1", "--no-deps"],
        cwd=root,
        text=True,
    ))
    errors = violations(graph)
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print(f"Architecture boundaries passed for {len(graph['workspace_members'])} workspace crates.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

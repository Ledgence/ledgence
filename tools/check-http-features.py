"""Check isolated HTTP adapter features without workspace feature unification."""

import os
from pathlib import Path
import re
import subprocess
import sys
import tomllib


ADAPTER = "ledgence-adapter-http"
TLS_PACKAGES = {"rustls", "tokio-rustls", "hyper-rustls", "ring", "aws-lc-rs",
                "native-tls", "openssl", "openssl-sys"}
SELECTIONS = (
    (None, set(), {"reqwest", "axum", "axum-core", "matchit", "http-body-util"} | TLS_PACKAGES),
    ("client", {"reqwest"}, {"axum", "axum-core", "matchit"}),
    ("server", {"axum", "axum-core", "http-body-util"}, {"reqwest"} | TLS_PACKAGES),
)


def main():
    root = Path(__file__).resolve().parents[1]
    cargo = os.environ.get("CARGO", "cargo")
    try:
        manifest = tomllib.loads((root / "crates" / ADAPTER / "Cargo.toml").read_text())
        if manifest.get("features", {}).get("default") != []:
            raise ValueError("HTTP adapter default features must stay explicitly empty")
        for feature, required, forbidden in SELECTIONS:
            label = feature or "no features"
            selection = ["-p", ADAPTER, "--no-default-features", "--locked"]
            if feature:
                selection.extend(["--features", feature])
            # Select just this package. --workspace/--all-features would unify
            # the client and server through composition/test crates, hiding leaks.
            tree = subprocess.check_output(
                [cargo, "tree", *selection, "--target", "all", "--edges", "normal,build",
                 "--prefix", "none", "--format", "{p}"],
                cwd=root, text=True,
            )
            names = set(re.findall(r"^([A-Za-z0-9_-]+) v", tree, flags=re.MULTILINE))
            if ADAPTER not in names:
                raise ValueError(f"{label}: Cargo returned no adapter dependency tree")
            missing = required - names
            unexpected = (forbidden & names) | {
                name for name in names if name == "sqlx" or name.startswith("sqlx-")
            }
            if missing or unexpected:
                raise ValueError(f"{label}: missing {sorted(missing)}, unexpected {sorted(unexpected)}")
            print(f"HTTP feature graph passed: {label}", flush=True)
            subprocess.run(
                [cargo, "clippy", *selection, "--all-targets", "--", "-D", "warnings"],
                cwd=root, check=True,
            )
        print("HTTP isolated feature checks passed", flush=True)
    except subprocess.CalledProcessError as error:
        print(f"HTTP feature check exited with status {error.returncode}", file=sys.stderr)
        return 1
    except (OSError, ValueError) as error:
        print(f"HTTP feature check failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

#!/usr/bin/env python3
"""Qualify published 0.9.0 migration, interrupted DDL and fresh restore (MIT)."""
from release.upgrade_gate import main
from execution_artifacts.upgrade import ArtifactUpgradeDeployment, exercise

if __name__ == "__main__":
    raise SystemExit(main(predecessor="0.9.0", deployment=ArtifactUpgradeDeployment, exercise=exercise, description=__doc__))

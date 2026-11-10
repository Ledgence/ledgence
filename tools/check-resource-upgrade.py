#!/usr/bin/env python3
"""Rehearse published 0.8.0 upgrade and externally declared restore quarantine (MIT)."""
from release.upgrade_gate import main
from resource_admission.upgrade import UpgradeDeployment, exercise

if __name__ == "__main__":
    raise SystemExit(main(predecessor="0.8.0", deployment=UpgradeDeployment, exercise=exercise, description=__doc__))

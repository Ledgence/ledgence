"""Verify the real worker command, scenario topology and equal-work inputs (MIT)."""

import subprocess
import sys
import unittest
from collections import Counter
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from queue_capacity.fixture import CapacityDeployment
from resource_admission.load import KEYS, MODES, SLOTS, Deployment, timing_input


class LoadPolicyTests(unittest.TestCase):
    def test_actual_consumer_arguments_and_reported_policy(self):
        for kind, wait, expected in (
            (CapacityDeployment, None, 0),
            (Deployment, None, None),
            (Deployment, 0, 0),
            (Deployment, 1000, 1000),
            (Deployment, 20000, 20000),
        ):
            with (
                self.subTest(kind=kind, wait=wait),
                patch.object(CapacityDeployment, "__init__", return_value=None),
            ):
                d = kind() if kind is CapacityDeployment else kind(acquire_wait_ms=wait)
                d.binaries = d.directory = d.root = d.worker_credentials = Path(
                    "/fixture"
                )
                d.server_url = d.python = "unused"
                d.scope = {"tenant_id": "test", "namespace": "load"}
                d.artifacts = SimpleNamespace(url="unused")
                d.processes, d.environment = [], {}
                with patch(
                    "queue_capacity.fixture.Process",
                    side_effect=lambda args, *_: SimpleNamespace(
                        process=SimpleNamespace(args=args)
                    ),
                ) as process:
                    d.consumer("main", "main", SLOTS)
                args = process.call_args.args[0]
                if expected is None:
                    self.assertNotIn("--acquire-wait-ms", args)
                else:
                    self.assertEqual(args.count("--acquire-wait-ms"), 1)
                    self.assertEqual(
                        args[args.index("--acquire-wait-ms") + 1], str(expected)
                    )
                if kind is Deployment:
                    for count, limit, capacity in (
                        (0, None, None),
                        (1, 4, 4),
                        (8, 4, 32),
                    ):
                        result = d.topology(count, limit)
                        self.assertEqual(result["polling"]["flag_ms"], expected)
                        self.assertEqual(
                            result["polling"]["effective_ms"],
                            20_000 if expected is None else expected,
                        )
                        self.assertEqual(
                            result["polling"]["basis"],
                            "worker_cli_default"
                            if expected is None
                            else "explicit_worker_argument",
                        )
                        self.assertEqual(result["total_worker_slots"], SLOTS)
                        self.assertEqual(
                            result["workers"]["main"]["polling"], result["polling"]
                        )
                        self.assertEqual(
                            result["polling"]["mode"],
                            "short_poll" if expected == 0 else "long_poll",
                        )
                        self.assertEqual(result["aggregate_reservation_cap"], capacity)

    def test_pressure_worker_slots_do_not_change_earlier_snapshots(self):
        with patch.object(CapacityDeployment, "__init__", return_value=None):
            d = Deployment()
        d.launched_workers["main"] = {"queue": "load", "slots": SLOTS}
        timing = d.topology(1, SLOTS)
        d.launched_workers["support"] = {"queue": "load-support", "slots": 2}
        pressure = d.topology(1, 2)
        self.assertEqual(set(timing["workers"]), {"main"})
        self.assertEqual(timing["total_worker_slots"], SLOTS)
        self.assertEqual(pressure["total_worker_slots"], SLOTS + 2)
        self.assertEqual(pressure["aggregate_reservation_cap"], 2)
        self.assertEqual(pressure["workers"]["support"]["queue"], "load-support")

    def test_invalid_wait_rejected_before_fixture_creation(self):
        for value in (-1, 20001, "abc"):
            with (
                self.subTest(value=value),
                patch.object(CapacityDeployment, "__init__") as setup,
            ):
                with self.assertRaises(ValueError):
                    Deployment(acquire_wait_ms=value)
                setup.assert_not_called()
                result = subprocess.run(
                    [
                        sys.executable,
                        str(Path(__file__).with_name("check-resource-load.py")),
                        "--binaries",
                        "unused",
                        "--evidence",
                        "unused",
                        "--acquire-wait-ms",
                        str(value),
                    ],
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=5,
                )
                self.assertEqual(result.returncode, 2)
                self.assertIn("--acquire-wait-ms", result.stderr)

    def test_every_arm_receives_the_same_work_and_slow_requests_rotate(self):
        durations = [150 if i % 8 == 0 else 5 for i in range(96)]
        keys = {f"load-{n}" for n in range(KEYS)}
        for mode in MODES:
            with self.subTest(mode=mode):
                work = [timing_input(mode, i) for i in range(96)]
                self.assertEqual([ms for _, ms in work], durations)
                actual = [key for key, _ in work]
                if mode == "independent":
                    self.assertEqual(Counter(actual), {key: 12 for key in keys})
                    self.assertEqual(set(actual[::8]), keys)
                    for block in range(12):
                        self.assertEqual(set(actual[block * 8 : block * 8 + 8]), keys)
                else:
                    self.assertEqual(
                        set(actual), {None if mode == "ordinary" else "load-hot"}
                    )

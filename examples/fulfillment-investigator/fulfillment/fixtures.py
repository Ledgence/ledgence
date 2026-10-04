"""Ledgence-owned, deterministic synthetic data; no accounts or customer data (MIT)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
import random

SOURCES = ("orders", "warehouse", "carrier", "support")
SCENARIOS = ("data-gap", "real-delay")
REVISIONS = ("initial", "corrected")
COHORT_SIZE = 40


def _timestamp(value: datetime) -> str:
    return value.isoformat(timespec="seconds").replace("+00:00", "Z")


def batches(scenario: str, revision: str) -> dict[str, dict]:
    if scenario not in SCENARIOS or revision not in REVISIONS:
        raise ValueError("unknown scenario or revision")
    records: dict[str, list[dict]] = {source: [] for source in SOURCES}
    for cohort, day in (("baseline", 1), ("current", 8)):
        base = datetime(2026, 9, day, 9, tzinfo=timezone.utc)
        late = {1, 2, 21, 22}
        if scenario == "real-delay" and cohort == "current":
            late = set(range(1, 15)) | {21, 22}
        for number in range(1, COHORT_SIZE + 1):
            order_id = f"{cohort}-{number:03}"
            created = base + timedelta(minutes=number)
            promised = created + timedelta(hours=48)
            carrier = "Northstar" if number <= 20 else "Southline"
            records["orders"].append({
                "id": order_id, "version": 1, "cohort": cohort,
                "created_at": _timestamp(created), "promised_at": _timestamp(promised),
                "carrier": carrier, "warehouse": "East" if number % 2 else "West",
            })
            # The missing batch hides on-time dispatches: a naive overdue-join
            # estimate appears elevated, but must never become a delivery metric.
            missing = scenario == "data-gap" and revision == "initial" and cohort == "current" and 3 <= number <= 14
            if not missing:
                records["warehouse"].append({
                    "id": f"dispatch-{order_id}", "version": 1, "order_id": order_id,
                    "warehouse": "East" if number % 2 else "West",
                    "dispatched_at": _timestamp(created + timedelta(hours=4)),
                })
            carrier_id = f"shipment-{order_id}"
            old = {"id": carrier_id, "version": 1, "order_id": order_id, "carrier": carrier,
                   "status": "in_transit", "observed_at": _timestamp(created + timedelta(hours=5)),
                   "delivered_at": None}
            delivered = promised + timedelta(hours=6 if number in late else -6)
            latest = {"id": carrier_id, "version": 2, "order_id": order_id, "carrier": carrier,
                      "status": "delivered", "observed_at": _timestamp(delivered),
                      "delivered_at": _timestamp(delivered)}
            # Newer state comes first; replayed older state must not overwrite it.
            records["carrier"].extend((latest, old))
            if number <= 4:
                records["carrier"].append(dict(latest))
            if number in late or number in {31, 32, 33, 34}:
                records["support"].append({
                    "id": f"ticket-{order_id}", "version": 1, "order_id": order_id,
                    "topic": "delivery_question" if number in late else "address_question",
                    "opened_at": _timestamp(promised + timedelta(hours=8)),
                })
    # Other source orders vary deterministically. Carrier order deliberately
    # exercises late arrival of an older version after its successor.
    for source in ("orders", "warehouse", "support"):
        random.Random(1729).shuffle(records[source])
    return {source: {"schema_version": 1, "synthetic": True, "scenario": scenario,
                     "source": source, "revision": revision, "records": rows,
                     "coverage": {"complete": not (scenario == "data-gap" and revision == "initial" and source == "warehouse"),
                                  "expected_entities": 80 if source != "support" else len(rows),
                                  "through": "2026-09-12T00:00:00Z"}}
            for source, rows in records.items()}

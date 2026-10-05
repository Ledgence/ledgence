"""Validate source batches, publish immutable SQLite snapshots, and query evidence (MIT)."""
from __future__ import annotations

from collections import Counter, defaultdict
from contextlib import closing
from datetime import datetime
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile

from .fixtures import SCENARIOS, SOURCES, REVISIONS, batches
from .storage import canonical_json, read_json, resolve_reference, store_path, write_bytes, write_json, write_named

TOPICS = ("carrier", "warehouse", "source_health", "support")
_FIELDS = {
    "orders": {"id", "version", "cohort", "created_at", "promised_at", "carrier", "warehouse"},
    "warehouse": {"id", "version", "order_id", "warehouse", "dispatched_at"},
    "carrier": {"id", "version", "order_id", "carrier", "status", "observed_at", "delivered_at"},
    "support": {"id", "version", "order_id", "topic", "opened_at"},
}


def prepare_data(store: str) -> dict:
    """Materialize both synthetic scenarios. Never reset or overwrite artifacts."""
    index = {"schema_version": 1, "synthetic": True, "scenarios": {}}
    for scenario in SCENARIOS:
        index["scenarios"][scenario] = {source: {} for source in SOURCES}
        for revision in REVISIONS:
            for source, batch in batches(scenario, revision).items():
                index["scenarios"][scenario][source][revision] = write_json(store, batch)
    write_named(store, "fixtures.json", canonical_json(index))
    return index


def _timestamp(value: object) -> bool:
    if not isinstance(value, str) or len(value) != 20 or not value.endswith("Z"):
        return False
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").isoformat() + "Z" == value
    except ValueError:
        return False


def _valid_record(source: str, row: object) -> bool:
    if not isinstance(row, dict) or set(row) != _FIELDS[source]:
        return False
    if type(row["version"]) is not int or row["version"] < 1:
        return False
    for key, value in row.items():
        if key == "version":
            continue
        if key == "delivered_at" and value is None:
            continue
        if key.endswith("_at"):
            if not _timestamp(value):
                return False
        elif not isinstance(value, str) or not 0 < len(value) <= 80:
            return False
    if source == "orders":
        return row["cohort"] in ("baseline", "current") and row["created_at"] < row["promised_at"]
    if source == "carrier":
        return (row["status"] in ("in_transit", "delivered")
                and (row["status"] == "delivered") == (row["delivered_at"] is not None)
                and (row["delivered_at"] is None or row["observed_at"] >= row["delivered_at"]))
    if source == "support":
        return row["topic"] in ("delivery_question", "address_question")
    return True


def _inspect(batch: dict, source: str, scenario: str) -> tuple[list[dict], dict, list[dict]]:
    if (not isinstance(batch, dict) or batch.get("schema_version") != 1
            or batch.get("synthetic") is not True or batch.get("source") != source
            or batch.get("scenario") != scenario or batch.get("revision") not in REVISIONS
            or not isinstance(batch.get("records"), list) or len(batch["records"]) > 5000):
        raise ValueError("invalid synthetic source batch")
    coverage = batch.get("coverage", {})
    if (not isinstance(coverage, dict) or type(coverage.get("complete")) is not bool
            or type(coverage.get("expected_entities")) is not int
            or not 0 <= coverage["expected_entities"] <= 5000
            or not _timestamp(coverage.get("through"))):
        raise ValueError("invalid source coverage declaration")
    groups: dict[str, dict[int, list[dict]]] = defaultdict(lambda: defaultdict(list))
    issues = []
    stats = {"input_count": len(batch["records"]), "normalized_count": 0,
             "duplicate_records": 0, "stale_records": 0, "quarantined_records": 0}
    for offset, row in enumerate(batch["records"]):
        if (not _valid_record(source, row) or any(
                value is not None and value > coverage["through"]
                for key, value in row.items() if key.endswith("_at"))):
            issues.append({"row": offset, "reason": "schema, value, or source-window violation"})
            stats["quarantined_records"] += 1
        else:
            groups[row["id"]][row["version"]].append(row)
    normalized = []
    for identity, versions in sorted(groups.items()):
        if any(len({canonical_json(row) for row in rows}) != 1 for rows in versions.values()):
            # A conflicting version poisons the identity, even if another version
            # is higher. Choosing an arbitrary payload would invent source truth.
            stats["quarantined_records"] += sum(len(rows) for rows in versions.values())
            issues.append({"id": identity, "reason": "conflicting payloads for the same identity and version"})
            continue
        stats["duplicate_records"] += sum(len(rows) - 1 for rows in versions.values())
        stats["stale_records"] += len(versions) - 1
        normalized.append(versions[max(versions)][0])
    stats["normalized_count"] = len(normalized)
    return normalized, stats, issues


def ingest_source(*, store: str, scenario: str, source: str, revision: str = "initial") -> dict:
    if scenario not in SCENARIOS or source not in SOURCES or revision not in REVISIONS:
        raise ValueError("unknown scenario, source, or revision")
    index = json.loads(store_path(store, "fixtures.json").read_bytes())
    raw = index["scenarios"][scenario][source][revision]
    batch = read_json(store, raw)
    if batch.get("revision") != revision:
        raise ValueError("fixture revision mismatch")
    _, counts, issues = _inspect(batch, source, scenario)
    return {"scenario": scenario, "source": source, "revision": revision, "raw": raw,
            "counts": counts, "quarantine": write_json(store, issues) if issues else None}


def _quality(rows: dict[str, list[dict]], counts: dict[str, dict], coverage: dict[str, dict]) -> dict:
    orders = {row["id"]: row for row in rows["orders"]}
    missing_sources = [source for source in SOURCES if source not in counts]
    incomplete_sources = [source for source in SOURCES if source in counts and (
        not coverage[source]["complete"]
        or counts[source]["normalized_count"] != coverage[source]["expected_entities"])]
    references = {source: Counter(row["order_id"] for row in rows[source])
                  for source in ("warehouse", "carrier", "support")}
    orphan_records = sum(sum(amount for identity, amount in entries.items() if identity not in orders)
                         for entries in references.values())
    duplicate_relations = sum(sum(max(amount - 1, 0) for amount in references[source].values())
                              for source in ("warehouse", "carrier"))
    missing_dispatches = sum(identity not in references["warehouse"] for identity in orders)
    missing_shipments = sum(identity not in references["carrier"] for identity in orders)
    unfinished_shipments = sum(row["status"] != "delivered" for row in rows["carrier"])
    inconsistent_records = 0
    dispatches = {row["order_id"]: row for row in rows["warehouse"]}
    for source in ("warehouse", "carrier", "support"):
        for row in rows[source]:
            order = orders.get(row["order_id"])
            if order is None:
                continue
            if source == "warehouse":
                inconsistent_records += int(row["warehouse"] != order["warehouse"]
                                            or row["dispatched_at"] < order["created_at"])
            elif source == "carrier":
                dispatch = dispatches.get(row["order_id"])
                inconsistent_records += int(row["carrier"] != order["carrier"]
                                            or row["observed_at"] < order["created_at"]
                                            or (row["delivered_at"] is not None and (
                                                row["delivered_at"] < order["created_at"]
                                                or (dispatch is not None and row["delivered_at"] < dispatch["dispatched_at"]))))
            else:
                inconsistent_records += int(row["opened_at"] < order["created_at"])
    cohorts = Counter(row["cohort"] for row in orders.values())
    result = {"missing_sources": missing_sources, "incomplete_sources": incomplete_sources,
              "missing_dispatches": missing_dispatches, "missing_shipments": missing_shipments,
              "unfinished_shipments": unfinished_shipments, "orphan_records": orphan_records,
              "duplicate_relations": duplicate_relations, "inconsistent_records": inconsistent_records,
              "cohort_orders": {cohort: cohorts[cohort] for cohort in ("baseline", "current")},
              "inconsistent_windows": int(len({item["through"] for item in coverage.values()}) > 1),
              "missing_cohorts": [cohort for cohort in ("baseline", "current") if not cohorts[cohort]],
              "source_counts": counts,
              **{key: sum(value[key] for value in counts.values())
                 for key in ("duplicate_records", "stale_records", "quarantined_records")}}
    result["provisional"] = None
    if missing_dispatches:
        shipments = {row["order_id"]: row for row in rows["carrier"]}
        provisional = {"label": "Incomplete-source estimate; not a valid delivery metric"}
        for cohort in ("baseline", "current"):
            selected = [row for row in orders.values() if row["cohort"] == cohort]
            apparent = sum(row["id"] not in dispatches or (
                shipments.get(row["id"], {}).get("delivered_at") is not None
                and shipments[row["id"]]["delivered_at"] > row["promised_at"]) for row in selected)
            provisional[f"{cohort}_orders"] = len(selected)
            provisional[f"{cohort}_late_or_unmatched"] = apparent
            provisional[f"{cohort}_late_or_unmatched_rate_pct"] = round(100 * apparent / len(selected), 2) if selected else None
        result["provisional"] = provisional
    return result


def _create_database(store: str, rows: dict[str, list[dict]], sources: dict[str, dict]) -> dict:
    # Each invocation owns a new temporary database. Only its closed immutable
    # bytes are published, avoiding a shared SQLite writer across fork branches.
    with tempfile.TemporaryDirectory(prefix="fulfillment-snapshot-") as directory:
        path = Path(directory) / "snapshot.sqlite"
        with closing(sqlite3.connect(path)) as connection, connection:
            connection.executescript("""
                PRAGMA user_version=1;
                CREATE TABLE orders (id TEXT PRIMARY KEY, version INTEGER, cohort TEXT,
                    created_at TEXT, promised_at TEXT, carrier TEXT, warehouse TEXT);
                CREATE TABLE dispatches (id TEXT PRIMARY KEY, version INTEGER, order_id TEXT UNIQUE,
                    warehouse TEXT, dispatched_at TEXT);
                CREATE TABLE shipments (id TEXT PRIMARY KEY, version INTEGER, order_id TEXT UNIQUE,
                    carrier TEXT, status TEXT, observed_at TEXT, delivered_at TEXT);
                CREATE TABLE support (id TEXT PRIMARY KEY, version INTEGER, order_id TEXT,
                    topic TEXT, opened_at TEXT);
                CREATE TABLE sources (source TEXT PRIMARY KEY, revision TEXT, input_count INTEGER,
                    normalized_count INTEGER, duplicate_records INTEGER, stale_records INTEGER,
                    quarantined_records INTEGER);
            """)
            table_names = {"orders": "orders", "warehouse": "dispatches", "carrier": "shipments", "support": "support"}
            for source in SOURCES:
                table = table_names[source]
                columns = [column[1] for column in connection.execute(f"PRAGMA table_info({table})")]
                connection.executemany(f"INSERT INTO {table} VALUES ({','.join('?' for _ in columns)})",
                                       [tuple(row[column] for column in columns) for row in rows[source]])
                value = sources[source]
                connection.execute("INSERT INTO sources VALUES (?,?,?,?,?,?,?)", (
                    source, value["revision"], *(value["counts"][key] for key in (
                        "input_count", "normalized_count", "duplicate_records", "stale_records", "quarantined_records"))))
        return write_bytes(store, path.read_bytes(), ".sqlite")


def assemble_snapshot(*, store: str, scenario: str, receipts: dict) -> dict:
    if scenario not in SCENARIOS or not isinstance(receipts, dict) or set(receipts) - set(SOURCES):
        raise ValueError("unknown scenario or source receipts")
    rows = {source: [] for source in SOURCES}
    counts, coverage, sources = {}, {}, {}
    for source in SOURCES:
        if source not in receipts:
            continue
        receipt = receipts[source]
        if (not isinstance(receipt, dict) or receipt.get("source") != source or receipt.get("scenario") != scenario
                or receipt.get("revision") not in REVISIONS):
            raise ValueError("source receipt identity mismatch")
        batch = read_json(store, receipt["raw"])
        if batch.get("revision") != receipt["revision"]:
            raise ValueError("source receipt revision mismatch")
        # Recompute every count from digest-verified raw data. A modified receipt
        # cannot suppress quality failures or inflate counts in a report.
        rows[source], counts[source], issues = _inspect(batch, source, scenario)
        coverage[source] = batch["coverage"]
        sources[source] = {"revision": receipt["revision"], "raw": receipt["raw"], "counts": counts[source]}
        if issues:
            sources[source]["quarantine"] = write_json(store, issues)
    quality = _quality(rows, counts, coverage)
    blockers = ("missing_sources", "incomplete_sources", "missing_dispatches", "missing_shipments",
                "unfinished_shipments", "orphan_records", "duplicate_relations", "inconsistent_records",
                "missing_cohorts", "quarantined_records", "inconsistent_windows")
    if any(quality[key] for key in blockers):
        return {"ready": False, "snapshot": None, "quality": quality,
                "quarantine": {source: value["quarantine"] for source, value in sources.items() if "quarantine" in value}}
    database = _create_database(store, rows, sources)
    manifest = write_json(store, {"schema_version": 1, "synthetic": True, "scenario": scenario,
                                 "database": database, "quality": quality, "sources": sources})
    return {"ready": True, "snapshot": {"snapshot_id": manifest["sha256"], "manifest": manifest, "database": database},
            "quality": quality, "quarantine": {}}


def open_snapshot(*, store: str, snapshot: dict) -> tuple[sqlite3.Connection, dict]:
    """Return a verified read-only connection; callers must close it."""
    manifest = read_json(store, snapshot["manifest"])
    if (snapshot.get("snapshot_id") != snapshot["manifest"]["sha256"]
            or manifest.get("schema_version") != 1 or manifest.get("synthetic") is not True
            or manifest.get("database") != snapshot.get("database")):
        raise ValueError("snapshot manifest binding mismatch")
    for source in manifest["sources"].values():
        resolve_reference(store, source["raw"])
    path = resolve_reference(store, snapshot["database"])
    connection = sqlite3.connect(path.as_uri() + "?mode=ro&immutable=1", uri=True)
    connection.execute("PRAGMA query_only=ON")
    connection.row_factory = sqlite3.Row
    return connection, manifest


_QUERIES = {
    "carrier": """SELECT o.cohort, o.carrier, COUNT(*) AS orders,
        SUM(s.delivered_at > o.promised_at) AS late
        FROM orders o JOIN shipments s ON s.order_id=o.id
        GROUP BY o.cohort,o.carrier ORDER BY o.cohort,o.carrier""",
    "warehouse": """SELECT o.cohort, COUNT(d.id) AS dispatches,
        ROUND(AVG((julianday(d.dispatched_at)-julianday(o.created_at))*24),2) AS avg_dispatch_hours,
        SUM(d.id IS NULL) AS missing_dispatches
        FROM orders o LEFT JOIN dispatches d ON d.order_id=o.id
        GROUP BY o.cohort ORDER BY o.cohort""",
    "source_health": "SELECT * FROM sources ORDER BY source",
    "support": """SELECT o.cohort, COUNT(s.id) AS tickets,
        SUM(CASE WHEN s.topic='delivery_question' THEN 1 ELSE 0 END) AS delivery_questions
        FROM orders o LEFT JOIN support s ON s.order_id=o.id
        GROUP BY o.cohort ORDER BY o.cohort""",
}


def analyze_snapshot(*, store: str, snapshot: dict, topic: str) -> dict:
    if topic not in TOPICS:
        raise ValueError("unknown analysis topic")
    connection, manifest = open_snapshot(store=store, snapshot=snapshot)
    try:
        results = [dict(row) for row in connection.execute(_QUERIES[topic])]
    finally:
        connection.close()
    metrics, claims = {}, []
    def claim(identity: str, text: str) -> None:
        claims.append({"id": f"{topic}.{identity}", "text": text})
    if topic == "carrier":
        for cohort in ("baseline", "current"):
            metrics[f"{cohort}_orders"] = sum(row["orders"] for row in results if row["cohort"] == cohort)
            metrics[f"{cohort}_late"] = sum(row["late"] for row in results if row["cohort"] == cohort)
            metrics[f"{cohort}_late_rate_pct"] = round(100 * metrics[f"{cohort}_late"] / metrics[f"{cohort}_orders"], 2)
        metrics["late_rate_delta_pp"] = round(metrics["current_late_rate_pct"] - metrics["baseline_late_rate_pct"], 2)
        metrics["by_carrier"] = []
        for carrier in sorted({row["carrier"] for row in results}):
            item = {"carrier": carrier, "baseline_orders": 0, "current_orders": 0,
                    "baseline_late": 0, "current_late": 0}
            for row in results:
                if row["carrier"] == carrier:
                    item[f'{row["cohort"]}_orders'] = row["orders"]
                    item[f'{row["cohort"]}_late'] = row["late"]
            item["additional_late"] = item["current_late"] - item["baseline_late"]
            metrics["by_carrier"].append(item)
        claim("late_rate", f"Late deliveries: {metrics['current_late']} of {metrics['current_orders']} current orders ({metrics['current_late_rate_pct']:g}%), versus {metrics['baseline_late']} of {metrics['baseline_orders']} baseline orders ({metrics['baseline_late_rate_pct']:g}%).")
        claim("comparison", f"The late-delivery rate changed by {metrics['late_rate_delta_pp']:g} percentage points between these synthetic cohorts.")
        for index, item in enumerate(metrics["by_carrier"]):
            claim(f"carrier_{index}", f"{item['carrier']}: {item['current_late']} late current orders versus {item['baseline_late']} baseline; {item['additional_late']} additional late deliveries. This association does not establish root cause.")
    elif topic == "warehouse":
        for row in results:
            metrics[f'{row["cohort"]}_dispatches'] = row["dispatches"]
            metrics[f'{row["cohort"]}_avg_dispatch_hours'] = row["avg_dispatch_hours"]
        metrics["missing_dispatches"] = sum(row["missing_dispatches"] for row in results)
        claim("dispatch_time", f"Average order-to-dispatch time is {metrics['current_avg_dispatch_hours']:g} hours currently and {metrics['baseline_avg_dispatch_hours']:g} hours in the baseline.")
        claim("coverage", f"The snapshot has {metrics['current_dispatches']} current and {metrics['baseline_dispatches']} baseline dispatches, with {metrics['missing_dispatches']} missing order-to-dispatch relationships.")
    elif topic == "source_health":
        metrics = {"source_count": len(results), "missing_sources": manifest["quality"]["missing_sources"],
                   "missing_dispatches": manifest["quality"]["missing_dispatches"],
                   "corrected_sources": [row["source"] for row in results if row["revision"] == "corrected"],
                   **{key: sum(row[key] for row in results) for key in ("duplicate_records", "stale_records", "quarantined_records")}}
        claim("reconciliation", f"All {metrics['source_count']} sources passed publication checks. Reconciliation removed {metrics['duplicate_records']} duplicate records and {metrics['stale_records']} older versions; {metrics['quarantined_records']} records are quarantined in this published snapshot.")
        if metrics["corrected_sources"]:
            claim("correction", "The published snapshot uses corrected source batches: " + ", ".join(metrics["corrected_sources"]) + ".")
        else:
            claim("coverage", "The published snapshot uses the initial complete source batches; no source correction was required.")
    else:
        for row in results:
            metrics[f'{row["cohort"]}_tickets'] = row["tickets"]
            metrics[f'{row["cohort"]}_delivery_questions'] = row["delivery_questions"]
        claim("delivery_questions", f"Support recorded {metrics['current_delivery_questions']} delivery questions currently versus {metrics['baseline_delivery_questions']} in the baseline.")
        claim("coverage", f"There are {metrics['current_tickets']} current and {metrics['baseline_tickets']} baseline support tickets. Ticket counts are supporting observations, not proof of a carrier's root cause.")
    sql = _QUERIES[topic]
    evidence = {"topic": topic, "snapshot_id": snapshot["snapshot_id"],
                "evidence_id": f"{topic}:{snapshot['snapshot_id']}", "synthetic": True,
                "metrics": metrics, "claims": claims, "findings": [item["text"] for item in claims],
                "query": {"sql": sql, "sha256": hashlib.sha256(sql.encode()).hexdigest()}}
    if len(canonical_json(evidence)) > 8192:
        raise ValueError("analysis evidence exceeds workflow reference budget")
    return evidence

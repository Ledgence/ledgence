"""Behavioral checks for reconciliation, snapshot publication, and SQL evidence (MIT)."""
import copy
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from fulfillment.data import analyze_snapshot, assemble_snapshot, ingest_source, open_snapshot, prepare_data
from fulfillment.fixtures import SOURCES
from fulfillment.storage import read_bytes, read_json, store_path, write_json, write_named


class DataTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="fulfillment-test-")
        self.addCleanup(self.directory.cleanup)
        self.store = self.directory.name
        self.index = prepare_data(self.store)

    def receipts(self, scenario="real-delay", corrected=False):
        return {source: ingest_source(store=self.store, scenario=scenario, source=source,
                                     revision="corrected" if corrected and source == "warehouse" else "initial")
                for source in SOURCES}

    def snapshot(self, scenario="real-delay", corrected=False):
        return assemble_snapshot(store=self.store, scenario=scenario,
                                 receipts=self.receipts(scenario, corrected))

    def change_batch(self, receipts, source, change):
        batch = read_json(self.store, receipts[source]["raw"])
        change(batch)
        receipts[source]["raw"] = write_json(self.store, batch)

    def test_incomplete_warehouse_blocks_publication_and_labels_naive_signal(self):
        result = self.snapshot("data-gap")
        self.assertFalse(result["ready"])
        self.assertIsNone(result["snapshot"])
        self.assertEqual(result["quality"]["missing_dispatches"], 12)
        self.assertEqual(result["quality"]["incomplete_sources"], ["warehouse"])
        provisional = result["quality"]["provisional"]
        self.assertEqual(provisional["current_late_or_unmatched_rate_pct"], 40)
        self.assertEqual(provisional["baseline_late_or_unmatched_rate_pct"], 10)
        self.assertIn("not a valid delivery metric", provisional["label"])
        self.assertEqual(list(Path(self.store).rglob("*.sqlite")), [])

    def test_correction_restores_normal_delivery_without_reingesting_other_sources(self):
        receipts = self.receipts("data-gap")
        original_refs = {source: receipt["raw"] for source, receipt in receipts.items()}
        receipts["warehouse"] = ingest_source(store=self.store, scenario="data-gap", source="warehouse", revision="corrected")
        result = assemble_snapshot(store=self.store, scenario="data-gap", receipts=receipts)
        self.assertTrue(result["ready"])
        for source in ("orders", "carrier", "support"):
            self.assertEqual(receipts[source]["raw"], original_refs[source])
        metrics = analyze_snapshot(store=self.store, snapshot=result["snapshot"], topic="carrier")["metrics"]
        self.assertEqual(metrics["current_late"], 4)
        self.assertEqual(metrics["late_rate_delta_pp"], 0)
        health = analyze_snapshot(store=self.store, snapshot=result["snapshot"], topic="source_health")
        self.assertEqual(health["metrics"]["corrected_sources"], ["warehouse"])

    def test_true_incident_is_measured_in_sql_and_concentrated_on_one_carrier(self):
        result = self.snapshot()
        snapshot = result["snapshot"]
        self.assertTrue(result["ready"])
        evidence = {topic: analyze_snapshot(store=self.store, snapshot=snapshot, topic=topic)
                    for topic in ("carrier", "warehouse", "source_health", "support")}
        carrier = evidence["carrier"]["metrics"]
        self.assertEqual((carrier["baseline_late"], carrier["current_late"]), (4, 16))
        self.assertEqual(carrier["late_rate_delta_pp"], 30)
        self.assertEqual([row["additional_late"] for row in carrier["by_carrier"]], [12, 0])
        self.assertEqual(evidence["warehouse"]["metrics"]["current_avg_dispatch_hours"], 4)
        self.assertEqual(evidence["support"]["metrics"]["current_delivery_questions"], 16)
        self.assertTrue(all(value["snapshot_id"] == snapshot["snapshot_id"] for value in evidence.values()))
        self.assertTrue(all(value["claims"] and value["query"]["sha256"] for value in evidence.values()))

    def test_duplicate_and_out_of_order_events_cannot_inflate_or_revert_shipments(self):
        result = self.snapshot()
        self.assertEqual(result["quality"]["duplicate_records"], 8)
        self.assertEqual(result["quality"]["stale_records"], 80)
        connection, _ = open_snapshot(store=self.store, snapshot=result["snapshot"])
        try:
            row = connection.execute("SELECT COUNT(*), MIN(version), COUNT(DISTINCT order_id) FROM shipments WHERE status='delivered'").fetchone()
            self.assertEqual(tuple(row), (80, 2, 80))
        finally:
            connection.close()
        receipts = self.receipts()
        self.change_batch(receipts, "carrier", lambda batch: batch["records"].reverse())
        reordered = assemble_snapshot(store=self.store, scenario="real-delay", receipts=receipts)
        self.assertEqual(result["snapshot"]["database"], reordered["snapshot"]["database"])

    def test_conflicting_same_version_payloads_quarantine_identity_and_block_snapshot(self):
        receipts = self.receipts()
        def conflict(batch):
            row = copy.deepcopy(batch["records"][0])
            row["carrier"] = "ConflictingCarrier"
            batch["records"].append(row)
        self.change_batch(receipts, "carrier", conflict)
        result = assemble_snapshot(store=self.store, scenario="real-delay", receipts=receipts)
        self.assertFalse(result["ready"])
        self.assertGreaterEqual(result["quality"]["quarantined_records"], 3)
        issues = read_json(self.store, result["quarantine"]["carrier"])
        self.assertIn("conflicting", issues[0]["reason"])

    def test_malformed_record_is_quarantined_without_silently_publishing_partial_data(self):
        receipts = self.receipts()
        self.change_batch(receipts, "orders", lambda batch: batch["records"][0].update(version=True))
        result = assemble_snapshot(store=self.store, scenario="real-delay", receipts=receipts)
        self.assertFalse(result["ready"])
        self.assertEqual(result["quality"]["quarantined_records"], 1)
        self.assertGreater(result["quality"]["orphan_records"], 0)

    def test_relation_validation_rejects_equal_count_but_wrong_joins(self):
        for source, change, metric in (
            ("support", lambda batch: batch["records"][0].update(order_id="unknown-order"), "orphan_records"),
            ("warehouse", lambda batch: batch["records"][0].update(order_id=batch["records"][1]["order_id"]), "duplicate_relations"),
            ("warehouse", lambda batch: batch["records"][0].update(warehouse="Unknown"), "inconsistent_records"),
        ):
            with self.subTest(source=source, metric=metric):
                receipts = self.receipts()
                self.change_batch(receipts, source, change)
                result = assemble_snapshot(store=self.store, scenario="real-delay", receipts=receipts)
                self.assertFalse(result["ready"])
                self.assertGreater(result["quality"][metric], 0)

    def test_counts_are_recomputed_from_raw_and_receipt_cannot_hide_quality_failure(self):
        receipts = self.receipts("data-gap")
        receipts["warehouse"]["counts"] = {key: 0 for key in receipts["warehouse"]["counts"]}
        result = assemble_snapshot(store=self.store, scenario="data-gap", receipts=receipts)
        self.assertFalse(result["ready"])
        self.assertEqual(result["quality"]["source_counts"]["warehouse"]["normalized_count"], 68)

    def test_missing_source_and_inconsistent_windows_block_publication(self):
        receipts = self.receipts()
        del receipts["support"]
        result = assemble_snapshot(store=self.store, scenario="real-delay", receipts=receipts)
        self.assertFalse(result["ready"])
        self.assertEqual(result["quality"]["missing_sources"], ["support"])
        receipts = self.receipts()
        self.change_batch(receipts, "support", lambda batch: batch["coverage"].update(through="2026-09-11T00:00:00Z"))
        result = assemble_snapshot(store=self.store, scenario="real-delay", receipts=receipts)
        self.assertFalse(result["ready"])
        self.assertEqual(result["quality"]["inconsistent_windows"], 1)

    def test_idempotent_parallel_builds_publish_identical_artifacts(self):
        self.assertEqual(prepare_data(self.store), self.index)
        with ThreadPoolExecutor(max_workers=3) as pool:
            results = list(pool.map(lambda _: self.snapshot(), range(3)))
        self.assertEqual(results[0], results[1])
        self.assertEqual(results[1], results[2])
        self.assertEqual(len(list(Path(self.store).rglob("*.sqlite"))), 1)
        self.assertEqual(list(Path(self.store).rglob(".pending-*")), [])

    def test_snapshot_is_read_only_and_all_artifact_references_are_digest_checked(self):
        snapshot = self.snapshot()["snapshot"]
        connection, _ = open_snapshot(store=self.store, snapshot=snapshot)
        try:
            with self.assertRaises(sqlite3.OperationalError):
                connection.execute("DELETE FROM orders")
        finally:
            connection.close()
        path = store_path(self.store, snapshot["database"]["path"])
        path.write_bytes(path.read_bytes() + b"tampered")
        with self.assertRaisesRegex(ValueError, "digest or length"):
            analyze_snapshot(store=self.store, snapshot=snapshot, topic="carrier")

    def test_raw_tampering_rejected_during_ingestion_and_analysis(self):
        snapshot = self.snapshot()["snapshot"]
        receipt = self.receipts()["carrier"]
        path = store_path(self.store, receipt["raw"]["path"])
        path.write_bytes(b"{}")
        with self.assertRaisesRegex(ValueError, "digest or length"):
            ingest_source(store=self.store, scenario="real-delay", source="carrier")
        with self.assertRaisesRegex(ValueError, "digest or length"):
            analyze_snapshot(store=self.store, snapshot=snapshot, topic="carrier")

    def test_reference_escape_and_manifest_substitution_are_rejected(self):
        snapshot = self.snapshot()["snapshot"]
        bad = copy.deepcopy(snapshot)
        bad["snapshot_id"] = "0" * 64
        with self.assertRaisesRegex(ValueError, "binding"):
            analyze_snapshot(store=self.store, snapshot=bad, topic="carrier")
        reference = dict(snapshot["manifest"], path="../outside.json")
        with self.assertRaisesRegex(ValueError, "escapes"):
            read_bytes(self.store, reference)
        with tempfile.TemporaryDirectory() as outside:
            (Path(self.store) / "escape").symlink_to(outside)
            with self.assertRaisesRegex(ValueError, "escapes"):
                write_named(self.store, "escape/report.json", b"{}")

    def test_prepare_does_not_destroy_modified_fixture_registry(self):
        registry = Path(self.store) / "fixtures.json"
        registry.write_text("operator edit\n")
        with self.assertRaisesRegex(ValueError, "immutable artifact differs"):
            prepare_data(self.store)
        self.assertEqual(registry.read_text(), "operator edit\n")


if __name__ == "__main__":
    unittest.main()

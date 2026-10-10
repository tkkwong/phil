"""Offline regressions for the append-only PAPER execution receipt store.

No test in this file contacts IBKR, starts TWS/Gateway, uses credentials,
or touches the protected journals. All storage is confined to temporary
directories supplied through the private test seams.
"""
from __future__ import annotations

import hashlib
import json
import pathlib
import sys
import tempfile
import threading
import unittest

REPOSITORY_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from ibkr import paper_execution_state as state  # noqa: E402
from manus import paper_locks  # noqa: E402


def base_receipt(**changes):
    record = {
        "event_type": "submission-attempted",
        "execution_id": "a" * 64,
        "intent_sha256": "b" * 64,
        "decision_id": "decision-1",
        "mapping_id": "fixture-mapping",
        "mapping_sha256": "c" * 64,
        "source_binding_sha256": "d" * 64,
        "order_ref": "phil5f3-" + "a" * 32,
        "account_id_masked": "***0011",
        "target": {"conid": 433489712, "symbol": "FIXTUREETF", "sec_type": "STK", "currency": "USD", "exchange": "SMART"},
        "order": {"action": "BUY", "quantity": 1, "order_type": "LMT", "limit_price": "99.00", "tif": "DAY", "outside_rth": False},
        "broker": {"client_id": 19, "order_id": None, "perm_id": None, "status": None},
        "reason_code": None,
    }
    record.update(changes)
    return record


class StateRootTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name) / "exec"

    def test_import_creates_no_receipt_root_or_file(self):
        # The temp root was never created by import; reading there must
        # simply find no receipts (and must not create directories).
        records = state.read_receipts(_execution_root=self.root)
        self.assertEqual(records, [])
        self.assertFalse(self.root.exists())

    def test_resolve_root_must_be_outside_repository(self):
        with self.assertRaises(state.ExecutionStateError) as caught:
            state.resolve_execution_root(REPOSITORY_ROOT / "journal")
        self.assertEqual(caught.exception.code, "receipt-write-failed")


class ReceiptBuildTests(unittest.TestCase):
    def test_record_hash_is_deterministic_and_field_complete(self):
        first = state.build_receipt(**base_receipt())
        second = state.build_receipt(**base_receipt())
        self.assertEqual(
            {key for key in first if key != "recorded_at_utc"},
            {key for key in second if key != "recorded_at_utc"},
        )
        self.assertEqual(state.record_sha256(first), first["record_sha256"])
        self.assertTrue(state.verify_record_sha256(first))

    def test_raw_account_id_is_rejected_by_mask_contract(self):
        with self.assertRaises(state.ExecutionStateError):
            state.build_receipt(**base_receipt(account_id_masked="DU0000011"))

    def test_unknown_event_type_and_reason_code_fail_closed(self):
        with self.assertRaises(state.ExecutionStateError):
            state.build_receipt(**base_receipt(event_type="filled-forever"))
        with self.assertRaises(state.ExecutionStateError):
            state.build_receipt(**base_receipt(reason_code="nonsense-code"))

    def test_closed_field_sets(self):
        with self.assertRaises(state.ExecutionStateError):
            state.build_receipt(**base_receipt(target={"conid": 1}))
        with self.assertRaises(state.ExecutionStateError):
            state.build_receipt(**base_receipt(order={"action": "BUY"}))
        with self.assertRaises(state.ExecutionStateError):
            state.build_receipt(**base_receipt(broker={"client_id": 19}))


class ReceiptStoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name) / "exec"
        self.lock_root = pathlib.Path(self.tmp.name) / "locks"

    def append(self, record):
        return state.append_receipt(record, _execution_root=self.root, _lock_root=self.lock_root)

    def test_append_then_read_roundtrip(self):
        record = state.build_receipt(**base_receipt())
        self.assertEqual(self.append(record), "appended")
        records = state.read_receipts(_execution_root=self.root)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["event_type"], "submission-attempted")

    def test_identical_event_is_idempotent_noop(self):
        record = state.build_receipt(**base_receipt())
        self.assertEqual(self.append(record), "appended")
        self.assertEqual(self.append(record), "idempotent")
        records = state.read_receipts(_execution_root=self.root)
        self.assertEqual(len(records), 1)

    def test_conflicting_event_fails_closed(self):
        first = state.build_receipt(**base_receipt())
        second = state.build_receipt(**base_receipt(reason_code="paper-order-rejected"))
        # Force the same event_id with different content.
        second["event_id"] = first["event_id"]
        second["record_sha256"] = state.record_sha256(second)
        self.assertEqual(self.append(first), "appended")
        with self.assertRaises(state.ExecutionStateError) as caught:
            self.append(second)
        self.assertEqual(caught.exception.code, "receipt-write-failed")
        # The prior record is unchanged.
        records = state.read_receipts(_execution_root=self.root)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["event_type"], "submission-attempted")

    def test_append_only_never_rewrites_old_records(self):
        first = state.build_receipt(**base_receipt())
        before = (self.root / state.RECEIPT_FILENAME)
        self.assertEqual(self.append(first), "appended")
        snapshot = before.read_bytes()
        outcome = state.build_receipt(
            **base_receipt(event_type="acknowledged", broker={"client_id": 19, "order_id": 11, "perm_id": 77, "status": "Submitted"})
        )
        self.append(outcome)
        after = before.read_bytes()
        self.assertTrue(after.startswith(snapshot))
        self.assertEqual(len(after.splitlines()), 2)

    def test_write_failure_leaves_prior_bytes_intact(self):
        record = state.build_receipt(**base_receipt())
        self.assertEqual(self.append(record), "appended")
        path = self.root / state.RECEIPT_FILENAME
        snapshot = path.read_bytes()
        bad = state.build_receipt(**base_receipt(event_type="submitted"))
        bad["event_id"] = "conflict-id"
        bad["record_sha256"] = state.record_sha256(bad)
        # A malformed existing line makes read fail closed before append.
        path.write_bytes(snapshot + b"not-json\n")
        with self.assertRaises(state.ExecutionStateError):
            self.append(bad)
        self.assertEqual(path.read_bytes(), snapshot + b"not-json\n")

    def test_corrupt_receipt_fails_closed_on_read(self):
        self.root.mkdir(parents=True, exist_ok=True)
        path = self.root / state.RECEIPT_FILENAME
        path.write_bytes(b'{"schema_version": "x"}\n')
        with self.assertRaises(state.ExecutionStateError):
            state.read_receipts(_execution_root=self.root)


class ReceiptLockTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name) / "exec"
        self.lock_root = pathlib.Path(self.tmp.name) / "locks"

    def test_parallel_writers_serialize_no_duplicate_lines(self):
        record = state.build_receipt(**base_receipt())
        results: list[str] = []
        errors: list[str] = []
        barrier = threading.Barrier(2)

        def worker():
            barrier.wait()
            try:
                results.append(
                    state.append_receipt(record, _execution_root=self.root, _lock_root=self.lock_root)
                )
            except state.ExecutionStateError as exc:
                errors.append(exc.code)

        threads = [threading.Thread(target=worker) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(5)
        # Exactly one appended and one idempotent (or one failed-lock); in
        # every accepted arrangement there is exactly ONE receipt line.
        records = state.read_receipts(_execution_root=self.root)
        self.assertEqual(len(records), 1)
        self.assertEqual(len(results) + len(errors), 2)
        self.assertTrue(all(result in ("appended", "idempotent") for result in results))


class ExecutionClaimTests(unittest.TestCase):
    """5F-3a2: the atomic cross-process execution claim."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = pathlib.Path(self.tmp.name) / "exec"
        self.lock_root = pathlib.Path(self.tmp.name) / "locks"

    def claim(self, record):
        return state.claim_submission_attempt(record, _execution_root=self.root, _lock_root=self.lock_root)

    def test_first_claim_returns_claimed_and_persists_one_attempted_record(self):
        record = state.build_receipt(**base_receipt())
        self.assertEqual(self.claim(record), "claimed")
        records = state.read_receipts(_execution_root=self.root)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["event_type"], "submission-attempted")

    def test_second_claim_same_execution_id_fails_closed_already_claimed(self):
        record = state.build_receipt(**base_receipt())
        self.assertEqual(self.claim(record), "claimed")
        with self.assertRaises(state.ExecutionStateError) as caught:
            self.claim(record)
        self.assertEqual(caught.exception.code, "execution-already-claimed")
        # No second record was appended.
        records = state.read_receipts(_execution_root=self.root)
        self.assertEqual(len(records), 1)

    def test_prior_uncertain_record_makes_claim_fail_closed_uncertain(self):
        uncertain = state.build_receipt(
            **base_receipt(event_type="submission-uncertain", reason_code="paper-submission-timeout",
                           broker={"client_id": 19, "order_id": 11, "perm_id": None, "status": None})
        )
        state.append_receipt(uncertain, _execution_root=self.root, _lock_root=self.lock_root)
        record = state.build_receipt(**base_receipt())
        with self.assertRaises(state.ExecutionStateError) as caught:
            self.claim(record)
        self.assertEqual(caught.exception.code, "execution-uncertain")
        records = state.read_receipts(_execution_root=self.root)
        self.assertEqual(len(records), 1)

    def test_prior_outcome_record_for_execution_id_blocks_claim(self):
        acknowledged = state.build_receipt(
            **base_receipt(event_type="acknowledged",
                           broker={"client_id": 19, "order_id": 11, "perm_id": 77, "status": "Submitted"})
        )
        state.append_receipt(acknowledged, _execution_root=self.root, _lock_root=self.lock_root)
        record = state.build_receipt(**base_receipt())
        with self.assertRaises(state.ExecutionStateError) as caught:
            self.claim(record)
        self.assertEqual(caught.exception.code, "execution-already-claimed")

    def test_claim_rejects_non_attempted_event_type(self):
        record = state.build_receipt(
            **base_receipt(event_type="acknowledged",
                           broker={"client_id": 19, "order_id": 11, "perm_id": 77, "status": "Submitted"})
        )
        with self.assertRaises(state.ExecutionStateError) as caught:
            self.claim(record)
        self.assertEqual(caught.exception.code, "intent-invalid")

    def test_claim_rejects_integrity_broken_record(self):
        record = state.build_receipt(**base_receipt())
        record["record_sha256"] = "0" * 64
        with self.assertRaises(state.ExecutionStateError) as caught:
            self.claim(record)
        self.assertEqual(caught.exception.code, "receipt-write-failed")

    def test_corrupt_existing_log_blocks_claim_fail_closed(self):
        self.root.mkdir(parents=True, exist_ok=True)
        (self.root / state.RECEIPT_FILENAME).write_bytes(b"corrupt\n")
        record = state.build_receipt(**base_receipt())
        with self.assertRaises(state.ExecutionStateError) as caught:
            self.claim(record)
        self.assertEqual(caught.exception.code, "receipt-write-failed")

    def test_claim_lock_never_held_during_long_operations(self):
        # Structural proof: the claim implementation performs no network,
        # adapter, or transport calls; it uses only the state module's
        # own file/lock machinery. (The executor-level cross-process test
        # in tests.test_ibkr_paper_execution proves the placement race.)
        import inspect
        source = inspect.getsource(state.claim_submission_attempt)
        for banned in ("requests", "urllib", "socket", "connect(", "place_order"):
            self.assertNotIn(banned, source)


if __name__ == "__main__":
    unittest.main()

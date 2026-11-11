"""Offline regression suite for typed append-only decision provenance (5E-6).

All persistence runs in temporary external directories. Production journals are
never touched; there is no network, no broker, and no paid call anywhere here.
"""
from __future__ import annotations

import json
import pathlib
import tempfile
import unittest
import unittest.mock
import uuid

from manus import decision_provenance as dp

CYCLE_ID = "123e4567-e89b-42d3-a456-426614174000"
INTENT_ID = "223e4567-e89b-42d3-a456-426614174000"


def _frozen(**changes):
    values = {
        "schema_version": dp.FROZEN_INPUT_SCHEMA_VERSION,
        "candidate_id": "candidate-100",
        "market_id": "market-100",
        "event_id": "event-100",
        "outcome": "Texas A&M",
        "end_date_utc": "2026-10-01T00:00:00Z",
        "category": "sports",
        "estimated_probability": 0.62,
        "forecast_disposition": "bet",
        "edge_class": "other",
        "rationale": "Offline frozen evidence.",
        "eligible_candidate_count": 1,
        "researchable": True,
        "decision_utc": "2026-09-27T05:00:00Z",
    }
    values.update(changes)
    return dp.frozen_input(**{
        key: value for key, value in values.items() if key != "schema_version"
    })


def _record(recorded_at_utc="2026-09-27T05:00:00Z", frozen=None, **changes):
    values = {
        "execution_mode": "PAPER",
        "recorded_at_utc": recorded_at_utc,
        "code_revision": None,
        "cycle_id": CYCLE_ID,
        "candidate_id": "candidate-100",
        "market_id": "market-100",
        "event_id": "event-100",
        "source_intent_id": INTENT_ID,
        "research_task_id": "task-1",
        "forecast_id": None,
        "research_provider": "manus",
        "research_model_id": None,
        "research_profile": "standard",
        "prompt_version": None,
        "prompt_sha256": None,
        "response_schema_version": None,
        "frozen": frozen if frozen is not None else _frozen(),
        "decision_action": "no-trade",
        "decision_stage": "decision-policy",
        "decision_phase": "final",
        "reason_code": "no-edge",
        "disposition": "no-edge",
        "estimated_probability": 0.62,
        "market_probability": None,
        "edge": None,
        "requested_notional": None,
        "placement_attempted": False,
        "placement_result": None,
        "placement_rejection_code": None,
    }
    values.update(changes)
    return dp.build_decision_record(**values)


class ProvenanceRootTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.temporary_directory.name) / "prov"

    def tearDown(self):
        self.temporary_directory.cleanup()

    def _path(self):
        return self.root / "decision_provenance.jsonl"

    def _append(self, record):
        return dp.append_decision_record(record, _provenance_root=self.root)

    def _read(self):
        return dp.read_decision_records(_provenance_root=self.root)

    def test_frozen_input_hash_is_deterministic(self):
        first = dp.frozen_input_sha256(_frozen())
        second = dp.frozen_input_sha256(_frozen())
        self.assertEqual(first, second)

    def test_frozen_input_hash_ignores_key_order(self):
        direct = _frozen()
        reordered = {
            key: direct[key]
            for key in sorted(direct, reverse=True)
        }
        self.assertEqual(
            dp.frozen_input_sha256(direct),
            dp.frozen_input_sha256(reordered),
        )

    def test_decision_relevant_change_changes_hash(self):
        base = dp.frozen_input_sha256(_frozen())
        changed = dp.frozen_input_sha256(_frozen(estimated_probability=0.63))
        self.assertNotEqual(base, changed)

    def test_invalid_required_input_fails_bounded(self):
        with self.assertRaisesRegex(dp.ProvenanceWriteError, "estimated_probability is invalid"):
            dp.frozen_input(
                candidate_id="candidate-100", market_id="market-100",
                event_id="e", outcome="o", category="sports",
                estimated_probability="not-a-number",
                forecast_disposition="no-edge",
            )

    def test_unknown_frozen_field_fails_bounded(self):
        # Unknown keyword arguments are rejected outright...
        with self.assertRaises(TypeError):
            dp.frozen_input(
                candidate_id="candidate-100", market_id="market-100",
                event_id=None, outcome="x", end_date_utc=None, category="sports",
                estimated_probability=0.5, forecast_disposition="no-edge",
                edge_class=None, rationale=None, eligible_candidate_count=1,
                researchable=True, decision_utc=None,
                best_bid=None, best_ask=None, liquidity=None,
                volume_24h=None, policy=None, open_positions=None,
                invented_field="nope",
            )
        # ...and unknown keys inside nested bounded documents too.
        with self.assertRaisesRegex(dp.ProvenanceWriteError, "policy is invalid"):
            dp.frozen_input(
                candidate_id="candidate-100", market_id="market-100",
                event_id=None, outcome="x", end_date_utc=None, category="sports",
                estimated_probability=0.5, forecast_disposition="no-edge",
                edge_class=None, rationale=None, eligible_candidate_count=1,
                researchable=True, decision_utc=None,
                policy={"invented": 1},
            )

    def test_open_position_rows_are_strictly_bounded(self):
        good = dp.frozen_input(
            candidate_id="c", market_id="m", event_id="e", outcome="o",
            end_date_utc=None, category="sports", estimated_probability=0.5,
            forecast_disposition="no-edge", edge_class=None, rationale=None,
            eligible_candidate_count=1, researchable=True, decision_utc=None,
            open_positions=[{
                "market_id": "market-100", "outcome": "Texas A&M",
                "event_id": "event-100", "stake_usd": 5.0, "shares": 10.0,
                "status": "open",
            }],
        )
        self.assertEqual(good["open_positions"][0]["event_id"], "event-100")
        with self.assertRaises(dp.ProvenanceWriteError):
            dp.frozen_input(
                candidate_id="c", market_id="m", event_id="e", outcome="o",
                end_date_utc=None, category="sports", estimated_probability=0.5,
                forecast_disposition="no-edge", edge_class=None, rationale=None,
                eligible_candidate_count=1, researchable=True, decision_utc=None,
                open_positions=[{"invented": "row"}],
            )

    def test_first_append_then_idempotent_duplicate(self):
        record = _record()
        self.assertEqual(self._append(record), "appended")
        self.assertEqual(self._append(record), "idempotent")
        records = self._read()
        self.assertEqual(len(records), 1)

    def test_conflicting_record_fails_closed(self):
        record = _record()
        self._append(record)
        conflicting = _record(reason_code="wide-spread-veto")
        with self.assertRaisesRegex(dp.ProvenanceWriteError, "integrity conflict"):
            self._append(conflicting)
        records = self._read()
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["reason_code"], "no-edge")

    def test_record_sha256_verifies_and_detects_tamper(self):
        record = _record()
        self.assertTrue(dp.verify_record_sha256(record))
        tampered = dict(record, decision_action="trade")
        self.assertFalse(dp.verify_record_sha256(tampered))

    def test_decision_id_binds_core_identity_and_is_deterministic(self):
        first = _record()
        second = _record()
        self.assertEqual(first["decision_id"], second["decision_id"])
        changed = _record(frozen=_frozen(estimated_probability=0.99))
        self.assertNotEqual(first["decision_id"], changed["decision_id"])

    def test_different_phase_changes_decision_id(self):
        attempt = _record(decision_phase="attempt")
        final = _record(decision_phase="final")
        self.assertNotEqual(attempt["decision_id"], final["decision_id"])

    def test_existing_records_are_never_overwritten(self):
        record = _record()
        self._append(record)
        before = self._path().read_bytes()
        conflicting = _record(placement_attempted=True)
        with self.assertRaises(dp.ProvenanceWriteError):
            self._append(conflicting)
        self.assertEqual(self._path().read_bytes(), before)

    def test_model_metadata_null_behavior_is_truthful(self):
        record = _record()
        self.assertIsNone(record["research_model_id"])
        self.assertIsNone(record["prompt_version"])
        self.assertIsNone(record["cycle_id"] if False else record.get("forecast_id"))
        known = _record(
            research_model_id="manus-standard-v1",
            prompt_version="manus-paper-cycle/v1",
            forecast_id="forecast-1",
        )
        self.assertEqual(known["research_model_id"], "manus-standard-v1")
        self.assertEqual(known["prompt_version"], "manus-paper-cycle/v1")

    def test_unknown_execution_mode_and_action_fail_bounded(self):
        with self.assertRaisesRegex(dp.ProvenanceWriteError, "execution_mode is invalid"):
            _record(execution_mode="LIVE")
        with self.assertRaisesRegex(dp.ProvenanceWriteError, "decision_action is invalid"):
            _record(decision_action="maybe")

    def test_reason_code_stays_bounded_and_unmapped(self):
        # The provenance layer keeps repository reason codes verbatim; a
        # code outside the bounded identifier shape fails closed rather
        # than being renamed or dropped.
        with self.assertRaisesRegex(dp.ProvenanceWriteError, "reason_code is invalid"):
            _record(reason_code="not in vocabulary!")
        self.assertEqual(_record(reason_code="not-in-vocabulary")["reason_code"], "not-in-vocabulary")

    def test_rejection_codes_and_trade_rejections_are_classified(self):
        rejection = _record(
            decision_action="rejected",
            decision_stage="placement",
            decision_phase="final",
            reason_code="spread-too-wide",
            disposition="bet",
            placement_attempted=True,
            placement_result="rejected",
            placement_rejection_code="spread-too-wide",
        )
        self.assertEqual(dp.append_decision_record(rejection, _provenance_root=self.root), "appended")
        records = self._read()
        self.assertEqual(records[0]["decision_action"], "rejected")
        self.assertEqual(records[0]["reason_code"], "spread-too-wide")

    def test_event_id_null_when_genuinely_unresolved(self):
        record = _record(event_id=None)
        self.assertIsNone(record["event_id"])

    def test_counts_derive_from_records_only(self):
        record = _record()
        dp.append_decision_record(record, _provenance_root=self.root)
        rejection = _record(
            decision_action="rejected",
            decision_stage="placement",
            reason_code="spread-too-wide",
            disposition="bet",
            placement_attempted=True,
            placement_result="rejected",
            placement_rejection_code="spread-too-wide",
        )
        dp.append_decision_record(rejection, _provenance_root=self.root)
        counts = dp.decision_counts(self._read())
        self.assertEqual(counts["decision_counts"], {"trade": 0, "no_trade": 1, "rejected": 1})
        self.assertEqual(counts["by_stage"], {"decision-policy": 1, "placement": 1})
        self.assertEqual(counts["by_reason"], {"no-edge": 1, "spread-too-wide": 1})

    def test_jsonl_output_is_canonical_and_hashable(self):
        record = _record()
        self._append(record)
        text = self._path().read_text(encoding="utf-8")
        loaded = json.loads(text)
        self.assertEqual(loaded["record_sha256"], record["record_sha256"])

    def test_storage_is_denied_inside_the_repository(self):
        import manus.paper_locks as locks
        record = _record()
        with self.assertRaises(dp.ProvenanceWriteError):
            dp.append_decision_record(record, _provenance_root=locks._repository_root() / "manus")

    def test_import_has_no_side_effects_or_network(self):
        source = pathlib.Path(dp.__file__).read_text(encoding="utf-8")
        self.assertNotIn("urllib", source)
        self.assertNotIn("requests", source)
        self.assertNotIn("httpx", source)
        self.assertNotIn("socket", source)
        self.assertNotIn("subprocess", source)




class ProvenanceWriterLockOwnershipTests(unittest.TestCase):
    """5E-6a: append_decision_record is the single provenance-writer owner.

    The lock/verify/append sequence must happen exactly once per append, with
    no nested or reentrant acquisition anywhere, and parallel writers must
    stay serialized by the fixed OS lock.
    """

    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.temporary_directory.name) / "prov"
        self.calls = []

    def tearDown(self):
        self.temporary_directory.cleanup()

    def _append(self, record):
        return dp.append_decision_record(record, _provenance_root=self.root)

    def _read(self):
        return dp.read_decision_records(_provenance_root=self.root)

    def test_append_acquires_provenance_writer_lock_exactly_once(self):
        import manus.paper_locks as paper_locks_module
        original = dp.paper_locks.acquire_provenance_writer_lock

        def counting_acquire(*args, **kwargs):
            self.calls.append("acquire")
            return original(*args, **kwargs)

        record = _record()
        with unittest.mock.patch.object(dp.paper_locks, "acquire_provenance_writer_lock", side_effect=counting_acquire):
            self.assertEqual(self._append(record), "appended")
        self.assertEqual(self.calls, ["acquire"])
        self.assertEqual(len(self._read()), 1)

    def test_identical_idempotent_append_stays_safe(self):
        record = _record()
        self.assertEqual(self._append(record), "appended")
        self.assertEqual(self._append(record), "idempotent")
        self.assertEqual(len(self._read()), 1)

    def test_conflicting_same_decision_id_remains_fail_closed(self):
        record = _record()
        self._append(record)
        conflicting = _record(reason_code="wide-spread-veto")
        with self.assertRaisesRegex(dp.ProvenanceWriteError, "integrity conflict"):
            self._append(conflicting)
        self.assertEqual(len(self._read()), 1)

    def test_parallel_writers_remain_serialized(self):
        import multiprocessing

        record = _record()
        context = multiprocessing.get_context("spawn")
        results = context.Queue()
        common = (_record, str(self.root))
        processes = [
            context.Process(target=_append_worker, args=common + (results,))
            for _ in range(4)
        ]
        for process in processes:
            process.start()
        for process in processes:
            process.join(30)
            self.assertEqual(process.exitcode, 0)
        outcomes = [results.get(timeout=5) for _ in range(4)]
        # The fixed OS lock serializes writers: exactly one process performs
        # the single real append; every other process either cannot enter
        # while the lock is held ("blocked") or acquires the lock afterward
        # and deterministically finds the identical record ("idempotent").
        # Which process wins is timing-dependent, not asserted.
        self.assertEqual(outcomes.count("appended"), 1)
        # Whether a loser reports "blocked" (lock held at its attempt) or
        # "idempotent" (it acquired the lock after the winner released it and
        # deterministically matched the record) is timing-dependent; both
        # prove serialization with no duplicate or lost record.
        losers = [outcome for outcome in outcomes if outcome != "appended"]
        self.assertEqual(len(losers), 3)
        self.assertTrue(
            all(outcome in {"blocked", "idempotent"} for outcome in losers),
            f"unexpected parallel outcomes: {losers}",
        )
        self.assertEqual(len(self._read()), 1)

    def test_failed_lock_acquisition_raises_bounded_write_error(self):
        import manus.paper_locks as locks
        record = _record()
        def unavailable(*args, **kwargs):
            raise locks.LockUnavailableError("held")
        with unittest.mock.patch.object(dp.paper_locks, "acquire_provenance_writer_lock", side_effect=unavailable):
            with self.assertRaisesRegex(dp.ProvenanceWriteError, "writer lock is unavailable"):
                self._append(record)
        # No file was created by the failed attempt.
        self.assertFalse((self.root / "decision_provenance.jsonl").exists())

    def test_no_reentrant_acquisition_is_required(self):
        # A non-reentrant second acquisition inside an already-held lock must
        # be unnecessary: the append path holds the lock exactly once (proven
        # by the exactly-once test) and never nests.
        import manus.paper_locks as locks
        record = _record()
        def deny_reentry(*args, **kwargs):
            if self.calls:
                raise locks.LockUnavailableError("nested acquisition attempted")
            self.calls.append("acquire")
            return locks._acquire(
                "provenance-writer",
                purpose="manus-decision-provenance",
                nonblocking=True,
                timeout_seconds=None,
                _lock_root=self.root.parent / "locks",
            )
        with unittest.mock.patch.object(dp.paper_locks, "acquire_provenance_writer_lock", side_effect=deny_reentry):
            self.assertEqual(self._append(record), "appended")
        self.assertEqual(self.calls, ["acquire"])


def _append_worker(record_factory, provenance_root, results):
    """Spawn target: one competing append against the shared log.

    Reports one of: "appended", "blocked" (writer lock held by a peer),
    or "error:<detail>" for anything unexpected.
    """
    try:
        outcome = dp.append_decision_record(
            record_factory(), _provenance_root=pathlib.Path(provenance_root)
        )
        results.put(outcome)
    except dp.ProvenanceWriteError as exc:
        message = str(exc)
        if "writer lock is unavailable" in message:
            results.put("blocked")
        elif "integrity conflict" in message:
            results.put("conflict")
        else:
            results.put(f"error:{message}")


if __name__ == "__main__":
    unittest.main()

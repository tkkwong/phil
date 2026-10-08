"""Offline tests for the manual fixed-staging Manus PAPER application bridge."""

import ast
import copy
import datetime as dt
import hashlib
import inspect
import io
import json
import multiprocessing
import os
import pathlib
import tempfile
import unittest
from contextlib import contextmanager, redirect_stderr
from unittest.mock import patch

from core import forecast as forecast_core
from core import ledger as ledger_core
from manus import paper_apply, paper_locks
from manus.paper_cycle_guardian import VALIDATION_VERSION, prepare_packet
from manus.research_transport import TRANSPORT_REQUEST_SCHEMA_VERSION


FIXTURE = {
    "generated_at": "2026-09-27T05:00:00Z",
    "candidates": [
        {
            "market_id": "market-100",
            "question": "Will Texas A&M win?",
            "end_date": "2026-10-01T00:00:00Z",
            "outcomes": ["Texas A&M", "Wake Forest"],
            "outcome_prices": [0.55, 0.45],
            "event_id": "event-100",
            "description": "Offline trusted fixture.",
        }
    ],
}
INTENT_ID = "123e4567-e89b-42d3-a456-426614174000"
NOW = dt.datetime(2026, 9, 27, 5, 0, tzinfo=dt.timezone.utc)


def json_rows(path):
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def _concurrent_apply_worker(
    fixture_path,
    staging_root,
    forecast_path,
    ledger_path,
    lock_root,
    provenance_root,
    hold_forecast,
    forecast_started,
    release_forecast,
    forecast_calls,
    results,
):
    """Spawn target that executes one real guarded non-bet application."""
    market = {
        "closed": False,
        "question": "Offline live market question",
        "slug": "market-100",
        "endDate": "2026-10-01T00:00:00Z",
        "outcomes": json.dumps(["Texas A&M", "Wake Forest"]),
        "clobTokenIds": json.dumps(["token-texas", "token-wake"]),
    }
    original_record_forecast = paper_apply._record_forecast

    def record_forecast(context, path):
        with pathlib.Path(forecast_calls).open("a", encoding="utf-8") as calls:
            calls.write("forecast\n")
        if hold_forecast:
            forecast_started.set()
            if not release_forecast.wait(10):
                raise TimeoutError("test release did not arrive")
        return original_record_forecast(context, path)

    try:
        with patch.object(forecast_core.pmapi, "gamma_market", return_value=market), \
             patch.object(forecast_core.pmapi, "market_tokens", return_value={"Texas A&M": "token-texas", "Wake Forest": "token-wake"}), \
             patch.object(forecast_core.pmapi, "best_prices", return_value=(0.50, 0.52)), \
             patch.object(paper_apply, "_record_forecast", side_effect=record_forecast):
            result = paper_apply.run(
                fixture_path,
                INTENT_ID,
                _staging_root=pathlib.Path(staging_root),
                _forecast_path=pathlib.Path(forecast_path),
                _ledger_path=pathlib.Path(ledger_path),
                _lock_root=pathlib.Path(lock_root),
                _provenance_root=pathlib.Path(provenance_root),
                _now=lambda: NOW,
                _placement_now=NOW,
            )
        results.put(("ok", result["application_state"], result["forecast_status"]))
    except Exception as exc:  # pragma: no cover - diagnostic sent to parent
        results.put(("error", str(exc)))


def _concurrent_bet_apply_worker(
    fixture_path,
    staging_root,
    forecast_path,
    ledger_path,
    lock_root,
    provenance_root,
    hold_placement,
    placement_started,
    release_placement,
    application_lock_attempted,
    forecast_calls,
    placement_calls,
    results,
):
    """Spawn target that exercises the existing guarded bet placement seam."""
    market = {
        "closed": False,
        "question": "Offline live market question",
        "slug": "market-100",
        "endDate": "2026-10-01T00:00:00Z",
        "outcomes": json.dumps(["Texas A&M", "Wake Forest"]),
        "clobTokenIds": json.dumps(["token-texas", "token-wake"]),
    }
    original_record_forecast = paper_apply._record_forecast
    original_record_placement = paper_apply._record_placement
    original_acquire_application_lock = paper_apply.paper_locks.acquire_application_lock

    def record_forecast(context, path):
        with pathlib.Path(forecast_calls).open("a", encoding="utf-8") as calls:
            calls.write("forecast\n")
        return original_record_forecast(context, path)

    def record_placement(context, *, forecast_path, ledger_path, now):
        with pathlib.Path(placement_calls).open("a", encoding="utf-8") as calls:
            calls.write("placement\n")
        if hold_placement:
            placement_started.set()
            if not release_placement.wait(10):
                raise TimeoutError("test release did not arrive")
        return original_record_placement(
            context,
            forecast_path=forecast_path,
            ledger_path=ledger_path,
            now=now,
        )

    def acquire_application_lock(*args, **kwargs):
        if application_lock_attempted is not None:
            application_lock_attempted.set()
        return original_acquire_application_lock(*args, **kwargs)

    try:
        with patch.object(forecast_core.pmapi, "gamma_market", return_value=market), \
             patch.object(forecast_core.pmapi, "market_tokens", return_value={"Texas A&M": "token-texas", "Wake Forest": "token-wake"}), \
             patch.object(forecast_core.pmapi, "best_prices", return_value=(0.50, 0.52)), \
             patch.object(ledger_core.pmapi, "gamma_markets", return_value=[{"id": "market-100", "events": [{"id": "event-100"}]}]), \
             patch.object(paper_apply, "_record_forecast", side_effect=record_forecast), \
             patch.object(paper_apply, "_record_placement", side_effect=record_placement), \
             patch.object(paper_apply.paper_locks, "acquire_application_lock", side_effect=acquire_application_lock):
            result = paper_apply.run(
                fixture_path,
                INTENT_ID,
                _staging_root=pathlib.Path(staging_root),
                _forecast_path=pathlib.Path(forecast_path),
                _ledger_path=pathlib.Path(ledger_path),
                _lock_root=pathlib.Path(lock_root),
                _provenance_root=pathlib.Path(provenance_root),
                _now=lambda: NOW,
                _placement_now=NOW,
            )
        results.put(("ok", result["application_state"], result["forecast_status"], result["placement_status"]))
    except Exception as exc:  # pragma: no cover - diagnostic sent to parent
        results.put(("error", str(exc)))


class PaperApplyTestsMixin:
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.temporary_directory.name)
        self.fixture = copy.deepcopy(FIXTURE)
        self.packet = prepare_packet(self.fixture)
        self.candidate = self.packet["candidates"][0]
        self.fixture_path = self.root / "fixture.json"
        self.fixture_bytes = json.dumps(self.fixture, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
        self.fixture_path.write_bytes(self.fixture_bytes)
        self.staging_root = self.root / "external-staging"
        self.forecast_path = self.root / "forecasts.jsonl"
        self.ledger_path = self.root / "ledger.jsonl"
        # Isolated provenance root for the production seam; real
        # %LOCALAPPDATA% storage is never touched by tests.
        self.provenance_root = self.root / "external-provenance"

    def tearDown(self):
        self.temporary_directory.cleanup()

    def intent(self, *, disposition="market-agrees", **changes):
        value = {
            "intent_id": INTENT_ID,
            "candidate_id": self.candidate["candidate_id"],
            "market_id": self.candidate["market_id"],
            "outcome": self.candidate["outcomes"][0],
            "estimated_probability": 0.62,
            "category": "sports",
            "rationale": "Offline staged evidence supports this research forecast.",
            "edge_class": "other",
            "mode": "PAPER",
            "forecast_disposition": disposition,
            "strategy_proposals": [],
        }
        value.update(changes)
        return value

    def stage(self, *, intent=None, metadata_changes=None, intent_bytes=None):
        intent = self.intent() if intent is None else intent
        raw_intent = (
            json.dumps(intent, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
            if intent_bytes is None else intent_bytes
        )
        directory = self.staging_root / self.packet["packet_id"] / INTENT_ID
        directory.mkdir(parents=True)
        metadata = {
            "api_version": "v2",
            "task_id": "task-1",
            "packet_id": self.packet["packet_id"],
            "candidate_id": intent.get("candidate_id"),
            "intent_id": INTENT_ID,
            "fixture_sha256": hashlib.sha256(self.fixture_bytes).hexdigest(),
            "request_sha256": "a" * 64,
            "transport_schema_version": TRANSPORT_REQUEST_SCHEMA_VERSION,
            "created_at": "2026-09-27T05:00:00Z",
            "completion_timestamp": "2026-09-27T05:00:00Z",
            "agent_profile": "standard",
            "requested_agent_profile": "standard",
            "resolved_agent_profile": None,
            "task_origin": "created",
            "task_created_this_invocation": True,
            "validation_version": VALIDATION_VERSION,
        }
        if metadata_changes:
            metadata.update(metadata_changes)
        (directory / "validated-intent.json").write_bytes(raw_intent)
        (directory / "run-meta.json").write_text(
            json.dumps(metadata, sort_keys=True, separators=(",", ":")), encoding="utf-8"
        )
        return intent, directory

    def receipt_path(self):
        return self.staging_root / "apply" / self.packet["packet_id"] / f"{INTENT_ID}.json"

    def receipt(self):
        return json.loads(self.receipt_path().read_text(encoding="utf-8"))

    def call(self, *, dry_run=False, **kwargs):
        return paper_apply.run(
            str(self.fixture_path), INTENT_ID, dry_run=dry_run,
            _staging_root=self.staging_root,
            _forecast_path=self.forecast_path,
            _ledger_path=self.ledger_path,
            _provenance_root=self.provenance_root,
            _now=lambda: NOW,
            _placement_now=NOW,
            **kwargs,
        )

    def market(self):
        return {
            "closed": False,
            "question": "Offline live market question",
            "slug": "market-100",
            "endDate": "2026-10-01T00:00:00Z",
            "outcomes": json.dumps(["Texas A&M", "Wake Forest"]),
            "clobTokenIds": json.dumps(["token-texas", "token-wake"]),
        }

    @contextmanager
    def public_market(self, prices=(0.50, 0.52)):
        with patch.object(forecast_core.pmapi, "gamma_market", return_value=self.market()), \
             patch.object(forecast_core.pmapi, "market_tokens", return_value={"Texas A&M": "token-texas", "Wake Forest": "token-wake"}), \
             patch.object(forecast_core.pmapi, "best_prices", return_value=prices), \
             patch.object(ledger_core.pmapi, "gamma_market", return_value=self.market()) as placement_market, \
             patch.object(ledger_core.pmapi, "market_tokens", return_value={"Texas A&M": "token-texas", "Wake Forest": "token-wake"}), \
             patch.object(ledger_core.pmapi, "best_prices", return_value=prices), \
             patch.object(ledger_core.pmapi, "gamma_markets", return_value=[{"id": "market-100", "events": [{"id": "event-100"}]}]):
            yield placement_market

    def write_forecast(self, intent=None, *, changes=None):
        intent = self.intent() if intent is None else intent
        row = {
            "id": "forecast-100",
            "ts": "2026-09-27T05:00:00Z",
            "market_id": intent["market_id"],
            "question": "Offline forecast",
            "slug": "market-100",
            "end_date": "2026-10-01T00:00:00Z",
            "outcome": intent["outcome"],
            "token_id": "token-texas",
            "est_prob": intent["estimated_probability"],
            "best_bid_at_record": 0.50,
            "best_ask_at_record": 0.52,
            "market_prob_at_record": 0.51,
            "category": intent["category"],
            "skip_reason": intent["forecast_disposition"],
            "fit_score": None,
            "note": intent["rationale"],
            "strategy_rev": "",
            "status": "open",
            "source_intent_id": intent["intent_id"],
        }
        if changes:
            row.update(changes)
        self.forecast_path.write_text(json.dumps(row) + "\n", encoding="utf-8")
        return row

    def write_placement(self, forecast, intent=None, *, changes=None):
        intent = self.intent(disposition="bet") if intent is None else intent
        row = {
            "id": "placement-100",
            "ts": "2026-09-27T05:01:00Z",
            "market_id": intent["market_id"],
            "question": "Offline placement",
            "slug": "market-100",
            "end_date": "2026-10-01T00:00:00Z",
            "outcome": intent["outcome"],
            "token_id": "token-texas",
            "entry_price": 0.52,
            "best_bid_at_entry": 0.50,
            "market_prob_at_entry": 0.52,
            "est_prob": intent["estimated_probability"],
            "edge": 0.10,
            "stake_usd": 5.0,
            "shares": 9.6154,
            "category": intent["category"],
            "edge_class": "manus-paper-only",
            "research_edge_class": intent["edge_class"],
            "rationale": intent["rationale"],
            "strategy_rev": "",
            "status": "open",
            "source_intent_id": intent["intent_id"],
            "source_forecast_id": forecast["id"],
            "source_packet_id": self.packet["packet_id"],
            "event_id": "event-100",
        }
        if changes:
            row.update(changes)
        self.ledger_path.write_text(json.dumps(row) + "\n", encoding="utf-8")
        return row

    def test_fixed_staged_path_is_derived_and_cli_exposes_only_three_controls(self):
        self.stage()
        result = self.call(dry_run=True)
        self.assertEqual(result["packet_id"], self.packet["packet_id"])
        self.assertEqual(result["intent_id"], INTENT_ID)
        self.assertTrue(result["staging_verified"])
        self.assertEqual(result["plan"], "record-forecast-only")
        parser = paper_apply.build_parser()
        options = {option for action in parser._actions for option in action.option_strings}
        self.assertEqual(options, {"-h", "--help", "--fixture", "--intent-id", "--dry-run"})
        for forbidden in ("--intent", "--intent-file", "--staging-root", "--forecast-path", "--ledger-path", "--stake", "--real", "--ibkr", "--pearl", "--retry", "--shell"):
            with self.subTest(forbidden=forbidden), redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    paper_apply.main(["--fixture", str(self.fixture_path), "--intent-id", INTENT_ID, forbidden, "x"])

    def test_canonical_intent_id_and_fixed_staging_root_fail_closed(self):
        self.stage()
        with self.assertRaisesRegex(paper_apply.PaperApplyError, "canonical lowercase UUIDv4"):
            paper_apply.run(str(self.fixture_path), INTENT_ID.upper(), dry_run=True, _staging_root=self.staging_root)
        with patch.dict("os.environ", {}, clear=True):
            with self.assertRaisesRegex(paper_apply.PaperApplyError, "Fixed staging root"):
                paper_apply.run(str(self.fixture_path), INTENT_ID, dry_run=True)

    def test_fixed_application_receipt_symlink_is_rejected(self):
        self.stage()
        redirected = self.root / "redirected-receipts"
        redirected.mkdir()
        try:
            (self.staging_root / "apply").symlink_to(redirected, target_is_directory=True)
        except OSError as exc:
            # A normal, non-elevated Windows account may lack the specific
            # CreateSymbolicLink privilege. Do not skip other setup failures;
            # Windows environments that can create symlinks still execute the
            # production rejection assertion below.
            if os.name == "nt" and getattr(exc, "winerror", None) == 1314:
                self.skipTest("Windows account lacks CreateSymbolicLink privilege (WinError 1314)")
            raise
        with self.assertRaisesRegex(paper_apply.PaperApplyError, "Fixed staged intent directory"):
            self.call(dry_run=True)

    def test_provenance_and_staged_intent_mismatches_reject_before_journal_or_market_io(self):
        cases = []
        cases.append(("packet", self.intent(), {"packet_id": "packet-other"}, "packet_id"))
        cases.append(("fixture", self.intent(), {"fixture_sha256": "0" * 64}, "fixture hash"))
        cases.append(("transport", self.intent(), {"transport_schema_version": "old"}, "schema version"))
        cases.append(("intent-id", self.intent(intent_id="223e4567-e89b-42d3-a456-426614174000"), None, "intent_id"))
        cases.append(("candidate", self.intent(candidate_id="cand-other"), None, "candidate_id"))
        cases.append(("market", self.intent(market_id="market-other"), None, "market_id"))
        cases.append(("outcome", self.intent(outcome="Other"), None, "outcome"))
        cases.append(("mode", self.intent(mode="LIVE"), None, "mode"))
        cases.append(("proposals", self.intent(strategy_proposals=[{"proposal_id": "proposal-a", "summary": "inert"}]), None, "strategy_proposals"))
        for label, intent, metadata, message in cases:
            with self.subTest(label=label):
                self.stage(intent=intent, metadata_changes=metadata)
                with patch.object(paper_apply, "record_candidate_forecast", side_effect=AssertionError("no forecast")), \
                     patch.object(paper_apply, "record_candidate_paper_placement", side_effect=AssertionError("no placement")), \
                     self.assertRaisesRegex(paper_apply.PaperApplyError, message):
                    self.call()
                self.assertFalse(self.forecast_path.exists())
                self.assertFalse(self.ledger_path.exists())
                self.assertFalse(self.receipt_path().exists())
                for path in list(self.staging_root.rglob("*")):
                    if path.is_file() and path.name in {"validated-intent.json", "run-meta.json"}:
                        continue
                    if path.is_file():
                        path.unlink()
                directory = self.staging_root / self.packet["packet_id"] / INTENT_ID
                for path in directory.iterdir():
                    path.unlink()
                directory.rmdir()

    def test_strict_fixture_bound_validator_is_invoked_again(self):
        self.stage()
        with patch.object(paper_apply, "validate_candidate_intent", wraps=paper_apply.validate_candidate_intent) as validator:
            self.call(dry_run=True)
        validator.assert_called_once()

    def test_dry_run_has_zero_market_journal_and_receipt_mutation(self):
        self.stage()
        with patch.object(paper_apply, "record_candidate_forecast", side_effect=AssertionError("no forecast")), \
             patch.object(paper_apply, "record_candidate_paper_placement", side_effect=AssertionError("no placement")):
            result = self.call(dry_run=True)
        self.assertEqual(result["plan"], "record-forecast-only")
        self.assertFalse(self.forecast_path.exists())
        self.assertFalse(self.ledger_path.exists())
        self.assertFalse(self.receipt_path().exists())

    def test_receipt_is_durable_before_forecast_and_receipt_failure_blocks_forecast(self):
        self.stage()
        with patch.object(paper_apply, "record_candidate_forecast", side_effect=lambda *args, **kwargs: {"forecast": {"recorded": "forecast-1"}}) as record:
            with patch.object(paper_apply, "_atomic_write_receipt", side_effect=paper_apply.PaperApplyError("disk")):
                with self.assertRaisesRegex(paper_apply.PaperApplyError, "disk"):
                    self.call()
        record.assert_not_called()
        self.assertFalse(self.forecast_path.exists())

    def test_non_bet_dispositions_record_one_forecast_and_never_place(self):
        for disposition in ("market-agrees", "no-edge", "outside-view-veto"):
            with self.subTest(disposition=disposition):
                self.stage(intent=self.intent(disposition=disposition))
                with self.public_market(), patch.object(paper_apply, "record_candidate_paper_placement", side_effect=AssertionError("must not place")) as place:
                    result = self.call()
                self.assertEqual(len(json_rows(self.forecast_path)), 1)
                self.assertFalse(self.ledger_path.exists())
                self.assertEqual(result["application_state"], "completed-no-placement")
                self.assertEqual(result["placement_status"], "not-eligible-disposition")
                place.assert_not_called()
                self.assertEqual(self.receipt()["state"], "completed-no-placement")
                self.forecast_path.unlink()
                self.receipt_path().unlink()
                self.receipt_path().parent.rmdir()
                directory = self.staging_root / self.packet["packet_id"] / INTENT_ID
                for child in directory.iterdir(): child.unlink()
                directory.rmdir()

    def test_two_processes_apply_one_intent_with_one_forecast_mutation(self):
        self.stage(intent=self.intent(disposition="market-agrees"))
        context = multiprocessing.get_context("spawn")
        lock_root = self.root / "fixed-locks"
        forecast_started = context.Event()
        release_forecast = context.Event()
        forecast_calls = self.root / "forecast-calls.txt"
        results = context.Queue()
        common = (
            str(self.fixture_path),
            str(self.staging_root),
            str(self.forecast_path),
            str(self.ledger_path),
            str(lock_root),
            str(self.provenance_root),
        )
        first = context.Process(
            target=_concurrent_apply_worker,
            args=common + (True, forecast_started, release_forecast, str(forecast_calls), results),
        )
        second = context.Process(
            target=_concurrent_apply_worker,
            args=common + (False, forecast_started, release_forecast, str(forecast_calls), results),
        )
        first.start()
        self.assertTrue(forecast_started.wait(10), "first application did not enter forecast mutation")
        second.start()
        # The second process cannot enter the guarded forecast action while the
        # first holds both application and journal-writer locks.
        self.assertEqual(forecast_calls.read_text(encoding="utf-8").splitlines(), ["forecast"])
        release_forecast.set()
        for process in (first, second):
            process.join(20)
            if process.is_alive():
                process.terminate()
                process.join(10)
            self.assertEqual(process.exitcode, 0)
        outcomes = sorted(results.get(timeout=5) for _ in range(2))
        self.assertEqual(outcomes, [
            ("ok", "completed-no-placement", "already-completed"),
            ("ok", "completed-no-placement", "recorded"),
        ])
        self.assertEqual(forecast_calls.read_text(encoding="utf-8").splitlines(), ["forecast"])
        self.assertEqual(len(json_rows(self.forecast_path)), 1)
        self.assertFalse(self.ledger_path.exists())
        self.assertEqual(self.receipt()["state"], "completed-no-placement")

    def test_two_processes_apply_one_bet_with_one_guarded_placement_mutation(self):
        intent = self.intent(disposition="bet")
        self.stage(intent=intent)
        context = multiprocessing.get_context("spawn")
        lock_root = self.root / "fixed-locks"
        placement_started = context.Event()
        release_placement = context.Event()
        second_application_lock_attempted = context.Event()
        forecast_calls = self.root / "forecast-calls.txt"
        placement_calls = self.root / "placement-calls.txt"
        results = context.Queue()
        common = (
            str(self.fixture_path),
            str(self.staging_root),
            str(self.forecast_path),
            str(self.ledger_path),
            str(lock_root),
            str(self.provenance_root),
        )
        first = context.Process(
            target=_concurrent_bet_apply_worker,
            args=common + (
                True,
                placement_started,
                release_placement,
                None,
                str(forecast_calls),
                str(placement_calls),
                results,
            ),
        )
        second = context.Process(
            target=_concurrent_bet_apply_worker,
            args=common + (
                False,
                placement_started,
                release_placement,
                second_application_lock_attempted,
                str(forecast_calls),
                str(placement_calls),
                results,
            ),
        )
        first.start()
        self.assertTrue(placement_started.wait(10), "first process did not enter guarded placement")
        second.start()
        self.assertTrue(
            second_application_lock_attempted.wait(10),
            "second process did not attempt the held application lock",
        )
        # Process 2 has now attempted the same intent lock while process 1 is
        # stopped inside the existing guardian placement seam. No second
        # forecast or placement call can occur before process 1 releases it.
        self.assertEqual(forecast_calls.read_text(encoding="utf-8").splitlines(), ["forecast"])
        self.assertEqual(placement_calls.read_text(encoding="utf-8").splitlines(), ["placement"])
        release_placement.set()
        for process in (first, second):
            process.join(20)
            if process.is_alive():
                process.terminate()
                process.join(10)
            self.assertEqual(process.exitcode, 0)
        outcomes = sorted(results.get(timeout=5) for _ in range(2))
        self.assertEqual(outcomes, [
            ("ok", "completed-placement", "already-completed", "already-completed"),
            ("ok", "completed-placement", "recorded", "placed"),
        ])
        forecasts = json_rows(self.forecast_path)
        ledger_rows = json_rows(self.ledger_path)
        # json_rows parses each row, so both returned JSONL files are valid.
        self.assertEqual(len(forecasts), 1)
        self.assertEqual(len(ledger_rows), 1)
        self.assertEqual(forecasts[0]["source_intent_id"], INTENT_ID)
        self.assertEqual(ledger_rows[0]["source_intent_id"], INTENT_ID)
        self.assertEqual(ledger_rows[0]["source_forecast_id"], forecasts[0]["id"])
        self.assertEqual(self.receipt()["state"], "completed-placement")
        self.assertEqual(self.receipt()["forecast_id"], forecasts[0]["id"])
        self.assertEqual(self.receipt()["placement_id"], ledger_rows[0]["id"])
        self.assertEqual(forecast_calls.read_text(encoding="utf-8").splitlines(), ["forecast"])
        self.assertEqual(placement_calls.read_text(encoding="utf-8").splitlines(), ["placement"])

    def test_dry_run_remains_lock_free_and_write_free(self):
        self.stage()
        lock_root = self.root / "fixed-locks"
        with paper_locks.acquire_application_lock(INTENT_ID, _lock_root=lock_root):
            result = self.call(dry_run=True, _lock_root=lock_root)
        self.assertEqual(result["plan"], "record-forecast-only")
        self.assertFalse(self.forecast_path.exists())
        self.assertFalse(self.ledger_path.exists())
        self.assertFalse(self.receipt_path().exists())

    def test_busy_journal_writer_lock_blocks_receipt_and_guardian_mutation(self):
        self.stage()
        lock_root = self.root / "fixed-locks"
        with paper_locks.acquire_journal_writer_lock(_lock_root=lock_root):
            with patch.object(paper_apply, "JOURNAL_WRITER_LOCK_WAIT_SECONDS", 0), \
                 patch.object(paper_apply, "record_candidate_forecast", side_effect=AssertionError("no forecast")):
                with self.assertRaisesRegex(paper_apply.PaperApplyError, "journal writer is busy"):
                    self.call(_lock_root=lock_root)
        self.assertFalse(self.receipt_path().exists())
        self.assertFalse(self.forecast_path.exists())
        self.assertFalse(self.ledger_path.exists())

    def test_bet_uses_existing_guarded_placement_once(self):
        intent = self.intent(disposition="bet")
        self.stage(intent=intent)
        with self.public_market(), patch.object(
            paper_apply, "record_candidate_paper_placement", wraps=paper_apply.record_candidate_paper_placement
        ) as guarded_placement:
            result = self.call()
        guarded_placement.assert_called_once()
        self.assertEqual(len(json_rows(self.forecast_path)), 1)
        self.assertEqual(len(json_rows(self.ledger_path)), 1)
        self.assertEqual(result["application_state"], "completed-placement")
        self.assertEqual(result["placement_status"], "placed")
        self.assertEqual(json_rows(self.ledger_path)[0]["edge_class"], "manus-paper-only")

    def test_staged_intent_cannot_supply_placement_controls(self):
        forbidden = self.intent()
        forbidden["stake"] = 500
        self.stage(intent=forbidden)
        with patch.object(paper_apply, "record_candidate_forecast", side_effect=AssertionError("no forecast")):
            with self.assertRaisesRegex(paper_apply.PaperApplyError, "strict fixture-bound"):
                self.call()
        self.assertFalse(self.receipt_path().exists())
        self.assertFalse(self.forecast_path.exists())
        self.assertFalse(self.ledger_path.exists())

    def test_placement_rejection_is_terminal_and_never_retried(self):
        intent = self.intent(disposition="bet")
        self.stage(intent=intent)
        with self.public_market(prices=(0.50, 0.61)):
            result = self.call()
        self.assertEqual(result["application_state"], "placement-rejected")
        self.assertEqual(len(json_rows(self.forecast_path)), 1)
        self.assertFalse(self.ledger_path.exists())
        with patch.object(paper_apply, "record_candidate_paper_placement", side_effect=AssertionError("no retry")) as placement:
            again = self.call()
        placement.assert_not_called()
        self.assertEqual(again["application_state"], "placement-rejected")

    def test_forecast_pending_recovers_exact_forecast_without_duplicate(self):
        intent = self.intent(disposition="market-agrees")
        self.stage(intent=intent)
        receipt = paper_apply._new_receipt(self.context(), lambda: NOW)
        receipt["state"] = "forecast-pending"
        paper_apply._atomic_write_receipt(self.receipt_path(), receipt)
        forecast = self.write_forecast(intent)
        with patch.object(paper_apply, "record_candidate_forecast", side_effect=AssertionError("no duplicate")):
            result = self.call()
        self.assertEqual(result["forecast_id"], forecast["id"])
        self.assertEqual(result["forecast_status"], "recovered")
        self.assertEqual(len(json_rows(self.forecast_path)), 1)
        self.assertEqual(self.receipt()["state"], "completed-no-placement")

    def test_forecast_recorded_receipt_without_forecast_fails_closed(self):
        self.stage()
        receipt = paper_apply._new_receipt(self.context(), lambda: NOW)
        receipt.update({"state": "forecast-recorded", "forecast_id": "forecast-missing"})
        paper_apply._atomic_write_receipt(self.receipt_path(), receipt)
        with patch.object(paper_apply, "record_candidate_forecast", side_effect=AssertionError("no retry")):
            with self.assertRaisesRegex(paper_apply.PaperApplyError, "forecast-recorded receipt"):
                self.call()
        self.assertFalse(self.forecast_path.exists())

    def test_receipt_failure_after_forecast_append_recovers_without_duplicate(self):
        self.stage()
        original = paper_apply._atomic_write_receipt
        calls = 0

        def fail_only_forecast_finalization(path, receipt):
            nonlocal calls
            calls += 1
            if calls == 3:
                raise paper_apply.PaperApplyError("simulated receipt finalization failure")
            return original(path, receipt)

        with self.public_market(), patch.object(
            paper_apply, "_atomic_write_receipt", side_effect=fail_only_forecast_finalization
        ):
            with self.assertRaisesRegex(paper_apply.PaperApplyError, "Forecast recorded"):
                self.call()
        self.assertEqual(len(json_rows(self.forecast_path)), 1)
        self.assertEqual(self.receipt()["state"], "forecast-pending")
        with patch.object(paper_apply, "record_candidate_forecast", side_effect=AssertionError("no duplicate")):
            recovered = self.call()
        self.assertEqual(recovered["forecast_status"], "recovered")
        self.assertEqual(len(json_rows(self.forecast_path)), 1)
        self.assertEqual(self.receipt()["state"], "completed-no-placement")

    def test_duplicate_or_mismatched_recovered_forecast_fails_closed(self):
        intent = self.intent()
        self.stage(intent=intent)
        receipt = paper_apply._new_receipt(self.context(), lambda: NOW)
        paper_apply._atomic_write_receipt(self.receipt_path(), receipt)
        first = self.write_forecast(intent)
        second = dict(first, id="forecast-duplicate")
        self.forecast_path.write_text(json.dumps(first) + "\n" + json.dumps(second) + "\n", encoding="utf-8")
        with self.assertRaisesRegex(paper_apply.PaperApplyError, "multiple"):
            self.call()
        self.forecast_path.write_text(json.dumps(self.write_forecast(intent, changes={"note": "wrong"})) + "\n", encoding="utf-8")
        with self.assertRaisesRegex(paper_apply.PaperApplyError, "note"):
            self.call()

    def test_placement_pending_recovers_exact_ledger_row_without_duplicate(self):
        intent = self.intent(disposition="bet")
        self.stage(intent=intent)
        forecast = self.write_forecast(intent)
        receipt = paper_apply._new_receipt(self.context(), lambda: NOW)
        receipt.update({"state": "placement-pending", "forecast_id": forecast["id"]})
        paper_apply._atomic_write_receipt(self.receipt_path(), receipt)
        placement = self.write_placement(forecast, intent)
        with patch.object(paper_apply, "record_candidate_paper_placement", side_effect=AssertionError("no duplicate")):
            result = self.call()
        self.assertEqual(result["placement_id"], placement["id"])
        self.assertEqual(result["placement_status"], "recovered")
        self.assertEqual(len(json_rows(self.ledger_path)), 1)
        self.assertEqual(self.receipt()["state"], "completed-placement")

    def test_orphaned_ledger_provenance_without_forecast_fails_closed(self):
        intent = self.intent(disposition="bet")
        self.stage(intent=intent)
        self.ledger_path.write_text(
            json.dumps({"source_intent_id": INTENT_ID, "id": "placement-orphan"}) + "\n",
            encoding="utf-8",
        )
        with patch.object(paper_apply, "record_candidate_forecast", side_effect=AssertionError("no retry")):
            with self.assertRaisesRegex(paper_apply.PaperApplyError, "PAPER ledger row exists"):
                self.call()
        self.assertFalse(self.forecast_path.exists())

    def test_receipt_failure_after_ledger_append_recovers_without_duplicate(self):
        intent = self.intent(disposition="bet")
        self.stage(intent=intent)
        original = paper_apply._atomic_write_receipt
        calls = 0

        def fail_only_placement_finalization(path, receipt):
            nonlocal calls
            calls += 1
            if calls == 5:
                raise paper_apply.PaperApplyError("simulated placement receipt finalization failure")
            return original(path, receipt)

        with self.public_market(), patch.object(
            paper_apply, "_atomic_write_receipt", side_effect=fail_only_placement_finalization
        ):
            with self.assertRaisesRegex(paper_apply.PaperApplyError, "PAPER placement recorded"):
                self.call()
        self.assertEqual(len(json_rows(self.forecast_path)), 1)
        self.assertEqual(len(json_rows(self.ledger_path)), 1)
        self.assertEqual(self.receipt()["state"], "placement-pending")
        with patch.object(paper_apply, "record_candidate_paper_placement", side_effect=AssertionError("no duplicate")):
            recovered = self.call()
        self.assertEqual(recovered["placement_status"], "recovered")
        self.assertEqual(len(json_rows(self.ledger_path)), 1)
        self.assertEqual(self.receipt()["state"], "completed-placement")

    def test_duplicate_or_mismatched_recovered_placement_fails_closed(self):
        intent = self.intent(disposition="bet")
        self.stage(intent=intent)
        forecast = self.write_forecast(intent)
        receipt = paper_apply._new_receipt(self.context(), lambda: NOW)
        receipt.update({"state": "placement-pending", "forecast_id": forecast["id"]})
        paper_apply._atomic_write_receipt(self.receipt_path(), receipt)
        first = self.write_placement(forecast, intent)
        second = dict(first, id="placement-duplicate")
        self.ledger_path.write_text(json.dumps(first) + "\n" + json.dumps(second) + "\n", encoding="utf-8")
        with self.assertRaisesRegex(paper_apply.PaperApplyError, "multiple"):
            self.call()
        self.ledger_path.write_text(
            json.dumps(self.write_placement(forecast, intent, changes={"source_packet_id": "packet-wrong"})) + "\n",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(paper_apply.PaperApplyError, "source_packet_id"):
            self.call()

    def test_completed_replays_reconcile_without_mutation_and_unexpected_ledger_fails(self):
        intent = self.intent(disposition="market-agrees")
        self.stage(intent=intent)
        forecast = self.write_forecast(intent)
        receipt = paper_apply._new_receipt(self.context(), lambda: NOW)
        receipt.update({"state": "completed-no-placement", "forecast_id": forecast["id"]})
        paper_apply._atomic_write_receipt(self.receipt_path(), receipt)
        with patch.object(paper_apply, "record_candidate_forecast", side_effect=AssertionError("no forecast")), \
             patch.object(paper_apply, "record_candidate_paper_placement", side_effect=AssertionError("no placement")):
            result = self.call()
        self.assertEqual(result["forecast_status"], "already-completed")
        self.write_placement(forecast, self.intent(disposition="bet"))
        with self.assertRaisesRegex(paper_apply.PaperApplyError, "unexpected PAPER ledger"):
            self.call()

    def test_completed_placement_replay_reconciles_without_mutation(self):
        intent = self.intent(disposition="bet")
        self.stage(intent=intent)
        forecast = self.write_forecast(intent)
        placement = self.write_placement(forecast, intent)
        receipt = paper_apply._new_receipt(self.context(), lambda: NOW)
        receipt.update({
            "state": "completed-placement", "forecast_id": forecast["id"], "placement_id": placement["id"],
        })
        paper_apply._atomic_write_receipt(self.receipt_path(), receipt)
        with patch.object(paper_apply, "record_candidate_forecast", side_effect=AssertionError("no forecast")), \
             patch.object(paper_apply, "record_candidate_paper_placement", side_effect=AssertionError("no placement")):
            result = self.call()
        self.assertEqual(result["forecast_status"], "already-completed")
        self.assertEqual(result["placement_status"], "already-completed")

    def test_forecast_rejection_is_terminal_without_automatic_retry(self):
        self.stage()
        with patch.object(paper_apply, "record_candidate_forecast", side_effect=paper_apply.PaperApplyError("rejected")):
            with self.assertRaisesRegex(paper_apply.PaperApplyError, "rejected"):
                self.call()
        self.assertEqual(self.receipt()["state"], "forecast-rejected")
        with patch.object(paper_apply, "record_candidate_forecast", side_effect=AssertionError("no retry")) as forecast:
            result = self.call()
        forecast.assert_not_called()
        self.assertEqual(result["application_state"], "forecast-rejected")

    def test_receipt_binding_and_secrecy_fail_closed(self):
        self.stage()
        receipt = paper_apply._new_receipt(self.context(), lambda: NOW)
        receipt["fixture_sha256"] = "0" * 64
        paper_apply._atomic_write_receipt(self.receipt_path(), receipt)
        with self.assertRaisesRegex(paper_apply.PaperApplyError, "fixture_sha256"):
            self.call()
        receipt = paper_apply._new_receipt(self.context(), lambda: NOW)
        paper_apply._atomic_write_receipt(self.receipt_path(), receipt)
        text = self.receipt_path().read_text(encoding="utf-8")
        self.assertNotIn("credential", text)
        self.assertNotIn("header", text)
        self.assertNotIn("rationale", text)

    def context(self):
        return paper_apply._load_and_validate_staging(str(self.fixture_path), INTENT_ID, self.staging_root)

    def test_static_isolation_excludes_manus_credentials_real_broker_and_command_paths(self):
        source = inspect.getsource(paper_apply)
        tree = ast.parse(source)
        imports, calls = set(), set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import): imports.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module: imports.add(node.module)
            elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name): calls.add(node.func.id)
        self.assertFalse(imports & {"subprocess", "urllib", "requests", "httpx", "ctypes"})
        self.assertFalse(any(name == "core.real" or name.startswith(("ibkr", "pearl")) for name in imports))
        self.assertFalse(calls & {"eval", "exec", "system", "__import__"})
        for forbidden in ("_read_windows_api_key", "task.create", "task.detail", "task.listMessages", "task.stop", "IBKR", "Pearl"):
            self.assertNotIn(forbidden, source)


class ProvenanceIntegrationTests(PaperApplyTestsMixin, unittest.TestCase):
    """5E-6: guarded decisions produce typed, append-only provenance records."""

    def _provenance_records(self):
        from manus import decision_provenance as dp
        return dp.read_decision_records(_provenance_root=self.provenance_root)

    def test_successful_bet_appends_attempt_and_final_trade_records(self):
        from manus import decision_provenance as dp
        intent = self.intent(disposition="bet")
        self.stage(intent=intent)
        with self.public_market(prices=(0.50, 0.52)):
            result = self.call()
        self.assertEqual(result["application_state"], "completed-placement")
        records = self._provenance_records()
        self.assertEqual(len(records), 2)
        attempt = [r for r in records if r["decision_phase"] == "attempt"]
        final = [r for r in records if r["decision_phase"] == "final"]
        self.assertEqual(len(attempt), 1)
        self.assertEqual(len(final), 1)
        self.assertEqual(attempt[0]["decision_action"], "trade")
        self.assertEqual(attempt[0]["decision_stage"], "placement")
        self.assertTrue(attempt[0]["placement_attempted"])
        self.assertEqual(final[0]["placement_result"], "placed")
        # Truthful provenance: the guarded fill payload has no same-snapshot
        # bid, so the midpoint stays null rather than guessing one from the
        # ask; edge is carried verbatim from the guarded payload.
        self.assertIsNone(final[0]["market_probability"])
        self.assertEqual(final[0]["edge"], 0.1)
        self.assertNotEqual(final[0]["decision_id"], attempt[0]["decision_id"])
        self.assertIsNotNone(final[0]["forecast_id"])
        self.assertEqual(final[0]["placement_result"], "placed")
        for record in records:
            self.assertTrue(dp.verify_record_sha256(record))

    def test_no_trade_disposition_appends_single_no_trade_record(self):
        intent = self.intent(disposition="no-edge")
        self.stage(intent=intent)
        with self.public_market(prices=(0.50, 0.52)):
            result = self.call()
        self.assertEqual(result["application_state"], "completed-no-placement")
        records = self._provenance_records()
        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record["decision_action"], "no-trade")
        self.assertEqual(record["decision_stage"], "decision-policy")
        self.assertEqual(record["decision_phase"], "final")
        self.assertFalse(record["placement_attempted"])
        self.assertEqual(record["reason_code"], "no-edge")

    def test_forecast_rejection_appends_rejected_record_with_code(self):
        self.stage()
        with patch.object(
            forecast_core.pmapi,
            "gamma_market",
            side_effect=OSError("gamma down"),
        ):
            with self.assertRaisesRegex(paper_apply.PaperApplyError, "Guarded forecast rejected"):
                self.call()
        records = self._provenance_records()
        self.assertEqual(len(records), 1)
        record = records[0]
        self.assertEqual(record["decision_action"], "rejected")
        self.assertEqual(record["decision_stage"], "forecast")
        self.assertEqual(record["reason_code"], "market-data-unavailable")
        self.assertFalse(record["placement_attempted"])

    def test_placement_rejection_appends_rejected_record_with_trusted_code(self):
        intent = self.intent(disposition="bet")
        self.stage(intent=intent)
        with self.public_market(prices=(0.50, 0.61)):
            self.call()
        records = self._provenance_records()
        self.assertEqual(len(records), 2)
        final = [r for r in records if r["decision_phase"] == "final"][0]
        self.assertEqual(final["decision_action"], "rejected")
        self.assertEqual(final["decision_stage"], "placement")
        self.assertEqual(final["reason_code"], "spread-too-wide")
        self.assertEqual(final["placement_rejection_code"], "spread-too-wide")
        self.assertTrue(final["placement_attempted"])

    def test_provenance_failure_fails_closed_before_placement(self):
        from manus import decision_provenance as dp
        intent = self.intent(disposition="bet")
        self.stage(intent=intent)
        ledger_before = self.ledger_path.read_bytes() if self.ledger_path.exists() else b""
        with self.public_market(prices=(0.50, 0.52)), \
             patch.object(
                 dp, "append_decision_record",
                 side_effect=dp.ProvenanceWriteError("Fixed decision provenance root is unavailable"),
             ):
            with self.assertRaises(paper_apply.PaperApplyError) as caught:
                self.call()
        self.assertEqual(caught.exception.code, "provenance-write-failed")
        self.assertEqual(
            self.ledger_path.read_bytes() if self.ledger_path.exists() else b"",
            ledger_before,
        )
        records = self._provenance_records()
        self.assertEqual(records, [])

    def test_replay_appends_nothing_and_is_read_only(self):
        from manus import decision_provenance as dp
        from manus import shadow_replay as sr
        intent = self.intent(disposition="bet")
        self.stage(intent=intent)
        with self.public_market(prices=(0.50, 0.52)):
            self.call()
        records = self._provenance_records()
        self.assertEqual(len(records), 2)
        # Operational records carry null price fields (no network I/O in the
        # observability path), so the bounded replay status is honest:
        # replay-input-incomplete, never a fabricated replayable claim.
        self.assertEqual(sr.replayability_status(records[1]), "replay-input-incomplete")
        with self.assertRaisesRegex(sr.ShadowReplayError, "replay-input-incomplete"):
            sr.replay_provenance_record(records[1])
        self.assertEqual(len(self._provenance_records()), 2)

    def test_provenance_records_contain_no_secrets_or_paths(self):
        from manus import decision_provenance
        intent = self.intent(disposition="bet")
        self.stage(intent=intent)
        with self.public_market(prices=(0.50, 0.52)):
            self.call()
        path = decision_provenance._provenance_path(self.provenance_root)
        self.assertTrue(path.is_file())
        text = path.read_text(encoding="utf-8")
        self.assertNotIn("credential", text)
        self.assertNotIn(str(self.root), text)
        self.assertNotIn("api_key", text)
        self.assertNotIn("Authorization", text)

    def test_dry_run_never_writes_provenance(self):
        self.stage()
        self.call(dry_run=True)
        self.assertEqual(self._provenance_records(), [])


if __name__ == "__main__":
    unittest.main()


class PaperApplyTests(PaperApplyTestsMixin, unittest.TestCase):
    """Original PAPER application regression suite."""


class RejectionCodeReceiptTests(PaperApplyTestsMixin, unittest.TestCase):
    """Rejected receipts persist one bounded, closed-vocabulary code."""

    def test_rejection_code_vocabulary_is_closed_and_classified(self):
        from manus.paper_apply import REJECTION_CODES, REJECTION_CLASSIFICATIONS
        self.assertLessEqual(ledger_core.PLACEMENT_REJECTION_CODES, REJECTION_CODES)
        self.assertLessEqual(forecast_core.FORECAST_REJECTION_CODES, REJECTION_CODES)
        self.assertEqual(set(REJECTION_CLASSIFICATIONS), REJECTION_CODES)
        for code, classification in REJECTION_CLASSIFICATIONS.items():
            self.assertIn(classification, {"provenance", "policy", "infrastructure", "internal"})

    def test_placement_rejection_receipt_persists_bounded_code(self):
        intent = self.intent(disposition="bet")
        self.stage(intent=intent)
        with self.public_market(prices=(0.50, 0.61)):
            result = self.call()
        self.assertEqual(result["application_state"], "placement-rejected")
        self.assertEqual(result["rejection_code"], "spread-too-wide")
        receipt = self.receipt()
        self.assertEqual(receipt["state"], "placement-rejected")
        self.assertEqual(receipt["rejection_code"], "spread-too-wide")
        # Replay keeps the persisted code and does not re-derive it.
        again = self.call()
        self.assertEqual(again["rejection_code"], "spread-too-wide")

    def test_market_data_failure_receipt_maps_to_infrastructure_code(self):
        intent = self.intent(disposition="bet")
        self.stage(intent=intent)
        # Patching the shared pmapi seam fails the guarded forecast recording
        # before placement: the receipt records the bounded infrastructure code.
        with patch.object(ledger_core.pmapi, "gamma_market", side_effect=OSError("gamma down")):
            with self.assertRaisesRegex(paper_apply.PaperApplyError, "Guarded forecast rejected"):
                self.call()
        self.assertEqual(self.receipt()["state"], "forecast-rejected")
        self.assertEqual(self.receipt()["rejection_code"], "market-data-unavailable")

    def test_forecast_rejection_receipt_persists_bounded_code(self):
        self.stage()
        with patch.object(
            paper_apply, "record_candidate_forecast",
            side_effect=paper_apply.PaperApplyError("rejected", code="extreme-disagreement"),
        ):
            with self.assertRaisesRegex(paper_apply.PaperApplyError, "rejected"):
                self.call()
        receipt = self.receipt()
        self.assertEqual(receipt["state"], "forecast-rejected")
        self.assertEqual(receipt["rejection_code"], "extreme-disagreement")

    def test_unexpected_forecast_failure_persists_unclassified(self):
        self.stage()
        with patch.object(
            paper_apply, "record_candidate_forecast",
            side_effect=paper_apply.PaperApplyError("boom"),
        ):
            with self.assertRaisesRegex(paper_apply.PaperApplyError, "boom"):
                self.call()
        receipt = self.receipt()
        self.assertEqual(receipt["state"], "forecast-rejected")
        self.assertEqual(receipt["rejection_code"], "unclassified")

    def test_successful_applications_have_no_rejection_code(self):
        intent = self.intent(disposition="bet")
        self.stage(intent=intent)
        with self.public_market():
            result = self.call()
        self.assertEqual(result["application_state"], "completed-placement")
        self.assertIsNone(result["rejection_code"])
        self.assertNotIn("rejection_code", self.receipt())

    def test_historical_receipt_without_code_still_replays(self):
        intent = self.intent(disposition="bet")
        self.stage(intent=intent)
        with self.public_market(prices=(0.50, 0.61)):
            self.call()
        receipt = self.receipt()
        del receipt["rejection_code"]
        paper_apply._atomic_write_receipt(self.receipt_path(), receipt)
        result = self.call()
        self.assertEqual(result["application_state"], "placement-rejected")
        self.assertIsNone(result["rejection_code"])

    def test_malformed_receipt_code_fails_closed(self):
        self.stage()
        receipt = paper_apply._new_receipt(self.context(), lambda: NOW)
        receipt["rejection_code"] = "totally-unknown-code"
        paper_apply._atomic_write_receipt(self.receipt_path(), receipt)
        with self.assertRaisesRegex(paper_apply.PaperApplyError, "rejection_code is invalid"):
            self.call()

    def test_error_state_receipt_persists_code(self):
        self.stage()
        receipt = paper_apply._new_receipt(self.context(), lambda: NOW)
        receipt["state"] = "error"
        paper_apply._atomic_write_receipt(self.receipt_path(), receipt)
        with patch.object(paper_apply, "record_candidate_forecast", side_effect=AssertionError("no forecast")):
            with patch.object(
                paper_apply, "record_candidate_paper_placement",
                side_effect=paper_apply.PaperApplyError("x", code="insufficient-cash"),
            ):
                # error state replays as terminal without a new guarded call;
                # this scenario instead covers the transition directly.
                pass
        transitioned = paper_apply._transition_receipt(
            self.receipt_path(), receipt, "error", now=lambda: NOW,
            rejection_code="insufficient-cash",
        )
        self.assertEqual(transitioned["rejection_code"], "insufficient-cash")

    def test_runner_summary_contract_via_terminal_update(self):
        from manus.paper_apply import REJECTION_CODES
        from manus import paper_runner
        # out-of-vocabulary values map to unclassified at the runner boundary
        class _Application(dict):
            pass
        self.assertEqual(
            paper_runner._bounded_rejection_code({"rejection_code": "edge-below-threshold"}),
            "edge-below-threshold",
        )
        self.assertEqual(
            paper_runner._bounded_rejection_code({"rejection_code": "made-up-code"}),
            "unclassified",
        )
        self.assertIsNone(paper_runner._bounded_rejection_code({}))

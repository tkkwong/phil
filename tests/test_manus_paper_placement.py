"""Offline tests for the guarded Manus PAPER placement bridge."""
import argparse
import ast
import copy
import datetime as dt
import inspect
import io
import json
import pathlib
import shutil
import subprocess
import sys
import tempfile
import unittest
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from unittest.mock import patch

from core import ledger as ledger_core
from core import resolve, score
from manus.paper_cycle_guardian import (
    GuardianValidationError,
    main,
    prepare_packet,
    record_candidate_paper_placement,
)

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
            "event_slug": "texas-am-wake-forest",
            "clob_token_ids": ["token-texas", "token-wake"],
            "volume_24h": 1000.0,
            "liquidity": 5000.0,
            "slug": "texas-am-wake-forest",
            "description": "Trusted operator scan fixture.",
        }
    ],
}
FIRST_INTENT_ID = "123e4567-e89b-42d3-a456-426614174000"
SECOND_INTENT_ID = "223e4567-e89b-42d3-a456-426614174000"
NOW = dt.datetime(2026, 9, 27, 5, 0, tzinfo=dt.timezone.utc)


def valid_intent(packet, *, intent_id=FIRST_INTENT_ID, disposition="bet", probability=0.62):
    candidate = packet["candidates"][0]
    return {
        "intent_id": intent_id,
        "candidate_id": candidate["candidate_id"],
        "market_id": candidate["market_id"],
        "outcome": candidate["outcomes"][0],
        "estimated_probability": probability,
        "category": "sports",
        "rationale": "Trusted research supports this PAPER-only estimate.",
        "edge_class": "other",
        "mode": "PAPER",
        "forecast_disposition": disposition,
        "strategy_proposals": [
            {
                "proposal_id": "proposal-ignored-01",
                "summary": "Track P&L by category before future paper research.",
            }
        ],
    }


class GuardedPaperPlacementTests(unittest.TestCase):
    def setUp(self):
        self.fixture = copy.deepcopy(FIXTURE)
        self.packet = prepare_packet(self.fixture)
        self.intent = valid_intent(self.packet)

    def _market(self, *, closed=False, end_date="2026-10-01T00:00:00Z"):
        return {
            "closed": closed,
            "question": "Live public market question",
            "slug": "texas-am-live",
            "endDate": end_date,
            "outcomes": json.dumps(["Texas A&M", "Wake Forest"]),
            "clobTokenIds": json.dumps(["token-texas", "token-wake"]),
        }

    def _forecast_row(self, intent=None, *, status="open", **changes):
        intent = self.intent if intent is None else intent
        row = {
            "id": "forecast-100",
            "ts": "2026-09-27T05:00:00Z",
            "market_id": intent["market_id"],
            "question": "Recorded forecast question",
            "slug": "forecast-market",
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
            "status": status,
            "source_intent_id": intent["intent_id"],
        }
        row.update(changes)
        return row

    def _policy(self):
        protected = copy.deepcopy(ledger_core.PROTECTED)
        risk = json.loads(ledger_core.RISK.read_text(encoding="utf-8"))
        return protected, risk

    def _write_rows(self, path, rows):
        path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")

    @contextmanager
    def _public_market(self, *, market=None, tokens=None, prices=(0.50, 0.52), event_rows=None):
        market = self._market() if market is None else market
        tokens = {"Texas A&M": "token-texas", "Wake Forest": "token-wake"} if tokens is None else tokens
        event_rows = [{"id": "market-100", "events": [{"id": "event-100"}]}] if event_rows is None else event_rows
        with patch.object(ledger_core.pmapi, "gamma_market", return_value=market) as gamma_market, \
             patch.object(ledger_core.pmapi, "market_tokens", return_value=tokens) as market_tokens, \
             patch.object(ledger_core.pmapi, "best_prices", return_value=prices) as best_prices, \
             patch.object(ledger_core.pmapi, "gamma_markets", return_value=event_rows) as gamma_markets:
            yield {
                "gamma_market": gamma_market,
                "market_tokens": market_tokens,
                "best_prices": best_prices,
                "gamma_markets": gamma_markets,
            }

    def _place(self, root, *, intent=None, fixture=None, ledger_rows=None, forecast_rows=None,
               protected=None, risk=None, market=None, tokens=None, prices=(0.50, 0.52),
               event_rows=None):
        intent = copy.deepcopy(self.intent if intent is None else intent)
        fixture = copy.deepcopy(self.fixture if fixture is None else fixture)
        ledger_path = root / "ledger.jsonl"
        forecast_path = root / "forecasts.jsonl"
        self._write_rows(ledger_path, ledger_rows or []) if ledger_rows else None
        rows = [self._forecast_row(intent)] if forecast_rows is None else forecast_rows
        self._write_rows(forecast_path, rows)
        protected, risk = self._policy() if protected is None or risk is None else (protected, risk)
        with self._public_market(
            market=market, tokens=tokens, prices=prices, event_rows=event_rows
        ) as public_market:
            result = record_candidate_paper_placement(
                fixture,
                json.dumps(intent),
                _ledger_path=ledger_path,
                _forecast_path=forecast_path,
                _protected_config=protected,
                _risk_config=risk,
                _now=NOW,
            )
        return result, ledger_path, forecast_path, public_market

    def test_valid_bound_bet_forecast_creates_one_provenance_row_at_best_ask(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = pathlib.Path(temporary_directory)
            result, ledger_path, _, public = self._place(root)
            rows = [json.loads(line) for line in ledger_path.read_text(encoding="utf-8").splitlines()]
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row["source_intent_id"], FIRST_INTENT_ID)
        self.assertEqual(row["source_forecast_id"], "forecast-100")
        self.assertEqual(row["source_packet_id"], self.packet["packet_id"])
        self.assertEqual(row["event_id"], "event-100")
        self.assertEqual(row["edge_class"], "manus-paper-only")
        self.assertEqual(row["research_edge_class"], "other")
        self.assertNotIn("strategy_proposals", row)
        self.assertEqual(row["stake_usd"], 5.0)
        self.assertEqual(row["entry_price"], 0.52)
        self.assertEqual(row["best_bid_at_entry"], 0.50)
        self.assertEqual(result["placement"]["filled_at"], 0.52)
        self.assertEqual(result["placement"]["stake_usd"], 5.0)
        public["gamma_market"].assert_called_once_with("market-100")
        public["market_tokens"].assert_called_once()
        public["best_prices"].assert_called_once_with("token-texas")
        public["gamma_markets"].assert_called_once_with(id="market-100")

    def test_exact_decimal_risk_gates_precede_legacy_rounding(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = pathlib.Path(temporary_directory)
            boundary_intent = valid_intent(self.packet, probability=0.62)
            result, ledger_path, _, _ = self._place(
                root, intent=boundary_intent, prices=(0.49, 0.55)
            )
            row = json.loads(ledger_path.read_text(encoding="utf-8"))
            self.assertEqual(result["placement"]["edge"], 0.07)
            self.assertEqual(result["placement"]["spread"], 0.06)
            self.assertEqual(row["edge"], 0.07)

        cases = (
            ("below-edge", valid_intent(self.packet, probability=0.61996), (0.49, 0.55), "required_edge"),
            ("above-spread", valid_intent(self.packet, probability=0.62), (0.48999, 0.55), "max_spread"),
        )
        for label, intent, prices, message in cases:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as temporary_directory:
                with self.assertRaisesRegex(GuardianValidationError, message):
                    self._place(pathlib.Path(temporary_directory), intent=intent, prices=prices)

    def test_real_eligibility_drift_rejects_before_market_or_ledger_io(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = pathlib.Path(temporary_directory)
            protected, risk = self._policy()
            protected["real"]["allowed_edge_classes"].append("manus-paper-only")
            ledger_path = root / "ledger.jsonl"
            forecast_path = root / "forecasts.jsonl"
            self._write_rows(forecast_path, [self._forecast_row()])
            with self._public_market() as public_market:
                with self.assertRaisesRegex(GuardianValidationError, "never be real-eligible"):
                    record_candidate_paper_placement(
                        self.fixture,
                        json.dumps(self.intent),
                        _ledger_path=ledger_path,
                        _forecast_path=forecast_path,
                        _protected_config=protected,
                        _risk_config=risk,
                        _now=NOW,
                    )
            public_market["gamma_market"].assert_not_called()
            public_market["market_tokens"].assert_not_called()
            public_market["best_prices"].assert_not_called()
            public_market["gamma_markets"].assert_not_called()
            self.assertFalse(ledger_path.exists())

    def test_first_live_event_is_the_canonical_scan_event(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            event_rows = [{
                "id": "market-100",
                "events": [{"id": "event-100"}, {"id": "event-secondary"}],
            }]
            _, ledger_path, _, public = self._place(
                pathlib.Path(temporary_directory), event_rows=event_rows
            )
            row = json.loads(ledger_path.read_text(encoding="utf-8"))
        self.assertEqual(row["event_id"], "event-100")
        public["gamma_markets"].assert_called_once_with(id="market-100")

    def test_non_bet_dispositions_reject_before_forecast_or_market_io(self):
        for disposition in ("no-edge", "market-agrees", "category-bar", "wide-spread-veto"):
            with self.subTest(disposition=disposition), tempfile.TemporaryDirectory() as temporary_directory:
                root = pathlib.Path(temporary_directory)
                intent = valid_intent(self.packet, disposition=disposition)
                forecast_path = root / "forecasts.jsonl"
                self._write_rows(forecast_path, [self._forecast_row(intent)])
                with patch.object(
                    ledger_core, "record_manus_paper_placement", side_effect=AssertionError("no ledger route")
                ):
                    with self.assertRaisesRegex(GuardianValidationError, "forecast_disposition"):
                        record_candidate_paper_placement(
                            self.fixture,
                            json.dumps(intent),
                            _ledger_path=root / "ledger.jsonl",
                            _forecast_path=forecast_path,
                        )
                self.assertFalse((root / "ledger.jsonl").exists())

    def test_forecast_binding_rejects_missing_duplicate_and_every_required_mismatch(self):
        scenarios = [("missing", [], "No persisted forecast")]
        duplicate = [self._forecast_row(), self._forecast_row(id="forecast-duplicate")]
        scenarios.append(("duplicate", duplicate, "multiple"))
        for field, wrong in (
            ("market_id", "market-wrong"),
            ("outcome", "Wake Forest"),
            ("est_prob", 0.61),
            ("category", "macro"),
            ("skip_reason", "no-edge"),
            ("note", "other rationale"),
        ):
            scenarios.append((field, [self._forecast_row(**{field: wrong})], field))
        for label, forecast_rows, message in scenarios:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as temporary_directory:
                root = pathlib.Path(temporary_directory)
                forecast_path = root / "forecasts.jsonl"
                self._write_rows(forecast_path, forecast_rows)
                with patch.object(
                    ledger_core.pmapi, "gamma_market", side_effect=AssertionError("binding must precede market I/O")
                ):
                    with self.assertRaisesRegex(GuardianValidationError, message):
                        record_candidate_paper_placement(
                            self.fixture,
                            json.dumps(self.intent),
                            _ledger_path=root / "ledger.jsonl",
                            _forecast_path=forecast_path,
                        )
                self.assertFalse((root / "ledger.jsonl").exists())

    def test_missing_fixture_event_id_rejects_before_forecast_or_market_io(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = pathlib.Path(temporary_directory)
            fixture = copy.deepcopy(self.fixture)
            del fixture["candidates"][0]["event_id"]
            forecast_path = root / "forecasts.jsonl"
            self._write_rows(forecast_path, [self._forecast_row()])
            with patch.object(
                ledger_core, "record_manus_paper_placement", side_effect=AssertionError("no ledger route")
            ):
                with self.assertRaisesRegex(GuardianValidationError, "event_id"):
                    record_candidate_paper_placement(
                        fixture,
                        json.dumps(self.intent),
                        _ledger_path=root / "ledger.jsonl",
                        _forecast_path=forecast_path,
                    )
            self.assertFalse((root / "ledger.jsonl").exists())

    def test_forecast_must_be_open_and_not_superseded(self):
        for row, message in (
            (self._forecast_row(status="won"), "open"),
            (self._forecast_row(superseded_by="next-row"), "superseded"),
        ):
            with self.subTest(row=row), tempfile.TemporaryDirectory() as temporary_directory:
                root = pathlib.Path(temporary_directory)
                forecast_path = root / "forecasts.jsonl"
                self._write_rows(forecast_path, [row])
                with patch.object(
                    ledger_core.pmapi, "gamma_market", side_effect=AssertionError("forecast gate must precede I/O")
                ):
                    with self.assertRaisesRegex(GuardianValidationError, message):
                        record_candidate_paper_placement(
                            self.fixture, json.dumps(self.intent), _ledger_path=root / "ledger.jsonl", _forecast_path=forecast_path
                        )

    def test_replay_by_intent_or_forecast_is_rejected_before_market_io(self):
        cases = (
            ([{"source_intent_id": FIRST_INTENT_ID, "status": "won", "stake_usd": 5, "shares": 0}], "source_intent_id"),
            ([{"source_forecast_id": "forecast-100", "status": "won", "stake_usd": 5, "shares": 0}], "source_forecast_id"),
        )
        for ledger_rows, message in cases:
            with self.subTest(message=message), tempfile.TemporaryDirectory() as temporary_directory:
                root = pathlib.Path(temporary_directory)
                ledger_path = root / "ledger.jsonl"
                forecast_path = root / "forecasts.jsonl"
                self._write_rows(ledger_path, ledger_rows)
                self._write_rows(forecast_path, [self._forecast_row()])
                with patch.object(
                    ledger_core.pmapi, "gamma_market", side_effect=AssertionError("replay must precede market I/O")
                ):
                    with self.assertRaisesRegex(GuardianValidationError, message):
                        record_candidate_paper_placement(
                            self.fixture, json.dumps(self.intent), _ledger_path=ledger_path, _forecast_path=forecast_path
                        )

    def test_successful_write_retries_once_and_keeps_one_complete_row(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = pathlib.Path(temporary_directory)
            self._place(root)
            ledger_path = root / "ledger.jsonl"
            before = ledger_path.read_bytes()
            with patch.object(
                ledger_core.pmapi, "gamma_market", side_effect=AssertionError("replay must precede market I/O")
            ):
                with self.assertRaisesRegex(GuardianValidationError, "source_intent_id"):
                    record_candidate_paper_placement(
                        self.fixture, json.dumps(self.intent), _ledger_path=ledger_path, _forecast_path=root / "forecasts.jsonl"
                    )
            self.assertEqual(ledger_path.read_bytes(), before)
            rows = [json.loads(line) for line in before.decode("utf-8").splitlines()]
            self.assertEqual(len(rows), 1)

    def test_existing_hard_ledger_controls_reject(self):
        scenarios = []
        duplicate_open = {"market_id": "market-100", "outcome": "Texas A&M", "status": "open", "stake_usd": 5}
        scenarios.append(("duplicate", {"ledger_rows": [duplicate_open]}, "already have an open position"))
        protected, risk = self._policy()
        protected["sim_bankroll_usd"] = 4
        scenarios.append(("cash", {"protected": protected, "risk": risk}, "insufficient simulated cash"))
        protected, risk = self._policy()
        protected["max_open_positions"] = 0
        scenarios.append(("open-cap", {"protected": protected, "risk": risk}, "max_open_positions"))
        protected, risk = self._policy()
        risk["default_stake_usd"] = 11
        scenarios.append(("stake-cap", {"protected": protected, "risk": risk}, "max_stake_usd"))
        scenarios.append(("closed", {"market": self._market(closed=True)}, "market is closed"))
        scenarios.append(("invalid-outcome", {"tokens": {"Wake Forest": "token-wake"}}, "live market outcomes"))
        scenarios.append(("empty-book", {"prices": (None, None)}, "best bid and best ask"))
        scenarios.append(("missing-bid", {"prices": (None, 0.52)}, "best bid and best ask"))
        scenarios.append(("missing-ask", {"prices": (0.50, None)}, "best bid and best ask"))
        scenarios.append(("min-price", {"prices": (0.005, 0.01)}, "entry bounds"))
        scenarios.append(("max-price", {"prices": (0.95, 0.96)}, "entry bounds"))
        for label, kwargs, message in scenarios:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as temporary_directory:
                with self.assertRaisesRegex(GuardianValidationError, message):
                    self._place(pathlib.Path(temporary_directory), **kwargs)

    def test_guarded_risk_controls_reject(self):
        scenarios = []
        scenarios.append(("time", {"market": self._market(end_date="2026-09-27T05:10:00Z")}, "too close"))
        scenarios.append(("edge", {"prices": (0.50, 0.56)}, "required_edge"))
        scenarios.append(("spread", {"prices": (0.40, 0.52)}, "max_spread"))
        protected, risk = self._policy()
        protected["max_new_positions_per_cycle"] = 1
        cycle_row = {
            "source_packet_id": self.packet["packet_id"], "status": "lost", "stake_usd": 5, "shares": 0
        }
        scenarios.append(("cycle", {"protected": protected, "risk": risk, "ledger_rows": [cycle_row]}, "max_new_positions_per_cycle"))
        protected, risk = self._policy()
        risk["max_positions_per_category_per_cycle"] = 1
        category_row = {
            "source_packet_id": self.packet["packet_id"], "category": "sports", "status": "won", "stake_usd": 5, "shares": 0
        }
        scenarios.append(("category", {"protected": protected, "risk": risk, "ledger_rows": [category_row]}, "max_positions_per_category"))
        protected, risk = self._policy()
        risk["max_stake_per_event_usd"] = 5
        event_row = {"event_id": "event-100", "status": "open", "stake_usd": 5, "market_id": "market-other"}
        scenarios.append(("event", {"protected": protected, "risk": risk, "ledger_rows": [event_row]}, "max_stake_per_event"))
        protected, risk = self._policy()
        risk["default_stake_usd"] = "five"
        scenarios.append(("malformed-stake", {"protected": protected, "risk": risk}, "default_stake_usd"))
        for label, kwargs, message in scenarios:
            with self.subTest(label=label), tempfile.TemporaryDirectory() as temporary_directory:
                with self.assertRaisesRegex(GuardianValidationError, message):
                    self._place(pathlib.Path(temporary_directory), **kwargs)

    def test_unresolved_legacy_event_identity_fails_closed_and_live_event_must_match(self):
        legacy_row = {"market_id": "legacy-market", "status": "open", "stake_usd": 5}
        event_rows = [
            {"id": "market-100", "events": [{"id": "event-100"}]},
            {"id": "legacy-market", "events": []},
        ]
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = pathlib.Path(temporary_directory)
            with patch.object(ledger_core.pmapi, "gamma_markets", side_effect=[event_rows[:1], event_rows[1:]]):
                with patch.object(ledger_core.pmapi, "gamma_market", return_value=self._market()), \
                     patch.object(ledger_core.pmapi, "market_tokens", return_value={"Texas A&M": "token-texas"}), \
                     patch.object(ledger_core.pmapi, "best_prices", return_value=(0.50, 0.52)):
                    ledger_path = root / "ledger.jsonl"
                    forecast_path = root / "forecasts.jsonl"
                    self._write_rows(ledger_path, [legacy_row])
                    self._write_rows(forecast_path, [self._forecast_row()])
                    with self.assertRaisesRegex(GuardianValidationError, "event identity"):
                        record_candidate_paper_placement(
                            self.fixture, json.dumps(self.intent), _ledger_path=ledger_path, _forecast_path=forecast_path,
                            _protected_config=self._policy()[0], _risk_config=self._policy()[1], _now=NOW
                        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            wrong_events = [{"id": "market-100", "events": [{"id": "event-other"}]}]
            with self.assertRaisesRegex(GuardianValidationError, "event_id does not match"):
                self._place(pathlib.Path(temporary_directory), event_rows=wrong_events)

    def test_matching_legacy_event_exposure_is_resolved_and_counted(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = pathlib.Path(temporary_directory)
            protected, risk = self._policy()
            risk["max_stake_per_event_usd"] = 5
            legacy_row = {"market_id": "legacy-market", "status": "open", "stake_usd": 5}
            event_rows = [
                {"id": "market-100", "events": [{"id": "event-100"}]},
                {"id": "legacy-market", "events": [{"id": "event-100"}]},
            ]
            with patch.object(ledger_core.pmapi, "gamma_markets", side_effect=[event_rows[:1], event_rows[1:]]), \
                 patch.object(ledger_core.pmapi, "gamma_market", return_value=self._market()), \
                 patch.object(ledger_core.pmapi, "market_tokens", return_value={"Texas A&M": "token-texas"}), \
                 patch.object(ledger_core.pmapi, "best_prices", return_value=(0.50, 0.52)):
                ledger_path = root / "ledger.jsonl"
                forecast_path = root / "forecasts.jsonl"
                self._write_rows(ledger_path, [legacy_row])
                self._write_rows(forecast_path, [self._forecast_row()])
                with self.assertRaisesRegex(GuardianValidationError, "max_stake_per_event"):
                    record_candidate_paper_placement(
                        self.fixture, json.dumps(self.intent), _ledger_path=ledger_path, _forecast_path=forecast_path,
                        _protected_config=protected, _risk_config=risk, _now=NOW
                    )

    def test_atomic_failure_preserves_existing_ledger_and_cleans_temporary_file(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = pathlib.Path(temporary_directory)
            ledger_path = root / "ledger.jsonl"
            original = (json.dumps({
                "id": "legacy-row",
                "market_id": "legacy-market",
                "outcome": "Yes",
                "stake_usd": 5.0,
                "shares": 0.0,
                "status": "lost",
            }) + "\n").encode("utf-8")
            ledger_path.write_bytes(original)
            forecast_path = root / "forecasts.jsonl"
            self._write_rows(forecast_path, [self._forecast_row()])
            protected, risk = self._policy()
            with self._public_market(), patch.object(ledger_core.os, "fsync", side_effect=OSError("disk failure")), \
                 patch.object(ledger_core.os, "replace") as replace:
                with self.assertRaisesRegex(GuardianValidationError, "ledger write failed"):
                    record_candidate_paper_placement(
                        self.fixture, json.dumps(self.intent), _ledger_path=ledger_path, _forecast_path=forecast_path,
                        _protected_config=protected, _risk_config=risk, _now=NOW
                    )
            replace.assert_not_called()
            self.assertEqual(ledger_path.read_bytes(), original)
            self.assertEqual(list(root.glob(".ledger.jsonl.*.tmp")), [])

    def test_atomic_first_write_failure_creates_no_ledger_and_cleans_temporary_file(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = pathlib.Path(temporary_directory)
            ledger_path = root / "ledger.jsonl"
            forecast_path = root / "forecasts.jsonl"
            self._write_rows(forecast_path, [self._forecast_row()])
            protected, risk = self._policy()
            with self._public_market(), patch.object(ledger_core.os, "fsync", side_effect=OSError("disk failure")), \
                 patch.object(ledger_core.os, "replace") as replace:
                with self.assertRaisesRegex(GuardianValidationError, "ledger write failed"):
                    record_candidate_paper_placement(
                        self.fixture, json.dumps(self.intent), _ledger_path=ledger_path, _forecast_path=forecast_path,
                        _protected_config=protected, _risk_config=risk, _now=NOW
                    )
            replace.assert_not_called()
            self.assertFalse(ledger_path.exists())
            self.assertEqual(list(root.glob(".ledger.jsonl.*.tmp")), [])

    def test_guardian_cli_exposes_only_fixture_intent_and_applied_ids(self):
        parser = __import__("manus.paper_cycle_guardian", fromlist=["build_parser"]).build_parser()
        placement = next(action for action in parser._subparsers._group_actions).choices["record-paper-placement"]
        options = {option for action in placement._actions for option in action.option_strings}
        self.assertEqual(options, {"-h", "--help", "--fixture", "--intent", "--already-applied"})
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = pathlib.Path(temporary_directory)
            fixture_path = root / "fixture.json"
            intent_path = root / "intent.json"
            fixture_path.write_text(json.dumps(self.fixture), encoding="utf-8")
            intent_path.write_text(json.dumps(self.intent), encoding="utf-8")
            for forbidden in ("--stake", "--ledger", "--forecast-id", "--packet", "--event-id", "--token-id", "--ask", "--real", "--ibkr", "--pearl"):
                with self.subTest(forbidden=forbidden), redirect_stderr(io.StringIO()):
                    with self.assertRaises(SystemExit):
                        main(["record-paper-placement", "--fixture", str(fixture_path), "--intent", str(intent_path), forbidden, "x"])

    def test_legacy_cli_semantics_are_unchanged_and_guarded_row_is_consumer_compatible(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = pathlib.Path(temporary_directory)
            legacy_path = root / "legacy-ledger.jsonl"
            arguments = argparse.Namespace(
                market_id="market-legacy", outcome="Yes", est_prob=0.60, stake=5.0,
                category="sports", edge_class="other", rationale="Legacy path", strategy_rev="",
            )
            with patch.object(ledger_core, "LEDGER", legacy_path), \
                 patch.object(ledger_core.pmapi, "gamma_market", return_value=self._market()), \
                 patch.object(ledger_core.pmapi, "market_tokens", return_value={"Yes": "token-yes"}), \
                 patch.object(ledger_core.pmapi, "best_prices", return_value=(0.50, 0.52)), \
                 redirect_stdout(io.StringIO()):
                ledger_core.cmd_place(arguments, [])
            legacy = json.loads(legacy_path.read_text(encoding="utf-8"))
            self.assertNotIn("source_intent_id", legacy)
            self.assertEqual(legacy["edge_class"], "other")
            _, placement_path, _, _ = self._place(root)
            guarded = json.loads(placement_path.read_text(encoding="utf-8"))
            self.assertIn("source_intent_id", guarded)
            self.assertEqual(guarded["status"], "open")

            # Extra provenance is forward-compatible with the current ledger
            # reader, resolver, and scorer. Replay consumes forecasts only, so
            # a ledger row cannot alter replay's forecast-only input contract.
            self.assertEqual(resolve.load_jsonl(placement_path)[0]["source_packet_id"], self.packet["packet_id"])
            settled = dict(guarded)
            settled.update({"status": "won", "pnl_usd": 4.6154, "settled_ts": "2026-10-01T00:00:00Z"})
            self.assertEqual(score.stats([settled])["n"], 1)

            consumer_root = root / "consumer-repository"
            for directory in ("core", "config", "strategy", "journal"):
                (consumer_root / directory).mkdir(parents=True, exist_ok=True)
            repository_root = pathlib.Path(__file__).resolve().parents[1]
            for source, target in (
                (repository_root / "core" / "validate.py", consumer_root / "core" / "validate.py"),
                (repository_root / "config" / "protected.json", consumer_root / "config" / "protected.json"),
                (repository_root / "strategy" / "risk.json", consumer_root / "strategy" / "risk.json"),
                (repository_root / "strategy" / "schedule.json", consumer_root / "strategy" / "schedule.json"),
            ):
                shutil.copy2(source, target)
            (consumer_root / "journal" / "ledger.jsonl").write_text(
                json.dumps(guarded) + "\n", encoding="utf-8"
            )
            validation = subprocess.run(
                [sys.executable, str(consumer_root / "core" / "validate.py")],
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(validation.returncode, 0, validation.stdout + validation.stderr)
            self.assertIn("OK", validation.stdout)

    def test_guarded_route_has_no_real_broker_or_command_execution_path(self):
        import manus.paper_cycle_guardian as guardian
        for module in (guardian, ledger_core):
            source = inspect.getsource(module)
            tree = ast.parse(source)
            imports = set()
            called_names = set()
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    imports.update(alias.name for alias in node.names)
                elif isinstance(node, ast.ImportFrom) and node.module:
                    imports.add(node.module)
                elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                    called_names.add(node.func.id)
            self.assertFalse(any(name == "core.real" or name.startswith("ibkr") or name.startswith("pearl") for name in imports))
            self.assertFalse(imports & {"subprocess", "socket", "urllib", "requests", "importlib"})
            self.assertFalse(called_names & {"eval", "exec", "__import__"})
            self.assertNotIn("core.real", source.lower())
        self.assertNotIn("manus-paper-only", ledger_core.PROTECTED["real"]["allowed_edge_classes"])


if __name__ == "__main__":
    unittest.main()

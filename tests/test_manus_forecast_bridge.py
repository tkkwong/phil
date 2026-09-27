"""Isolated tests for the guarded Manus forecast-recording bridge."""

import argparse
import ast
import copy
import hashlib
import inspect
import io
import json
import pathlib
import shutil
import subprocess
import sys
import tempfile
import unittest
from contextlib import ExitStack, contextmanager, redirect_stderr, redirect_stdout
from unittest.mock import patch

from core import forecast as forecast_core
from core import replay, score
from manus.paper_cycle_guardian import (
    GuardianValidationError,
    main,
    prepare_packet,
    record_candidate_forecast,
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
            "description": "Trusted operator scan fixture.",
        }
    ],
}

FIRST_INTENT_ID = "123e4567-e89b-42d3-a456-426614174000"
SECOND_INTENT_ID = "223e4567-e89b-42d3-a456-426614174000"


def valid_intent(packet, *, intent_id=FIRST_INTENT_ID, probability=0.62, disposition="no-edge"):
    candidate = packet["candidates"][0]
    return {
        "intent_id": intent_id,
        "candidate_id": candidate["candidate_id"],
        "market_id": candidate["market_id"],
        "outcome": candidate["outcomes"][0],
        "estimated_probability": probability,
        "category": "sports",
        "rationale": "A data-only forecast rationale; no action follows.",
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


def snapshot_tree(root):
    return {
        path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in root.rglob("*")
        if path.is_file()
    }


class GuardedForecastBridgeTests(unittest.TestCase):
    def setUp(self):
        self.fixture = copy.deepcopy(FIXTURE)
        self.packet = prepare_packet(self.fixture)
        self.intent = valid_intent(self.packet)

    def _market(self):
        return {
            "closed": False,
            "question": "Live public market question",
            "slug": "texas-am-live",
            "endDate": "2026-10-01T00:00:00Z",
            "outcomes": json.dumps(["Texas A&M", "Wake Forest"]),
            "clobTokenIds": json.dumps(["token-texas", "token-wake"]),
            "liquidityNum": "1234.5",
            "volume24hr": "4321.0",
        }

    @contextmanager
    def _public_market(self, *, market=None, tokens=None, prices=(0.49, 0.51)):
        market = self._market() if market is None else market
        tokens = {"Texas A&M": "token-texas", "Wake Forest": "token-wake"} if tokens is None else tokens
        with patch.object(forecast_core.pmapi, "gamma_market", return_value=market) as gamma_market, \
             patch.object(forecast_core.pmapi, "market_tokens", return_value=tokens) as market_tokens, \
             patch.object(forecast_core.pmapi, "best_prices", return_value=prices) as best_prices:
            yield {
                "gamma_market": gamma_market,
                "market_tokens": market_tokens,
                "best_prices": best_prices,
            }

    def _read_rows(self, forecast_path):
        if not forecast_path.exists():
            return []
        return [json.loads(line) for line in forecast_path.read_text(encoding="utf-8").splitlines() if line]

    def _existing_forecast_row(self):
        return {
            "id": "existing-row",
            "market_id": "market-existing",
            "outcome": "Yes",
            "status": "open",
        }

    def test_valid_fixture_bound_intent_records_one_forecast_and_only_forecast_file(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = pathlib.Path(temporary_directory)
            forecast_path = root / "forecasts.jsonl"
            before = snapshot_tree(root)
            with self._public_market() as public_market:
                result = record_candidate_forecast(
                    self.fixture, json.dumps(self.intent), _forecast_path=forecast_path
                )

            after = snapshot_tree(root)
            self.assertEqual(before, {})
            self.assertEqual(set(after), {"forecasts.jsonl"})
            rows = self._read_rows(forecast_path)
            self.assertEqual(len(rows), 1)
            row = rows[0]
            candidate = self.packet["candidates"][0]
            self.assertEqual(row["market_id"], candidate["market_id"])
            self.assertEqual(row["outcome"], candidate["outcomes"][0])
            self.assertEqual(row["est_prob"], self.intent["estimated_probability"])
            self.assertEqual(row["category"], self.intent["category"])
            self.assertEqual(row["skip_reason"], self.intent["forecast_disposition"])
            self.assertEqual(row["note"], self.intent["rationale"])
            self.assertEqual(row["source_intent_id"], self.intent["intent_id"])
            self.assertNotIn(self.intent["strategy_proposals"][0]["summary"], json.dumps(row))
            self.assertEqual(result["source_intent_id"], self.intent["intent_id"])
            public_market["gamma_market"].assert_called_once_with(candidate["market_id"])
            public_market["market_tokens"].assert_called_once()
            public_market["best_prices"].assert_called_once_with("token-texas")

    def test_bet_disposition_records_only_a_forecast_and_never_touches_ledger(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = pathlib.Path(temporary_directory)
            forecast_path = root / "forecasts.jsonl"
            ledger_path = root / "ledger.jsonl"
            ledger_path.write_text('{"ledger":"unchanged"}\n', encoding="utf-8")
            before = snapshot_tree(root)
            intent = valid_intent(self.packet, disposition="bet")
            with self._public_market():
                record_candidate_forecast(self.fixture, json.dumps(intent), _forecast_path=forecast_path)
            after = snapshot_tree(root)
            self.assertEqual(before["ledger.jsonl"], after["ledger.jsonl"])
            self.assertEqual(set(after), {"forecasts.jsonl", "ledger.jsonl"})
            self.assertEqual(self._read_rows(forecast_path)[0]["skip_reason"], "bet")

    def test_failed_forecast_write_never_touches_a_ledger_file(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = pathlib.Path(temporary_directory)
            forecast_path = root / "forecasts.jsonl"
            ledger_path = root / "ledger.jsonl"
            ledger_path.write_text('{"ledger":"unchanged"}\n', encoding="utf-8")
            before = snapshot_tree(root)
            with self._public_market(), patch.object(
                forecast_core, "_write_record", side_effect=OSError("simulated disk failure")
            ):
                with self.assertRaisesRegex(GuardianValidationError, "Forecast write failed"):
                    record_candidate_forecast(
                        self.fixture, json.dumps(self.intent), _forecast_path=forecast_path
                    )
            self.assertEqual(snapshot_tree(root), before)

    def test_atomic_failure_preserves_existing_forecast_book_and_cleans_temp_file(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = pathlib.Path(temporary_directory)
            forecast_path = root / "forecasts.jsonl"
            existing_bytes = (json.dumps(self._existing_forecast_row()) + "\n").encode("utf-8")
            forecast_path.write_bytes(existing_bytes)

            with self._public_market(), patch.object(
                forecast_core.os, "fsync", side_effect=OSError("simulated fsync failure")
            ), patch.object(forecast_core.os, "replace") as replace:
                with self.assertRaisesRegex(GuardianValidationError, "Forecast write failed"):
                    record_candidate_forecast(
                        self.fixture, json.dumps(self.intent), _forecast_path=forecast_path
                    )

            replace.assert_not_called()
            self.assertEqual(forecast_path.read_bytes(), existing_bytes)
            self.assertEqual(list(root.glob(".forecasts.jsonl.*.tmp")), [])

    def test_atomic_failure_does_not_create_missing_forecast_book_or_leave_temp_file(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = pathlib.Path(temporary_directory)
            forecast_path = root / "forecasts.jsonl"

            with self._public_market(), patch.object(
                forecast_core.os, "fsync", side_effect=OSError("simulated fsync failure")
            ), patch.object(forecast_core.os, "replace") as replace:
                with self.assertRaisesRegex(GuardianValidationError, "Forecast write failed"):
                    record_candidate_forecast(
                        self.fixture, json.dumps(self.intent), _forecast_path=forecast_path
                    )

            replace.assert_not_called()
            self.assertFalse(forecast_path.exists())
            self.assertEqual(list(root.glob(".forecasts.jsonl.*.tmp")), [])

    def test_atomic_success_preserves_existing_bytes_and_appends_one_complete_jsonl_row(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = pathlib.Path(temporary_directory)
            forecast_path = root / "forecasts.jsonl"
            existing_bytes = json.dumps(self._existing_forecast_row()).encode("utf-8")
            forecast_path.write_bytes(existing_bytes)

            with self._public_market():
                record_candidate_forecast(
                    self.fixture, json.dumps(self.intent), _forecast_path=forecast_path
                )

            written = forecast_path.read_bytes()
            self.assertTrue(written.startswith(existing_bytes))
            self.assertEqual(written[len(existing_bytes):len(existing_bytes) + 1], b"\n")
            rows = self._read_rows(forecast_path)
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[1]["source_intent_id"], self.intent["intent_id"])
            self.assertEqual(rows[1]["market_id"], self.packet["candidates"][0]["market_id"])
            self.assertTrue(written.endswith(b"\n"))

    def test_replay_is_rejected_even_after_the_existing_forecast_is_settled(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            forecast_path = pathlib.Path(temporary_directory) / "forecasts.jsonl"
            with self._public_market():
                record_candidate_forecast(self.fixture, json.dumps(self.intent), _forecast_path=forecast_path)
            rows = self._read_rows(forecast_path)
            rows[0]["status"] = "won"
            rows[0]["settled_ts"] = "2026-10-02T00:00:00Z"
            forecast_path.write_text(json.dumps(rows[0]) + "\n", encoding="utf-8")
            before = forecast_path.read_bytes()

            with patch.object(
                forecast_core.pmapi, "gamma_market", side_effect=AssertionError("replay must not read market")
            ):
                with self.assertRaisesRegex(GuardianValidationError, "source_intent_id has already"):
                    record_candidate_forecast(self.fixture, json.dumps(self.intent), _forecast_path=forecast_path)
            self.assertEqual(forecast_path.read_bytes(), before)

    def test_different_intent_on_an_open_market_still_hits_normal_duplicate_guard(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            forecast_path = pathlib.Path(temporary_directory) / "forecasts.jsonl"
            with self._public_market():
                record_candidate_forecast(self.fixture, json.dumps(self.intent), _forecast_path=forecast_path)
            second = valid_intent(self.packet, intent_id=SECOND_INTENT_ID)
            before = forecast_path.read_bytes()
            with patch.object(
                forecast_core.pmapi, "gamma_market", side_effect=AssertionError("duplicate must precede market I/O")
            ):
                with self.assertRaisesRegex(GuardianValidationError, "already have an open forecast"):
                    record_candidate_forecast(self.fixture, json.dumps(second), _forecast_path=forecast_path)
            self.assertEqual(forecast_path.read_bytes(), before)

    def test_fixture_and_intent_rejections_happen_before_forecast_mutation(self):
        invalid_cases = []

        unknown_candidate = copy.deepcopy(self.intent)
        unknown_candidate["candidate_id"] = "cand-unknown"
        invalid_cases.append((json.dumps(unknown_candidate), "candidate_id"))

        wrong_market = copy.deepcopy(self.intent)
        wrong_market["market_id"] = "market-other"
        invalid_cases.append((json.dumps(wrong_market), "market_id"))

        wrong_outcome = copy.deepcopy(self.intent)
        wrong_outcome["outcome"] = "Wrong"
        invalid_cases.append((json.dumps(wrong_outcome), "outcome"))

        non_paper = copy.deepcopy(self.intent)
        non_paper["mode"] = "LIVE"
        invalid_cases.append((json.dumps(non_paper), "mode must be PAPER"))

        invalid_probability = copy.deepcopy(self.intent)
        invalid_probability["estimated_probability"] = 1
        invalid_cases.append((json.dumps(invalid_probability), "estimated_probability"))

        invalid_cases.append(("{malformed", "Malformed JSON"))

        for document, message in invalid_cases:
            with self.subTest(message=message), tempfile.TemporaryDirectory() as temporary_directory:
                root = pathlib.Path(temporary_directory)
                forecast_path = root / "forecasts.jsonl"
                before = snapshot_tree(root)
                with patch.object(
                    forecast_core, "record_forecast", side_effect=AssertionError("must not record")
                ):
                    with self.assertRaisesRegex(GuardianValidationError, message):
                        record_candidate_forecast(self.fixture, document, _forecast_path=forecast_path)
                self.assertEqual(snapshot_tree(root), before)

    def test_extreme_lookup_and_price_rejections_leave_no_forecast_file(self):
        cases = (
            (valid_intent(self.packet, probability=0.99), self._market(), None, (0.49, 0.51), "differs"),
            (self.intent, RuntimeError("Gamma unavailable"), None, (0.49, 0.51), "market-data lookup failed"),
            (self.intent, self._market(), {"Wake Forest": "token-wake"}, (0.49, 0.51), "outcome"),
            (self.intent, self._market(), None, (None, None), "empty book"),
        )
        for intent, market, tokens, prices, message in cases:
            with self.subTest(message=message), tempfile.TemporaryDirectory() as temporary_directory:
                root = pathlib.Path(temporary_directory)
                forecast_path = root / "forecasts.jsonl"
                if isinstance(market, Exception):
                    stack = ExitStack()
                    stack.enter_context(
                        patch.object(forecast_core.pmapi, "gamma_market", side_effect=market)
                    )
                else:
                    stack = self._public_market(market=market, tokens=tokens, prices=prices)
                with stack:
                    with self.assertRaisesRegex(GuardianValidationError, message):
                        record_candidate_forecast(self.fixture, json.dumps(intent), _forecast_path=forecast_path)
                self.assertFalse(forecast_path.exists())

    def test_legacy_forecast_recording_keeps_source_intent_id_optional(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            forecast_path = pathlib.Path(temporary_directory) / "forecasts.jsonl"
            with self._public_market():
                result = forecast_core.record_forecast(
                    market_id="market-100",
                    outcome="Texas A&M",
                    est_prob=0.62,
                    category="sports",
                    skip_reason="no-edge",
                    note="Legacy CLI-equivalent forecast.",
                    forecast_path=forecast_path,
                )
            self.assertNotIn("source_intent_id", result)
            self.assertNotIn("source_intent_id", self._read_rows(forecast_path)[0])

        args = argparse.Namespace(
            market_id="market-100",
            outcome="Texas A&M",
            est_prob=0.62,
            category="sports",
            skip_reason="no-edge",
            fit_score=None,
            note="legacy",
            strategy_rev="",
            confirm_extreme=False,
            supersede=False,
        )
        stdout = io.StringIO()
        with patch.object(forecast_core, "record_forecast", return_value={"recorded": "legacy-row"}) as record:
            with redirect_stdout(stdout):
                forecast_core.cmd_record(args, [])
        self.assertNotIn("source_intent_id", record.call_args.kwargs)
        self.assertEqual(json.loads(stdout.getvalue())["recorded"], "legacy-row")

    def test_protected_forecast_function_rejects_noncanonical_provenance_before_market_read(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            forecast_path = pathlib.Path(temporary_directory) / "forecasts.jsonl"
            with patch.object(
                forecast_core.pmapi, "gamma_market", side_effect=AssertionError("must not read market")
            ):
                with self.assertRaisesRegex(forecast_core.ForecastRecordError, "canonical UUIDv4"):
                    forecast_core.record_forecast(
                        market_id="market-100",
                        outcome="Texas A&M",
                        est_prob=0.62,
                        category="sports",
                        skip_reason="no-edge",
                        source_intent_id="not-a-uuid",
                        forecast_path=forecast_path,
                    )
            self.assertFalse(forecast_path.exists())

    def test_guardian_cli_exposes_no_forecast_controls_or_mutation_paths(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = pathlib.Path(temporary_directory)
            fixture_path = root / "fixture.json"
            intent_path = root / "intent.json"
            fixture_path.write_text(json.dumps(self.fixture), encoding="utf-8")
            intent_path.write_text(json.dumps(self.intent), encoding="utf-8")
            for forbidden in (
                "--packet",
                "--output",
                "--supersede",
                "--confirm-extreme",
                "--ledger",
                "--journal",
                "--real",
                "--ibkr",
                "--pearl",
            ):
                with self.subTest(forbidden=forbidden), redirect_stderr(io.StringIO()):
                    with self.assertRaises(SystemExit):
                        main(
                            [
                                "record-forecast",
                                "--fixture",
                                str(fixture_path),
                                "--intent",
                                str(intent_path),
                                forbidden,
                                "ignored",
                            ]
                        )

    def test_old_and_new_forecast_rows_are_accepted_by_validate_score_and_replay_consumers(self):
        old_row = {
            "id": "old-forecast-1",
            "ts": "2026-09-27T05:00:00Z",
            "market_id": "market-old",
            "question": "Old forecast question",
            "slug": "old-forecast",
            "end_date": "2026-10-01T00:00:00Z",
            "outcome": "Yes",
            "token_id": "token-old",
            "est_prob": 0.6,
            "best_bid_at_record": 0.49,
            "best_ask_at_record": 0.51,
            "market_prob_at_record": 0.5,
            "category": "sports",
            "skip_reason": "no-edge",
            "fit_score": None,
            "note": "Old row without provenance.",
            "strategy_rev": "",
            "status": "won",
            "settled_ts": "2026-10-02T00:00:00Z",
            "outcome_won": "Yes",
        }
        new_row = dict(old_row)
        new_row["id"] = "new-forecast-1"
        new_row["market_id"] = "market-new"
        new_row["source_intent_id"] = FIRST_INTENT_ID

        report = score.forecast_report([old_row, new_row])
        self.assertEqual(report["overall"]["n"], 2)
        self.assertNotIn("status", replay.visible(old_row))
        self.assertNotIn("settled_ts", replay.visible(old_row))
        self.assertEqual(replay.visible(new_row)["source_intent_id"], FIRST_INTENT_ID)

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = pathlib.Path(temporary_directory)
            (root / "core").mkdir()
            (root / "config").mkdir()
            (root / "strategy").mkdir()
            (root / "journal").mkdir()
            repository_root = pathlib.Path(__file__).resolve().parents[1]
            shutil.copy2(repository_root / "core" / "validate.py", root / "core" / "validate.py")
            shutil.copy2(repository_root / "config" / "protected.json", root / "config" / "protected.json")
            shutil.copy2(repository_root / "strategy" / "risk.json", root / "strategy" / "risk.json")
            shutil.copy2(repository_root / "strategy" / "schedule.json", root / "strategy" / "schedule.json")
            (root / "journal" / "ledger.jsonl").write_text("", encoding="utf-8")
            (root / "journal" / "forecasts.jsonl").write_text(
                "\n".join(json.dumps(row) for row in (old_row, new_row)) + "\n",
                encoding="utf-8",
            )
            validation = subprocess.run(
                [sys.executable, str(root / "core" / "validate.py")],
                check=False,
                capture_output=True,
                text=True,
            )
        self.assertEqual(validation.returncode, 0, validation.stdout + validation.stderr)
        self.assertIn("OK", validation.stdout)

    def test_recording_modules_cannot_reach_ledger_real_or_broker_routes(self):
        import manus.paper_cycle_guardian as guardian

        for module in (guardian, forecast_core):
            tree = ast.parse(inspect.getsource(module))
            imported_modules = set()
            called_names = set()
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    imported_modules.update(alias.name for alias in node.names)
                elif isinstance(node, ast.ImportFrom) and node.module:
                    imported_modules.add(node.module)
                elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                    called_names.add(node.func.id)
            self.assertFalse(
                any(
                    module_name == "core.ledger"
                    or module_name == "core.real"
                    or module_name.startswith("ibkr")
                    or module_name.startswith("pearl")
                    for module_name in imported_modules
                )
            )
            self.assertFalse(called_names & {"eval", "exec", "__import__"})


if __name__ == "__main__":
    unittest.main()

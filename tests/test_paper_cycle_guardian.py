"""Deterministic offline tests for the data-only PAPER cycle guardian."""

import ast
import copy
import inspect
import io
import json
import pathlib
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest.mock import patch

from manus.paper_cycle_guardian import (
    GuardianValidationError,
    main,
    prepare_packet,
    validate_candidate_intent,
)


FIXTURE = {
    "generated_at": "2026-09-26T15:00:00Z",
    "candidates": [
        {
            "market_id": "market-100",
            "question": "Will Texas A&M win?",
            "end_date": "2026-10-01T00:00:00Z",
            "outcomes": ["Texas A&M", "Wake Forest"],
            "outcome_prices": [0.55, 0.45],
            "event_id": "event-9",
            "event_slug": "texas-am-wake-forest",
            "clob_token_ids": ["token-a", "token-b"],
            "volume_24h": 1000.0,
            "liquidity": 5000.0,
            "slug": "texas-am-wake-forest",
            "description": "Resolves from the final result.",
        },
        {
            "market_id": "market-200",
            "question": "Will Miami (OH) win?",
            "end_date": "2026-10-02T00:00:00Z",
            "outcomes": ["Miami (OH)", "100 Thieves"],
            "outcome_prices": [0.4, 0.6],
        },
    ],
}

REALISTIC_SCAN_FIXTURE = {
    "generated_at": "2026-09-26T15:00:00Z",
    "candidates": [
        {
            "market_id": "market-realistic-1",
            "question": "Will Example's proposal pass? (2026)",
            "end_date": "2026-09-27T03:59:59.999Z",
            "outcomes": ["Yes", "No"],
            "outcome_prices": [0.52, 0.48],
            "event_id": "event-realistic-1",
            "event_slug": "example-proposal",
            "clob_token_ids": ["token-yes", "token-no"],
            "volume_24h": 1234.5,
            "liquidity": 9876.5,
            "slug": "example-proposal",
            "description": (
                "Resolution terms follow the official record.\r\n"
                "\r\n"
                "See https://example.test/rules?source=market&v=2.\t"
                "Punctuation: (Yes/No), 50%—quoted."
            ),
        }
    ],
}


def valid_intent(packet, candidate_index=0):
    candidate = packet["candidates"][candidate_index]
    return {
        "intent_id": "123e4567-e89b-42d3-a456-426614174000",
        "candidate_id": candidate["candidate_id"],
        "market_id": candidate["market_id"],
        "outcome": candidate["outcomes"][0],
        "estimated_probability": 0.62,
        "category": "sports",
        "rationale": "Research supports a PAPER-only estimate; no action follows.",
        "edge_class": "other",
        "mode": "PAPER",
        "forecast_disposition": "no-edge",
        "strategy_proposals": [
            {
                "proposal_id": "proposal-edge-01",
                "summary": "Track P&L by category before future paper research.",
            }
        ],
    }


class PaperCycleGuardianTests(unittest.TestCase):
    def setUp(self):
        self.fixture = copy.deepcopy(FIXTURE)
        self.packet = prepare_packet(self.fixture)

    def test_prepare_is_paper_only_and_deterministic(self):
        second = prepare_packet(copy.deepcopy(FIXTURE))
        reordered = copy.deepcopy(FIXTURE)
        reordered["candidates"].reverse()
        self.assertEqual(self.packet, second)
        self.assertEqual(self.packet, prepare_packet(reordered))
        self.assertEqual(self.packet["packet_version"], "paper-candidate-packet/v1")
        self.assertEqual(self.packet["mode"], "PAPER")
        self.assertEqual(self.packet["generated_at"], FIXTURE["generated_at"])
        self.assertTrue(self.packet["packet_id"].startswith("packet-"))
        self.assertEqual(
            [candidate["market_id"] for candidate in self.packet["candidates"]],
            ["market-100", "market-200"],
        )

    def test_prepare_accepts_and_normalizes_supported_utc_timestamps(self):
        cases = (
            ("2026-09-27T03:59:59Z", "2026-09-27T03:59:59Z"),
            ("2026-09-27T03:59:59.999Z", "2026-09-27T03:59:59.999Z"),
            ("2026-09-27T03:59:59+00:00", "2026-09-27T03:59:59Z"),
        )
        for source_timestamp, expected_timestamp in cases:
            with self.subTest(source_timestamp=source_timestamp):
                fixture = copy.deepcopy(FIXTURE)
                fixture["generated_at"] = source_timestamp
                fixture["candidates"][0]["end_date"] = source_timestamp
                packet = prepare_packet(fixture)
                self.assertEqual(packet["generated_at"], expected_timestamp)
                self.assertEqual(packet["candidates"][0]["end_date"], expected_timestamp)

    def test_equivalent_utc_timestamps_normalize_identically(self):
        canonical = copy.deepcopy(FIXTURE)
        canonical["generated_at"] = "2026-09-27T03:59:59Z"
        canonical["candidates"][0]["end_date"] = "2026-09-27T03:59:59.999Z"

        equivalent = copy.deepcopy(canonical)
        equivalent["generated_at"] = "2026-09-27T04:59:59+01:00"
        equivalent["candidates"][0]["end_date"] = "2026-09-27T04:59:59.999+01:00"

        self.assertEqual(prepare_packet(canonical), prepare_packet(equivalent))

    def test_prepare_rejects_malformed_and_timezone_naive_timestamps(self):
        for timestamp, error in (
            ("2026-09-27 03:59:59Z", "ISO-8601"),
            ("2026-09-27T03:59:59", "explicit UTC offset"),
        ):
            for field in ("generated_at", "end_date"):
                with self.subTest(timestamp=timestamp, field=field):
                    fixture = copy.deepcopy(FIXTURE)
                    if field == "generated_at":
                        fixture[field] = timestamp
                    else:
                        fixture["candidates"][0][field] = timestamp
                    with self.assertRaisesRegex(GuardianValidationError, error):
                        prepare_packet(fixture)

    def test_prepare_candidate_ids_are_deterministic_not_authentication(self):
        changed = copy.deepcopy(FIXTURE)
        changed["candidates"][0]["question"] = "Will Texas A&M win the opener?"
        changed_packet = prepare_packet(changed)
        self.assertNotEqual(
            self.packet["candidates"][0]["candidate_id"],
            changed_packet["candidates"][0]["candidate_id"],
        )
        self.assertNotEqual(self.packet["packet_id"], changed_packet["packet_id"])
        self.assertTrue(all(item["candidate_id"].startswith("cand-") for item in self.packet["candidates"]))

    def test_prepare_emits_only_permitted_candidate_fields(self):
        allowed = {
            "candidate_id",
            "market_id",
            "question",
            "outcomes",
            "outcome_prices",
            "end_date",
            "description",
        }
        for candidate in self.packet["candidates"]:
            self.assertLessEqual(set(candidate), allowed)
            self.assertNotIn("clob_token_ids", candidate)
            self.assertNotIn("event_id", candidate)
            self.assertNotIn("volume_24h", candidate)
            self.assertNotIn("liquidity", candidate)

    def test_prepare_accepts_realistic_multiline_scan_description(self):
        packet = prepare_packet(copy.deepcopy(REALISTIC_SCAN_FIXTURE))
        description = packet["candidates"][0]["description"]
        self.assertEqual(
            description,
            "Resolution terms follow the official record.\n\n"
            "See https://example.test/rules?source=market&v=2. "
            "Punctuation: (Yes/No), 50%—quoted.",
        )
        self.assertIn("https://example.test/", description)
        self.assertEqual(packet["mode"], "PAPER")

    def test_market_text_is_quoted_data_not_interpreted_as_instructions(self):
        fixture = copy.deepcopy(FIXTURE)
        fixture["candidates"][0]["question"] = "Can bash, git, or C:\\tmp appear in a quoted question?"
        fixture["candidates"][0]["outcomes"] = ["Yes; quoted", "No | quoted"]
        fixture["candidates"][0]["description"] = "See ../terms and https://example.test/path?x=1&y=2."
        packet = prepare_packet(fixture)
        candidate = packet["candidates"][0]
        self.assertIn("bash", candidate["question"])
        self.assertEqual(candidate["outcomes"], ["Yes; quoted", "No | quoted"])
        self.assertIn("../terms", candidate["description"])

    def test_prepare_rejects_inappropriate_controls_malformed_source_and_unknown_fields(self):
        controls = copy.deepcopy(FIXTURE)
        controls["candidates"][0]["description"] = "not\x00allowed"
        with self.assertRaisesRegex(GuardianValidationError, "control character"):
            prepare_packet(controls)

        malformed = copy.deepcopy(FIXTURE)
        malformed["candidates"][0]["outcome_prices"] = [0.55]
        with self.assertRaisesRegex(GuardianValidationError, "align"):
            prepare_packet(malformed)

        unknown = copy.deepcopy(FIXTURE)
        unknown["candidates"][0]["extra"] = "not allowed"
        with self.assertRaisesRegex(GuardianValidationError, "Unknown field"):
            prepare_packet(unknown)

    def test_prepare_rejects_duplicate_market(self):
        duplicate = copy.deepcopy(FIXTURE)
        duplicate["candidates"].append(copy.deepcopy(duplicate["candidates"][0]))
        with self.assertRaisesRegex(GuardianValidationError, "Duplicate market_id"):
            prepare_packet(duplicate)

    def test_matching_intent_against_original_fixture_succeeds_with_data_only_result(self):
        intent = valid_intent(self.packet)
        result = validate_candidate_intent(self.fixture, json.dumps(intent))
        self.assertEqual(result["validation_version"], "paper-guardian-validation/v1")
        self.assertEqual(result["packet_id"], self.packet["packet_id"])
        self.assertEqual(result["mode"], "PAPER")
        self.assertEqual(result["intent"], intent)

    def test_changed_trusted_candidate_data_rejects_old_candidate_id(self):
        intent = valid_intent(self.packet)
        changed_fixture = copy.deepcopy(self.fixture)
        changed_fixture["candidates"][0]["question"] = "Will Texas A&M win the opener?"
        with self.assertRaisesRegex(GuardianValidationError, "candidate_id"):
            validate_candidate_intent(changed_fixture, json.dumps(intent))

    def test_unknown_candidate_id_is_rejected(self):
        intent = valid_intent(self.packet)
        intent["candidate_id"] = "cand-unknown"
        with self.assertRaisesRegex(GuardianValidationError, "candidate_id"):
            validate_candidate_intent(self.fixture, json.dumps(intent))

    def test_wrong_market_id_is_rejected(self):
        intent = valid_intent(self.packet)
        intent["market_id"] = "market-other"
        with self.assertRaisesRegex(GuardianValidationError, "market_id"):
            validate_candidate_intent(self.fixture, json.dumps(intent))

    def test_wrong_or_non_exact_outcome_is_rejected(self):
        for outcome in ("No", "texas a&m", "Texas A&M "):
            with self.subTest(outcome=outcome):
                intent = valid_intent(self.packet)
                intent["outcome"] = outcome
                with self.assertRaisesRegex(GuardianValidationError, "outcome"):
                    validate_candidate_intent(self.fixture, json.dumps(intent))

    def test_non_paper_intent_is_rejected(self):
        intent = valid_intent(self.packet)
        intent["mode"] = "LIVE"
        with self.assertRaisesRegex(GuardianValidationError, "mode must be PAPER"):
            validate_candidate_intent(self.fixture, json.dumps(intent))

    def test_duplicate_intent_id_is_rejected(self):
        intent = valid_intent(self.packet)
        with self.assertRaisesRegex(GuardianValidationError, "Duplicate intent_id"):
            validate_candidate_intent(self.fixture, json.dumps(intent), [intent["intent_id"]])

    def test_rationale_and_strategy_proposals_are_inert_data(self):
        intent = valid_intent(self.packet)
        intent["rationale"] = "touch guardian-should-not-write"
        intent["strategy_proposals"][0]["summary"] = "Compare S&P 500 pricing before paper bets."
        result = validate_candidate_intent(self.fixture, json.dumps(intent))
        self.assertEqual(result["intent"]["rationale"], intent["rationale"])
        self.assertEqual(result["intent"]["strategy_proposals"], intent["strategy_proposals"])

    def test_existing_intent_validator_rejects_paths_and_commands(self):
        path_intent = valid_intent(self.packet)
        path_intent["market_id"] = "/tmp/market"
        with self.assertRaisesRegex(GuardianValidationError, "Absolute path"):
            validate_candidate_intent(self.fixture, json.dumps(path_intent))

        command_intent = valid_intent(self.packet)
        command_intent["shell_command"] = "rm -rf /"
        with self.assertRaisesRegex(GuardianValidationError, "Forbidden field"):
            validate_candidate_intent(self.fixture, json.dumps(command_intent))

    def test_cli_is_stdout_only_and_performs_no_filesystem_writes(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = pathlib.Path(temporary_directory)
            fixture_path = root / "fixture.json"
            intent_path = root / "intent.json"
            fixture_path.write_text(json.dumps(FIXTURE), encoding="utf-8")
            intent_path.write_text(json.dumps(valid_intent(self.packet)), encoding="utf-8")
            before = set(root.iterdir())

            prepare_stdout = io.StringIO()
            with patch("manus.paper_cycle_guardian.pathlib.Path.write_text", side_effect=AssertionError):
                with redirect_stdout(prepare_stdout):
                    self.assertEqual(main(["prepare", "--fixture", str(fixture_path)]), 0)
            self.assertEqual(json.loads(prepare_stdout.getvalue()), self.packet)
            self.assertEqual(set(root.iterdir()), before)

            validate_stdout = io.StringIO()
            with patch("manus.paper_cycle_guardian.pathlib.Path.write_text", side_effect=AssertionError):
                with redirect_stdout(validate_stdout):
                    self.assertEqual(
                        main(["validate-intent", "--fixture", str(fixture_path), "--intent", str(intent_path)]),
                        0,
                    )
            self.assertEqual(json.loads(validate_stdout.getvalue())["mode"], "PAPER")
            self.assertEqual(set(root.iterdir()), before)

    def test_cli_does_not_accept_external_packet_or_output_paths(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = pathlib.Path(temporary_directory)
            fixture_path = root / "fixture.json"
            intent_path = root / "intent.json"
            forged_packet_path = root / "forged-packet.json"
            fixture_path.write_text(json.dumps(FIXTURE), encoding="utf-8")
            intent_path.write_text(json.dumps(valid_intent(self.packet)), encoding="utf-8")
            forged_fixture = copy.deepcopy(FIXTURE)
            forged_fixture["candidates"][0]["market_id"] = "market-forged"
            forged_packet = prepare_packet(forged_fixture)
            forged_packet_path.write_text(json.dumps(forged_packet), encoding="utf-8")

            for arguments in (
                [
                    "validate-intent",
                    "--fixture",
                    str(fixture_path),
                    "--intent",
                    str(intent_path),
                    "--packet",
                    str(forged_packet_path),
                ],
                ["prepare", "--fixture", str(fixture_path), "--output", str(root / "packet.json")],
            ):
                with self.subTest(arguments=arguments), redirect_stderr(io.StringIO()):
                    with self.assertRaises(SystemExit):
                        main(arguments)

    def test_cli_rejects_forbidden_execution_options(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            fixture_path = pathlib.Path(temporary_directory) / "fixture.json"
            fixture_path.write_text(json.dumps(FIXTURE), encoding="utf-8")
            for forbidden in ("--real", "--live-trading", "--execute", "--order", "--ibkr", "--pearl"):
                with self.subTest(forbidden=forbidden), redirect_stderr(io.StringIO()):
                    with self.assertRaises(SystemExit):
                        main(["prepare", "--fixture", str(fixture_path), forbidden])

    def test_guardian_source_has_no_execution_network_or_real_dependencies(self):
        import manus.paper_cycle_guardian as guardian

        source = inspect.getsource(guardian)
        tree = ast.parse(source)
        imports = set()
        called_names = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imports.add(node.module.split(".")[0])
            elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                called_names.add(node.func.id)
        self.assertFalse(imports & {"subprocess", "importlib", "socket", "urllib", "requests"})
        self.assertFalse(called_names & {"eval", "exec", "__import__"})
        self.assertNotIn("core.real", source.lower())
        self.assertNotIn("shell=true", source.lower())
        self.assertNotIn("write_text", source)
        self.assertNotIn("write_bytes", source)


if __name__ == "__main__":
    unittest.main()

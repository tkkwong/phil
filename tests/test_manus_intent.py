"""Unit tests for the data-only Manus intent validator."""

import copy
import json
import unittest

from manus.intent_validator import IntentValidationError, validate_intent


VALID_PAPER_INTENT = {
    "intent_id": "123e4567-e89b-42d3-a456-426614174000",
    "candidate_id": "cand-20260926-01",
    "market_id": "market:example-001",
    "outcome": "Miami (OH)",
    "estimated_probability": 0.62,
    "category": "macro",
    "rationale": "The outcome is underpriced relative to the evidence.",
    "edge_class": "calibration",
    "mode": "PAPER",
    "forecast_disposition": "no-edge",
    "strategy_proposals": [
        {
            "proposal_id": "proposal-calibration-01",
            "summary": "Review calibration before the next paper forecast batch.",
        }
    ],
}


class ManusIntentValidatorTests(unittest.TestCase):
    def validate(self, payload, applied_ids=None):
        return validate_intent(json.dumps(payload), applied_ids)

    def test_accepts_one_valid_paper_intent(self):
        accepted = self.validate(VALID_PAPER_INTENT)
        self.assertEqual(accepted, VALID_PAPER_INTENT)

    def test_accepts_real_phil_outcome_labels(self):
        for outcome in ("Texas A&M", "Wake Forest", "100 Thieves", "Hawai'i", "Miami (OH)"):
            with self.subTest(outcome=outcome):
                payload = copy.deepcopy(VALID_PAPER_INTENT)
                payload["outcome"] = outcome
                self.assertEqual(self.validate(payload)["outcome"], outcome)

    def test_accepts_documented_forecast_dispositions(self):
        for disposition in ("bet", "no-edge", "market-agrees"):
            with self.subTest(disposition=disposition):
                payload = copy.deepcopy(VALID_PAPER_INTENT)
                payload["forecast_disposition"] = disposition
                self.assertEqual(self.validate(payload)["forecast_disposition"], disposition)

    def test_accepts_legitimate_trading_strategy_proposal(self):
        for summary in (
            "Require a larger edge before paper bets in thin markets.",
            "For paper markets, buy and sell only after reviewing trade order size and position concentration.",
            "Compare S&P 500 pricing before paper bets.",
            "Track P&L by category.",
        ):
            with self.subTest(summary=summary):
                payload = copy.deepcopy(VALID_PAPER_INTENT)
                payload["strategy_proposals"][0]["summary"] = summary
                self.assertEqual(self.validate(payload), payload)

    def test_rejects_malformed_json(self):
        with self.assertRaises(IntentValidationError):
            validate_intent('{"intent_id": ')

    def test_rejects_duplicate_json_keys(self):
        document = json.dumps(VALID_PAPER_INTENT)
        document = document.replace(
            '"rationale": "The outcome is underpriced relative to the evidence.",',
            '"rationale": "first", "rationale": "second",',
        )
        with self.assertRaisesRegex(IntentValidationError, "Duplicate JSON field"):
            validate_intent(document)

    def test_rejects_unknown_fields(self):
        payload = copy.deepcopy(VALID_PAPER_INTENT)
        payload["unexpected"] = True
        with self.assertRaisesRegex(IntentValidationError, "Unknown field"):
            self.validate(payload)

    def test_rejects_missing_required_field(self):
        payload = copy.deepcopy(VALID_PAPER_INTENT)
        del payload["mode"]
        with self.assertRaisesRegex(IntentValidationError, "Missing required field"):
            self.validate(payload)

    def test_rejects_invalid_probability(self):
        for probability in (0, 1, -0.01, 1.01, "0.62", True):
            with self.subTest(probability=probability):
                payload = copy.deepcopy(VALID_PAPER_INTENT)
                payload["estimated_probability"] = probability
                with self.assertRaisesRegex(IntentValidationError, "estimated_probability"):
                    self.validate(payload)

    def test_rejects_nan_and_infinity(self):
        for value in ("NaN", "Infinity", "-Infinity"):
            with self.subTest(value=value):
                document = json.dumps(VALID_PAPER_INTENT).replace("0.62", value, 1)
                with self.assertRaisesRegex(IntentValidationError, "Non-standard JSON number"):
                    validate_intent(document)

    def test_rejects_path_traversal(self):
        payload = copy.deepcopy(VALID_PAPER_INTENT)
        payload["candidate_id"] = "candidate/../escape"
        with self.assertRaisesRegex(IntentValidationError, "Path traversal"):
            self.validate(payload)

    def test_rejects_absolute_paths(self):
        payload = copy.deepcopy(VALID_PAPER_INTENT)
        payload["market_id"] = "/tmp/market"
        with self.assertRaisesRegex(IntentValidationError, "Absolute path"):
            self.validate(payload)

    def test_rejects_windows_absolute_path(self):
        payload = copy.deepcopy(VALID_PAPER_INTENT)
        payload["outcome"] = r"C:\\temp\\market"
        with self.assertRaisesRegex(IntentValidationError, "Absolute path"):
            self.validate(payload)

    def test_rejects_shell_or_command_like_fields(self):
        payload = copy.deepcopy(VALID_PAPER_INTENT)
        payload["shell_command"] = "rm -rf /"
        with self.assertRaisesRegex(IntentValidationError, "Forbidden field"):
            self.validate(payload)

    def test_rejects_executable_command_in_strategy_proposal(self):
        payload = copy.deepcopy(VALID_PAPER_INTENT)
        payload["strategy_proposals"][0]["summary"] = "git commit the changes"
        with self.assertRaisesRegex(IntentValidationError, "Command or shell|Git operation"):
            self.validate(payload)

    def test_rejects_shell_syntax_in_strategy_proposal(self):
        for summary in (
            "echo one && echo two",
            "status | grep open",
            "echo `whoami`",
            "echo $(whoami)",
            "echo one; echo two",
        ):
            with self.subTest(summary=summary):
                payload = copy.deepcopy(VALID_PAPER_INTENT)
                payload["strategy_proposals"][0]["summary"] = summary
                with self.assertRaisesRegex(IntentValidationError, "Command or shell"):
                    self.validate(payload)

    def test_rejects_file_mutation_and_credential_material_in_strategy_proposal(self):
        for summary, error in (
            ("Write the file config/settings.json.", "File mutation"),
            ("api_key: not-a-real-key", "Credential material"),
        ):
            with self.subTest(summary=summary):
                payload = copy.deepcopy(VALID_PAPER_INTENT)
                payload["strategy_proposals"][0]["summary"] = summary
                with self.assertRaisesRegex(IntentValidationError, error):
                    self.validate(payload)

    def test_rejects_real_trading_field(self):
        payload = copy.deepcopy(VALID_PAPER_INTENT)
        payload["trade_request"] = "buy"
        with self.assertRaisesRegex(IntentValidationError, "Forbidden field"):
            self.validate(payload)

    def test_rejects_real_trading_field_inside_strategy_proposal(self):
        payload = copy.deepcopy(VALID_PAPER_INTENT)
        payload["strategy_proposals"][0]["trade_request"] = "buy"
        with self.assertRaisesRegex(IntentValidationError, "Forbidden field"):
            self.validate(payload)

    def test_rejects_ibkr_order_or_execution_field(self):
        payload = copy.deepcopy(VALID_PAPER_INTENT)
        payload["ibkr_order"] = {"side": "BUY"}
        with self.assertRaisesRegex(IntentValidationError, "Forbidden field"):
            self.validate(payload)

    def test_rejects_pearl_field(self):
        payload = copy.deepcopy(VALID_PAPER_INTENT)
        payload["pearl_connect"] = "do not contact"
        with self.assertRaisesRegex(IntentValidationError, "Forbidden field"):
            self.validate(payload)

    def test_rejects_credential_field(self):
        payload = copy.deepcopy(VALID_PAPER_INTENT)
        payload["api_key"] = "not-a-real-key"
        with self.assertRaisesRegex(IntentValidationError, "Forbidden field"):
            self.validate(payload)

    def test_rejects_mode_other_than_paper(self):
        payload = copy.deepcopy(VALID_PAPER_INTENT)
        payload["mode"] = "LIVE"
        with self.assertRaisesRegex(IntentValidationError, "mode must be PAPER"):
            self.validate(payload)

    def test_rejects_duplicate_intent_id_from_supplied_list(self):
        with self.assertRaisesRegex(IntentValidationError, "Duplicate intent_id"):
            self.validate(VALID_PAPER_INTENT, [VALID_PAPER_INTENT["intent_id"]])


if __name__ == "__main__":
    unittest.main()

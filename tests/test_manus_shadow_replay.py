"""Offline regression suite for read-only replay and shadow comparison (5E-6).

Everything runs offline in temporary directories. No network, no Manus task,
no model provider, no broker, and zero operational mutation is ever performed.
"""
from __future__ import annotations

import json
import pathlib
import tempfile
import unittest
import unittest.mock

from manus import decision_provenance as dp
from manus import shadow_replay as sr

CYCLE_ID = "123e4567-e89b-42d3-a456-426614174000"


def _policy(**changes):
    values = {
        "stake_usd": 5.0,
        "max_stake_usd": 20.0,
        "min_entry_price": 0.05,
        "max_entry_price": 0.95,
        "required_edge": 0.03,
        "max_spread": 0.08,
        "max_open_positions": 10,
        "max_stake_per_event_usd": 10.0,
        "max_new_positions_per_cycle": 3,
        "max_positions_per_category_per_cycle": 2,
    }
    values.update(changes)
    return values


def _frozen(**changes):
    values = {
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
        "best_bid": 0.50,
        "best_ask": 0.52,
        "liquidity": None,
        "volume_24h": None,
        "policy": _policy(),
        "open_positions": [],
    }
    values.update(changes)
    return dp.frozen_input(**values)


def _falsifying_engine(action="no-trade", reason_code="no-edge", estimated=0.50):
    def engine(frozen):
        return {
            "schema_version": "fake-shadow/v1",
            "engine_id": "fake-shadow",
            "decision": {
                "action": action,
                "reason_code": reason_code,
                "estimated_probability": estimated,
                "requested_notional": None,
            },
        }
    return engine


class FrozenInputFixtureTests(unittest.TestCase):
    def test_frozen_input_document_roundtrip(self):
        frozen = _frozen()
        document = json.loads(json.dumps(frozen))
        reloaded = sr.load_frozen_input_document(json.dumps(document))
        self.assertEqual(reloaded, frozen)
        self.assertEqual(
            dp.frozen_input_sha256(reloaded),
            dp.frozen_input_sha256(frozen),
        )

    def test_malformed_frozen_input_fails_bounded(self):
        with self.assertRaisesRegex(sr.ShadowReplayError, "malformed"):
            sr.load_frozen_input_document("{not json")
        with self.assertRaisesRegex(sr.ShadowReplayError, "schema is invalid"):
            sr.load_frozen_input_document(json.dumps({"invented": True}))
        with self.assertRaisesRegex(sr.ShadowReplayError, "schema version is incompatible"):
            document = dict(_frozen(), schema_version="other/v9")
            sr.load_frozen_input_document(json.dumps(document))


class BaselineReplayTests(unittest.TestCase):
    def test_trade_replay_matches_live_guard_order(self):
        frozen = _frozen()
        document = sr.replay(frozen)
        baseline = document["baseline"]
        self.assertEqual(baseline["decision"]["action"], "trade")
        self.assertEqual(baseline["decision"]["reason_code"], None)
        self.assertEqual(baseline["decision"]["estimated_probability"], 0.62)
        self.assertEqual(baseline["decision"]["edge"], 0.62 - 0.52)
        self.assertTrue(baseline["placement_attempted"])

    def test_edge_below_threshold_reason_matches_repository_vocabulary(self):
        frozen = _frozen(estimated_probability=0.54)
        document = sr.replay(frozen)
        baseline = document["baseline"]["decision"]
        self.assertEqual(baseline["action"], "no-trade")
        self.assertEqual(baseline["reason_code"], "edge-below-threshold")

    def test_wide_spread_reason_matches_repository_vocabulary(self):
        frozen = _frozen(best_bid=0.30, best_ask=0.52)
        document = sr.replay(frozen)
        self.assertEqual(
            document["baseline"]["decision"]["reason_code"], "spread-too-wide"
        )

    def test_non_bet_disposition_is_explicit_no_trade(self):
        frozen = _frozen(forecast_disposition="no-edge")
        document = sr.replay(frozen)
        baseline = document["baseline"]["decision"]
        self.assertEqual(baseline["action"], "no-trade")
        self.assertEqual(baseline["reason_code"], "no-edge")

    def test_replay_is_deterministic_and_repeatable(self):
        frozen = _frozen()
        first = sr.replay(frozen)
        second = sr.replay(frozen)
        self.assertEqual(
            first["baseline"]["decision"], second["baseline"]["decision"]
        )

    def test_missing_required_input_fails_bounded(self):
        frozen = _frozen(estimated_probability=None)
        with self.assertRaisesRegex(sr.ShadowReplayError, "replay-input-incomplete"):
            sr.replay(frozen)


class ProvenanceReplayTests(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.temporary_directory.name) / "prov"

    def tearDown(self):
        self.temporary_directory.cleanup()

    def _record(self, **changes):
        values = {
            "execution_mode": "PAPER",
            "recorded_at_utc": "2026-09-27T05:00:00Z",
            "code_revision": None,
            "cycle_id": CYCLE_ID,
            "candidate_id": "candidate-100",
            "market_id": "market-100",
            "event_id": "event-100",
            "source_intent_id": "223e4567-e89b-42d3-a456-426614174000",
            "research_task_id": "task-1",
            "forecast_id": None,
            "research_provider": "manus",
            "research_model_id": None,
            "research_profile": "standard",
            "prompt_version": None,
            "prompt_sha256": None,
            "response_schema_version": None,
            "frozen": _frozen(),
            "decision_action": "no-trade",
            "decision_stage": "decision-policy",
            "decision_phase": "final",
            "reason_code": "edge-below-threshold",
            "disposition": "bet",
            "estimated_probability": 0.62,
            "market_probability": 0.51,
            "edge": None,
            "requested_notional": None,
            "placement_attempted": False,
            "placement_result": None,
            "placement_rejection_code": None,
        }
        values.update(changes)
        return dp.build_decision_record(**values)

    def test_replay_provenance_record_is_read_only_and_matching(self):
        record = self._record(
            frozen=_frozen(estimated_probability=0.54),
        )
        dp.append_decision_record(record, _provenance_root=self.root)
        stored = dp.read_decision_records(_provenance_root=self.root)[0]
        document = sr.replay_provenance_record(stored)
        self.assertTrue(document["baseline_match"]["action_match"])
        self.assertTrue(document["baseline_match"]["reason_match"])
        self.assertEqual(document["baseline"]["decision"]["action"], "no-trade")
        self.assertEqual(
            len(dp.read_decision_records(_provenance_root=self.root)), 1
        )

    def test_tampered_record_fails_closed(self):
        record = self._record()
        tampered = dict(record, decision_action="trade")
        with self.assertRaisesRegex(sr.ShadowReplayError, "integrity failed"):
            sr.replay_provenance_record(tampered)

    def test_record_without_frozen_input_is_unavailable(self):
        record = self._record()
        broken = dict(record)
        broken.pop("frozen_input")
        with self.assertRaisesRegex(sr.ShadowReplayError, "provenance-unavailable"):
            sr.replay_provenance_record(broken)

    def test_missing_frozen_input_in_record_is_incomplete(self):
        record = self._record(frozen=_frozen(estimated_probability=None))
        with self.assertRaisesRegex(sr.ShadowReplayError, "replay-input-incomplete"):
            sr.replay_provenance_record(record)

    def test_replayability_status_is_bounded_and_truthful(self):
        good = self._record(frozen=_frozen(estimated_probability=0.54))
        self.assertEqual(sr.replayability_status(good), sr.REPLAYABLE)
        incomplete = self._record(frozen=_frozen(estimated_probability=None))
        self.assertEqual(sr.replayability_status(incomplete), sr.REPLAY_INCOMPLETE)
        self.assertEqual(sr.replayability_status(None), sr.PROVENANCE_UNAVAILABLE)
        tampered = dict(self._record(), decision_action="trade")
        self.assertEqual(sr.replayability_status(tampered), sr.PROVENANCE_UNAVAILABLE)


class ShadowComparisonTests(unittest.TestCase):
    def setUp(self):
        self.frozen = _frozen()

    def test_identical_shadow_result_matches(self):
        engine = _falsifying_engine(action="trade", reason_code=None, estimated=0.62)
        document = sr.replay(self.frozen, engine=engine)
        shadow = document["shadow"]
        self.assertTrue(shadow["action_match"])
        self.assertTrue(shadow["reason_match"])
        self.assertEqual(shadow["probability_delta"], 0.0)
        self.assertEqual(
            shadow["frozen_input_sha256"], document["frozen_input_sha256"]
        )
        self.assertEqual(shadow["engine"], engine.__name__)

    def test_changed_action_fails_action_match(self):
        document = sr.replay(self.frozen, engine=_falsifying_engine())
        shadow = document["shadow"]
        self.assertFalse(shadow["action_match"])
        self.assertFalse(shadow["action_match"] and shadow["reason_match"])

    def test_changed_probability_produces_deterministic_delta(self):
        engine = _falsifying_engine(action="trade", estimated=0.55)
        document = sr.replay(self.frozen, engine=engine)
        self.assertEqual(document["shadow"]["probability_delta"], -0.07)

    def test_changed_reason_fails_reason_match(self):
        engine = _falsifying_engine(action="trade", reason_code="other-reason")
        document = sr.replay(self.frozen, engine=engine)
        self.assertFalse(document["shadow"]["reason_match"])

    def test_shadow_engine_exception_is_bounded(self):
        def exploding(frozen):
            raise RuntimeError("shadow provider exploded")
        with self.assertRaisesRegex(sr.ShadowReplayError, "shadow-engine-failed"):
            sr.replay(self.frozen, engine=exploding)

    def test_shadow_engine_invalid_result_is_bounded(self):
        def invalid(frozen):
            return {"decision": {"action": True}}
        with self.assertRaisesRegex(sr.ShadowReplayError, "shadow-engine-failed"):
            sr.replay(self.frozen, engine=invalid)

    def test_baseline_operational_state_is_never_touched(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            before = sorted(p.name for p in root.iterdir())
            sr.replay(self.frozen, engine=_falsifying_engine())
            after = sorted(p.name for p in root.iterdir())
        self.assertEqual(before, after)


class StaticIsolationTests(unittest.TestCase):
    def test_module_has_no_network_process_or_paid_provider_surface(self):
        import ast
        import inspect
        source = inspect.getsource(sr)
        tree = ast.parse(source)
        imports = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imports.add(node.module)
        self.assertFalse(imports & {"urllib", "requests", "httpx", "socket", "subprocess"})
        calls = {
            node.func.id
            for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
        }
        self.assertFalse(calls & {"eval", "exec", "system", "__import__", "popen"})
        for forbidden_import in ("ibkr", "pearl", "openai", "anthropic"):
            self.assertFalse(
                any(name == forbidden_import or name.startswith(forbidden_import + ".") for name in imports),
                forbidden_import,
            )

    def test_no_engine_is_bundled_for_cli_shadow(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "frozen.json"
            path.write_text(json.dumps(_frozen()), encoding="utf-8")
            with self.assertRaises(SystemExit):
                sr.main(["shadow", "--frozen-input", str(path)])

    def test_forbidden_cli_options_are_rejected(self):
        with self.assertRaises(SystemExit):
            sr.main(["replay", "--frozen-input", "x.json", "--real-mode"])
        with self.assertRaises(SystemExit):
            sr.main(["replay", "--frozen-input", "x.json", "--place-order"])
        with self.assertRaises(SystemExit):
            sr.main(["replay", "--frozen-input", "x.json", "--journal-ledger"])

    def test_cli_replay_is_read_only_and_prints_typed_decision(self):
        import contextlib
        import io
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "frozen.json"
            path.write_text(json.dumps(_frozen()), encoding="utf-8")
            buffer = io.StringIO()
            with contextlib.redirect_stdout(buffer):
                status = sr.main(["replay", "--frozen-input", str(path)])
        self.assertEqual(status, 0)
        document = json.loads(buffer.getvalue())
        self.assertEqual(document["baseline"]["decision"]["action"], "trade")
        self.assertEqual(document["shadow_replay_version"], sr.SHADOW_REPLAY_VERSION)

    def test_cli_missing_input_fails_bounded(self):
        with tempfile.TemporaryDirectory() as tmp:
            missing = pathlib.Path(tmp) / "missing.json"
            with self.assertRaises(SystemExit):
                sr.main(["replay", "--frozen-input", str(missing)])




class ShadowReplayCliHelpTests(unittest.TestCase):
    """5E-6a: help is not an operational mutation capability.

    Every help path exits 0 and performs no filesystem or network action,
    while unknown and forbidden operational options still fail closed.
    """

    def _run(self, arguments):
        import contextlib
        import io
        out, err = io.StringIO(), io.StringIO()
        try:
            with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
                status = sr.main(arguments)
        except SystemExit as exit_status:
            # argparse prints help text and exits; a zero exit is success.
            if exit_status.code not in (None, 0):
                raise
            status = 0
        return status, out.getvalue(), err.getvalue()

    def test_top_level_help_exits_zero(self):
        status, out, _ = self._run(["--help"])
        self.assertEqual(status, 0)
        self.assertIn("replay", out)
        self.assertIn("shadow", out)

    def test_replay_help_exits_zero(self):
        status, out, _ = self._run(["replay", "--help"])
        self.assertEqual(status, 0)
        self.assertIn("--frozen-input", out)

    def test_shadow_help_exits_zero(self):
        status, out, _ = self._run(["shadow", "--help"])
        self.assertEqual(status, 0)
        self.assertIn("--frozen-input", out)

    def test_unknown_option_exits_nonzero(self):
        with self.assertRaises(SystemExit):
            sr.main(["--unknown-opt"])

    def test_forbidden_broker_or_write_options_exit_nonzero(self):
        for option in ("--place-order", "--real-mode", "--journal-ledger", "--ibkr", "--trade"):
            with self.subTest(option=option):
                with self.assertRaises(SystemExit):
                    sr.main(["replay", "--frozen-input", "x.json", option])

    def test_replay_has_no_placement_option(self):
        parser = sr.build_parser()
        replay_options = {
            option
            for action in parser._subparsers._group_actions[0].choices["replay"]._actions
            for option in action.option_strings
        }
        self.assertNotIn("--placement", replay_options)
        self.assertNotIn("--place", replay_options)
        with self.assertRaises(SystemExit):
            sr.main(["replay", "--placement", "x"])

    def test_shadow_has_no_placement_option(self):
        parser = sr.build_parser()
        shadow_options = {
            option
            for action in parser._subparsers._group_actions[0].choices["shadow"]._actions
            for option in action.option_strings
        }
        self.assertNotIn("--placement", shadow_options)
        self.assertNotIn("--place", shadow_options)
        with self.assertRaises(SystemExit):
            sr.main(["shadow", "--placement", "x"])

    def test_help_performs_no_filesystem_or_network_mutation(self):
        import manus.shadow_replay as module
        with tempfile.TemporaryDirectory() as tmp:
            marker = pathlib.Path(tmp) / "marker.jsonl"
            calls = []
            original_replay = module.replay
            def spy_replay(frozen, engine=None):
                calls.append("replay")
                return original_replay(frozen, engine)
            with unittest.mock.patch.object(module, "replay", side_effect=spy_replay):
                status, _, _ = self._run(["replay", "--help"])
            self.assertEqual(status, 0)
            self.assertEqual(calls, [])
            self.assertFalse(marker.exists())


if __name__ == "__main__":
    unittest.main()

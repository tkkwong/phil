"""Offline regressions for the bounded manual Manus PAPER cycle runner."""
from __future__ import annotations

import ast
import datetime as dt
import hashlib
import inspect
import io
import json
import multiprocessing
import os
import pathlib
import stat
import tempfile
import unittest
import uuid
from contextlib import contextmanager, redirect_stderr
from types import SimpleNamespace
from unittest.mock import patch

from core import forecast as forecast_core
from core import ledger as ledger_core
from manus import paper_apply, paper_locks, paper_runner, research_transport
from manus.paper_cycle_guardian import SCAN_SOURCE_FIELDS, VALIDATION_VERSION, prepare_packet
from manus.research_transport import TRANSPORT_REQUEST_SCHEMA_VERSION


CYCLE_ID = uuid.UUID("123e4567-e89b-42d3-a456-426614174000")
INTENT_ID = "223e4567-e89b-42d3-a456-426614174000"
NOW = dt.datetime(2026, 9, 29, 12, tzinfo=dt.timezone.utc)


def provider_metadata(tag_ids=("1",), *, status="ok", labels=None, slugs=None):
    labels = labels or {}
    slugs = slugs or {}
    return {
        "market_tags_status": status,
        "market_tags": [
            {
                "id": tag_id,
                "slug": slugs.get(tag_id, f"tag-{tag_id}"),
                "label": labels.get(tag_id, f"Label {tag_id}"),
            }
            for tag_id in tag_ids
        ],
    }


def candidate(market_id: str, *, metadata=None, question=None, description=None) -> dict:
    return {
        "market_id": market_id,
        "question": question or f"Will {market_id} happen?",
        "end_date": "2026-10-01T00:00:00.000Z",
        "event_id": f"event-{market_id}",
        "event_slug": f"event-{market_id}",
        "outcomes": ["Yes", "No"],
        "outcome_prices": [0.5, 0.5],
        "clob_token_ids": [f"yes-{market_id}", f"no-{market_id}"],
        "volume_24h": 100.0,
        "liquidity": 100.0,
        "slug": f"slug-{market_id}",
        "description": description or f"Synthetic protected candidate {market_id}.",
        "provider_metadata": provider_metadata() if metadata is None else metadata,
    }


def stale_candidate(market_id: str, *, metadata=None) -> dict:
    """Fresh-looking candidate whose end_date is already past the test clock."""
    return {**candidate(market_id, metadata=metadata), "end_date": "2026-09-29T11:59:59Z"}


def boundary_candidate(market_id: str, *, metadata=None) -> dict:
    """Candidate exactly at the protected minimum time to resolution."""
    minutes = paper_runner.core_scan.PROTECTED["min_minutes_to_resolution"]
    boundary = NOW + dt.timedelta(minutes=minutes)
    return {**candidate(market_id, metadata=metadata), "end_date": boundary.strftime("%Y-%m-%dT%H:%M:%S.000Z")}


def _hold_cycle_lock(lock_root: str, ready, release) -> None:
    """Spawn target holding the real fixed-root OS cycle lock."""
    with paper_locks.acquire_cycle_lock(nonblocking=True, _lock_root=pathlib.Path(lock_root)):
        ready.set()
        release.wait(10)


def _forbidden_metadata_scan(*, include_provider_metadata, max_candidates):
    raise AssertionError(
        f"lock-losing process attempted protected scan: "
        f"include_provider_metadata={include_provider_metadata!r}, max_candidates={max_candidates!r}"
    )


def _forbidden_research(*_args, **_kwargs):
    raise AssertionError("lock-losing process attempted research")


def _forbidden_application(*_args, **_kwargs):
    raise AssertionError("lock-losing process attempted application")


def _losing_runner_worker(runner_root: str, lock_root: str, result) -> None:
    """Spawn target proving a different process cannot enter a held cycle."""
    try:
        paper_runner.run(
            manus_task_budget=0,
            _runner_root=pathlib.Path(runner_root),
            _lock_root=pathlib.Path(lock_root),
            _scan_candidates=_forbidden_metadata_scan,
            _research_run=_forbidden_research,
            _apply_run=_forbidden_application,
        )
    except paper_runner.PaperRunnerError as exc:
        result.put(("runner-error", str(exc)))
    except BaseException as exc:  # pragma: no cover - diagnostic for a violated boundary
        result.put(("unexpected", f"{type(exc).__name__}: {exc}"))
    else:  # pragma: no cover - diagnostic for a violated boundary
        result.put(("unexpected", "runner succeeded"))


class PaperRunnerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.root = pathlib.Path(self.temporary_directory.name)
        self.runner_root = self.root / "external-runner"
        self.lock_root = self.root / "external-locks"
        self.staging_root = self.root / "external-staging"
        self.forecast_path = self.root / "forecasts.jsonl"
        self.ledger_path = self.root / "ledger.jsonl"
        # Isolated provenance root for the production seam; real
        # %LOCALAPPDATA% storage is never touched by tests.
        self.provenance_root = self.root / "external-provenance"
        self.calls: dict[str, list] = {"scan": [], "research": [], "apply": []}
        self.application_outputs: list[dict] = []
        self.spawn_context = multiprocessing.get_context("spawn")
        repository = pathlib.Path(__file__).resolve().parents[1]
        self.repository_journals = [repository / "journal" / "forecasts.jsonl", repository / "journal" / "ledger.jsonl"]
        self.repository_before = [(hashlib.sha256(path.read_bytes()).hexdigest(), len(path.read_text(encoding="utf-8").splitlines())) for path in self.repository_journals]

    def assert_repository_journals_unchanged(self):
        after = [(hashlib.sha256(path.read_bytes()).hexdigest(), len(path.read_text(encoding="utf-8").splitlines())) for path in self.repository_journals]
        self.assertEqual(after, self.repository_before)

    def scan(self, values: list[dict]):
        def implementation(*, include_provider_metadata, max_candidates):
            self.calls["scan"].append({"include_provider_metadata": include_provider_metadata, "max_candidates": max_candidates})
            return [dict(value) for value in values]

        return implementation

    def research(self, *, reject_budget_zero: bool = False, result: dict | None = None):
        def implementation(fixture_path, candidate_id, *, allow_new_task):
            self.calls["research"].append({"fixture": fixture_path, "candidate_id": candidate_id, "allow_new_task": allow_new_task})
            if reject_budget_zero and not allow_new_task:
                raise research_transport.ResearchTransportError("Current invocation is not authorized to create a new Manus task")
            return result or {
                "validated": True,
                "intent_id": INTENT_ID,
                "task_id": "task-1",
                "task_created_this_invocation": bool(allow_new_task),
            }

        return implementation

    def application(self, result: dict | None = None):
        def implementation(fixture_path, intent_id):
            self.calls["apply"].append({"fixture": fixture_path, "intent_id": intent_id})
            return result or {
                "application_state": "completed-no-placement",
                "forecast_id": "forecast-1",
                "placement_id": None,
                "forecast_status": "recorded",
                "placement_status": "not-eligible-disposition",
            }

        return implementation

    def run_runner(self, *, candidates=None, research=None, application=None, budget=0, ceiling=None, **kwargs):
        return paper_runner.run(
            manus_task_budget=budget,
            manus_soft_credit_ceiling=ceiling,
            _runner_root=self.runner_root,
            _lock_root=self.lock_root,
            _scan_candidates=self.scan(candidates if candidates is not None else [candidate("100")]),
            _research_run=research or self.research(),
            _apply_run=application or self.application(),
            _now=lambda: NOW,
            _new_uuid=lambda: CYCLE_ID,
            **kwargs,
        )

    def active_pointer(self) -> dict:
        return json.loads((self.runner_root / "active-cycle.json").read_text(encoding="utf-8"))

    def active_cycle(self) -> dict:
        pointer = self.active_pointer()
        return json.loads((self.runner_root / "cycles" / pointer["cycle_id"] / "cycle.json").read_text(encoding="utf-8"))

    def fixture_path(self) -> pathlib.Path:
        return self.runner_root / "cycles" / str(CYCLE_ID) / "fixture.json"

    def scan_assertion(self):
        self.assertEqual(self.calls["scan"], [{"include_provider_metadata": True, "max_candidates": 20}])

    def test_dry_run_uses_protected_metadata_scan_without_persistent_state_or_mutation(self):
        result = paper_runner.run(
            dry_run=True,
            manus_task_budget=0,
            _runner_root=self.runner_root,
            _lock_root=self.lock_root,
            _scan_candidates=self.scan([candidate("100")]),
            _research_run=lambda *_args, **_kwargs: self.fail("research called"),
            _apply_run=lambda *_args, **_kwargs: self.fail("application called"),
            _now=lambda: NOW,
            _new_uuid=lambda: CYCLE_ID,
        )
        self.assertTrue(result["dry_run"])
        self.assertEqual(result["cycle_state"], "selected")
        self.scan_assertion()
        self.assertFalse(self.runner_root.exists())
        self.assertFalse(self.lock_root.exists())
        self.assertFalse(self.staging_root.exists())
        self.assert_repository_journals_unchanged()

    def test_dry_run_remains_lock_free_while_cycle_lock_is_held(self):
        with paper_locks.acquire_cycle_lock(nonblocking=True, _lock_root=self.lock_root):
            result = paper_runner.run(
                dry_run=True,
                manus_task_budget=0,
                _runner_root=self.runner_root,
                _lock_root=self.lock_root,
                _scan_candidates=self.scan([candidate("100")]),
                _research_run=lambda *_args, **_kwargs: self.fail("research called"),
                _apply_run=lambda *_args, **_kwargs: self.fail("application called"),
                _now=lambda: NOW,
                _new_uuid=lambda: CYCLE_ID,
            )
        self.assertEqual(result["cycle_state"], "selected")
        self.scan_assertion()
        self.assertFalse(self.runner_root.exists())
        self.assert_repository_journals_unchanged()

    def test_first_eligible_candidate_uses_only_provider_ids_and_preserves_scanner_order(self):
        values = [
            candidate("first", metadata=provider_metadata(("999",))),
            candidate("second", metadata=provider_metadata(("1",), labels={"1": "Totally unrelated prose"}, slugs={"1": "not-policy"})),
            candidate("third", metadata=provider_metadata(("21",))),
        ]
        result = self.run_runner(candidates=values, research=self.research(reject_budget_zero=True), budget=0)
        self.assertEqual(result["market_id"], "second")
        self.assertEqual(self.calls["research"][0]["candidate_id"], self.active_cycle()["candidate_id"])
        self.scan_assertion()
        evidence = self.active_cycle()["selection_evidence"]
        self.assertEqual(evidence["market_id"], "second")
        self.assertEqual(evidence["provider_metadata"], values[1]["provider_metadata"])

    def test_excluded_id_overrides_allowed_and_unknown_ids_do_not_authorize(self):
        values = [
            candidate("both", metadata=provider_metadata(("1", "2"))),
            candidate("unknown", metadata=provider_metadata(("999",))),
            candidate("eligible", metadata=provider_metadata(("64", "999"))),
        ]
        result = self.run_runner(candidates=values, research=self.research(reject_budget_zero=True), budget=0)
        self.assertEqual(result["market_id"], "eligible")
        self.assertEqual(self.active_cycle()["selection_evidence"]["provider_metadata"]["market_tags"][1]["id"], "999")

    def test_empty_unavailable_invalid_and_malformed_metadata_fail_closed_to_no_candidate(self):
        malformed = {"market_tags_status": "ok", "market_tags": [{"id": "1", "slug": "x"}]}
        values = [
            candidate("empty", metadata=provider_metadata(())),
            candidate("unavailable", metadata=provider_metadata(status="unavailable")),
            candidate("invalid", metadata=provider_metadata(status="invalid")),
            candidate("malformed", metadata=malformed),
        ]
        result = self.run_runner(candidates=values, budget=0)
        self.assertEqual(result["cycle_state"], "completed-no-candidate")
        self.assertEqual(result["safe_reason"], "no-provider-tag-eligible-candidate")
        self.assertEqual(self.calls["research"], [])
        self.assertEqual(self.calls["apply"], [])

    def test_question_category_slug_and_description_text_are_not_selection_authority(self):
        values = [
            candidate(
                "text-only",
                metadata=provider_metadata(("999",), labels={"999": "Sports Crypto Election"}, slugs={"999": "sports"}),
                question="Will sports crypto political election happen?",
                description="sports weather crypto commercial economics",
            )
        ]
        result = self.run_runner(candidates=values, budget=0)
        self.assertEqual(result["cycle_state"], "completed-no-candidate")
        self.assertEqual(self.calls["research"], [])

    def test_stale_first_candidate_is_skipped_for_later_fresh_candidate(self):
        values = [
            stale_candidate("stale", metadata=provider_metadata(("1",))),
            candidate("fresh", metadata=provider_metadata(("1",))),
        ]
        result = self.run_runner(candidates=values, research=self.research(reject_budget_zero=True), budget=0)
        self.assertEqual(result["market_id"], "fresh")
        self.assertEqual(self.calls["research"][0]["candidate_id"], self.active_cycle()["candidate_id"])
        self.scan_assertion()

    def test_all_tag_eligible_candidates_stale_yields_no_candidate_and_no_research(self):
        result = self.run_runner(
            candidates=[stale_candidate("stale", metadata=provider_metadata(("1",)))],
            research=self.research(reject_budget_zero=True),
            budget=0,
        )
        self.assertEqual(result["cycle_state"], "completed-no-candidate")
        self.assertEqual(result["safe_reason"], "no-provider-tag-eligible-candidate")
        self.assertEqual(self.calls["research"], [])
        self.assertEqual(self.calls["apply"], [])
        self.assert_repository_journals_unchanged()

    def test_boundary_candidate_is_still_researchable(self):
        result = self.run_runner(
            candidates=[boundary_candidate("boundary", metadata=provider_metadata(("1",)))],
            research=self.research(reject_budget_zero=True),
            budget=0,
        )
        self.assertEqual(result["market_id"], "boundary")
        self.assertEqual(result["cycle_state"], "research-pending")

    def test_dry_run_skips_stale_candidates_with_same_rule(self):
        result = paper_runner.run(
            dry_run=True,
            manus_task_budget=0,
            _runner_root=self.runner_root,
            _lock_root=self.lock_root,
            _scan_candidates=self.scan([stale_candidate("stale", metadata=provider_metadata(("1",))), candidate("fresh", metadata=provider_metadata(("1",)))]),
            _research_run=lambda *_args, **_kwargs: self.fail("research called"),
            _apply_run=lambda *_args, **_kwargs: self.fail("application called"),
            _now=lambda: NOW,
            _new_uuid=lambda: CYCLE_ID,
        )
        self.assertEqual(result["cycle_state"], "selected")
        self.assertEqual(result["market_id"], "fresh")

    def test_dry_run_reports_no_candidate_when_all_stale(self):
        result = paper_runner.run(
            dry_run=True,
            manus_task_budget=0,
            _runner_root=self.runner_root,
            _lock_root=self.lock_root,
            _scan_candidates=self.scan([stale_candidate("stale", metadata=provider_metadata(("1",)))]),
            _research_run=lambda *_args, **_kwargs: self.fail("research called"),
            _apply_run=lambda *_args, **_kwargs: self.fail("application called"),
            _now=lambda: NOW,
            _new_uuid=lambda: CYCLE_ID,
        )
        self.assertEqual(result["cycle_state"], "completed-no-candidate")

    def test_selected_candidate_becoming_stale_before_research_terminates_safely(self):
        # First invocation selects and freezes the candidate, then stops before
        # research (budget 0). The clock then advances past the resolution floor.
        self.run_runner(candidates=[candidate("frozen", metadata=provider_metadata(("1",)))], research=self.research(reject_budget_zero=True), budget=0)
        first_cycle = self.active_cycle()
        self.assertEqual(first_cycle["state"], "research-pending")
        research_calls_before = len(self.calls["research"])
        # The frozen candidate resolves 2026-10-01T00:00Z; advancing the clock
        # past (end_date - resolution floor) makes it unresearchable on resume.
        expired = dt.datetime(2026, 9, 30, 23, 45, tzinfo=dt.timezone.utc)
        second = paper_runner.run(
            manus_task_budget=1,
            manus_soft_credit_ceiling=10,
            _runner_root=self.runner_root,
            _lock_root=self.lock_root,
            _scan_candidates=self.scan([candidate("frozen", metadata=provider_metadata(("1",)))]),
            _research_run=self.research(),
            _apply_run=self.application(),
            _now=lambda: expired,
            _new_uuid=lambda: CYCLE_ID,
        )
        self.assertEqual(second["cycle_state"], "failed-terminal")
        self.assertEqual(second["safe_reason"], "selected-candidate-no-longer-researchable")
        self.assertEqual(len(self.calls["research"]), research_calls_before)
        self.assertEqual(self.calls["apply"], [])
        cycle = self.active_cycle()
        self.assertEqual(cycle["candidate_id"], first_cycle["candidate_id"])
        self.assertEqual(cycle["selection_evidence"], first_cycle["selection_evidence"])
        self.assertEqual(cycle["selection_evidence_sha256"], first_cycle["selection_evidence_sha256"])
        self.assertEqual(cycle["counters"], {"candidates_selected": 1, "new_manus_tasks": 0, "logical_applications": 0})
        self.assertEqual(cycle["intent_id"], None)
        self.assertEqual(cycle["task_id"], None)
        self.assertEqual(cycle["forecast_id"], None)
        self.assertEqual(cycle["placement_id"], None)
        self.assertEqual(cycle["application_state"], None)
        self.assert_repository_journals_unchanged()

    def test_fixture_excludes_provider_metadata_and_imports_public_guardian_contract(self):
        self.run_runner(research=self.research(reject_budget_zero=True), budget=0)
        fixture = json.loads(self.fixture_path().read_text(encoding="utf-8"))
        self.assertNotIn("provider_metadata", fixture["candidates"][0])
        self.assertTrue(set(fixture["candidates"][0]).issubset(SCAN_SOURCE_FIELDS))
        source = inspect.getsource(paper_runner)
        self.assertIn("from manus.paper_cycle_guardian import SCAN_SOURCE_FIELDS", source)
        self.assertNotIn("__import__(", source)
        self.assertNotIn("_SCAN_SOURCE_FIELDS", source)

    def test_frozen_evidence_hash_mismatch_fails_closed_without_rescan_or_replacement(self):
        self.run_runner(research=self.research(reject_budget_zero=True), budget=0)
        cycle = self.active_cycle()
        cycle["selection_evidence_sha256"] = "0" * 64
        cycle_path = self.runner_root / "cycles" / str(CYCLE_ID) / "cycle.json"
        cycle_path.write_text(json.dumps(cycle), encoding="utf-8")
        with self.assertRaisesRegex(paper_runner.PaperRunnerError, "selection evidence hash"):
            self.run_runner(candidates=[candidate("replacement")], research=self.research(reject_budget_zero=True), budget=0)
        self.scan_assertion()

    def test_frozen_candidate_and_evidence_never_rescan_or_replace(self):
        first = self.run_runner(research=self.research(reject_budget_zero=True), budget=0)
        first_cycle = self.active_cycle()
        second = self.run_runner(candidates=[candidate("replacement", metadata=provider_metadata(("21",)))], research=self.research(reject_budget_zero=True), budget=0)
        self.assertEqual(first["cycle_id"], second["cycle_id"])
        self.assertEqual(self.active_cycle()["candidate_id"], first_cycle["candidate_id"])
        self.assertEqual(self.active_cycle()["selection_evidence"], first_cycle["selection_evidence"])
        self.scan_assertion()
        self.assertEqual([call["allow_new_task"] for call in self.calls["research"]], [False, False])

    def test_budget_is_current_invocation_only_and_zero_fresh_cycle_stays_pending(self):
        first = self.run_runner(research=self.research(reject_budget_zero=True), budget=0)
        self.assertEqual(first["cycle_state"], "research-pending")
        self.assertEqual(first["safe_reason"], "current-invocation-research-authorization-required")
        self.assertEqual(self.active_cycle()["counters"], {"candidates_selected": 1, "new_manus_tasks": 0, "logical_applications": 0})
        second = self.run_runner(research=self.research(), budget=1, ceiling=10)
        self.assertEqual(second["cycle_id"], first["cycle_id"])
        self.assertEqual([call["allow_new_task"] for call in self.calls["research"]], [False, True])
        self.assertEqual(second["new_manus_tasks"], 1)

    def test_historical_ceiling_never_authorizes_budget_zero(self):
        self.run_runner(research=self.research(reject_budget_zero=True), budget=0)
        cycle = self.active_cycle()
        cycle["advisory_soft_credit_ceiling"] = 100
        cycle_path = self.runner_root / "cycles" / str(CYCLE_ID) / "cycle.json"
        cycle_path.write_text(json.dumps(cycle), encoding="utf-8")
        self.run_runner(research=self.research(reject_budget_zero=True), budget=0)
        self.assertEqual([call["allow_new_task"] for call in self.calls["research"]], [False, False])
        self.assertEqual(self.active_cycle()["state"], "research-pending")

    def test_budget_zero_known_task_and_validated_staging_reconcile_without_new_creation(self):
        result = self.run_runner(
            research=self.research(
                result={
                    "validated": True,
                    "intent_id": INTENT_ID,
                    "task_id": "known-task",
                    "task_created_this_invocation": False,
                }
            ),
            budget=0,
        )
        self.assertEqual(self.calls["research"], [{"fixture": str(self.fixture_path()), "candidate_id": self.active_cycle()["candidate_id"], "allow_new_task": False}])
        self.assertEqual(result["new_manus_tasks"], 0)
        self.assertEqual(result["task_id"], "known-task")
        self.assertEqual(result["cycle_state"], "completed")
        self.assertEqual(len(self.calls["apply"]), 1)

    def test_ambiguous_research_retains_first_eligible_cycle_without_fallback_or_rescan(self):
        def ambiguous(*_args, **_kwargs):
            self.calls["research"].append("ambiguous")
            raise research_transport.ResearchTransportError("operator reconciliation required")

        result = self.run_runner(
            candidates=[candidate("first"), candidate("second", metadata=provider_metadata(("21",)))],
            research=ambiguous,
            budget=1,
            ceiling=20,
        )
        self.assertEqual(result["cycle_state"], "research-pending")
        self.assertEqual(result["safe_reason"], "research-transport-reconciliation-required")
        self.assertEqual(self.active_cycle()["market_id"], "first")
        self.assertEqual(self.calls["research"], ["ambiguous"])
        self.scan_assertion()

    def test_frozen_fixture_sha_mismatch_fails_closed_before_rescan_or_replacement(self):
        self.run_runner(research=self.research(reject_budget_zero=True), budget=0)
        original = self.fixture_path().read_text(encoding="utf-8")
        self.fixture_path().write_text("\n" + original, encoding="utf-8")
        with self.assertRaisesRegex(paper_runner.PaperRunnerError, "fixture hash"):
            self.run_runner(candidates=[candidate("replacement")], research=self.research(reject_budget_zero=True), budget=0)
        self.scan_assertion()
        self.assertEqual(len(self.calls["research"]), 1)

    def test_fixture_only_crash_window_fails_closed_without_rescan_or_candidate_substitution(self):
        root = paper_runner._prepare_runner_root(self.runner_root)
        prepared = paper_runner._new_cycle(str(CYCLE_ID), lambda: NOW, None)
        paper_runner._persist_cycle(root, prepared, lambda: NOW)
        fixture, _metadata = paper_runner._fixture_from_scan([candidate("frozen")], lambda: NOW)
        paper_runner._write_fixture(self.fixture_path(), fixture)
        with self.assertRaisesRegex(paper_runner.PaperRunnerError, "selection evidence is unavailable"):
            self.run_runner(candidates=[candidate("replacement")], budget=0)
        self.assertEqual(self.calls["scan"], [])
        self.assertIsNone(self.active_cycle()["candidate_id"])

    def test_malformed_logical_application_counter_fails_closed_before_research_or_apply(self):
        self.run_runner(research=self.research(reject_budget_zero=True), budget=0)
        cycle = self.active_cycle()
        cycle["counters"]["logical_applications"] = 1
        cycle_path = self.runner_root / "cycles" / str(CYCLE_ID) / "cycle.json"
        cycle_path.write_text(json.dumps(cycle), encoding="utf-8")
        with self.assertRaisesRegex(paper_runner.PaperRunnerError, "logical application counter"):
            self.run_runner(candidates=[candidate("should-not-scan")], budget=0)
        self.scan_assertion()
        self.assertEqual(len(self.calls["research"]), 1)
        self.assertEqual(self.calls["apply"], [])

    def test_malformed_active_pointer_fails_closed_before_scan(self):
        self.runner_root.mkdir()
        pointer = self.runner_root / "active-cycle.json"
        cases = (
            "not json",
            json.dumps({"runner_version": paper_runner.RUNNER_VERSION, "cycle_id": "not-a-uuid", "state": "prepared"}),
            json.dumps({"runner_version": paper_runner.RUNNER_VERSION, "cycle_id": str(CYCLE_ID), "state": "prepared", "path": "../escape"}),
        )
        for document in cases:
            with self.subTest(document=document):
                pointer.write_text(document, encoding="utf-8")
                with self.assertRaisesRegex(paper_runner.PaperRunnerError, "Active cycle pointer"):
                    self.run_runner(candidates=[candidate("should-not-scan")], budget=0)
        self.assertEqual(self.calls["scan"], [])

    def test_cli_budget_and_credit_ceiling_validation_remain_strict(self):
        with self.assertRaisesRegex(paper_runner.PaperRunnerError, "requires a soft credit ceiling"):
            paper_runner.run(dry_run=True, manus_task_budget=1, _scan_candidates=lambda **_kwargs: [])
        with self.assertRaisesRegex(paper_runner.PaperRunnerError, "from 1 to 100"):
            paper_runner.run(
                dry_run=True,
                manus_task_budget=0,
                manus_soft_credit_ceiling=101,
                _scan_candidates=lambda **_kwargs: [],
            )
        with self.assertRaisesRegex(paper_runner.PaperRunnerError, "must be 0 or 1"):
            paper_runner.run(dry_run=True, manus_task_budget=2, _scan_candidates=lambda **_kwargs: [])

    def test_application_state_is_terminal_authority_not_placement_status(self):
        completed = {
            "application_state": "completed-no-placement",
            "forecast_id": "forecast-1",
            "placement_id": None,
            "forecast_status": "recorded",
            "placement_status": "rejected",
        }
        with self.assertRaisesRegex(paper_runner.PaperRunnerError, "completed-no-placement result is inconsistent"):
            self.run_runner(application=self.application(completed), budget=0)
        self.assertEqual(self.active_cycle()["state"], "application-pending")
        self.assertEqual(self.active_cycle()["counters"]["logical_applications"], 1)

    def test_unknown_application_state_leaves_pending_without_second_logical_initiation(self):
        malformed = {
            "application_state": "mystery",
            "forecast_id": None,
            "placement_id": None,
            "forecast_status": "none",
            "placement_status": "none",
        }
        with self.assertRaisesRegex(paper_runner.PaperRunnerError, "nonterminal, or unknown"):
            self.run_runner(application=self.application(malformed), budget=0)
        cycle = self.active_cycle()
        self.assertEqual(cycle["state"], "application-pending")
        self.assertEqual(cycle["counters"]["logical_applications"], 1)
        self.assertEqual(len(self.calls["apply"]), 1)

    def test_application_state_mapping_for_terminal_fake_results(self):
        cases = (
            (
                {
                    "application_state": "completed-placement", "forecast_id": "forecast-1", "placement_id": "placement-1",
                    "forecast_status": "recovered", "placement_status": "recovered",
                },
                "completed", "none",
            ),
            (
                {
                    "application_state": "placement-rejected", "forecast_id": "forecast-1", "placement_id": None,
                    "forecast_status": "terminal", "placement_status": "rejected",
                },
                "failed-terminal", "paper-placement-rejected",
            ),
            (
                {
                    "application_state": "forecast-rejected", "forecast_id": None, "placement_id": None,
                    "forecast_status": "rejected", "placement_status": "not-attempted",
                },
                "failed-terminal", "paper-forecast-rejected",
            ),
            (
                {
                    "application_state": "error", "forecast_id": None, "placement_id": None,
                    "forecast_status": "terminal", "placement_status": "not-attempted",
                },
                "failed-terminal", "paper-application-error",
            ),
        )
        for application, state, reason in cases:
            with self.subTest(application_state=application["application_state"]):
                with tempfile.TemporaryDirectory() as directory:
                    self.runner_root = pathlib.Path(directory) / "runner"
                    self.lock_root = pathlib.Path(directory) / "locks"
                    result = self.run_runner(application=self.application(application), budget=0)
                    self.assertEqual(result["cycle_state"], state)
                    self.assertEqual(result["safe_reason"], reason)

    def _staged_research(self, *, disposition="market-agrees"):
        def implementation(fixture_path, candidate_id, *, allow_new_task):
            self.calls["research"].append({"fixture": fixture_path, "candidate_id": candidate_id, "allow_new_task": allow_new_task})
            fixture_bytes = pathlib.Path(fixture_path).read_bytes()
            fixture = json.loads(fixture_bytes)
            packet = prepare_packet(fixture)
            selected = next(item for item in packet["candidates"] if item["candidate_id"] == candidate_id)
            intent = {
                "intent_id": INTENT_ID,
                "candidate_id": candidate_id,
                "market_id": selected["market_id"],
                "outcome": selected["outcomes"][0],
                "estimated_probability": 0.62,
                "category": "sports",
                "rationale": "Offline runner integration evidence.",
                "edge_class": "other",
                "mode": "PAPER",
                "forecast_disposition": disposition,
                "strategy_proposals": [],
            }
            directory = self.staging_root / packet["packet_id"] / INTENT_ID
            directory.mkdir(parents=True, exist_ok=True)
            (directory / "validated-intent.json").write_text(json.dumps(intent, sort_keys=True, separators=(",", ":")), encoding="utf-8")
            metadata = {
                "packet_id": packet["packet_id"],
                "intent_id": INTENT_ID,
                "candidate_id": candidate_id,
                "fixture_sha256": hashlib.sha256(fixture_bytes).hexdigest(),
                "transport_schema_version": TRANSPORT_REQUEST_SCHEMA_VERSION,
                "validation_version": VALIDATION_VERSION,
            }
            (directory / "run-meta.json").write_text(json.dumps(metadata, sort_keys=True, separators=(",", ":")), encoding="utf-8")
            return {"validated": True, "intent_id": INTENT_ID, "task_id": "known-task", "task_created_this_invocation": False}

        return implementation

    @contextmanager
    def public_market(self, *, prices=(0.50, 0.52)):
        market = {
            "closed": False,
            "question": "Offline public market question",
            "slug": "slug-100",
            "endDate": "2026-10-01T00:00:00Z",
            "outcomes": json.dumps(["Yes", "No"]),
            "clobTokenIds": json.dumps(["yes-100", "no-100"]),
        }
        with patch.object(forecast_core.pmapi, "gamma_market", return_value=market), \
             patch.object(forecast_core.pmapi, "market_tokens", return_value={"Yes": "yes-100", "No": "no-100"}), \
             patch.object(forecast_core.pmapi, "best_prices", return_value=prices), \
             patch.object(ledger_core.pmapi, "gamma_market", return_value=market), \
             patch.object(ledger_core.pmapi, "market_tokens", return_value={"Yes": "yes-100", "No": "no-100"}), \
             patch.object(ledger_core.pmapi, "best_prices", return_value=prices), \
             patch.object(ledger_core.pmapi, "gamma_markets", return_value=[{"id": "100", "events": [{"id": "event-100"}]}]):
            yield

    def production_apply(self):
        def implementation(fixture_path, intent_id):
            self.calls["apply"].append({"fixture": fixture_path, "intent_id": intent_id})
            result = paper_apply.run(
                fixture_path,
                intent_id,
                _staging_root=self.staging_root,
                _forecast_path=self.forecast_path,
                _ledger_path=self.ledger_path,
                _lock_root=self.lock_root,
                _provenance_root=self.provenance_root,
                _now=lambda: NOW,
                _placement_now=NOW,
            )
            self.application_outputs.append(result)
            return result

        return implementation

    def _replay_after_terminal_persist_crash(self, *, disposition, prices=(0.50, 0.52), expect_state="completed"):
        original_persist = paper_runner._persist_cycle
        failed = False

        def fail_terminal(root, cycle, now):
            nonlocal failed
            if cycle["state"] == expect_state and not failed:
                failed = True
                raise paper_runner.PaperRunnerError("simulated terminal runner receipt failure")
            return original_persist(root, cycle, now)

        with self.public_market(prices=prices), patch.object(paper_runner, "_persist_cycle", side_effect=fail_terminal):
            with self.assertRaisesRegex(paper_runner.PaperRunnerError, "terminal runner receipt"):
                self.run_runner(research=self._staged_research(disposition=disposition), application=self.production_apply(), budget=0)
        self.assertEqual(self.active_cycle()["state"], "application-pending")
        self.assertEqual(self.active_cycle()["counters"]["logical_applications"], 1)
        with self.public_market(prices=prices):
            result = self.run_runner(research=self._staged_research(disposition=disposition), application=self.production_apply(), budget=0)
        self.assertEqual(result["cycle_state"], expect_state)
        self.assertEqual(self.calls["scan"], [{"include_provider_metadata": True, "max_candidates": 20}])
        self.assertEqual(len(self.calls["research"]), 1)
        self.assertEqual(len(self.calls["apply"]), 2)
        self.assertEqual(self.active_cycle()["counters"]["logical_applications"], 1)
        self.assert_repository_journals_unchanged()
        return result

    def test_real_paper_apply_completed_no_placement_first_and_replay(self):
        result = self._replay_after_terminal_persist_crash(disposition="market-agrees")
        self.assertEqual(result["application_state"], "completed-no-placement")
        self.assertEqual(self.application_outputs[0]["forecast_status"], "recorded")
        self.assertEqual(self.application_outputs[1]["forecast_status"], "already-completed")
        self.assertEqual(len(self.forecast_path.read_text(encoding="utf-8").splitlines()), 1)
        self.assertFalse(self.ledger_path.exists())

    def test_real_paper_apply_completed_placement_replay_and_recovered_forms(self):
        result = self._replay_after_terminal_persist_crash(disposition="bet")
        self.assertEqual(result["application_state"], "completed-placement")
        self.assertEqual(self.application_outputs[0]["forecast_status"], "recorded")
        self.assertEqual(self.application_outputs[0]["placement_status"], "placed")
        self.assertEqual(self.application_outputs[1]["forecast_status"], "already-completed")
        self.assertEqual(self.application_outputs[1]["placement_status"], "already-completed")
        self.assertEqual(len(self.forecast_path.read_text(encoding="utf-8").splitlines()), 1)
        self.assertEqual(len(self.ledger_path.read_text(encoding="utf-8").splitlines()), 1)

    def test_real_paper_apply_placement_pending_recovery_returns_recovered_forms_once(self):
        """Exercise paper_apply's real post-ledger, pre-receipt crash path.

        This is intentionally not the already-completed replay above. The first
        runner invocation uses the existing receipt-finalization fault seam
        after the guarded ledger append. The second runner invocation reaches
        the durable placement-pending receipt, where paper_apply itself returns
        both recovered statuses without a second placement attempt.
        """
        original_write = paper_apply._atomic_write_receipt
        writes = 0

        def fail_only_completed_placement_receipt(path, receipt):
            nonlocal writes
            writes += 1
            # prepared -> forecast-pending -> forecast-recorded ->
            # placement-pending -> completed-placement
            if writes == 5:
                raise paper_apply.PaperApplyError("simulated placement receipt finalization failure")
            return original_write(path, receipt)

        with self.public_market(), patch.object(
            paper_apply, "_atomic_write_receipt", side_effect=fail_only_completed_placement_receipt
        ):
            first = self.run_runner(
                research=self._staged_research(disposition="bet"),
                application=self.production_apply(),
                budget=0,
            )
        self.assertEqual(first["cycle_state"], "application-pending")
        self.assertEqual(first["safe_reason"], "application-reconciliation-required")
        self.assertEqual(self.active_cycle()["counters"]["logical_applications"], 1)
        packet = prepare_packet(json.loads(self.fixture_path().read_text(encoding="utf-8")))
        receipt_path = self.staging_root / "apply" / packet["packet_id"] / f"{INTENT_ID}.json"
        self.assertEqual(json.loads(receipt_path.read_text(encoding="utf-8"))["state"], "placement-pending")
        self.assertEqual(len(self.forecast_path.read_text(encoding="utf-8").splitlines()), 1)
        self.assertEqual(len(self.ledger_path.read_text(encoding="utf-8").splitlines()), 1)

        with self.public_market(), patch.object(
            paper_apply, "_record_placement", side_effect=AssertionError("no second logical placement")
        ):
            recovered = self.run_runner(
                research=self._staged_research(disposition="bet"),
                application=self.production_apply(),
                budget=0,
            )
        self.assertEqual(recovered["cycle_state"], "completed")
        self.assertEqual(recovered["application_state"], "completed-placement")
        self.assertEqual(self.application_outputs[-1]["forecast_status"], "recovered")
        self.assertEqual(self.application_outputs[-1]["placement_status"], "recovered")
        self.assertEqual(recovered["logical_applications"], 1)
        self.assertEqual(len(self.calls["apply"]), 2)
        self.assertEqual(len(self.forecast_path.read_text(encoding="utf-8").splitlines()), 1)
        self.assertEqual(len(self.ledger_path.read_text(encoding="utf-8").splitlines()), 1)
        self.assert_repository_journals_unchanged()

    def test_real_paper_apply_placement_rejected_replay_is_terminal(self):
        result = self._replay_after_terminal_persist_crash(disposition="bet", prices=(0.50, 0.61), expect_state="failed-terminal")
        self.assertEqual(result["application_state"], "placement-rejected")
        self.assertEqual(result["safe_reason"], "paper-placement-rejected")
        self.assertEqual(len(self.forecast_path.read_text(encoding="utf-8").splitlines()), 1)
        self.assertFalse(self.ledger_path.exists())

    def test_real_paper_apply_forecast_rejected_maps_terminal_on_replay(self):
        with patch.object(paper_apply, "_record_forecast", side_effect=paper_apply.PaperApplyError("forced forecast failure")):
            first = self.run_runner(research=self._staged_research(), application=self.production_apply(), budget=0)
        self.assertEqual(first["cycle_state"], "application-pending")
        self.assertEqual(self.active_cycle()["counters"]["logical_applications"], 1)
        second = self.run_runner(research=self._staged_research(), application=self.production_apply(), budget=0)
        self.assertEqual(second["cycle_state"], "failed-terminal")
        self.assertEqual(second["application_state"], "forecast-rejected")
        self.assertEqual(second["safe_reason"], "paper-forecast-rejected")
        self.assertEqual(len(self.calls["apply"]), 2)
        self.assert_repository_journals_unchanged()

    def test_real_paper_apply_durable_error_maps_terminal(self):
        def error_receipt_apply(fixture_path, intent_id):
            # Establish fixed staging through the same production runner seam,
            # then write an existing durable terminal error receipt and invoke
            # actual paper_apply.run to return its authoritative result.
            fixture, fixture_sha256 = paper_apply._load_fixture(fixture_path)
            packet = prepare_packet(fixture)
            context = paper_apply._load_and_validate_staging(fixture_path, intent_id, self.staging_root)
            receipt = paper_apply._new_receipt(context, lambda: NOW)
            receipt["state"] = "error"
            paper_apply._atomic_write_receipt(context["receipt_path"], receipt)
            self.calls["apply"].append({"fixture": fixture_path, "intent_id": intent_id})
            return paper_apply.run(
                fixture_path, intent_id, _staging_root=self.staging_root,
                _forecast_path=self.forecast_path, _ledger_path=self.ledger_path,
                _lock_root=self.lock_root, _provenance_root=self.provenance_root,
                _now=lambda: NOW, _placement_now=NOW,
            )

        result = self.run_runner(research=self._staged_research(), application=error_receipt_apply, budget=0)
        self.assertEqual(result["cycle_state"], "failed-terminal")
        self.assertEqual(result["application_state"], "error")
        self.assertEqual(result["safe_reason"], "paper-application-error")
        self.assertFalse(self.forecast_path.exists())
        self.assertFalse(self.ledger_path.exists())
        self.assert_repository_journals_unchanged()

    def test_cli_is_bounded_and_runner_has_no_direct_execution_network_or_policy_route(self):
        parser = paper_runner.build_parser()
        options = {option for action in parser._actions for option in action.option_strings}
        self.assertEqual(options - {"-h", "--help"}, {"--dry-run", "--manus-task-budget", "--manus-soft-credit-ceiling"})
        for forbidden in ("--real", "--live", "--broker", "--ibkr", "--pearl", "--shell", "--runner-root", "--staging-root", "--journal-path", "--schedule"):
            with self.subTest(forbidden=forbidden), redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    paper_runner.main([forbidden, "x"])
        source = inspect.getsource(paper_runner)
        tree = ast.parse(source)
        imports = set()
        calls = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imports.add(node.module)
            elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                calls.add(node.func.id)
        self.assertFalse(any(name.startswith(("core.real", "core.forecast", "core.ledger")) for name in imports))
        self.assertFalse(imports & {"subprocess", "requests", "httpx", "aiohttp", "ctypes", "win32cred", "urllib"})
        self.assertFalse(calls & {"eval", "exec", "system", "open"})
        for forbidden in ("TASK_CREATE_ENDPOINT", "CREDENTIAL_TARGET", "journal/forecasts.jsonl", "journal/ledger.jsonl", "core.real", "IBKR", "Pearl", "gamma_market", "gamma_markets"):
            self.assertNotIn(forbidden, source)
        self.assertIn("research_transport.run", source)
        self.assertIn("paper_apply.run", source)
        self.assertIn("core_scan.scan_candidates", source)

    def test_cycle_lock_loser_performs_no_scan_or_runner_mutation(self):
        with paper_locks.acquire_cycle_lock(nonblocking=True, _lock_root=self.lock_root):
            with self.assertRaisesRegex(paper_runner.PaperRunnerError, "already running"):
                self.run_runner(budget=0)
        self.assertEqual(self.calls["scan"], [])
        self.assertFalse(self.runner_root.exists())

    def test_spawned_cycle_lock_loser_performs_no_scan_research_application_or_state_mutation(self):
        ready = self.spawn_context.Event()
        release = self.spawn_context.Event()
        holder = self.spawn_context.Process(target=_hold_cycle_lock, args=(str(self.lock_root), ready, release))
        holder.start()
        self.assertTrue(ready.wait(10), "holder did not acquire the cycle lock")
        self.addCleanup(self._stop_spawned_process, holder, release)

        result = self.spawn_context.Queue()
        loser = self.spawn_context.Process(
            target=_losing_runner_worker,
            args=(str(self.runner_root), str(self.lock_root), result),
        )
        loser.start()
        loser.join(20)
        if loser.is_alive():
            loser.terminate()
            loser.join(10)
        self.assertEqual(loser.exitcode, 0)
        status, message = result.get(timeout=5)
        self.assertEqual(status, "runner-error")
        self.assertIn("already running", message)
        self.assertEqual(self.calls["scan"], [])
        self.assertEqual(self.calls["research"], [])
        self.assertEqual(self.calls["apply"], [])
        self.assertFalse(self.runner_root.exists())
        self.assert_repository_journals_unchanged()

    @staticmethod
    def _stop_spawned_process(process, release) -> None:
        release.set()
        process.join(10)
        if process.is_alive():
            process.terminate()
            process.join(10)

    def test_runner_root_symlink_fails_closed_when_supported(self):
        target = self.root / "runner-target"
        target.mkdir()
        try:
            self.runner_root.symlink_to(target, target_is_directory=True)
        except OSError as exc:
            # A normal, non-elevated Windows account may lack only the specific
            # CreateSymbolicLink privilege. Windows Developer Mode/elevated
            # environments execute the actual production rejection assertion.
            if os.name == "nt" and getattr(exc, "winerror", None) == 1314:
                self.skipTest("Windows account lacks CreateSymbolicLink privilege (WinError 1314)")
            raise
        with self.assertRaisesRegex(paper_runner.PaperRunnerError, "runner root"):
            self.run_runner(candidates=[candidate("should-not-scan")], budget=0)
        self.assertEqual(self.calls["scan"], [])
        self.assertEqual(self.calls["research"], [])
        self.assertEqual(self.calls["apply"], [])
        self.assert_repository_journals_unchanged()

    def test_simulated_windows_reparse_runner_root_fails_before_scan(self):
        real_check = paper_locks.path_is_unsafe_indirection

        def reparse_root(path):
            return pathlib.Path(path) == self.runner_root or real_check(path)

        with patch.object(paper_locks, "path_is_unsafe_indirection", side_effect=reparse_root):
            with self.assertRaisesRegex(paper_runner.PaperRunnerError, "runner root"):
                self.run_runner(candidates=[candidate("should-not-scan")], budget=0)
        self.assertEqual(self.calls["scan"], [])
        self.assertEqual(self.calls["research"], [])
        self.assertEqual(self.calls["apply"], [])
        self.assert_repository_journals_unchanged()

    def test_simulated_windows_reparse_cycle_directory_rejects_active_cycle_before_rescan(self):
        self.run_runner(candidates=[candidate("existing")], budget=0)
        cycle_directory = self.fixture_path().parent
        self.calls["scan"].clear()
        self.calls["research"].clear()
        self.calls["apply"].clear()
        real_stat = pathlib.Path.stat

        def stat_with_reparse(path, *args, **kwargs):
            if pathlib.Path(path) == cycle_directory:
                return SimpleNamespace(st_file_attributes=0x400, st_mode=stat.S_IFDIR)
            return real_stat(path, *args, **kwargs)

        with patch.object(paper_locks.stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400, create=True), \
             patch.object(pathlib.Path, "stat", new=stat_with_reparse):
            with self.assertRaisesRegex(paper_runner.PaperRunnerError, "Cycle state is unavailable"):
                self.run_runner(candidates=[candidate("must-not-replace")], budget=0)
        self.assertEqual(self.calls["scan"], [])
        self.assertEqual(self.calls["research"], [])
        self.assertEqual(self.calls["apply"], [])
        self.assert_repository_journals_unchanged()

    def test_simulated_windows_reparse_cycle_directory_rejects_fixture_before_read(self):
        self.run_runner(candidates=[candidate("existing")], budget=0)
        fixture_path = self.fixture_path()
        cycle_directory = fixture_path.parent
        real_stat = pathlib.Path.stat

        def stat_with_reparse(path, *args, **kwargs):
            if pathlib.Path(path) == cycle_directory:
                return SimpleNamespace(st_file_attributes=0x400, st_mode=stat.S_IFDIR)
            return real_stat(path, *args, **kwargs)

        with patch.object(paper_locks.stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400, create=True), \
             patch.object(pathlib.Path, "stat", new=stat_with_reparse), \
             patch.object(pathlib.Path, "read_bytes", side_effect=AssertionError("fixture contents must not be read")):
            with self.assertRaisesRegex(paper_runner.PaperRunnerError, "Cycle fixture is unavailable"):
                paper_runner._load_fixture(fixture_path)
        self.assert_repository_journals_unchanged()


if __name__ == "__main__":
    unittest.main()

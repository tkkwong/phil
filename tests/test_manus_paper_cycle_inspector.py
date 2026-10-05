"""Offline regressions for the strictly read-only PAPER cycle inspector."""
from __future__ import annotations

import ast
import datetime as dt
import hashlib
import inspect
import io
import json
import pathlib
import tempfile
import unittest
import uuid
from contextlib import redirect_stderr, redirect_stdout
from unittest.mock import patch

from manus import paper_cycle_inspector, paper_runner
from manus.paper_cycle_guardian import prepare_packet


CYCLE_ID = "123e4567-e89b-42d3-a456-426614174000"
OTHER_CYCLE_ID = "223e4567-e89b-42d3-a456-426614174000"
NOW = dt.datetime(2026, 9, 29, 12, 0, 0, tzinfo=dt.timezone.utc)
FLOOR_MINUTES = paper_runner.core_scan.PROTECTED["min_minutes_to_resolution"]


def provider_metadata(tag_ids=("1",)):
    return {
        "market_tags_status": "ok",
        "market_tags": [
            {"id": tag_id, "slug": f"tag-{tag_id}", "label": f"Label {tag_id}"}
            for tag_id in tag_ids
        ],
    }


def candidate(market_id: str, *, end_date: str = "2026-10-01T00:00:00.000Z") -> dict:
    return {
        "market_id": market_id,
        "question": f"Will {market_id} happen?",
        "end_date": end_date,
        "event_id": f"event-{market_id}",
        "event_slug": f"event-{market_id}",
        "outcomes": ["Yes", "No"],
        "outcome_prices": [0.5, 0.5],
        "clob_token_ids": [f"yes-{market_id}", f"no-{market_id}"],
        "volume_24h": 100.0,
        "liquidity": 100.0,
        "slug": f"slug-{market_id}",
        "description": f"Synthetic protected candidate {market_id}.",
        "provider_metadata": provider_metadata(("1",)),
    }


class PaperCycleInspectorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.root = pathlib.Path(self.temporary_directory.name)
        self.runner_root = self.root / "external-runner"
        self.repository_journals = [
            pathlib.Path(paper_runner._repository_root()) / "journal" / "forecasts.jsonl",
            pathlib.Path(paper_runner._repository_root()) / "journal" / "ledger.jsonl",
        ]
        self.journals_before = [
            hashlib.sha256(path.read_bytes()).hexdigest() for path in self.repository_journals
        ]

    def assert_repository_journals_unchanged(self) -> None:
        after = [hashlib.sha256(path.read_bytes()).hexdigest() for path in self.repository_journals]
        self.assertEqual(after, self.journals_before)

    # -- seeding helpers (tests may mutate their own temporary roots) --------

    def seed_fixture(self, candidates: list[dict]) -> tuple[dict, str]:
        fixture, _metadata = paper_runner._fixture_from_scan(candidates, lambda: NOW)
        fixture_path = paper_runner._fixture_path(self.runner_root, CYCLE_ID)
        fixture_path.parent.mkdir(parents=True, exist_ok=True)
        fixture_sha256 = paper_runner._write_fixture(fixture_path, fixture)
        return fixture, fixture_sha256

    def seed_selected_cycle(self, candidates: list[dict], *, state: str = "research-pending") -> dict:
        fixture, fixture_sha256 = self.seed_fixture(candidates)
        packet = prepare_packet(fixture)
        frozen = packet["candidates"][0]
        cycle = paper_runner._new_cycle(CYCLE_ID, lambda: NOW, None)
        evidence = paper_runner._selection_evidence(frozen["market_id"], provider_metadata(("1",)))
        cycle.update(
            {
                "state": "selected",
                "fixture_sha256": fixture_sha256,
                "packet_id": packet["packet_id"],
                "candidate_id": frozen["candidate_id"],
                "market_id": frozen["market_id"],
                "selection_evidence": evidence,
                "selection_evidence_sha256": paper_runner._selection_evidence_sha256(evidence),
                "safe_reason": "none",
                "counters": {"candidates_selected": 1, "new_manus_tasks": 0, "logical_applications": 0},
            }
        )
        if state != "selected":
            cycle["state"] = state
        return paper_runner._persist_cycle(self.runner_root, cycle, lambda: NOW)

    def seed_research_pending_cycle(self, candidates: list[dict], *, fixture: bool = True) -> dict:
        if not fixture:
            cycle = paper_runner._new_cycle(CYCLE_ID, lambda: NOW, None)
            fixture_document, _ = paper_runner._fixture_from_scan(candidates, lambda: NOW)
            packet = prepare_packet(fixture_document)
            evidence = paper_runner._selection_evidence(candidates[0]["market_id"], provider_metadata(("1",)))
            cycle.update(
                {
                    "state": "research-pending",
                    "fixture_sha256": "0" * 64,
                    "packet_id": packet["packet_id"],
                    "candidate_id": packet["candidates"][0]["candidate_id"],
                    "market_id": candidates[0]["market_id"],
                    "selection_evidence": evidence,
                    "selection_evidence_sha256": paper_runner._selection_evidence_sha256(evidence),
                    "safe_reason": "none",
                    "counters": {"candidates_selected": 1, "new_manus_tasks": 0, "logical_applications": 0},
                }
            )
            return paper_runner._persist_cycle(self.runner_root, cycle, lambda: NOW)
        return self.seed_selected_cycle(candidates, state="research-pending")

    def cycle_file(self) -> pathlib.Path:
        return paper_runner._cycle_path(self.runner_root, CYCLE_ID)

    def file_hashes(self) -> dict[str, str]:
        paths = {
            "cycle": self.cycle_file(),
            "fixture": paper_runner._fixture_path(self.runner_root, CYCLE_ID),
            "pointer": paper_runner._active_pointer_path(self.runner_root),
        }
        return {name: hashlib.sha256(path.read_bytes()).hexdigest() for name, path in paths.items() if path.exists()}

    # -- required regressions ------------------------------------------------

    def test_no_active_pointer_returns_safe_empty_status_and_creates_nothing(self):
        result = paper_cycle_inspector.status(now=lambda: NOW, _runner_root=self.runner_root)
        self.assertEqual(result["active_cycle_id"], None)
        self.assertEqual(result["cycle_state"], None)
        self.assertEqual(result["researchable_now"], None)
        self.assertEqual(result["current_time_utc"], "2026-09-29T12:00:00Z")
        self.assertEqual(
            result["minimum_researchable_end_date"],
            (NOW + dt.timedelta(minutes=FLOOR_MINUTES)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        )
        self.assertFalse(self.runner_root.exists())

    def test_valid_research_pending_fresh_candidate_reports_correct_binding(self):
        self.seed_research_pending_cycle([candidate("100")])
        result = paper_cycle_inspector.status(now=lambda: NOW, _runner_root=self.runner_root)
        self.assertEqual(result["active_cycle_id"], CYCLE_ID)
        self.assertEqual(result["cycle_state"], "research-pending")
        self.assertEqual(result["market_id"], "100")
        # The guardian normalizes fixture end dates to whole-second UTC form.
        self.assertEqual(result["candidate_end_date"], "2026-10-01T00:00:00Z")
        self.assertTrue(result["researchable_now"])
        self.assertEqual(result["new_manus_tasks"], 0)
        self.assertEqual(result["logical_applications"], 0)
        self.assertEqual(result["intent_id"], None)
        self.assertEqual(result["task_id"], None)

    def test_valid_research_pending_stale_candidate_reports_false_without_mutation(self):
        self.seed_research_pending_cycle([candidate("stale", end_date="2026-09-29T12:19:59.000Z")])
        before = self.file_hashes()
        result = paper_cycle_inspector.status(now=lambda: NOW, _runner_root=self.runner_root)
        self.assertFalse(result["researchable_now"])
        self.assertEqual(self.file_hashes(), before)
        self.assertEqual(
            self.cycle_file().read_bytes(),
            (self.runner_root / "cycles" / CYCLE_ID / "cycle.json").read_bytes(),
        )

    def test_boundary_end_date_equals_protected_floor_is_researchable(self):
        boundary = (NOW + dt.timedelta(minutes=FLOOR_MINUTES)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        self.seed_research_pending_cycle([candidate("boundary", end_date=boundary)])
        result = paper_cycle_inspector.status(now=lambda: NOW, _runner_root=self.runner_root)
        self.assertTrue(result["researchable_now"])
        self.assertEqual(result["candidate_end_date"], "2026-09-29T12:20:00Z")

    def test_candidate_identity_mismatch_with_frozen_fixture_fails_closed(self):
        # The persisted cycle references a candidate the frozen fixture never
        # contained; inspection must fail closed without substitution.
        self.seed_research_pending_cycle([candidate("real")])
        cycle_path = self.cycle_file()
        cycle = json.loads(cycle_path.read_text(encoding="utf-8"))
        cycle["market_id"] = "ghost"
        evidence = paper_runner._selection_evidence("ghost", provider_metadata(("1",)))
        cycle["selection_evidence"] = evidence
        cycle["selection_evidence_sha256"] = paper_runner._selection_evidence_sha256(evidence)
        cycle_path.write_text(json.dumps(cycle), encoding="utf-8")
        with self.assertRaisesRegex(
            paper_cycle_inspector.PaperCycleInspectorError, "absent from the frozen fixture"
        ):
            paper_cycle_inspector.status(now=lambda: NOW, _runner_root=self.runner_root)

    def test_malformed_active_pointer_fails_closed(self):
        self.runner_root.mkdir(parents=True)
        pointer = paper_runner._active_pointer_path(self.runner_root)
        pointer.parent.mkdir(parents=True, exist_ok=True)
        pointer.write_text('{"runner_version": "paper-runner/v2"}', encoding="utf-8")
        with self.assertRaisesRegex(paper_cycle_inspector.PaperCycleInspectorError, "malformed"):
            paper_cycle_inspector.status(now=lambda: NOW, _runner_root=self.runner_root)

    def test_malformed_cycle_json_fails_closed_on_explicit_lookup(self):
        self.runner_root.mkdir(parents=True)
        cycle_path = self.cycle_file()
        cycle_path.parent.mkdir(parents=True, exist_ok=True)
        cycle_path.write_text("{not json", encoding="utf-8")
        with self.assertRaisesRegex(paper_cycle_inspector.PaperCycleInspectorError, "malformed"):
            paper_cycle_inspector.cycle_status(CYCLE_ID, now=lambda: NOW, _runner_root=self.runner_root)

    def test_missing_fixture_fails_closed_where_fixture_is_required(self):
        self.seed_research_pending_cycle([candidate("100")])
        paper_runner._fixture_path(self.runner_root, CYCLE_ID).unlink()
        with self.assertRaisesRegex(paper_cycle_inspector.PaperCycleInspectorError, "fixture"):
            paper_cycle_inspector.status(now=lambda: NOW, _runner_root=self.runner_root)

    def test_fixture_hash_mismatch_fails_closed(self):
        self.seed_research_pending_cycle([candidate("100")])
        fixture_path = paper_runner._fixture_path(self.runner_root, CYCLE_ID)
        document = json.loads(fixture_path.read_text(encoding="utf-8"))
        document["candidates"][0]["question"] = "Tampered question?"
        fixture_path.write_text(json.dumps(document), encoding="utf-8")
        with self.assertRaisesRegex(paper_cycle_inspector.PaperCycleInspectorError, "provenance conflicts"):
            paper_cycle_inspector.status(now=lambda: NOW, _runner_root=self.runner_root)

    def test_unsafe_indirection_fails_closed_using_existing_path_safety(self):
        # Runner root behind a symlink is rejected by the shared runner
        # validation before any file is read.
        link_root = self.root / "linked-runner"
        link_root.symlink_to(self.root / "elsewhere")
        with self.assertRaises(paper_cycle_inspector.PaperCycleInspectorError):
            paper_cycle_inspector.status(now=lambda: NOW, _runner_root=link_root)
        # A symlinked cycle file is rejected by the shared regular-file read.
        self.seed_research_pending_cycle([candidate("100")])
        real_cycle = self.cycle_file()
        cycle_bytes = real_cycle.read_bytes()
        external = self.root / "external-cycle.json"
        external.write_bytes(cycle_bytes)
        real_cycle.unlink()
        real_cycle.symlink_to(external)
        with self.assertRaises(paper_cycle_inspector.PaperCycleInspectorError):
            paper_cycle_inspector.status(now=lambda: NOW, _runner_root=self.runner_root)

    def test_inspector_never_calls_protected_mutation_or_network_surfaces(self):
        self.seed_research_pending_cycle([candidate("100")])
        forbidden = [
            ("core.scan.scan_candidates", "scan_candidates"),
            ("core.pmapi.gamma_markets", "gamma_markets"),
            ("core.pmapi.gamma_market_tags", "gamma_market_tags"),
            ("manus.research_transport.run", "run"),
            ("manus.paper_apply.run", "run"),
            ("core.forecast", "record_forecast"),
            ("core.ledger", "place_paper_order"),
            ("paper_runner.run", "run"),
        ]
        with patch("core.scan.scan_candidates") as scan_call, \
             patch("core.pmapi.gamma_markets") as gamma_markets, \
             patch("core.pmapi.gamma_market_tags") as gamma_tags, \
             patch("manus.research_transport.run") as research_run, \
             patch("manus.paper_apply.run") as apply_run, \
             patch("manus.paper_runner.run") as runner_run:
            first = paper_cycle_inspector.status(now=lambda: NOW, _runner_root=self.runner_root)
            second = paper_cycle_inspector.status(now=lambda: NOW, _runner_root=self.runner_root)
        self.assertEqual(first, second)
        for call in (scan_call, gamma_markets, gamma_tags, research_run, apply_run, runner_run):
            call.assert_not_called()
        # Static source audit: no forbidden imports or mutating call shapes.
        source = inspect.getsource(paper_cycle_inspector)
        tree = ast.parse(source)
        imports = set()
        calls = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imports.add(node.module)
            elif isinstance(node, ast.Call):
                function = node.func
                if isinstance(function, ast.Attribute):
                    calls.add(function.attr)
                elif isinstance(function, ast.Name):
                    calls.add(function.id)
        self.assertFalse(imports & {"subprocess", "requests", "httpx", "aiohttp", "urllib", "socket", "ctypes", "win32cred", "schedgui"})
        self.assertFalse(calls & {"mkdir", "unlink", "rmtree", "write_bytes", "write_text", "system", "Popen", "acquire_cycle_lock"})
        self.assertNotIn("paper_runner.run(", source)
        self.assertNotIn("research_transport.run", source)
        self.assertNotIn("paper_apply.run", source)

    def test_repository_journals_remain_byte_identical(self):
        self.seed_research_pending_cycle([candidate("100")])
        paper_cycle_inspector.status(now=lambda: NOW, _runner_root=self.runner_root)
        paper_cycle_inspector.cycle_status(CYCLE_ID, now=lambda: NOW, _runner_root=self.runner_root)
        self.assert_repository_journals_unchanged()

    def test_inspection_changes_no_persisted_bytes(self):
        self.seed_research_pending_cycle([candidate("100")])
        before = self.file_hashes()
        paper_cycle_inspector.status(now=lambda: NOW, _runner_root=self.runner_root)
        paper_cycle_inspector.cycle_status(CYCLE_ID, now=lambda: NOW, _runner_root=self.runner_root)
        self.assertEqual(self.file_hashes(), before)

    def test_repeated_inspection_is_idempotent_for_one_clock(self):
        self.seed_research_pending_cycle([candidate("100")])
        first = paper_cycle_inspector.status(now=lambda: NOW, _runner_root=self.runner_root)
        second = paper_cycle_inspector.status(now=lambda: NOW, _runner_root=self.runner_root)
        third = paper_cycle_inspector.cycle_status(CYCLE_ID, now=lambda: NOW, _runner_root=self.runner_root)
        self.assertEqual(first, second)
        self.assertEqual(first["active_cycle_id"], third["active_cycle_id"])
        self.assertEqual(first["researchable_now"], third["researchable_now"])
        self.assertTrue(third["is_active_cycle"])

    # -- CLI restrictions ----------------------------------------------------

    def test_cli_exposes_only_read_only_operations(self):
        parser = paper_cycle_inspector.build_parser()
        operations = {
            action.dest for action in parser._actions
            if getattr(action, "dest", None) == "operation"
        }
        self.assertEqual(operations, {"operation"})
        help_text = parser.format_help()
        for operation in ("status", "cycle"):
            self.assertIn(operation, help_text)
        for forbidden in (
            "--research-budget", "--credit-ceiling", "--rescan", "--resume",
            "--repair", "--apply", "--record", "--placement", "--delete",
            "--reset", "--enable", "--disable", "--run",
        ):
            with self.subTest(forbidden=forbidden), redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    paper_cycle_inspector.main([forbidden])
        with redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                paper_cycle_inspector.main(["--runner-root", "x"])
        with redirect_stderr(io.StringIO()):
            with self.assertRaises(SystemExit):
                paper_cycle_inspector.main(["mutate"])

    def test_cli_prints_canonical_json_and_fails_closed_on_error(self):
        self.seed_research_pending_cycle([candidate("100")])
        stdout = io.StringIO()
        # The production CLI resolves the fixed runtime root through the same
        # seam the runner uses; tests inject the temporary root there.
        with patch.object(paper_runner, "_resolve_runner_root", return_value=self.runner_root), \
             redirect_stdout(stdout):
            exit_code = paper_cycle_inspector.main(["status"])
        self.assertEqual(exit_code, 0)
        document = json.loads(stdout.getvalue())
        self.assertEqual(document["active_cycle_id"], CYCLE_ID)
        self.assertEqual(
            stdout.getvalue().strip(),
            json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False),
        )
        stderr = io.StringIO()
        with patch.object(paper_runner, "_resolve_runner_root", return_value=self.runner_root), \
             redirect_stderr(stderr):
            exit_code = paper_cycle_inspector.main(["cycle", "00000000-0000-4000-8000-000000000000"])
        self.assertEqual(exit_code, 1)
        self.assertIn("inspector:", stderr.getvalue())
        self.assertNotIn(str(self.runner_root), stderr.getvalue())

    def test_invalid_clock_fails_closed(self):
        with self.assertRaisesRegex(paper_cycle_inspector.PaperCycleInspectorError, "clock"):
            paper_cycle_inspector.status(now=lambda: dt.datetime(2026, 9, 29, 12, tzinfo=None), _runner_root=self.runner_root)


def sys_module():
    import sys

    return sys


if __name__ == "__main__":
    unittest.main()

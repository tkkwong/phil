"""Offline regressions for the disabled-by-default scheduled PAPER wrapper."""
from __future__ import annotations

import ast
import datetime as dt
import hashlib
import inspect
import io
import json
import os
import pathlib
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from types import SimpleNamespace
from unittest.mock import Mock, patch

from manus import paper_locks, paper_runner, scheduled_paper


NOW = dt.datetime(2026, 9, 30, 12, tzinfo=dt.timezone.utc)
CYCLE_ID = "123e4567-e89b-42d3-a456-426614174000"
INTENT_ID = "223e4567-e89b-42d3-a456-426614174000"


def runner_result(**changes):
    value = {
        "mode": "PAPER",
        "dry_run": False,
        "cycle_id": CYCLE_ID,
        "cycle_state": "completed",
        "candidate_id": "candidate-1",
        "market_id": "market-1",
        "task_id": "known-task",
        "intent_id": INTENT_ID,
        "application_state": "completed-no-placement",
        "forecast_id": "forecast-1",
        "placement_id": None,
        "new_manus_tasks": 0,
        "logical_applications": 1,
        "forecasts_recorded": 1,
        "placements_recorded": 0,
        "safe_reason": "none",
    }
    value.update(changes)
    return value


class ScheduledPaperTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.root = pathlib.Path(self.temporary_directory.name)
        self.local_appdata = self.root / "local-appdata"
        self.scheduler_root = self.local_appdata / "phil-manus" / "scheduler"
        self.runner = Mock(return_value=runner_result())
        repository = pathlib.Path(__file__).resolve().parents[1]
        self.repository_journals = [repository / "journal" / "forecasts.jsonl", repository / "journal" / "ledger.jsonl"]
        self.repository_before = [
            (hashlib.sha256(path.read_bytes()).hexdigest(), len(path.read_text(encoding="utf-8").splitlines()))
            for path in self.repository_journals
        ]

    def assert_repository_journals_unchanged(self) -> None:
        after = [
            (hashlib.sha256(path.read_bytes()).hexdigest(), len(path.read_text(encoding="utf-8").splitlines()))
            for path in self.repository_journals
        ]
        self.assertEqual(after, self.repository_before)

    def marker_path(self) -> pathlib.Path:
        return self.scheduler_root / "enabled.json"

    def last_run_path(self) -> pathlib.Path:
        return self.scheduler_root / "last-run.json"

    def write_marker(self, enabled: bool = True, **extra) -> None:
        self.scheduler_root.mkdir(parents=True, exist_ok=True)
        document = {"scheduler_version": scheduled_paper.SCHEDULER_VERSION, "enabled": enabled, **extra}
        self.marker_path().write_text(json.dumps(document), encoding="utf-8")

    def call(self):
        return scheduled_paper.run(
            _scheduler_root=self.scheduler_root,
            _runner_run=self.runner,
            _now=lambda: NOW,
        )

    def test_disabled_by_default_does_not_create_state_or_invoke_runner(self):
        result = self.call()
        self.assertEqual(result, {"scheduler_version": scheduled_paper.SCHEDULER_VERSION, "outcome": "disabled", "enabled": False})
        self.runner.assert_not_called()
        self.assertFalse(self.scheduler_root.exists())
        self.assert_repository_journals_unchanged()

    def test_explicit_disabled_marker_does_not_invoke_runner_or_write_result(self):
        self.write_marker(False)
        result = self.call()
        self.assertFalse(result["enabled"])
        self.runner.assert_not_called()
        self.assertFalse(self.last_run_path().exists())
        self.assert_repository_journals_unchanged()

    def test_exact_enabled_marker_calls_runner_once_with_permanently_zero_authority(self):
        self.write_marker(True)
        result = self.call()
        self.runner.assert_called_once_with(
            dry_run=False,
            manus_task_budget=0,
            manus_soft_credit_ceiling=None,
        )
        self.assertEqual(result["outcome"], "runner-result")
        self.assertEqual(json.loads(self.last_run_path().read_text(encoding="utf-8")), result)
        self.assert_repository_journals_unchanged()

    def test_research_pending_manual_authorization_is_safely_persisted_without_second_call(self):
        self.write_marker(True)
        self.runner.return_value = runner_result(
            cycle_state="research-pending",
            intent_id=None,
            application_state=None,
            forecast_id=None,
            logical_applications=0,
            forecasts_recorded=0,
            safe_reason="current-invocation-research-authorization-required",
        )
        result = self.call()
        self.assertEqual(result["cycle_state"], "research-pending")
        self.assertEqual(result["safe_reason"], "current-invocation-research-authorization-required")
        self.runner.assert_called_once()
        self.assertEqual(json.loads(self.last_run_path().read_text(encoding="utf-8"))["new_manus_tasks"], 0)

    def test_completed_result_writes_only_whitelisted_safe_scalars(self):
        self.write_marker(True)
        self.runner.return_value = runner_result(secret="must-not-persist", rationale="must-not-persist", description="must-not-persist")
        result = self.call()
        persisted = json.loads(self.last_run_path().read_text(encoding="utf-8"))
        expected = {
            "scheduler_version", "invocation_time", "outcome", "cycle_id", "cycle_state", "market_id", "task_id",
            "intent_id", "application_state", "forecast_id", "placement_id", "new_manus_tasks", "logical_applications",
            "forecasts_recorded", "placements_recorded", "safe_reason",
        }
        self.assertEqual(set(result), expected)
        self.assertEqual(set(persisted), expected)
        encoded = self.last_run_path().read_text(encoding="utf-8")
        self.assertNotIn("must-not-persist", encoded)
        self.assertNotIn("secret", encoded)

    def test_known_runner_failure_writes_generic_safe_failure_once_without_raw_message(self):
        self.write_marker(True)
        self.runner.side_effect = paper_runner.PaperRunnerError("secret credential diagnostic")
        with self.assertRaisesRegex(scheduled_paper.ScheduledPaperError, "failed closed"):
            self.call()
        self.runner.assert_called_once()
        persisted = self.last_run_path().read_text(encoding="utf-8")
        self.assertEqual(json.loads(persisted)["outcome"], "runner-failed-closed")
        self.assertNotIn("secret credential diagnostic", persisted)

    def test_unexpected_runner_failure_writes_generic_safe_failure_once_without_raw_message(self):
        self.write_marker(True)
        self.runner.side_effect = RuntimeError("unexpected sensitive provider body")
        with self.assertRaisesRegex(scheduled_paper.ScheduledPaperError, "failed closed"):
            self.call()
        self.runner.assert_called_once()
        persisted = self.last_run_path().read_text(encoding="utf-8")
        self.assertEqual(json.loads(persisted)["outcome"], "runner-failed-closed")
        self.assertNotIn("unexpected sensitive provider body", persisted)

    def test_nonzero_new_manus_tasks_is_safety_failure_without_retry_or_success(self):
        self.write_marker(True)
        self.runner.return_value = runner_result(new_manus_tasks=1)
        with self.assertRaisesRegex(scheduled_paper.ScheduledPaperSafetyError, "prohibited"):
            self.call()
        self.runner.assert_called_once()
        persisted = json.loads(self.last_run_path().read_text(encoding="utf-8"))
        self.assertEqual(persisted["outcome"], "scheduler-safety-failure")
        self.assertNotEqual(persisted["outcome"], "runner-result")

    def test_malformed_duplicate_unknown_and_oversized_markers_fail_closed_before_runner(self):
        cases = (
            b"not-json",
            b'{"scheduler_version":"scheduled-paper/v1","scheduler_version":"scheduled-paper/v1","enabled":true}',
            b'{"scheduler_version":"scheduled-paper/v1","enabled":true,"unknown":false}',
            b'{"scheduler_version":"scheduled-paper/v1","enabled":1}',
            json.dumps({"scheduler_version": scheduled_paper.SCHEDULER_VERSION, "enabled": True, "padding": "x" * 2048}).encode("utf-8"),
        )
        for raw in cases:
            with self.subTest(raw=raw[:32]):
                self.scheduler_root.mkdir(parents=True, exist_ok=True)
                self.marker_path().write_bytes(raw)
                with self.assertRaises(scheduled_paper.ScheduledPaperError):
                    self.call()
                self.runner.assert_not_called()
                self.assertFalse(self.last_run_path().exists())

    def test_oversized_marker_is_rejected_by_bounded_read_before_runner(self):
        self.write_marker(True)
        marker = self.marker_path()
        read_sizes: list[int] = []
        real_open = pathlib.Path.open

        class BoundedReader(io.BytesIO):
            def read(self, size=-1):
                read_sizes.append(size)
                return super().read(size)

        def marker_open(path, *args, **kwargs):
            if pathlib.Path(path) == marker:
                return BoundedReader(b"x" * (scheduled_paper._MARKER_MAX_BYTES + 100))
            return real_open(path, *args, **kwargs)

        with patch.object(pathlib.Path, "open", new=marker_open):
            with self.assertRaises(scheduled_paper.ScheduledPaperError):
                self.call()
        self.assertEqual(read_sizes, [scheduled_paper._MARKER_MAX_BYTES + 1])
        self.runner.assert_not_called()
        self.assertFalse(self.last_run_path().exists())

    def test_symlink_marker_and_root_fail_closed_where_supported(self):
        target = self.root / "target"
        target.mkdir()
        marker_target = target / "enabled.json"
        marker_target.write_text(json.dumps({"scheduler_version": scheduled_paper.SCHEDULER_VERSION, "enabled": True}), encoding="utf-8")
        self.scheduler_root.mkdir(parents=True, exist_ok=True)
        try:
            self.marker_path().symlink_to(marker_target)
        except OSError as exc:
            if os.name == "nt" and getattr(exc, "winerror", None) == 1314:
                self.skipTest("Windows account lacks CreateSymbolicLink privilege (WinError 1314)")
            raise
        with self.assertRaises(scheduled_paper.ScheduledPaperError):
            self.call()
        self.runner.assert_not_called()

        self.marker_path().unlink()
        root_link = self.root / "linked-scheduler"
        try:
            root_link.symlink_to(target, target_is_directory=True)
        except OSError as exc:
            if os.name == "nt" and getattr(exc, "winerror", None) == 1314:
                self.skipTest("Windows account lacks CreateSymbolicLink privilege (WinError 1314)")
            raise
        with self.assertRaises(scheduled_paper.ScheduledPaperError):
            scheduled_paper.run(_scheduler_root=root_link, _runner_run=self.runner, _now=lambda: NOW)
        self.runner.assert_not_called()

    def test_simulated_windows_reparse_root_fails_closed_before_runner(self):
        self.write_marker(True)
        real_check = paper_locks.path_is_unsafe_indirection

        def reparse_root(path):
            return pathlib.Path(path) == self.scheduler_root or real_check(path)

        with patch.object(paper_locks, "path_is_unsafe_indirection", side_effect=reparse_root):
            with self.assertRaises(scheduled_paper.ScheduledPaperError):
                self.call()
        self.runner.assert_not_called()

    def test_status_only_reads_fixed_marker_without_runner_or_state_creation(self):
        result = scheduled_paper.status(_scheduler_root=self.scheduler_root)
        self.assertEqual(result["outcome"], "disabled")
        self.runner.assert_not_called()
        self.assertFalse(self.scheduler_root.exists())
        self.write_marker(True)
        result = scheduled_paper.status(_scheduler_root=self.scheduler_root)
        self.assertEqual(result, {"scheduler_version": scheduled_paper.SCHEDULER_VERSION, "outcome": "enabled", "enabled": True})
        self.runner.assert_not_called()
        self.assertFalse(self.last_run_path().exists())

    def test_cli_exposes_status_only_and_rejects_forbidden_options_before_runner(self):
        forbidden = (
            "--enable", "--disable", "--budget=1", "--manus-task-budget=1", "--manus-soft-credit-ceiling=10",
            "--dry-run", "--real", "--live", "--broker", "--ibkr", "--pearl", "--runner-root=x",
            "--scheduler-root=x", "--staging-root=x", "--journal-path=x", "--schedule=x", "--interval=1", "--retry",
        )
        for option in forbidden:
            with self.subTest(option=option):
                with self.assertRaises(SystemExit) as result:
                    scheduled_paper.main([option])
                self.assertEqual(result.exception.code, 2)
        with patch.dict(os.environ, {"LOCALAPPDATA": str(self.local_appdata)}, clear=False), \
             patch.object(scheduled_paper, "paper_runner") as protected_runner, \
             redirect_stdout(io.StringIO()) as output:
            self.assertEqual(scheduled_paper.main(["--status"]), 0)
        protected_runner.run.assert_not_called()
        self.assertEqual(json.loads(output.getvalue())["outcome"], "disabled")
        self.assertFalse(self.scheduler_root.exists())

    def test_cli_runner_failure_redacts_sensitive_exception_from_stdout_and_stderr(self):
        self.write_marker(True)
        sensitive = "SCHEDULED-PAPER-UNIQUE-SENSITIVE-DIAGNOSTIC"
        self.runner.side_effect = paper_runner.PaperRunnerError(sensitive)
        real_run = scheduled_paper.run

        def production_run():
            return real_run(_runner_run=self.runner, _now=lambda: NOW)

        stdout, stderr = io.StringIO(), io.StringIO()
        with patch.dict(os.environ, {"LOCALAPPDATA": str(self.local_appdata)}, clear=False), \
             patch.object(scheduled_paper, "run", side_effect=production_run) as protected_run, \
             redirect_stdout(stdout), redirect_stderr(stderr):
            with self.assertRaises(SystemExit) as result:
                scheduled_paper.main([])
        self.assertEqual(result.exception.code, 2)
        protected_run.assert_called_once_with()
        self.runner.assert_called_once_with(
            dry_run=False,
            manus_task_budget=0,
            manus_soft_credit_ceiling=None,
        )
        self.assertNotIn(sensitive, stdout.getvalue())
        self.assertNotIn(sensitive, stderr.getvalue())
        persisted = self.last_run_path().read_text(encoding="utf-8")
        self.assertEqual(json.loads(persisted)["outcome"], "runner-failed-closed")
        self.assertNotIn(sensitive, persisted)

    def test_last_run_atomic_failure_preserves_previous_document_and_cleans_temp_file(self):
        self.write_marker(True)
        original = b'{"old":"summary"}\n'
        self.last_run_path().write_bytes(original)
        with patch.object(scheduled_paper.os, "replace", side_effect=OSError("simulated")):
            with self.assertRaises(scheduled_paper.ScheduledPaperError):
                self.call()
        self.assertEqual(self.last_run_path().read_bytes(), original)
        self.assertEqual(list(self.scheduler_root.glob(".last-run.json.*.tmp")), [])

    def test_static_isolation_has_only_runner_cycle_dependency_and_no_scheduling_or_network_route(self):
        source = inspect.getsource(scheduled_paper)
        tree = ast.parse(source)
        imports, calls = set(), set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imports.add(node.module)
            elif isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                calls.add(node.func.id)
        forbidden_imports = {"subprocess", "ctypes", "urllib", "requests", "httpx", "research_transport", "paper_apply", "core.scan"}
        self.assertFalse(imports & forbidden_imports)
        self.assertFalse(calls & {"eval", "exec", "system", "sleep", "__import__"})
        for forbidden in ("schtasks", "Register-ScheduledTask", "TaskScheduler", "PowerShell", "IBKR", "Pearl", "wallet"):
            self.assertNotIn(forbidden, source)
        self.assertIn("paper_runner.run", source)

    def test_wrapper_never_mutates_repository_or_journals(self):
        self.write_marker(True)
        self.call()
        self.assert_repository_journals_unchanged()
        repository = pathlib.Path(__file__).resolve().parents[1]
        self.assertFalse((repository / "runtime" / "manus").exists())


if __name__ == "__main__":
    unittest.main()

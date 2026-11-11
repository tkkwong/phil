"""Offline, mocked regressions for fixed Windows PAPER task controls."""
from __future__ import annotations

import ast
import datetime as dt
import hashlib
import inspect
import io
import json
import os
import pathlib
import subprocess
import sys
import tempfile
import unittest
import xml.etree.ElementTree as element_tree
from contextlib import redirect_stderr, redirect_stdout
from types import SimpleNamespace
from unittest.mock import Mock, patch

from manus import paper_locks, scheduler_admin


NOW = dt.datetime(2026, 10, 1, 12, 34, 56, tzinfo=dt.timezone.utc)
XML_NAMESPACE = {"task": "http://schemas.microsoft.com/windows/2004/02/mit/task"}


class SchedulerAdminTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary_directory.cleanup)
        self.root = pathlib.Path(self.temporary_directory.name)
        self.scheduler_root = self.root / "external-state" / "phil-manus" / "scheduler"
        self.system_root = self.root / "Windows"
        self.schtasks = self.system_root / "System32" / "schtasks.exe"
        self.schtasks.parent.mkdir(parents=True)
        self.schtasks.write_bytes(b"offline mock only\n")
        self.environment = {
            "SystemRoot": str(self.system_root),
            "USERDOMAIN": "TESTDOMAIN",
            "USERNAME": "operator",
        }
        self.subprocess_run = Mock(return_value=SimpleNamespace(returncode=0, stdout=b"sensitive output", stderr=b"sensitive error"))
        repository = pathlib.Path(__file__).resolve().parents[1]
        self.repository = repository
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

    def write_marker(self, enabled: bool) -> None:
        self.scheduler_root.mkdir(parents=True, exist_ok=True)
        self.marker_path().write_text(
            json.dumps({"scheduler_version": "scheduled-paper/v1", "enabled": enabled}),
            encoding="utf-8",
        )

    def marker_enabled(self) -> bool:
        return json.loads(self.marker_path().read_text(encoding="utf-8"))["enabled"]

    def generated_xml_paths(self) -> list[pathlib.Path]:
        return list(self.scheduler_root.glob(f"{scheduler_admin._TASK_XML_PREFIX}*{scheduler_admin._TASK_XML_SUFFIX}"))

    def install(self, **changes):
        values = {
            "_scheduler_root": self.scheduler_root,
            "_environment": self.environment,
            "_subprocess_run": self.subprocess_run,
            "_now": lambda: NOW,
        }
        values.update(changes)
        return scheduler_admin.install(**values)

    def assert_schtasks_call(self, expected_arguments: list[str]) -> None:
        self.subprocess_run.assert_called_once_with(
            [str(self.schtasks), *expected_arguments],
            check=False,
            shell=False,
            stdin=subprocess.DEVNULL,
            timeout=scheduler_admin._SCHTASKS_TIMEOUT_SECONDS,
            capture_output=True,
            text=False,
        )

    def assert_marker_false_after_failure(self, call) -> None:
        self.write_marker(True)
        with self.assertRaises(scheduler_admin.SchedulerAdminError):
            call()
        self.assertFalse(self.marker_enabled())
        self.assertEqual(self.generated_xml_paths(), [])
        self.assert_repository_journals_unchanged()

    def test_status_missing_marker_is_disabled_read_only_and_output_is_bounded(self):
        result = scheduler_admin.status(
            _scheduler_root=self.scheduler_root,
            _environment=self.environment,
            _subprocess_run=self.subprocess_run,
        )
        self.assertEqual(
            result,
            {
                "scheduler_admin_version": "scheduler-admin/v2",
                "task_name": "Phil Manus PAPER Hourly",
                "task_state": "installed",
                "wrapper_enabled": False,
            },
        )
        self.assertFalse(self.scheduler_root.exists())
        self.assert_schtasks_call(["/Query", "/TN", "Phil Manus PAPER Hourly"])
        encoded = json.dumps(result)
        self.assertNotIn("sensitive output", encoded)
        self.assertNotIn("sensitive error", encoded)
        self.assert_repository_journals_unchanged()

    def test_install_disarms_before_any_later_validation_and_creates_exact_xml_task(self):
        self.write_marker(True)
        captured_xml = []

        def create_only(command, **kwargs):
            self.assertFalse(self.marker_enabled(), "install must disarm before /Create")
            self.assertEqual(command[0], str(self.schtasks))
            definition_path = pathlib.Path(command[command.index("/XML") + 1])
            self.assertTrue(definition_path.exists())
            captured_xml.append(definition_path.read_bytes())
            return SimpleNamespace(returncode=0, stdout=b"ignored", stderr=b"ignored")

        self.subprocess_run.side_effect = create_only
        result = self.install()
        self.assertFalse(self.marker_enabled())
        self.assertEqual(result["task_state"], "installed")
        self.assertFalse(result["wrapper_enabled"])
        self.assertEqual(self.generated_xml_paths(), [])
        command = self.subprocess_run.call_args.args[0]
        self.assertEqual(command[:5], [str(self.schtasks), "/Create", "/TN", "Phil Manus PAPER Hourly", "/XML"])
        self.assertEqual(command[-1], "/F")
        self.assertNotIn("/RU", command)
        self.assertNotIn("/RP", command)
        self.assertNotIn("/NP", command)
        self.assertEqual(len(captured_xml), 1)
        root = element_tree.fromstring(captured_xml[0])
        self.assertEqual(root.findtext("task:RegistrationInfo/task:URI", namespaces=XML_NAMESPACE), "\\Phil Manus PAPER Hourly")
        self.assertEqual(root.findtext("task:Principals/task:Principal/task:UserId", namespaces=XML_NAMESPACE), "TESTDOMAIN\\operator")
        self.assertEqual(root.findtext("task:Principals/task:Principal/task:LogonType", namespaces=XML_NAMESPACE), "InteractiveToken")
        self.assertEqual(root.findtext("task:Principals/task:Principal/task:RunLevel", namespaces=XML_NAMESPACE), "LeastPrivilege")
        time_triggers = root.findall("task:Triggers/task:TimeTrigger", namespaces=XML_NAMESPACE)
        self.assertEqual(len(time_triggers), 1)
        self.assertEqual(root.findall("task:Triggers/task:CalendarTrigger", namespaces=XML_NAMESPACE), [])
        self.assertEqual([element.tag.rsplit("}", 1)[-1] for element in time_triggers[0]], ["StartBoundary", "Repetition"])
        repetition = time_triggers[0].find("task:Repetition", namespaces=XML_NAMESPACE)
        self.assertIsNotNone(repetition)
        self.assertEqual([element.tag.rsplit("}", 1)[-1] for element in repetition], ["Interval"])
        self.assertEqual(repetition.findtext("task:Interval", namespaces=XML_NAMESPACE), "PT1H")
        self.assertIsNone(repetition.find("task:Duration", namespaces=XML_NAMESPACE))
        self.assertIsNone(time_triggers[0].find("task:ScheduleByDay", namespaces=XML_NAMESPACE))
        self.assertIsNone(time_triggers[0].find("task:EndBoundary", namespaces=XML_NAMESPACE))
        self.assertIsNone(repetition.find("task:StopAtDurationEnd", namespaces=XML_NAMESPACE))
        self.assertEqual(root.findtext("task:Settings/task:MultipleInstancesPolicy", namespaces=XML_NAMESPACE), "IgnoreNew")
        self.assertEqual(root.findtext("task:Settings/task:DisallowStartIfOnBatteries", namespaces=XML_NAMESPACE), "false")
        self.assertEqual(root.findtext("task:Settings/task:StopIfGoingOnBatteries", namespaces=XML_NAMESPACE), "false")
        self.assertEqual(root.findtext("task:Settings/task:AllowStartOnDemand", namespaces=XML_NAMESPACE), "false")
        self.assertEqual(root.findtext("task:Settings/task:AllowHardTerminate", namespaces=XML_NAMESPACE), "false")
        self.assertEqual(root.findtext("task:Settings/task:ExecutionTimeLimit", namespaces=XML_NAMESPACE), "PT0S")
        self.assertIsNone(root.find("task:Settings/task:RestartOnFailure", namespaces=XML_NAMESPACE))
        self.assertIsNone(root.find("task:Settings/task:WakeToRun", namespaces=XML_NAMESPACE))
        self.assertIsNone(root.find("task:Settings/task:StartWhenAvailable", namespaces=XML_NAMESPACE))
        self.assertEqual(root.findtext("task:Actions/task:Exec/task:Command", namespaces=XML_NAMESPACE), sys.executable)
        self.assertEqual(
            root.findtext("task:Actions/task:Exec/task:Arguments", namespaces=XML_NAMESPACE),
            subprocess.list2cmdline([str(self.repository / "scheduled_paper_task.py")]),
        )
        self.assertEqual(len(root.findall("task:Actions/task:Exec", namespaces=XML_NAMESPACE)), 1)
        self.assertEqual(captured_xml[0][:2], b"\xff\xfe")
        serialized = captured_xml[0].decode("utf-16")
        self.assertIn("encoding='utf-16'", serialized)
        for forbidden in ("Password", "S4U", "SYSTEM", "cmd.exe", "PowerShell", "manus-task-budget", "real", "IBKR", "Pearl"):
            self.assertNotIn(forbidden, serialized)
        self.assert_repository_journals_unchanged()

    def test_generated_task_xml_is_utf16_le_bom_with_matching_declaration_and_identical_semantics(self):
        payload = scheduler_admin._build_task_xml(
            account="TESTDOMAIN\\operator",
            python_executable=pathlib.Path(sys.executable),
            launcher=self.repository / "scheduled_paper_task.py",
            now=lambda: NOW,
        )
        # Real Windows schtasks /Create rejected UTF-8 XML with
        # "(1,40): unable to switch the encoding"; UTF-16 LE + BOM + matching
        # declaration was accepted with unchanged task semantics.
        self.assertEqual(payload[:2], b"\xff\xfe")
        text = payload.decode("utf-16")
        self.assertIn("encoding='utf-16'", text)
        root = element_tree.fromstring(payload)
        self.assertEqual(root.findtext("task:RegistrationInfo/task:URI", namespaces=XML_NAMESPACE), "\\Phil Manus PAPER Hourly")
        self.assertEqual(root.findtext("task:Principals/task:Principal/task:UserId", namespaces=XML_NAMESPACE), "TESTDOMAIN\\operator")
        self.assertEqual(root.findtext("task:Principals/task:Principal/task:LogonType", namespaces=XML_NAMESPACE), "InteractiveToken")
        self.assertEqual(root.findtext("task:Principals/task:Principal/task:RunLevel", namespaces=XML_NAMESPACE), "LeastPrivilege")
        time_triggers = root.findall("task:Triggers/task:TimeTrigger", namespaces=XML_NAMESPACE)
        self.assertEqual(len(time_triggers), 1)
        repetition = time_triggers[0].find("task:Repetition", namespaces=XML_NAMESPACE)
        self.assertEqual(repetition.findtext("task:Interval", namespaces=XML_NAMESPACE), "PT1H")
        self.assertIsNone(repetition.find("task:Duration", namespaces=XML_NAMESPACE))
        self.assertEqual(root.findtext("task:Settings/task:MultipleInstancesPolicy", namespaces=XML_NAMESPACE), "IgnoreNew")
        self.assertEqual(root.findtext("task:Settings/task:DisallowStartIfOnBatteries", namespaces=XML_NAMESPACE), "false")
        self.assertEqual(root.findtext("task:Settings/task:StopIfGoingOnBatteries", namespaces=XML_NAMESPACE), "false")
        self.assertEqual(root.findtext("task:Settings/task:AllowStartOnDemand", namespaces=XML_NAMESPACE), "false")
        self.assertEqual(root.findtext("task:Settings/task:AllowHardTerminate", namespaces=XML_NAMESPACE), "false")
        self.assertEqual(root.findtext("task:Settings/task:ExecutionTimeLimit", namespaces=XML_NAMESPACE), "PT0S")
        self.assertEqual(root.findtext("task:Actions/task:Exec/task:Command", namespaces=XML_NAMESPACE), sys.executable)
        self.assertEqual(
            root.findtext("task:Actions/task:Exec/task:Arguments", namespaces=XML_NAMESPACE),
            subprocess.list2cmdline([str(self.repository / "scheduled_paper_task.py")]),
        )
        self.assertLessEqual(len(payload), scheduler_admin._TASK_XML_MAX_BYTES)
        self.assert_repository_journals_unchanged()

    def test_xml_launcher_path_with_spaces_is_one_windows_argument(self):
        launcher = self.root / "repository with spaces" / "scheduled paper task.py"
        payload = scheduler_admin._build_task_xml(
            account="TESTDOMAIN\\operator",
            python_executable=pathlib.Path(sys.executable),
            launcher=launcher,
            now=lambda: NOW,
        )
        root = element_tree.fromstring(payload)
        arguments = root.findtext("task:Actions/task:Exec/task:Arguments", namespaces=XML_NAMESPACE)
        self.assertEqual(arguments, subprocess.list2cmdline([str(launcher)]))
        self.assertTrue(arguments.startswith('"') and arguments.endswith('"'))
        self.assertNotIn("cmd.exe", payload.decode("utf-16"))
        self.assert_repository_journals_unchanged()

    def test_install_disarms_stale_marker_before_missing_systemroot_invalid_account_launcher_python_and_xml_failures(self):
        cases = {
            "missing-systemroot": lambda: self.install(_environment={"USERDOMAIN": "TESTDOMAIN", "USERNAME": "operator"}),
            "invalid-account": lambda: self.install(_environment={**self.environment, "USERNAME": "bad/name"}),
            "missing-launcher": lambda: self.install(_repository_root=lambda: self.root / "missing-repository"),
            "invalid-python": lambda: self.install(_python_executable=str(self.root / "missing-python.exe")),
            "xml-builder": lambda: self.install(_xml_builder=lambda **kwargs: (_ for _ in ()).throw(RuntimeError("raw XML failure"))),
        }
        for label, operation in cases.items():
            with self.subTest(label=label):
                self.subprocess_run.reset_mock()
                self.assert_marker_false_after_failure(operation)
                self.subprocess_run.assert_not_called()

    def test_install_disarms_stale_marker_before_unsafe_or_missing_schtasks(self):
        self.write_marker(True)
        self.schtasks.unlink()
        with self.assertRaises(scheduler_admin.SchedulerAdminError):
            self.install()
        self.assertFalse(self.marker_enabled())
        self.subprocess_run.assert_not_called()
        self.assertEqual(self.generated_xml_paths(), [])

        self.schtasks.write_bytes(b"offline mock only\n")
        self.write_marker(True)
        real_check = paper_locks.path_is_unsafe_indirection
        with patch.object(
            paper_locks,
            "path_is_unsafe_indirection",
            side_effect=lambda path: pathlib.Path(path) == self.schtasks or real_check(path),
        ):
            with self.assertRaises(scheduler_admin.SchedulerAdminError):
                self.install()
        self.assertFalse(self.marker_enabled())
        self.subprocess_run.assert_not_called()
        self.assertEqual(self.generated_xml_paths(), [])
        self.assert_repository_journals_unchanged()

    def test_install_disarms_before_xml_temp_write_failure(self):
        self.write_marker(True)
        with self.assertRaises(scheduler_admin.SchedulerAdminError):
            self.install(_xml_writer=lambda root, payload: (_ for _ in ()).throw(OSError("simulated")))
        self.assertFalse(self.marker_enabled())
        self.subprocess_run.assert_not_called()
        self.assertEqual(self.generated_xml_paths(), [])
        self.assert_repository_journals_unchanged()

    def test_install_disarms_stale_marker_before_unsafe_launcher_validation(self):
        self.write_marker(True)
        real_check = paper_locks.path_is_unsafe_indirection
        with patch.object(
            paper_locks,
            "path_is_unsafe_indirection",
            side_effect=lambda path: pathlib.Path(path) == self.repository or real_check(path),
        ):
            with self.assertRaises(scheduler_admin.SchedulerAdminError):
                self.install()
        self.assertFalse(self.marker_enabled())
        self.subprocess_run.assert_not_called()
        self.assertEqual(self.generated_xml_paths(), [])
        self.assert_repository_journals_unchanged()

    def test_install_nonzero_timeout_and_unexpected_exception_remove_temp_xml_and_leave_disarmed(self):
        cases = {
            "nonzero": lambda: SimpleNamespace(returncode=1, stdout=b"raw stdout", stderr=b"raw stderr"),
            "timeout": lambda: (_ for _ in ()).throw(subprocess.TimeoutExpired(["ignored"], 1)),
            "unexpected": lambda: (_ for _ in ()).throw(RuntimeError("unexpected raw diagnostic")),
        }
        for label, outcome in cases.items():
            with self.subTest(label=label):
                self.write_marker(True)
                self.subprocess_run.reset_mock()
                self.subprocess_run.side_effect = lambda command, **kwargs: outcome()
                with self.assertRaises(scheduler_admin.SchedulerAdminError) as result:
                    self.install()
                self.assertFalse(self.marker_enabled())
                self.assertEqual(self.subprocess_run.call_count, 1)
                self.assertEqual(self.generated_xml_paths(), [])
                self.assertNotIn("raw", str(result.exception))
                self.assert_repository_journals_unchanged()

    def test_enable_queries_then_arms_without_runner_or_task_execution(self):
        result = scheduler_admin.enable(
            _scheduler_root=self.scheduler_root,
            _environment=self.environment,
            _subprocess_run=self.subprocess_run,
        )
        self.assertTrue(self.marker_enabled())
        self.assertEqual(result["task_state"], "installed")
        self.assertTrue(result["wrapper_enabled"])
        self.assert_schtasks_call(["/Query", "/TN", "Phil Manus PAPER Hourly"])
        self.assert_repository_journals_unchanged()

    def test_enable_query_failure_does_not_arm_marker(self):
        self.subprocess_run.return_value = SimpleNamespace(returncode=1, stdout=b"raw query", stderr=b"raw query")
        with self.assertRaisesRegex(scheduler_admin.SchedulerAdminError, "task is unavailable"):
            scheduler_admin.enable(
                _scheduler_root=self.scheduler_root,
                _environment=self.environment,
                _subprocess_run=self.subprocess_run,
            )
        self.assertFalse(self.marker_path().exists())
        self.assertEqual(self.subprocess_run.call_count, 1)
        self.assert_repository_journals_unchanged()

    def test_status_nonzero_query_is_not_confirmed_and_never_exposes_raw_output(self):
        self.subprocess_run.return_value = SimpleNamespace(
            returncode=1,
            stdout=b"localized scheduler diagnostic",
            stderr=b"localized scheduler error",
        )
        result = scheduler_admin.status(
            _scheduler_root=self.scheduler_root,
            _environment=self.environment,
            _subprocess_run=self.subprocess_run,
        )
        self.assertEqual(result["task_state"], "not-confirmed")
        self.assertFalse(result["wrapper_enabled"])
        encoded = json.dumps(result)
        self.assertNotIn("localized scheduler", encoded)
        self.assert_schtasks_call(["/Query", "/TN", "Phil Manus PAPER Hourly"])
        self.assertFalse(self.scheduler_root.exists())
        source = inspect.getsource(scheduler_admin)
        self.assertNotIn(".stdout", source)
        self.assertNotIn(".stderr", source)
        self.assert_repository_journals_unchanged()

    def test_disable_is_idempotent_never_calls_schtasks_and_reports_not_queried(self):
        self.write_marker(True)
        first = scheduler_admin.disable(_scheduler_root=self.scheduler_root)
        second = scheduler_admin.disable(_scheduler_root=self.scheduler_root)
        self.assertFalse(self.marker_enabled())
        self.assertEqual(first, second)
        self.assertEqual(first["task_state"], "not-queried")
        self.assertFalse(first["wrapper_enabled"])
        self.subprocess_run.assert_not_called()
        self.assert_repository_journals_unchanged()

    def test_uninstall_disarms_before_missing_systemroot_unsafe_schtasks_timeout_and_delete_failure(self):
        cases = {
            "missing-systemroot": lambda: scheduler_admin.uninstall(
                _scheduler_root=self.scheduler_root,
                _environment={},
                _subprocess_run=self.subprocess_run,
            ),
            "timeout": lambda: scheduler_admin.uninstall(
                _scheduler_root=self.scheduler_root,
                _environment=self.environment,
                _subprocess_run=lambda command, **kwargs: (_ for _ in ()).throw(subprocess.TimeoutExpired(command, 1)),
            ),
            "nonzero": lambda: scheduler_admin.uninstall(
                _scheduler_root=self.scheduler_root,
                _environment=self.environment,
                _subprocess_run=lambda command, **kwargs: SimpleNamespace(returncode=1, stdout=b"raw", stderr=b"raw"),
            ),
        }
        for label, operation in cases.items():
            with self.subTest(label=label):
                self.write_marker(True)
                with self.assertRaises(scheduler_admin.SchedulerAdminError):
                    operation()
                self.assertFalse(self.marker_enabled())
                self.assert_repository_journals_unchanged()

        self.write_marker(True)
        real_check = paper_locks.path_is_unsafe_indirection
        with patch.object(
            paper_locks,
            "path_is_unsafe_indirection",
            side_effect=lambda path: pathlib.Path(path) == self.schtasks or real_check(path),
        ):
            with self.assertRaises(scheduler_admin.SchedulerAdminError):
                scheduler_admin.uninstall(
                    _scheduler_root=self.scheduler_root,
                    _environment=self.environment,
                    _subprocess_run=self.subprocess_run,
                )
        self.assertFalse(self.marker_enabled())
        self.assert_repository_journals_unchanged()

    def test_uninstall_success_disarms_then_uses_exact_fixed_delete(self):
        self.write_marker(True)

        def delete_only(command, **kwargs):
            self.assertFalse(self.marker_enabled(), "uninstall must disarm before /Delete")
            return SimpleNamespace(returncode=0, stdout=b"ignored", stderr=b"ignored")

        self.subprocess_run.side_effect = delete_only
        result = scheduler_admin.uninstall(
            _scheduler_root=self.scheduler_root,
            _environment=self.environment,
            _subprocess_run=self.subprocess_run,
        )
        self.assertFalse(self.marker_enabled())
        self.assertEqual(result["task_state"], "absent")
        self.assert_schtasks_call(["/Delete", "/TN", "Phil Manus PAPER Hourly", "/F"])
        self.assert_repository_journals_unchanged()

    def test_malformed_and_simulated_reparse_marker_state_fail_closed(self):
        self.scheduler_root.mkdir(parents=True)
        self.marker_path().write_text('{"enabled":true,"enabled":false}', encoding="utf-8")
        with self.assertRaises(scheduler_admin.SchedulerAdminError):
            scheduler_admin.disable(_scheduler_root=self.scheduler_root)
        self.subprocess_run.assert_not_called()

        self.marker_path().unlink()
        real_check = paper_locks.path_is_unsafe_indirection
        with patch.object(
            paper_locks,
            "path_is_unsafe_indirection",
            side_effect=lambda path: pathlib.Path(path) == self.scheduler_root or real_check(path),
        ):
            with self.assertRaises(scheduler_admin.SchedulerAdminError):
                scheduler_admin.status(
                    _scheduler_root=self.scheduler_root,
                    _environment=self.environment,
                    _subprocess_run=self.subprocess_run,
                )
        self.subprocess_run.assert_not_called()
        self.assert_repository_journals_unchanged()

    def test_cli_has_exact_operations_and_rejects_controls_without_calling_operations(self):
        parser = scheduler_admin.build_parser()
        self.assertEqual({action.dest for action in parser._actions}, {"operation"})
        for control in (
            "--path=x", "--root=x", "--task-name=x", "--cadence=hourly", "--interval=1", "--command=x",
            "--executable=x", "--python=x", "--budget=1", "--credit=1", "--dry-run", "--live", "--real",
            "--ibkr", "--pearl", "--broker", "--wallet", "--retry", "--run-now", "--force", "--shell", "--powershell",
            "run", "trigger", "execute", "test-run", "extra",
        ):
            with self.subTest(control=control), redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit) as result:
                    scheduler_admin.main([control])
                self.assertEqual(result.exception.code, 2)
        safe_result = {
            "scheduler_admin_version": "scheduler-admin/v2",
            "task_name": "Phil Manus PAPER Hourly",
            "task_state": "not-queried",
            "wrapper_enabled": False,
        }
        for operation in ("status", "install", "enable", "disable", "uninstall"):
            with self.subTest(operation=operation), patch.object(scheduler_admin, operation, return_value=safe_result) as operation_mock, redirect_stdout(io.StringIO()) as output:
                self.assertEqual(scheduler_admin.main([operation]), 0)
            operation_mock.assert_called_once_with()
            self.assertEqual(json.loads(output.getvalue()), safe_result)

    def test_cli_redacts_operator_failure(self):
        sensitive = "SCHEDULER-ADMIN-UNIQUE-SENSITIVE-DIAGNOSTIC"
        stdout, stderr = io.StringIO(), io.StringIO()
        with patch.object(scheduler_admin, "install", side_effect=scheduler_admin.SchedulerAdminError(sensitive)) as install_mock, \
             redirect_stdout(stdout), redirect_stderr(stderr):
            with self.assertRaises(SystemExit) as result:
                scheduler_admin.main(["install"])
        self.assertEqual(result.exception.code, 2)
        install_mock.assert_called_once_with()
        self.assertNotIn(sensitive, stdout.getvalue())
        self.assertNotIn(sensitive, stderr.getvalue())

    def test_cli_parse_rejections_do_not_echo_invalid_or_extra_secret_arguments(self):
        sentinels = (
            ["INVALID-OP-UNIQUE-SECRET-9cf1"],
            ["status", "EXTRA-ARG-UNIQUE-SECRET-a4e8"],
        )
        for arguments in sentinels:
            with self.subTest(arguments=arguments):
                stdout, stderr = io.StringIO(), io.StringIO()
                with patch.object(scheduler_admin, "status") as status_mock, \
                     patch.object(scheduler_admin, "install") as install_mock, \
                     patch.object(scheduler_admin, "enable") as enable_mock, \
                     patch.object(scheduler_admin, "disable") as disable_mock, \
                     patch.object(scheduler_admin, "uninstall") as uninstall_mock, \
                     redirect_stdout(stdout), redirect_stderr(stderr):
                    with self.assertRaises(SystemExit) as result:
                        scheduler_admin.main(arguments)
                self.assertEqual(result.exception.code, 2)
                for sentinel in arguments:
                    self.assertNotIn(sentinel, stdout.getvalue())
                    self.assertNotIn(sentinel, stderr.getvalue())
                for operation_mock in (status_mock, install_mock, enable_mock, disable_mock, uninstall_mock):
                    operation_mock.assert_not_called()
                self.assert_repository_journals_unchanged()

    def test_static_isolation_allows_only_fixed_local_schtasks_process(self):
        source = inspect.getsource(scheduler_admin)
        tree = ast.parse(source)
        imports = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imports.add(node.module)
        self.assertFalse(imports & {"requests", "httpx", "urllib", "ctypes", "win32com", "research_transport", "paper_apply", "core.scan", "paper_runner"})
        self.assertIn("subprocess", imports)
        for forbidden in ("cmd.exe", "powershell.exe", "pwsh.exe", "schtasks /run", "/RU", "/RP", "/NP", "SYSTEM", "S4U", "HIGHEST", "manus_task_budget=1"):
            self.assertNotIn(forbidden, source)
        self.assertIn("shell=False", source)
        self.assertIn("stdin=subprocess.DEVNULL", source)
        self.assertIn("timeout=_SCHTASKS_TIMEOUT_SECONDS", source)
        self.assertIn("capture_output=True", source)
        self.assertIn("element_tree", source)
        self.assert_repository_journals_unchanged()


if __name__ == "__main__":
    unittest.main()

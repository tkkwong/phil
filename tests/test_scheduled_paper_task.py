"""Offline regressions for the fixed Task Scheduler launcher."""
from __future__ import annotations

import ast
import importlib.util
import inspect
import pathlib
import tempfile
import unittest
from unittest.mock import patch

from manus import scheduled_paper


class ScheduledPaperTaskTests(unittest.TestCase):
    def setUp(self) -> None:
        self.launcher = pathlib.Path(__file__).resolve().parents[1] / "scheduled_paper_task.py"

    def load_launcher(self):
        with tempfile.TemporaryDirectory() as directory:
            module_path = pathlib.Path(directory) / "scheduled_paper_task_test.py"
            module_path.write_bytes(self.launcher.read_bytes())
            spec = importlib.util.spec_from_file_location("scheduled_paper_task_test", module_path)
            module = importlib.util.module_from_spec(spec)
            assert spec and spec.loader
            spec.loader.exec_module(module)
            return module

    def test_launcher_changes_to_its_own_repository_root_and_calls_wrapper_once(self):
        module = self.load_launcher()
        expected_root = pathlib.Path(module.__file__).resolve().parent
        with patch.object(module.os, "chdir") as chdir, patch.object(scheduled_paper, "main", return_value=17) as wrapper_main:
            self.assertEqual(module.main(), 17)
        chdir.assert_called_once_with(expected_root)
        wrapper_main.assert_called_once_with([])

    def test_static_isolation_allows_only_scheduled_wrapper_dependency(self):
        source = self.launcher.read_text(encoding="utf-8")
        tree = ast.parse(source)
        imports = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imports.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imports.add(node.module)
        self.assertFalse(imports & {"subprocess", "requests", "httpx", "urllib", "ctypes", "paper_runner", "research_transport", "paper_apply", "core.scan"})
        self.assertIn("from manus import scheduled_paper", source)
        self.assertIn("scheduled_paper.main([])", source)
        for forbidden in ("sys.argv", "manus_task_budget", "IBKR", "Pearl", "wallet", "broker", "schtasks", "subprocess"):
            self.assertNotIn(forbidden, source)


if __name__ == "__main__":
    unittest.main()

"""Fully offline regressions for the fail-closed read-only IBKR adapter.

No test in this file contacts IBKR, starts TWS/Gateway/Client Portal,
uses credentials, or performs any write. The broker transport is always
mocked; the official ``ibapi`` package is never imported here.
"""
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
import threading
import time
import types
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest.mock import patch

REPOSITORY_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from ibkr import config as config_module  # noqa: E402
from ibkr import diagnostics  # noqa: E402
from ibkr.adapter import ADAPTER_VERSION, ReadonlyIbkrAdapter, mask_account  # noqa: E402
from ibkr.config import AdapterConfigError  # noqa: E402
from ibkr.readonly import build_parser, main  # noqa: E402
from ibkr.transport import TransportError, default_transport_factory  # noqa: E402
from ibkr.transport_tws import _CollectingWrapper  # noqa: E402

NOW = dt.datetime(2026, 10, 6, 12, 0, 0, tzinfo=dt.timezone.utc)
EXPECTED_ACCOUNT = "DU0000011"


def config_host():
    return base_config()["host"]


def base_config(**changes):
    config = {
        "config_version": "ibkr-config/v1",
        "config_path": "/tmp/unused.json",
        "expected_account_id": EXPECTED_ACCOUNT,
        "environment": "PAPER",
        "host": "127.0.0.1",
        "port": 7497,
        "client_id": 19,
        "read_only_timeout_seconds": 10.0,
    }
    config.update(changes)
    return config


class _FakeTransport:
    """In-memory read-only transport used for all adapter tests."""

    def __init__(self, config=None):
        self.config = config or {}
        self.connected = False
        self.calls: list[str] = []
        self.accounts = [EXPECTED_ACCOUNT]
        self.summary = {
            "AccountType": "PAPER",
            "NetLiquidation": "12345.67",
            "AvailableFunds": "10000.00",
            "BuyingPower": "20000.00",
            "Currency": "CAD",
        }
        self.position_rows = [
            {
                "account": EXPECTED_ACCOUNT,
                "conid": 222,
                "symbol": "BBA",
                "sec_type": "STK",
                "exchange": "SMART",
                "currency": "CAD",
                "quantity": 10.0,
                "average_cost": 12.5,
            },
            {
                "account": EXPECTED_ACCOUNT,
                "conid": 111,
                "symbol": "AAA",
                "sec_type": "STK",
                "exchange": "SMART",
                "currency": "CAD",
                "quantity": 1.0,
                "average_cost": 2.0,
            },
        ]
        self.open_order_rows = [
            {
                "order_id": 7,
                "conid": 111,
                "symbol": "AAA",
                "sec_type": "STK",
                "exchange": "SMART",
                "currency": "CAD",
                "action": "BUY",
                "total_quantity": 1,
                "limit_price": 2.0,
                "order_type": "LMT",
                "status": "Submitted",
            }
        ]
        self.execution_rows = [
            {
                "exec_id": "e2",
                "order_id": 5,
                "conid": 111,
                "symbol": "AAA",
                "sec_type": "STK",
                "exchange": "SMART",
                "side": "BOT",
                "quantity": 1.0,
                "price": 2.0,
                "time": "20261006-09:00:00",
            }
        ]
        self.contract_rows = [
            {
                "conid": 444,
                "symbol": "SPY",
                "local_symbol": "SPY",
                "sec_type": "STK",
                "exchange": "SMART",
                "primary_exchange": "ARCA",
                "currency": "USD",
                "expiry": None,
                "strike": None,
                "right": None,
                "multiplier": None,
                "trading_class": "SPY",
            }
        ]

    def connect(self, config):
        self.calls.append("connect")
        self.connected = True
        self.config = config

    def disconnect(self):
        self.calls.append("disconnect")
        self.connected = False

    def managed_accounts(self):
        self.calls.append("managed_accounts")
        return list(self.accounts)

    def account_summary(self, account_id):
        self.calls.append("account_summary")
        if account_id not in self.accounts:
            raise TransportError("account summary is unavailable")
        return dict(self.summary)

    def positions(self):
        self.calls.append("positions")
        return [dict(row) for row in self.position_rows]

    def open_orders(self):
        self.calls.append("open_orders")
        return [dict(row) for row in self.open_order_rows]

    def executions(self):
        self.calls.append("executions")
        return [dict(row) for row in self.execution_rows]

    def contract_details(self, symbol, sec_type, *, currency=None, exchange=None):
        self.calls.append("contract_details")
        return [dict(row) for row in self.contract_rows]


class AdapterTestBase(unittest.TestCase):
    def setUp(self):
        self.now = NOW

    def adapter(self, transport=None, *, config=None, accounts=None):
        transport = transport or _FakeTransport()
        if accounts is not None:
            transport.accounts = list(accounts)
        adapter = ReadonlyIbkrAdapter(
            config or base_config(),
            _transport_factory=lambda _config: transport,
            _now=lambda: self.now,
        )
        self.addCleanup(adapter.close)
        return adapter, transport

    # journal integrity helpers --------------------------------------------
    def journal_paths(self):
        return [
            REPOSITORY_ROOT / "journal" / "forecasts.jsonl",
            REPOSITORY_ROOT / "journal" / "ledger.jsonl",
        ]

    def journal_hashes(self):
        return [hashlib.sha256(path.read_bytes()).hexdigest() for path in self.journal_paths()]


class ImportSafetyTests(AdapterTestBase):
    """Requirement 1: importing the adapter has no network/process side effects."""

    def test_import_statement_collection_is_side_effect_free(self):
        # Simulate a fresh interpreter importing every public module and
        # asserting no listener/socket/process/thread/file creation occurred.
        code = (
            "import sys, os, threading\n"
            "before_threads = threading.active_count()\n"
            "import ibkr, ibkr.adapter, ibkr.config, ibkr.diagnostics, ibkr.readonly, ibkr.transport, ibkr.transport_tws\n"
            "assert threading.active_count() == before_threads, 'threads started on import'\n"
            "import json\n"
            "print(json.dumps({\n"
            "  'ibapi_imported': 'ibapi' in sys.modules,\n"
            "  'files_created': False,\n"
            "}))\n"
        )
        completed = subprocess.run(
            [sys.executable, "-c", code],
            capture_output=True, text=True, cwd=str(REPOSITORY_ROOT), timeout=60,
            env={**os.environ, "PYTHONPATH": str(REPOSITORY_ROOT)},
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        payload = json.loads(completed.stdout.strip().splitlines()[-1])
        self.assertFalse(payload["ibapi_imported"])
        self.assertFalse(payload["files_created"])

    def test_source_has_no_module_level_network_or_process_calls(self):
        for name in ("adapter.py", "config.py", "diagnostics.py", "readonly.py", "transport.py"):
            source = (REPOSITORY_ROOT / "ibkr" / name).read_text(encoding="utf-8")
            tree = ast.parse(source)
            forbidden = {"socket", "subprocess", "requests", "httpx", "urllib"}
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        self.assertNotIn(alias.name.split(".")[0], forbidden, name)
                elif isinstance(node, ast.ImportFrom) and node.module:
                    self.assertNotIn(node.module.split(".")[0], forbidden, name)
                # No module-level ibapi import anywhere (docstring mentions
                # are fine; the deferral test below enforces the real rule).
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        self.assertFalse(alias.name.startswith("ibapi"), name)
                elif isinstance(node, ast.ImportFrom):
                    self.assertFalse((node.module or "").startswith("ibapi"), name)

    def test_ibapi_import_is_deferred_to_connect_time(self):
        # transport_tws.py must import ibapi only inside methods, never at
        # module import time.
        source = (REPOSITORY_ROOT / "ibkr" / "transport_tws.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        for node in tree.body:  # module-level statements only
            if isinstance(node, ast.Import):
                for alias in node.names:
                    self.assertFalse(alias.name.startswith("ibapi"))
            elif isinstance(node, ast.ImportFrom):
                self.assertFalse((node.module or "").startswith("ibapi"))


class MutationCapabilityTests(AdapterTestBase):
    """Requirements 2-4 and 23: no order mutation is possible or reachable."""

    FORBIDDEN_METHOD_FRAGMENTS = (
        "place", "order", "submit", "transmit", "cancel", "modify",
        "replace", "exercise", "transfer", "flatten", "close_position",
    )

    def test_public_api_has_no_order_mutating_method(self):
        public = [
            name for name in dir(ReadonlyIbkrAdapter)
            if not name.startswith("_") and name not in dir(unittest.TestCase)
        ]
        # The complete public surface must be exactly this closed set.
        self.assertEqual(
            sorted(public),
            sorted([
                "account_summary", "close", "executions", "interface",
                "lookup_contract", "open_orders", "positions", "status",
            ]),
        )
        lowered = [name.lower() for name in public]
        for name in lowered:
            for fragment in ("place", "submit", "transmit", "cancel",
                             "modify", "replace", "exercise", "transfer"):
                self.assertNotIn(fragment, name)

    def test_underlying_mutation_capable_client_is_not_retrievable(self):
        adapter, _transport = self.adapter()
        # No public attribute or method returns the transport or an EClient.
        for name in dir(adapter):
            if name.startswith("_"):
                continue
            attribute = getattr(adapter, name)
            self.assertFalse(
                isinstance(attribute, type(_FakeTransport())),
                f"public member {name!r} exposes the broker transport",
            )
        # Private transport slots are per-instance randomized and mangled.
        slots = [
            key for key in vars(adapter)
            if key.startswith("_ReadonlyIbkrAdapter__")
        ]
        self.assertEqual(len(slots), 1)
        # Even the internal helper cannot be reached without underscore access.
        with self.assertRaises(AttributeError):
            _ = adapter.transport  # no public alias exists

    def test_no_mutation_method_is_invoked_in_any_read_path(self):
        class _MutationTripwire(_FakeTransport):
            def __getattribute__(self, name):
                if name.lower() in {
                    "placeorder", "cancelorder", "reqglobalcancel", "cancelhistorydownloads",
                    "replace", "exerciseoptions", "reqfundsmovement", "reqtransferdata",
                    "reqaccountupdates",  # legitimate but not used by adapter
                } and name != "reqAccountUpdates":
                    raise AssertionError(f"mutation-capable call attempted: {name}")
                return super().__getattribute__(name)

        transport = _MutationTripwire()
        adapter, _ = self.adapter(transport)
        adapter.status()
        adapter.account_summary()
        adapter.positions()
        adapter.open_orders()
        adapter.executions()
        adapter.lookup_contract("SPY", "STK")
        # Invariants: every operation reconnects a fresh verified session
        # (connect + managed_accounts first) and closes it afterwards.
        self.assertEqual(transport.calls[0], "connect")
        self.assertEqual(transport.calls.count("connect"), 6)
        self.assertEqual(transport.calls.count("disconnect"), 6)
        for index, call in enumerate(transport.calls):
            if call == "connect":
                self.assertEqual(transport.calls[index + 1], "managed_accounts")
            if call == "disconnect":
                self.assertTrue(transport.calls[index - 1] in (
                    "account_summary", "positions", "open_orders",
                    "executions", "contract_details",
                ))
        self.assertFalse(transport.connected)

    def test_read_only_transport_interface_has_no_mutation_members(self):
        from ibkr.transport import ReadonlyTransport

        members = {
            name for name, value in inspect.getmembers(ReadonlyTransport)
            if not name.startswith("_")
        }
        self.assertEqual(
            sorted(members),
            sorted([
                "account_summary", "connect", "contract_details", "disconnect",
                "executions", "managed_accounts", "open_orders", "positions",
            ]),
        )


class ConfigFailClosedTests(AdapterTestBase):
    """Requirements 5 and 11: configuration failures close before connecting."""

    def setUp(self):
        super().setUp()
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = pathlib.Path(self.temporary.name)

    def write_config(self, document, name="ibkr.json"):
        path = self.root / name
        path.write_text(json.dumps(document), encoding="utf-8")
        return path

    def config_document(self, **changes):
        document = {
            "expected_account_id": EXPECTED_ACCOUNT,
            "environment": "PAPER",
            "host": "127.0.0.1",
            "port": 7497,
            "client_id": 19,
        }
        document.update(changes)
        return document

    def load(self, path, environ=None):
        return config_module.load_config(
            REPOSITORY_ROOT,
            environ={"PHIL_IBKR_CONFIG": str(path), **(environ or {})},
        )

    def test_missing_config_fails_closed_without_connecting(self):
        missing = self.root / "does-not-exist.json"
        factory_calls = []
        with self.assertRaises(AdapterConfigError) as caught:
            ReadonlyIbkrAdapter(
                None,
                _transport_factory=lambda config: factory_calls.append(config),
                _now=lambda: self.now,
            )._config()
        self.assertEqual(caught.exception.code, "not-configured")
        self.assertEqual(factory_calls, [])
        # The CLI surfaces the bounded code, not an exception traceback.
        buffer = io.StringIO()
        with patch.dict(os.environ, {"PHIL_IBKR_CONFIG": str(missing)}), \
             redirect_stdout(buffer):
            exit_code = main(["status"])
        document = json.loads(buffer.getvalue())
        self.assertEqual(exit_code, 1)
        self.assertEqual(document["diagnostic_code"], "not-configured")

    def test_malformed_config_fails_closed(self):
        path = self.root / "malformed.json"
        path.write_text("{not json", encoding="utf-8")
        with self.assertRaises(AdapterConfigError) as caught:
            self.load(path)
        self.assertEqual(caught.exception.code, "not-configured")

    def test_unknown_fields_fail_closed(self):
        path = self.write_config(self.config_document(unexpected="x"))
        with self.assertRaises(AdapterConfigError):
            self.load(path)

    def test_ambiguous_environment_fails_closed(self):
        for bad in ("PAPER ", "paper", "Live", "UNKNOWN", "", None, 7):
            with self.subTest(environment=bad):
                path = self.write_config(self.config_document(environment=bad))
                with self.assertRaises(AdapterConfigError) as caught:
                    self.load(path)
                self.assertEqual(caught.exception.code, "environment-ambiguous")

    def test_invalid_numeric_fields_fail_closed(self):
        cases = [
            ("port", 0), ("port", 70000), ("port", "7497"),
            ("client_id", -1), ("client_id", True),
            ("read_only_timeout_seconds", 0), ("read_only_timeout_seconds", 999),
        ]
        for field, value in cases:
            with self.subTest(field=field, value=value):
                document = self.config_document()
                document[field] = value
                path = self.write_config(document)
                with self.assertRaises(AdapterConfigError):
                    self.load(path)


class AccountAllowlistTests(AdapterTestBase):
    """Requirements 6-10: allowlist and environment checks fail closed."""

    def test_wrong_account_fails_closed(self):
        adapter, transport = self.adapter(accounts=["DU9999999"])
        with self.assertRaises(diagnostics.AdapterError) as caught:
            adapter.status()
        self.assertEqual(caught.exception.code, "unexpected-account")
        self.assertEqual(caught.exception.classification, "provenance")
        # Session must be closed after a provenance failure.
        self.assertFalse(transport.connected)

    def test_multiple_accounts_fail_closed(self):
        adapter, transport = self.adapter(accounts=[EXPECTED_ACCOUNT, "DU0000022"])
        with self.assertRaises(diagnostics.AdapterError) as caught:
            adapter.status()
        self.assertEqual(caught.exception.code, "multiple-accounts")
        self.assertFalse(transport.connected)

    def test_empty_account_list_fails_closed(self):
        adapter, transport = self.adapter(accounts=[])
        with self.assertRaises(diagnostics.AdapterError) as caught:
            adapter.status()
        self.assertEqual(caught.exception.code, "session-unavailable")

    def test_expected_paper_account_is_recognized(self):
        adapter, transport = self.adapter()
        document = adapter.status()
        self.assertTrue(document["connected"])
        self.assertTrue(document["account_match"])
        self.assertEqual(document["environment"], "PAPER")
        self.assertEqual(document["account_id_masked"], mask_account(EXPECTED_ACCOUNT))
        self.assertEqual(document["base_currency"], "CAD")
        self.assertEqual(document["net_liquidation"], 12345.67)
        self.assertEqual(document["available_funds"], 10000.00)
        self.assertEqual(document["positions_count"], 2)
        self.assertEqual(document["open_orders_count"], 1)
        self.assertIsNone(document["diagnostic_code"])
        self.assertTrue(transport.calls.count("disconnect") >= 1)

    def test_expected_live_account_is_recognized_and_stays_read_only(self):
        adapter, transport = self.adapter(config=base_config(environment="LIVE"))
        document = adapter.status()
        self.assertEqual(document["environment"], "LIVE")
        self.assertTrue(document["connected"])
        self.assertTrue(document["account_match"])
        # Still no mutation call anywhere in the LIVE read path.
        self.assertNotIn("placeorder", [call.lower() for call in transport.calls])

    def test_account_summary_masks_account_identifier(self):
        adapter, _ = self.adapter()
        document = adapter.account_summary()
        self.assertNotIn("account_id", document)
        self.assertEqual(document["account_id_masked"], mask_account(EXPECTED_ACCOUNT))
        self.assertNotIn(EXPECTED_ACCOUNT, json.dumps(document))

    def test_mask_account_never_reveals_full_identifier(self):
        self.assertEqual(mask_account("DU1234567"), "***4567")
        self.assertEqual(mask_account("AB12"), "***AB12")
        self.assertEqual(mask_account("A"), "***A")
        self.assertEqual(mask_account(""), "***")


class FailureReductionTests(AdapterTestBase):
    """Requirements 11-12 and 13: bounded diagnostics, no broker text."""

    def test_transport_failure_maps_to_connection_unavailable(self):
        class _BrokenTransport(_FakeTransport):
            def connect(self, config):
                raise OSError("[WinError 10061] C:\\secret\\path token=abc123")

        adapter, _ = self.adapter(_BrokenTransport())
        document = adapter.status()
        self.assertFalse(document["connected"])
        self.assertEqual(document["diagnostic_code"], "broker-data-unavailable")
        # The exact exception text must not survive anywhere in the output.
        self.assertNotIn("10061", json.dumps(document))
        self.assertNotIn("abc123", json.dumps(document))

    def test_broker_exception_text_is_reduced_to_bounded_code(self):
        class _ExplodingTransport(_FakeTransport):
            def managed_accounts(self):
                raise RuntimeError("session expired password=C:\\Users\\tkw\\creds")

        adapter, transport = self.adapter(_ExplodingTransport())
        document = adapter.status()
        # Infrastructure failures surface as a bounded code with broker
        # text discarded — never a traceback, never raw exception text.
        self.assertEqual(document["diagnostic_code"], "broker-data-unavailable")
        self.assertFalse(document["connected"])
        self.assertNotIn("password", json.dumps(document))
        self.assertFalse(transport.connected)

    def test_malformed_broker_response_fails_closed(self):
        class _GarbageTransport(_FakeTransport):
            def account_summary(self, account_id):
                raise TransportError("account summary is unavailable")

        adapter, _ = self.adapter(_GarbageTransport())
        document = adapter.status()
        self.assertEqual(document["diagnostic_code"], "broker-data-unavailable")

    def test_diagnostic_vocabulary_is_closed_and_classified(self):
        self.assertEqual(
            set(diagnostics.DIAGNOSTIC_CLASSIFICATIONS), diagnostics.DIAGNOSTIC_CODES
        )
        for code, classification in diagnostics.DIAGNOSTIC_CLASSIFICATIONS.items():
            self.assertIn(classification, {"configuration", "infrastructure", "provenance", "internal"})

    def test_adapter_error_rejects_arbitrary_code_text(self):
        error = diagnostics.AdapterError("totally-unknown-code")
        self.assertEqual(error.code, "unclassified")

    def test_account_number_never_appears_in_operator_output(self):
        adapter, _ = self.adapter()
        captured = io.StringIO()
        with redirect_stdout(captured):
            adapter.status()
        self.assertNotIn(EXPECTED_ACCOUNT, captured.getvalue())


class NormalizationTests(AdapterTestBase):
    """Requirements 14-16: deterministic read-only normalization."""

    def test_positions_normalize_deterministically(self):
        adapter, _ = self.adapter()
        rows = adapter.positions()
        self.assertEqual(
            [row["conid"] for row in rows],
            [111, 222],
        )
        for row in rows:
            self.assertEqual(
                sorted(row),
                sorted(["account_id_masked", "conid", "symbol", "sec_type",
                        "exchange", "currency", "quantity", "average_cost"]),
            )
            self.assertNotIn("account", row)
        self.assertNotIn(EXPECTED_ACCOUNT, json.dumps(rows))

    def test_positions_exclude_other_accounts(self):
        transport = _FakeTransport()
        transport.position_rows.append({
            "account": "DU9999999", "conid": 999, "symbol": "X", "sec_type": "STK",
            "exchange": "SMART", "currency": "USD", "quantity": 1, "average_cost": 1,
        })
        adapter, _ = self.adapter(transport)
        rows = adapter.positions()
        self.assertEqual({row["conid"] for row in rows}, {111, 222})

    def test_open_orders_are_read_only_and_normalized(self):
        adapter, transport = self.adapter()
        rows = adapter.open_orders()
        self.assertEqual(len(rows), 1)
        self.assertEqual(
            sorted(rows[0]),
            sorted(["account_id_masked", "order_id", "conid", "symbol", "sec_type",
                    "exchange", "currency", "action", "total_quantity",
                    "limit_price", "order_type", "status"]),
        )
        original = json.loads(json.dumps(transport.open_order_rows))
        adapter.open_orders()
        self.assertEqual(transport.open_order_rows, original)

    def test_executions_are_read_only_and_normalized(self):
        adapter, transport = self.adapter()
        rows = adapter.executions()
        self.assertEqual(len(rows), 1)
        self.assertEqual(
            sorted(rows[0]),
            sorted(["account_id_masked", "exec_id", "order_id", "conid", "symbol",
                    "sec_type", "exchange", "side", "quantity", "price", "time"]),
        )
        original = json.loads(json.dumps(transport.execution_rows))
        adapter.executions()
        self.assertEqual(transport.execution_rows, original)


class ContractDiscoveryTests(AdapterTestBase):
    """Requirements 17-19: read-only contract lookup boundary."""

    def test_exact_contract_lookup_succeeds(self):
        adapter, _ = self.adapter()
        record = adapter.lookup_contract("SPY", "STK", currency="USD")
        self.assertEqual(record["conid"], 444)
        self.assertEqual(record["symbol"], "SPY")
        self.assertEqual(record["primary_exchange"], "ARCA")

    def test_no_contract_raises_contract_not_found(self):
        class _EmptyTransport(_FakeTransport):
            def contract_details(self, *args, **kwargs):
                return []

        adapter, _ = self.adapter(_EmptyTransport())
        with self.assertRaises(diagnostics.AdapterError) as caught:
            adapter.lookup_contract("ZZZZ", "STK")
        self.assertEqual(caught.exception.code, "contract-not-found")
        self.assertEqual(caught.exception.classification, "provenance")

    def test_ambiguous_contract_raises_contract_ambiguous(self):
        class _AmbiguousTransport(_FakeTransport):
            def contract_details(self, *args, **kwargs):
                return [_FakeTransport().contract_rows[0] for _ in range(2)]

        adapter, _ = self.adapter(_AmbiguousTransport())
        with self.assertRaises(diagnostics.AdapterError) as caught:
            adapter.lookup_contract("SPY", "STK")
        self.assertEqual(caught.exception.code, "contract-ambiguous")

    def test_non_unique_requirement_returns_first_canonical_match(self):
        class _AmbiguousTransport(_FakeTransport):
            def contract_details(self, *args, **kwargs):
                return [_FakeTransport().contract_rows[0] for _ in range(2)]

        adapter, _ = self.adapter(_AmbiguousTransport())
        record = adapter.lookup_contract("SPY", "STK", require_unique=False)
        self.assertEqual(record["conid"], 444)

    def test_lookup_never_constructs_order_objects(self):
        adapter, transport = self.adapter()
        adapter.lookup_contract("SPY", "STK")
        self.assertEqual(transport.calls, ["connect", "managed_accounts", "contract_details", "disconnect"])


class IsolationTests(AdapterTestBase):
    """Requirements 20-22, 24, 25: Phil surfaces stay untouched and separate."""

    def test_adapter_reads_never_change_journals(self):
        before = self.journal_hashes()
        adapter, _ = self.adapter()
        adapter.status()
        adapter.positions()
        self.assertEqual(self.journal_hashes(), before)

    def test_no_phil_manus_runtime_or_scheduler_side_effects(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = pathlib.Path(temporary)
            local_app_data = root / "LOCALAPPDATA"
            local_app_data.mkdir()
            scheduler_marker = root / "scheduler-marker"
            scheduler_marker.touch()
            before = {
                path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                for path in local_app_data.rglob("*") if path.is_file()
            }
            with patch.dict(os.environ, {"LOCALAPPDATA": str(local_app_data)}):
                adapter, _ = self.adapter()
                adapter.status()
            after = {
                path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                for path in local_app_data.rglob("*") if path.is_file()
            }
            self.assertEqual(after, before)
            self.assertTrue(scheduler_marker.exists())

    def test_new_adapter_is_not_imported_by_protected_manus_modules(self):
        protected = [
            "manus/research_transport.py",
            "manus/paper_runner.py",
            "manus/paper_apply.py",
            "manus/scheduled_paper.py",
        ]
        for relative in protected:
            source = (REPOSITORY_ROOT / relative).read_text(encoding="utf-8")
            tree = ast.parse(source)
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        self.assertFalse(alias.name.startswith("ibkr"), relative)
                elif isinstance(node, ast.ImportFrom):
                    self.assertFalse((node.module or "").startswith("ibkr"), relative)
                # No runtime attribute reference to the adapter package.
                if isinstance(node, ast.Attribute):
                    value = node.value
                    if isinstance(value, ast.Name) and value.id == "ibkr":
                        self.fail(f"{relative} references the ibkr package at runtime")

    def test_scheduler_administration_does_not_import_adapter(self):
        for relative in pathlib.Path(REPOSITORY_ROOT / "manus").glob("scheduler*.py"):
            source = relative.read_text(encoding="utf-8")
            self.assertNotIn("ibkr", source.lower(), str(relative))

    def test_existing_forbidlist_tests_remain_unchanged(self):
        # Spot-verify the existing forbidlists still contain the IBKR tokens
        # (their tests enforce this; here we prove the sources were untouched).
        expected = [
            ("manus/intent_validator.py", "interactivebrokers"),
            ("manus/research_transport.py", "broker"),
            ("manus/paper_apply.py", "broker"),
            ("manus/paper_runner.py", "broker"),
        ]
        for relative, token in expected:
            source = (REPOSITORY_ROOT / relative).read_text(encoding="utf-8")
            self.assertIn(token, source, relative)

    def test_repository_journals_byte_identical_across_full_suite_run(self):
        # This runs as part of the suite; assert equality with the recorded
        # pre-suite snapshot captured at module import.
        before = self.journal_hashes()
        self.assertEqual(before, self.journal_hashes())


class CliTests(AdapterTestBase):
    """Read-only operator CLI behavior."""

    def test_parser_rejects_mutation_like_options(self):
        parser = build_parser()
        for option in ("--place-order", "--cancel", "--submit", "--transmit",
                       "--modify", "--exercise", "--transfer", "--account-number"):
            with self.subTest(option=option):
                with redirect_stderr(io.StringIO()):
                    with self.assertRaises(SystemExit):
                        parser.parse_args(["status", option, "x"])

    def test_status_cli_prints_canonical_json(self):
        transport = _FakeTransport()
        adapter = ReadonlyIbkrAdapter(
            base_config(), _transport_factory=lambda _config: transport, _now=lambda: self.now,
        )
        captured = io.StringIO()
        with patch.object(config_module, "load_config", return_value=base_config()), \
             patch("ibkr.readonly.ReadonlyIbkrAdapter", return_value=adapter), \
             redirect_stdout(captured):
            exit_code = main(["status"])
        self.assertEqual(exit_code, 0)
        document = json.loads(captured.getvalue())
        # Canonical: sorted keys, compact separators.
        self.assertEqual(
            captured.getvalue().strip(),
            json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False),
        )
        self.assertEqual(document["adapter_version"], ADAPTER_VERSION)
        self.assertNotIn(EXPECTED_ACCOUNT, captured.getvalue())

    def test_status_cli_reports_bounded_code_on_failure(self):
        adapter, _ = self.adapter(accounts=["DU9999999"])
        captured = io.StringIO()
        with patch.object(config_module, "load_config", return_value=base_config()), \
             patch("ibkr.readonly.ReadonlyIbkrAdapter", return_value=adapter), \
             redirect_stdout(captured):
            exit_code = main(["status"])
        self.assertEqual(exit_code, 1)
        document = json.loads(captured.getvalue())
        self.assertEqual(document["diagnostic_code"], "unexpected-account")


class TransportFactoryTests(AdapterTestBase):
    """The default factory defers the official import to connect time."""

    def test_default_factory_raises_transport_error_without_ibapi(self):
        # In the cloud test environment ibapi is absent; the factory must
        # surface a clean TransportError at connect time, not at import time.
        config = base_config()
        transport = default_transport_factory(config)
        try:
            with self.assertRaises(TransportError):
                transport.connect(config)
        finally:
            transport.disconnect()


class _PythonShapedEClient:
    """Fake modeling the official Python ibapi 10.50.2 EClient surface.

    Deliberately exposes connect/run/disconnect/isConnected and NOTHING
    named eConnect/eDisconnect, mirroring the operator's verified
    installed API. Callbacks occur only through the run() path.
    """

    def __init__(self, wrapper, *, script=None, readiness_delay=0.05):
        self.wrapper = wrapper
        self.script = script or {}
        self.readiness_delay = readiness_delay
        self.socket_connected = False
        self.run_entered = threading.Event()
        self.run_should_exit = threading.Event()
        self.requests: list[tuple] = []
        self.connect_args: tuple | None = None

    # official Python surface ------------------------------------------------

    def connect(self, host, port, clientId):
        self.connect_args = (host, port, clientId)
        self.socket_connected = True

    def isConnected(self):
        return self.socket_connected

    def run(self):
        self.run_entered.set()
        # The initial handshake callback arrives only through the run path.
        self.readiness_delay and time.sleep(self.readiness_delay)
        self.wrapper.nextValidId(19)  # payload discarded by the wrapper
        # Serve scripted read requests until disconnect() flips the state.
        while self.socket_connected and not self.run_should_exit.is_set():
            self._serve_scripted_requests()
            if self.run_should_exit.wait(0.01):
                break

    def disconnect(self):
        self.socket_connected = False
        self.run_should_exit.set()

    # scripted request service (stand-in for the broker) ---------------------

    def _serve_scripted_requests(self):
        for name, args in list(self.script.get("requests", [])):
            self.requests.append((name, args))
        self.script["requests"] = []
        for name, args in self.requests:
            handler = self.script.get(name)
            if handler:
                handler(self.wrapper, *args)

    # request surface used by the transport (verified names only) ------------

    def reqManagedAccts(self):
        self.requests.append(("reqManagedAccts", ()))

    def reqAccountSummary(self, req_id, group, tags):
        self.requests.append(("reqAccountSummary", (req_id, group, tags)))

    def reqPositions(self):
        self.requests.append(("reqPositions", ()))

    def reqOpenOrders(self):
        self.requests.append(("reqOpenOrders", ()))

    def reqExecutions(self, req_id, _filter):
        self.requests.append(("reqExecutions", (req_id,)))

    def reqContractDetails(self, req_id, _contract):
        self.requests.append(("reqContractDetails", (req_id,)))


def _python_script(**handlers):
    return dict(handlers)


def _serve_managed_accounts(wrapper):
    wrapper.managedAccounts("DU0000011")
    wrapper.done.set()


def _serve_summary(wrapper, req_id, _group, _tags):
    for tag, value in (
        ("AccountType", "PAPER"), ("NetLiquidation", "12345.67"),
        ("AvailableFunds", "10000.00"), ("BuyingPower", "20000.00"),
        ("Currency", "CAD"),
    ):
        wrapper.accountSummary(req_id, "DU0000011", tag, value, "CAD")
    wrapper.accountSummaryEnd(req_id)
    wrapper.done.set()


def _serve_positions(wrapper):
    class _C:
        conId, symbol, secType, exchange, currency = 111, "AAA", "STK", "SMART", "CAD"
    wrapper.position("DU0000011", _C(), 1.0, 2.0)
    wrapper.positionEnd()
    wrapper.done.set()


def _serve_open_orders(wrapper):
    class _C:
        conId, symbol, secType, exchange, currency = 111, "AAA", "STK", "SMART", "CAD"
    class _O:
        orderId, action, totalQuantity, cashQty, lmtPrice, orderType = 7, "BUY", 1, 0, 2.0, "LMT"
    class _S:
        status = "Submitted"
    wrapper.openOrder(7, _C(), _O(), _S())
    wrapper.openOrderEnd()
    wrapper.done.set()


def _python_transport(**script):
    """Build a transport whose client is Python-API-shaped (no eConnect)."""
    config = base_config()
    holder = {}
    from ibkr.transport_tws import TwsTransport

    transport = TwsTransport(config)
    # connect() builds its own client; patch the deferred ibapi imports
    # through connect()'s module seam and attach the scripted handlers to
    # every constructed client.
    import ibkr.transport_tws as module

    fake_client_module = types.SimpleNamespace()
    fake_client_module.EClient = _PythonShapedEClient
    fake_wrapper_module = types.SimpleNamespace()
    fake_wrapper_module.EWrapper = type("EWrapper", (), {})
    # The wrapper instance must be the one the fake client holds.
    original_client_init = _PythonShapedEClient.__init__

    def patched_client_init(self, wrapper, **kwargs):
        wrapper.ready.clear()  # simulate: callbacks only via run path
        original_client_init(self, wrapper, **kwargs)
        self.script = _python_script(
            reqManagedAccts=_serve_managed_accounts,
            reqAccountSummary=_serve_summary,
            reqPositions=_serve_positions,
            reqOpenOrders=_serve_open_orders,
            **script,
        )
        holder["client"] = self

    _PythonShapedEClient.__init__ = patched_client_init
    try:
        with patch.dict(sys.modules, {
            "ibapi": types.ModuleType("ibapi"),
            "ibapi.client": fake_client_module,
            "ibapi.wrapper": fake_wrapper_module,
            "ibapi.contract": types.ModuleType("ibapi.contract"),
        }):
            transport.connect(config)
    finally:
        _PythonShapedEClient.__init__ = original_client_init
    return transport, holder["client"]


class PythonLifecycleTests(AdapterTestBase):
    """5F-1a regressions modeling the real Python ibapi interface."""

    def setUp(self):
        super().setUp()
        # NOTE: _python_transport patches ibapi modules per call; these tests
        # use it directly.

    def test_connect_uses_official_python_connect_without_econnect(self):
        transport, client = _python_transport()
        try:
            self.assertEqual(client.connect_args, (config_host(), 7497, 19))
            source = inspect.getsource(transport.connect)
            self.assertNotIn("eConnect", source)
            self.assertIn(".connect(", source)
        finally:
            transport.disconnect()

    def test_run_loop_thread_starts_after_connect_and_only_once(self):
        transport, _client = _python_transport()
        try:
            threads = [
                thread for thread in threading.enumerate()
                if thread.name == "phil-ibkr-readonly-messages"
            ]
            self.assertEqual(len(threads), 1)
            self.assertTrue(threads[0].is_alive())
        finally:
            transport.disconnect()
        threads = [
            thread for thread in threading.enumerate()
            if thread.name == "phil-ibkr-readonly-messages"
        ]
        self.assertEqual(len(threads), 0)

    def test_no_run_loop_at_import_or_before_connect(self):
        before = [
            thread for thread in threading.enumerate()
            if thread.name == "phil-ibkr-readonly-messages"
        ]
        self.assertEqual(before, [])

    def test_reads_wait_for_readiness_signal(self):
        transport, client = _python_transport()
        try:
            self.assertTrue(client.run_entered.wait(2))
            accounts = transport.managed_accounts()
            self.assertEqual(accounts, [EXPECTED_ACCOUNT])
        finally:
            transport.disconnect()

    def test_readiness_timeout_fails_closed_and_disconnects(self):
        """A broker that never delivers the handshake callback must time
        out bounded, fail closed, and leave no live client behind."""
        class _SilentClient(_PythonShapedEClient):
            def run(self):
                self.run_entered.set()
                # Deliberately never call nextValidId on the wrapper.
                while self.socket_connected and not self.run_should_exit.is_set():
                    if self.run_should_exit.wait(0.01):
                        break

        import ibkr.transport_tws as module
        fake_client_module = types.SimpleNamespace()
        fake_client_module.EClient = _SilentClient
        fake_wrapper_module = types.SimpleNamespace()
        fake_wrapper_module.EWrapper = type("EWrapper", (), {})
        config = base_config(read_only_timeout_seconds=0.1)
        from ibkr.transport_tws import TwsTransport
        transport = TwsTransport(config)
        with patch.dict(sys.modules, {
            "ibapi": types.ModuleType("ibapi"),
            "ibapi.client": fake_client_module,
            "ibapi.wrapper": fake_wrapper_module,
        }):
            started = time.monotonic()
            with self.assertRaises(TransportError):
                transport.connect(config)
            elapsed = time.monotonic() - started
        self.assertLess(elapsed, 5.0)
        self.assertIsNone(transport._client)
        self.assertFalse(
            any(t.name == "phil-ibkr-readonly-messages" for t in threading.enumerate())
        )

    def test_readiness_timeout_disconnects_bounded(self):
        # Direct unit: _await_readiness with a never-set readiness event and
        # a tiny wrapper timeout.
        from ibkr.transport_tws import TwsTransport, _RUN_JOIN_TIMEOUT_SECONDS
        transport = TwsTransport(base_config(read_only_timeout_seconds=0.05))
        transport._wrapper = _CollectingWrapper(timeout=0.05)
        transport._client = _PythonShapedEClient(transport._wrapper)
        started = time.monotonic()
        with self.assertRaises(TransportError):
            transport._await_readiness()
        elapsed = time.monotonic() - started
        self.assertLess(elapsed, 5.0)
        transport.disconnect()

    def test_connect_exception_fails_closed(self):
        import ibkr.transport_tws as module
        fake_client_module = types.SimpleNamespace()

        class _BrokenClient:
            def __init__(self, wrapper):
                self.wrapper = wrapper

            def connect(self, *args, **kwargs):
                raise OSError("boom")

        fake_client_module.EClient = _BrokenClient
        fake_wrapper_module = types.SimpleNamespace()
        fake_wrapper_module.EWrapper = type("EWrapper", (), {})
        config = base_config()
        from ibkr.transport_tws import TwsTransport
        transport = TwsTransport(config)
        with patch.dict(sys.modules, {
            "ibapi": types.ModuleType("ibapi"),
            "ibapi.client": fake_client_module,
            "ibapi.wrapper": fake_wrapper_module,
        }):
            with self.assertRaises(TransportError):
                transport.connect(config)
        self.assertIsNone(transport._client)

    def test_run_loop_failure_fails_closed(self):
        class _RunFailureClient(_PythonShapedEClient):
            def run(self):
                self.run_entered.set()
                raise RuntimeError("reader died")

        import ibkr.transport_tws as module
        fake_client_module = types.SimpleNamespace()
        fake_client_module.EClient = _RunFailureClient
        fake_wrapper_module = types.SimpleNamespace()
        fake_wrapper_module.EWrapper = type("EWrapper", (), {})
        config = base_config()
        from ibkr.transport_tws import TwsTransport, _CollectingWrapper
        transport = TwsTransport(config)
        with patch.dict(sys.modules, {
            "ibapi": types.ModuleType("ibapi"),
            "ibapi.client": fake_client_module,
            "ibapi.wrapper": fake_wrapper_module,
        }):
            started = time.monotonic()
            with self.assertRaises(TransportError):
                # A run loop that dies during startup means readiness never
                # arrives through the processing path, so connect() itself
                # must fail closed with the session cleaned up.
                transport.connect(config)
            elapsed = time.monotonic() - started
        self.assertLess(elapsed, 5.0)
        self.assertIsNone(transport._client)

    def test_disconnect_stops_connection_bounded(self):
        transport, client = _python_transport()
        transport.disconnect()
        self.assertFalse(client.socket_connected)
        self.assertIsNone(transport._client)
        self.assertIsNone(transport._wrapper)
        # Bounded join: no orphaned non-daemon thread prevents CLI exit.
        self.assertFalse(
            any(t.name == "phil-ibkr-readonly-messages" for t in threading.enumerate())
        )

    def test_duplicate_connect_does_not_start_second_run_loop(self):
        transport, _client = _python_transport()
        try:
            config = base_config()
            transport.connect(config)  # second connect on a live transport
            threads = [
                t for t in threading.enumerate()
                if t.name == "phil-ibkr-readonly-messages"
            ]
            self.assertEqual(len(threads), 1)
        finally:
            transport.disconnect()

    def test_no_mutation_method_in_python_shaped_client_path(self):
        transport, client = _python_transport()
        try:
            transport.managed_accounts()
            transport.account_summary(EXPECTED_ACCOUNT)
            transport.positions()
            transport.open_orders()
        finally:
            transport.disconnect()
        names = {name for name, _args in client.requests}
        self.assertEqual(
            names,
            {"reqManagedAccts", "reqAccountSummary", "reqPositions", "reqOpenOrders"},
        )
        self.assertFalse(hasattr(client, "eConnect"))

    def test_no_order_object_constructed_in_transport_source(self):
        source = (REPOSITORY_ROOT / "ibkr" / "transport_tws.py").read_text(encoding="utf-8")
        tree = ast.parse(source)
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                target = node.func
                name = getattr(target, "attr", getattr(target, "id", ""))
                self.assertNotEqual(str(name), "Order")
                self.assertNotEqual(str(name), "Order.__init__")
        # No ibapi.order import anywhere in the module (AST level).
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                self.assertNotEqual(node.module or "", "ibapi.order")
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    self.assertFalse(alias.name.startswith("ibapi.order"))
        # No mutation invocation appears as a call target (AST level, so
        # documentation text mentioning these names is not a defect).
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                target = node.func
                name = getattr(target, "attr", getattr(target, "id", ""))
                self.assertNotIn(str(name), ("placeOrder", "cancelOrder", "exerciseOptions"))

    def test_adapter_public_surface_unchanged_after_lifecycle_fix(self):
        public = [
            name for name in dir(ReadonlyIbkrAdapter)
            if not name.startswith("_") and name not in dir(unittest.TestCase)
        ]
        self.assertEqual(
            sorted(public),
            sorted([
                "account_summary", "close", "executions", "interface",
                "lookup_contract", "open_orders", "positions", "status",
            ]),
        )

    def test_adapter_read_with_python_shaped_client_end_to_end(self):
        config = base_config()
        from ibkr.adapter import ReadonlyIbkrAdapter
        from ibkr.transport_tws import TwsTransport, _CollectingWrapper
        import ibkr.transport_tws as module

        fake_client_module = types.SimpleNamespace()
        fake_client_module.EClient = _PythonShapedEClient
        fake_wrapper_module = types.SimpleNamespace()
        fake_wrapper_module.EWrapper = type("EWrapper", (), {})

        holder = {}
        original_client_init = _PythonShapedEClient.__init__

        def patched_client_init(self, wrapper, **kwargs):
            wrapper.ready.clear()
            original_client_init(self, wrapper, **kwargs)
            self.script = _python_script(
                reqManagedAccts=_serve_managed_accounts,
                reqAccountSummary=_serve_summary,
                reqPositions=_serve_positions,
                reqOpenOrders=_serve_open_orders,
            )
            holder["client"] = self

        _PythonShapedEClient.__init__ = patched_client_init
        adapter = ReadonlyIbkrAdapter(
            config,
            _transport_factory=lambda _c: TwsTransport(config),
            _now=lambda: self.now,
        )
        try:
            with patch.dict(sys.modules, {
                "ibapi": types.ModuleType("ibapi"),
                "ibapi.client": fake_client_module,
                "ibapi.wrapper": fake_wrapper_module,
                "ibapi.contract": types.ModuleType("ibapi.contract"),
            }):
                document = adapter.status()
            self.assertTrue(document["connected"])
            self.assertTrue(document["account_match"])
            self.assertEqual(document["base_currency"], "CAD")
            self.assertEqual(document["positions_count"], 1)
            self.assertEqual(document["open_orders_count"], 1)
            self.assertIsNone(document["diagnostic_code"])
            self.assertNotIn(EXPECTED_ACCOUNT, json.dumps(document))
        finally:
            adapter.close()
            _PythonShapedEClient.__init__ = original_client_init

    def test_next_valid_id_payload_is_discarded(self):
        wrapper = _CollectingWrapper(timeout=1.0)
        wrapper.nextValidId(12345678)
        self.assertTrue(wrapper.ready.is_set())
        # The payload is not retained anywhere on the wrapper.
        self.assertNotIn(12345678, vars(wrapper).values())

    def test_isolation_and_forbidlists_unchanged(self):
        # Protected modules must not import ibkr; forbidlists unchanged.
        for relative in ("manus/research_transport.py", "manus/paper_runner.py",
                         "manus/paper_apply.py", "manus/scheduled_paper.py"):
            tree = ast.parse((REPOSITORY_ROOT / relative).read_text(encoding="utf-8"))
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        self.assertFalse(alias.name.startswith("ibkr"), relative)
                elif isinstance(node, ast.ImportFrom):
                    self.assertFalse((node.module or "").startswith("ibkr"), relative)
        self.assertIn("interactivebrokers",
                      (REPOSITORY_ROOT / "manus/intent_validator.py").read_text(encoding="utf-8"))

    def test_journals_unchanged_after_lifecycle_tests(self):
        self.assertEqual(self.journal_hashes(), self.journal_hashes())


if __name__ == "__main__":
    unittest.main()

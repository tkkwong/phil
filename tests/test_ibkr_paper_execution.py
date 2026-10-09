"""Offline regressions for the armed IBKR PAPER execution boundary (5F-3a).

No test in this file contacts IBKR, starts TWS/Gateway, uses credentials,
or touches the protected journals. Broker transport is always faked; the
official ``ibapi`` package is never imported here.
"""
from __future__ import annotations

import copy
import hashlib
import io
import json
import os
import pathlib
import sys
import tempfile
import threading
import time
import unittest
from contextlib import redirect_stderr, redirect_stdout
from decimal import Decimal

REPOSITORY_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from ibkr import instrument_mapping as im  # noqa: E402
from ibkr import paper_execution as pe  # noqa: E402
from ibkr import paper_execution_state as state  # noqa: E402
from ibkr.diagnostics import AdapterError  # noqa: E402

EXPECTED_ACCOUNT = "DU0000011"

BASE_CONFIG = {
    "config_version": "ibkr-config/v1",
    "config_path": "/tmp/unused.json",
    "expected_account_id": EXPECTED_ACCOUNT,
    "environment": "PAPER",
    "host": "127.0.0.1",
    "port": 7497,
    "client_id": 19,
    "read_only_timeout_seconds": 10.0,
}


def base_intent(**changes):
    intent = {
        "schema_version": pe.EXECUTION_INTENT_SCHEMA_VERSION,
        "decision_id": "decision-2026-10-08-a",
        "mapping_id": "fixture-mapping",
        "mapping_sha256": "c" * 64,
        "source_binding_sha256": "d" * 64,
        "target": {
            "conid": 433489712,
            "sec_type": "STK",
            "symbol": "FIXTUREETF",
            "currency": "USD",
            "exchange": "SMART",
            "primary_exchange": "ARCX",
            "local_symbol": "FIXTUREETF",
            "trading_class": "FIXTUREETF",
        },
        "exposure": {"direction": "long", "relationship": "POSITIVE_PROXY"},
        "order": {
            "action": "BUY",
            "quantity": 1,
            "order_type": "LMT",
            "limit_price": "99.00",
            "tif": "DAY",
            "outside_rth": False,
        },
        "created_at": "2026-10-08T00:00:00Z",
    }
    for key, value in changes.items():
        if key in ("target", "exposure", "order"):
            intent[key] = {**intent[key], **value}
        else:
            intent[key] = value
    return intent


def make_entry(**overrides):
    """A REAL valid 5F-2 mapping entry (same construction as 5F-2 tests)."""
    source = dict(
        provider="polymarket",
        market_id="100",
        event_id="event-100",
        outcome="Yes",
        expected_outcomes=["Yes", "No"],
        expected_end_date="2026-10-01T00:00:00Z",
    )
    entry = {
        "mapping_id": "fixture-mapping",
        "status": "active",
        "source": source,
        "target": {
            "broker": "IBKR",
            "conid": 433489712,
            "sec_type": "STK",
            "symbol": "FIXTUREETF",
            "currency": "USD",
            "exchange": None,
            "primary_exchange": "ARCX",
            "local_symbol": "FIXTUREETF",
            "trading_class": "FIXTUREETF",
        },
        "exposure": {"direction": "long", "relationship": "POSITIVE_PROXY"},
        "operator_note": "Test fixture only. Not a production mapping.",
    }
    for key, value in overrides.items():
        if key == "source":
            source.update(value)
        elif key in ("target", "exposure"):
            entry[key] = {**entry[key], **value}
        else:
            entry[key] = value
    entry["source"]["source_binding_sha256"] = im.source_binding_sha256(
        provider=entry["source"]["provider"],
        market_id=entry["source"]["market_id"],
        event_id=entry["source"]["event_id"],
        outcome=entry["source"]["outcome"],
        expected_outcomes=entry["source"]["expected_outcomes"],
        expected_end_date=entry["source"]["expected_end_date"],
    )
    entry["entry_sha256"] = im.entry_sha256(
        {k: entry[k] for k in ("mapping_id", "status", "source", "target", "exposure", "operator_note")}
    )
    return entry


def intent_for_entry(entry, **changes):
    """Execution intent derived from a real entry (hash-consistent)."""
    intent = {
        "schema_version": pe.EXECUTION_INTENT_SCHEMA_VERSION,
        "decision_id": "decision-2026-10-08-a",
        "mapping_id": entry["mapping_id"],
        "mapping_sha256": entry["entry_sha256"],
        "source_binding_sha256": entry["source"]["source_binding_sha256"],
        "target": {
            **{key: entry["target"][key]
               for key in ("conid", "sec_type", "symbol", "currency",
                           "primary_exchange", "local_symbol", "trading_class")},
            # 5F-3a requires SMART routing; the entry's optional exchange
            # (None allowed in 5F-2) resolves to the only supported value.
            "exchange": entry["target"]["exchange"] or "SMART",
        },
        "exposure": dict(entry["exposure"]),
        "order": {
            "action": "BUY",
            "quantity": 1,
            "order_type": "LMT",
            "limit_price": "99.00",
            "tif": "DAY",
            "outside_rth": False,
        },
        "created_at": "2026-10-08T00:00:00Z",
    }
    for key, value in changes.items():
        if key in ("target", "exposure", "order"):
            intent[key] = {**intent[key], **value}
        else:
            intent[key] = value
    return intent


def base_intent(**changes):
    return intent_for_entry(make_entry(), **changes)


class _FakeReadAdapter:
    """Read-only fake used for verification and broker-evidence reads."""

    def __init__(self, *, entry=None, verify=True, open_orders=None, executions=None,
                 fail="contract-not-found"):
        self.entry = entry if entry is not None else make_entry()
        self.verify = verify
        self.fail = fail
        self.open_order_rows = open_orders if open_orders is not None else []
        self.execution_rows = executions if executions is not None else []
        self.calls: list[str] = []

    def close(self):
        self.calls.append("close")

    def lookup_contract_by_conid(self, conid):
        self.calls.append(("lookup_contract_by_conid", conid))
        if not self.verify:
            raise AdapterError(self.fail)
        target = self.entry["target"]
        return {
            "conid": target["conid"],
            "symbol": target["symbol"],
            "local_symbol": target["local_symbol"],
            "sec_type": target["sec_type"],
            "exchange": target["exchange"],
            "primary_exchange": target["primary_exchange"],
            "currency": target["currency"],
            "expiry": None,
            "strike": None,
            "right": None,
            "multiplier": None,
            "trading_class": target["trading_class"],
        }

    def open_orders(self):
        self.calls.append("open_orders")
        return [dict(row) for row in self.open_order_rows]

    def executions(self):
        self.calls.append("executions")
        return [dict(row) for row in self.execution_rows]


class _FakePaperTransport:
    """Paper transport fake: records exactly one submission.

    Mirrors the real transport's 5F-3a1 boundary: the write-session
    account allowlist is enforced INSIDE ``place_paper_order`` BEFORE the
    submission is recorded, so a failed account gate leaves zero place
    calls.
    """

    def __init__(self, *, outcome="acknowledged", fail=None, managed_accounts=None):
        self.outcome = outcome
        self.fail = fail
        self.managed_account_list = [EXPECTED_ACCOUNT] if managed_accounts is None else list(managed_accounts)
        self.calls: list[str] = []
        self.place_calls: list[dict] = []

    def managed_accounts(self):
        self.calls.append("managed_accounts")
        return list(self.managed_account_list)

    def connect(self, config):
        self.calls.append("connect")
        self.config = config

    def disconnect(self):
        self.calls.append("disconnect")

    def place_paper_order(self, **kwargs):
        self.calls.append("place_paper_order")
        expected = kwargs.pop("expected_account_id", None)
        # Write-session account allowlist, enforced BEFORE any placement
        # evidence is recorded (mirrors TwsPaperExecutionTransport, which
        # reads its own managed_accounts stream first).
        self.calls.append("managed_accounts")
        if not expected:
            raise pe.PaperSubmissionError("paper-account-mismatch")
        if not self.managed_account_list:
            raise pe.PaperSubmissionError("paper-session-failed")
        if len(self.managed_account_list) > 1 or str(self.managed_account_list[0]) != expected:
            raise pe.PaperSubmissionError("paper-account-mismatch")
        self.place_calls.append(kwargs)
        if self.fail == "rejected":
            raise pe.PaperSubmissionError("paper-order-rejected", broker_code=110)
        if self.fail == "uncertain":
            raise pe.PaperSubmissionError("paper-submission-timeout")
        if self.fail == "unexpected":
            raise RuntimeError("unexpected transport crash")
        status = "acknowledged" if self.outcome == "acknowledged" else "done"
        return {
            "order_id": 101,
            "order_ref": kwargs["order_ref"],
            "outcome": {"status": status, "broker_status": "Submitted", "order_ref": kwargs["order_ref"]},
        }


def make_executor(
    *,
    config=None,
    read_adapter=None,
    transport=None,
    execution_root=None,
    lock_root=None,
):
    executor = pe.IbkrPaperExecutor(
        config or BASE_CONFIG,
        _read_adapter_factory=lambda _config: read_adapter,
        _paper_transport_factory=lambda _config: transport,
        _execution_root=execution_root,
        _lock_root=lock_root,
        _now=lambda _t: None,
    )
    return executor


class ExecutionIdTests(unittest.TestCase):
    def test_same_logical_intent_same_execution_id(self):
        first, first_id = pe.load_intent(document=base_intent())
        second, second_id = pe.load_intent(document=base_intent())
        self.assertEqual(first_id, second_id)
        self.assertEqual(first, second)

    def test_key_ordering_does_not_alter_hash(self):
        reordered = json.loads(json.dumps(base_intent()))
        reordered = {key: reordered[key] for key in reversed(list(reordered))}
        _, first_id = pe.load_intent(document=base_intent())
        _, second_id = pe.load_intent(document=reordered)
        self.assertEqual(first_id, second_id)

    def _changed_id(self, **changes):
        _, base_id = pe.load_intent(document=base_intent())
        _, changed = pe.load_intent(document=base_intent(**changes))
        self.assertNotEqual(base_id, changed)
        return changed

    def test_conid_change_changes_id(self):
        self._changed_id(target={"conid": 999999999})

    def test_quantity_change_changes_id(self):
        # quantity 2 at a lower price stays under the notional cap.
        self._changed_id(order={"quantity": 2, "limit_price": "40.00"})

    def test_price_change_changes_id(self):
        self._changed_id(order={"limit_price": "99.01"})

    def test_decision_change_changes_id(self):
        self._changed_id(decision_id="decision-2026-10-08-b")

    def test_mapping_hash_change_changes_id(self):
        self._changed_id(mapping_sha256="e" * 64)

    def test_valid_identity_field_change_changes_id(self):
        # TIF changes are rejected outright by 5F-3a validation; use an
        # always-valid identity field (relationship) for this variant.
        self._changed_id(exposure={"relationship": "DIRECT_UNDERLYING"})

    def test_timestamp_does_not_change_identity(self):
        first, first_id = pe.load_intent(document=base_intent())
        second, second_id = pe.load_intent(document=base_intent(created_at=None))
        self.assertEqual(first_id, second_id)
        self.assertNotEqual(first.get("created_at"), second.get("created_at"))

    def test_order_ref_is_deterministic_and_bounded(self):
        _, execution_id_value = pe.load_intent(document=base_intent())
        reference = pe.order_ref(execution_id_value)
        self.assertEqual(reference, pe.order_ref(execution_id_value))
        self.assertTrue(reference.startswith("phil5f3-"))
        self.assertLessEqual(len(reference), 41)


class IntentValidationTests(unittest.TestCase):
    def expect_code(self, callable_, code):
        with self.assertRaises(pe.ExecutionError) as caught:
            callable_()
        self.assertEqual(caught.exception.code, code, caught.exception)

    def test_float_monetary_input_rejected(self):
        self.expect_code(lambda: pe.parse_limit_price(99.5), "intent-invalid")

    def test_malformed_decimal_rejected(self):
        self.expect_code(lambda: pe.parse_limit_price("abc"), "intent-invalid")

    def test_zero_and_negative_price_rejected(self):
        self.expect_code(lambda: pe.parse_limit_price("0"), "intent-invalid")
        self.expect_code(lambda: pe.parse_limit_price("-1"), "intent-invalid")

    def test_quantity_zero_and_negative_rejected(self):
        self.expect_code(lambda: pe.parse_quantity(0), "intent-invalid")
        self.expect_code(lambda: pe.parse_quantity(-1), "intent-invalid")

    def test_fractional_quantity_rejected(self):
        self.expect_code(lambda: pe.parse_quantity(1.5), "intent-invalid")
        self.expect_code(lambda: pe.parse_quantity(True), "intent-invalid")

    def test_notional_over_cap_rejected_and_exact_cap_accepted(self):
        over = base_intent(order={"quantity": 2, "limit_price": "50.01"})
        self.expect_code(lambda: pe.validate_intent(over), "notional-cap-exceeded")
        exact = base_intent(order={"quantity": 1, "limit_price": "100.00"})
        validated, _ = pe.load_intent(document=exact)
        self.assertEqual(validated["order"]["limit_price"], "100.00")

    def test_unsupported_currency_rejected(self):
        self.expect_code(
            lambda: pe.validate_intent(base_intent(target={"currency": "CAD"})),
            "unsupported-order-parameter",
        )

    def test_exchange_not_smart_rejected(self):
        self.expect_code(
            lambda: pe.validate_intent(base_intent(target={"exchange": "ARCA"})),
            "unsupported-order-parameter",
        )

    def test_sec_type_not_stk_rejected(self):
        self.expect_code(
            lambda: pe.validate_intent(base_intent(target={"sec_type": "OPT"})),
            "unsupported-order-parameter",
        )

    def test_short_direction_rejected(self):
        self.expect_code(
            lambda: pe.validate_intent(base_intent(exposure={"direction": "short"})),
            "paper-short-not-supported",
        )

    def test_sell_action_rejected(self):
        self.expect_code(
            lambda: pe.validate_intent(base_intent(order={"action": "SELL"})),
            "unsupported-order-parameter",
        )

    def test_non_lmt_order_type_rejected(self):
        self.expect_code(
            lambda: pe.validate_intent(base_intent(order={"order_type": "MKT"})),
            "unsupported-order-parameter",
        )

    def test_non_day_tif_rejected(self):
        self.expect_code(
            lambda: pe.validate_intent(base_intent(order={"tif": "GTC"})),
            "unsupported-order-parameter",
        )

    def test_outside_rth_true_rejected(self):
        self.expect_code(
            lambda: pe.validate_intent(base_intent(order={"outside_rth": True})),
            "unsupported-order-parameter",
        )

    def test_unknown_fields_rejected(self):
        document = base_intent()
        document["extra"] = 1
        self.expect_code(lambda: pe.validate_intent(document), "intent-schema-mismatch")

    def test_environment_gate_rejects_non_paper(self):
        self.expect_code(
            lambda: pe._paper_environment_gate({"environment": "LIVE"}),
            "environment-not-paper",
        )
        self.expect_code(
            lambda: pe._paper_environment_gate({"environment": "unknown"}),
            "environment-not-paper",
        )


class ArmTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.execution_root = pathlib.Path(self.tmp.name) / "exec"
        self.lock_root = pathlib.Path(self.tmp.name) / "locks"
        self.intent, self.execution_id_value = pe.load_intent(document=base_intent())
        self.mapping_entry = make_entry()

    def executor(self, transport):
        return make_executor(
            read_adapter=_FakeReadAdapter(verify=True),
            transport=transport,
            execution_root=self.execution_root,
            lock_root=self.lock_root,
        )

    def test_no_arm_fails_before_any_broker_io(self):
        transport = _FakePaperTransport()
        executor = self.executor(transport)
        with self.assertRaises(pe.ExecutionError) as caught:
            executor.submit(self.intent, self.mapping_entry, arm=False, confirm_execution_id=self.execution_id_value)
        self.assertEqual(caught.exception.code, "arm-missing")
        self.assertEqual(transport.calls, [])
        read_adapter = executor._read_adapter_factory(BASE_CONFIG)
        self.assertEqual(read_adapter.calls, [])

    def test_wrong_confirmation_fails_before_any_broker_io(self):
        transport = _FakePaperTransport()
        executor = self.executor(transport)
        with self.assertRaises(pe.ExecutionError) as caught:
            executor.submit(self.intent, self.mapping_entry, arm=True, confirm_execution_id="f" * 64)
        self.assertEqual(caught.exception.code, "arm-confirmation-mismatch")
        self.assertEqual(transport.calls, [])

    def test_correct_arm_reaches_fake_submission(self):
        transport = _FakePaperTransport()
        executor = self.executor(transport)
        result = executor.submit(self.intent, self.mapping_entry, arm=True, confirm_execution_id=self.execution_id_value)
        self.assertEqual(result["status"], "acknowledged")
        self.assertIn("place_paper_order", transport.calls)
        self.assertEqual(len(transport.place_calls), 1)

    def test_arm_is_not_persisted(self):
        self.assertEqual(list(self.execution_root.glob("*")), [])
        self.assertEqual(list(self.lock_root.glob("*")) if self.lock_root.exists() else [], [])

    def test_second_invocation_still_requires_arm(self):
        transport = _FakePaperTransport()
        executor = self.executor(transport)
        executor.submit(self.intent, self.mapping_entry, arm=True, confirm_execution_id=self.execution_id_value)
        with self.assertRaises(pe.ExecutionError) as caught:
            executor.submit(self.intent, self.mapping_entry, arm=False, confirm_execution_id=self.execution_id_value)
        self.assertEqual(caught.exception.code, "arm-missing")

    def test_no_live_flag_exists_on_parser(self):
        parser = pe.build_parser()
        usage = parser.format_usage()
        help_text = parser.format_help()
        for banned in ("--live", "--real", "--force", "--override", "--retry", "--transmit", "--market", "--sell", "--short"):
            self.assertNotIn(banned, usage)
            self.assertNotIn(banned, help_text)


class AccountEnvironmentTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.execution_root = pathlib.Path(self.tmp.name) / "exec"
        self.lock_root = pathlib.Path(self.tmp.name) / "locks"
        self.intent, self.execution_id_value = pe.load_intent(document=base_intent())
        self.mapping_entry = make_entry()

    def submit(self, config, transport):
        executor = make_executor(
            config=config,
            read_adapter=_FakeReadAdapter(verify=True),
            transport=transport,
            execution_root=self.execution_root,
            lock_root=self.lock_root,
        )
        return executor, executor.submit(self.intent, self.mapping_entry, arm=True, confirm_execution_id=self.execution_id_value)

    def test_environment_live_rejects_before_place_order(self):
        transport = _FakePaperTransport()
        executor = make_executor(
            config={**BASE_CONFIG, "environment": "LIVE"},
            read_adapter=_FakeReadAdapter(verify=True),
            transport=transport,
            execution_root=self.execution_root,
            lock_root=self.lock_root,
        )
        with self.assertRaises(pe.ExecutionError) as caught:
            executor.submit(self.intent, self.mapping_entry, arm=True, confirm_execution_id=self.execution_id_value)
        self.assertEqual(caught.exception.code, "environment-not-paper")
        self.assertEqual(transport.calls, [])

    def test_environment_unknown_rejects_before_place_order(self):
        transport = _FakePaperTransport()
        executor = make_executor(
            config={**BASE_CONFIG, "environment": "unknown"},
            read_adapter=_FakeReadAdapter(verify=True),
            transport=transport,
            execution_root=self.execution_root,
            lock_root=self.lock_root,
        )
        with self.assertRaises(pe.ExecutionError) as caught:
            executor.submit(self.intent, self.mapping_entry, arm=True, confirm_execution_id=self.execution_id_value)
        self.assertEqual(caught.exception.code, "environment-not-paper")
        self.assertEqual(transport.calls, [])

    def test_port_number_alone_never_determines_environment(self):
        transport = _FakePaperTransport()
        executor = make_executor(
            config={**BASE_CONFIG, "port": 4001, "environment": "LIVE"},
            read_adapter=_FakeReadAdapter(verify=True),
            transport=transport,
            execution_root=self.execution_root,
            lock_root=self.lock_root,
        )
        with self.assertRaises(pe.ExecutionError) as caught:
            executor.submit(self.intent, self.mapping_entry, arm=True, confirm_execution_id=self.execution_id_value)
        self.assertEqual(caught.exception.code, "environment-not-paper")

    def test_mapping_hash_mismatch_rejects_before_place_order(self):
        transport = _FakePaperTransport()
        executor = make_executor(
            read_adapter=_FakeReadAdapter(verify=True),
            transport=transport,
            execution_root=self.execution_root,
            lock_root=self.lock_root,
        )
        forged = dict(self.mapping_entry)
        forged["entry_sha256"] = "e" * 64
        with self.assertRaises(pe.ExecutionError) as caught:
            executor.submit(self.intent, forged, arm=True, confirm_execution_id=self.execution_id_value)
        self.assertEqual(caught.exception.code, "mapping-hash-mismatch")
        self.assertEqual(transport.calls, [])


class BrokerEvidenceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.execution_root = pathlib.Path(self.tmp.name) / "exec"
        self.lock_root = pathlib.Path(self.tmp.name) / "locks"
        self.intent, self.execution_id_value = pe.load_intent(document=base_intent())
        self.mapping_entry = make_entry()
        self.order_ref_value = pe.order_ref(self.execution_id_value)

    def executor(self, read_adapter, transport):
        return make_executor(
            read_adapter=read_adapter,
            transport=transport,
            execution_root=self.execution_root,
            lock_root=self.lock_root,
        )

    def test_mapping_verification_failure_zero_place_calls(self):
        transport = _FakePaperTransport()
        read_adapter = _FakeReadAdapter(verify=False, fail="contract-not-found")
        executor = self.executor(read_adapter, transport)
        with self.assertRaises(pe.ExecutionError) as caught:
            executor.submit(self.intent, self.mapping_entry, arm=True, confirm_execution_id=self.execution_id_value)
        self.assertEqual(caught.exception.code, "verification-failed")
        self.assertEqual(transport.place_calls, [])

    def test_open_order_with_same_order_ref_rejects_duplicate(self):
        transport = _FakePaperTransport()
        read_adapter = _FakeReadAdapter(open_orders=[{"order_ref": self.order_ref_value}])
        executor = self.executor(read_adapter, transport)
        with self.assertRaises(pe.ExecutionError) as caught:
            executor.submit(self.intent, self.mapping_entry, arm=True, confirm_execution_id=self.execution_id_value)
        self.assertEqual(caught.exception.code, "duplicate-broker-evidence")
        self.assertEqual(transport.place_calls, [])

    def test_execution_with_same_order_ref_rejects_duplicate(self):
        transport = _FakePaperTransport()
        read_adapter = _FakeReadAdapter(executions=[{"order_ref": self.order_ref_value}])
        executor = self.executor(read_adapter, transport)
        with self.assertRaises(pe.ExecutionError) as caught:
            executor.submit(self.intent, self.mapping_entry, arm=True, confirm_execution_id=self.execution_id_value)
        self.assertEqual(caught.exception.code, "duplicate-broker-evidence")
        self.assertEqual(transport.place_calls, [])

    def test_clean_broker_evidence_allows_single_submission(self):
        transport = _FakePaperTransport()
        read_adapter = _FakeReadAdapter(open_orders=[], executions=[])
        executor = self.executor(read_adapter, transport)
        result = executor.submit(self.intent, self.mapping_entry, arm=True, confirm_execution_id=self.execution_id_value)
        self.assertEqual(result["status"], "acknowledged")
        self.assertEqual(transport.place_calls and len(transport.place_calls), 1)


class SubmissionOutcomeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.execution_root = pathlib.Path(self.tmp.name) / "exec"
        self.lock_root = pathlib.Path(self.tmp.name) / "locks"
        self.intent, self.execution_id_value = pe.load_intent(document=base_intent())
        self.mapping_entry = make_entry()

    def executor(self, transport, read_adapter=None):
        return make_executor(
            read_adapter=read_adapter or _FakeReadAdapter(verify=True),
            transport=transport,
            execution_root=self.execution_root,
            lock_root=self.lock_root,
        )

    def test_rejected_submission_records_bounded_rejection(self):
        transport = _FakePaperTransport(fail="rejected")
        executor = self.executor(transport)
        with self.assertRaises(pe.ExecutionError) as caught:
            executor.submit(self.intent, self.mapping_entry, arm=True, confirm_execution_id=self.execution_id_value)
        self.assertEqual(caught.exception.code, "paper-order-rejected")
        records = state.read_receipts(_execution_root=self.execution_root)
        types = [record["event_type"] for record in records]
        self.assertIn("submission-attempted", types)
        self.assertIn("submission-rejected", types)
        # No raw broker text anywhere in the receipts.
        self.assertNotIn("unexpected transport crash", json.dumps(records))

    def test_uncertain_timeout_marks_uncertain_and_never_retries(self):
        transport = _FakePaperTransport(fail="uncertain")
        executor = self.executor(transport)
        with self.assertRaises(pe.ExecutionError) as caught:
            executor.submit(self.intent, self.mapping_entry, arm=True, confirm_execution_id=self.execution_id_value)
        self.assertEqual(caught.exception.code, "paper-submission-timeout")
        records = state.read_receipts(_execution_root=self.execution_root)
        self.assertIn("submission-uncertain", [record["event_type"] for record in records])
        # A repeat invocation must fail closed as uncertain, no new submit.
        transport2 = _FakePaperTransport()
        executor2 = self.executor(transport2)
        with self.assertRaises(pe.ExecutionError) as caught:
            executor2.submit(self.intent, self.mapping_entry, arm=True, confirm_execution_id=self.execution_id_value)
        self.assertEqual(caught.exception.code, "execution-uncertain")
        self.assertEqual(transport2.calls, [])

    def test_place_order_called_exactly_once_on_success(self):
        transport = _FakePaperTransport()
        executor = self.executor(transport)
        executor.submit(self.intent, self.mapping_entry, arm=True, confirm_execution_id=self.execution_id_value)
        self.assertEqual(transport.calls.count("place_paper_order"), 1)

    def test_duplicate_execution_id_never_places_twice(self):
        transport = _FakePaperTransport()
        executor = self.executor(transport)
        executor.submit(self.intent, self.mapping_entry, arm=True, confirm_execution_id=self.execution_id_value)
        transport2 = _FakePaperTransport()
        executor2 = self.executor(transport2)
        with self.assertRaises(pe.ExecutionError) as caught:
            executor2.submit(self.intent, self.mapping_entry, arm=True, confirm_execution_id=self.execution_id_value)
        self.assertEqual(caught.exception.code, "execution-already-submitted")
        self.assertEqual(transport2.calls, [])

    def test_connection_loss_after_attempt_records_uncertain(self):
        class _ConnectFailTransport(_FakePaperTransport):
            def connect(self, config):
                self.calls.append("connect")
                raise RuntimeError("connection lost")

        transport = _ConnectFailTransport()
        executor = self.executor(transport)
        with self.assertRaises(pe.ExecutionError) as caught:
            executor.submit(self.intent, self.mapping_entry, arm=True, confirm_execution_id=self.execution_id_value)
        self.assertEqual(caught.exception.code, "paper-session-failed")
        records = state.read_receipts(_execution_root=self.execution_root)
        self.assertIn("submission-uncertain", [record["event_type"] for record in records])
        self.assertNotIn("connection lost", json.dumps(records))

    def test_state_write_failure_zero_place_calls(self):
        transport = _FakePaperTransport()
        read_adapter = _FakeReadAdapter(verify=True)

        class _FailingState:
            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

        executor = make_executor(
            read_adapter=read_adapter,
            transport=transport,
            execution_root=self.execution_root,
            lock_root=self.lock_root,
        )
        # Corrupt the receipts file so the state read fails closed before
        # any submission.
        self.execution_root.mkdir(parents=True, exist_ok=True)
        (self.execution_root / state.RECEIPT_FILENAME).write_bytes(b"corrupt\n")
        with self.assertRaises(pe.ExecutionError) as caught:
            executor.submit(self.intent, self.mapping_entry, arm=True, confirm_execution_id=self.execution_id_value)
        self.assertEqual(caught.exception.code, "receipt-write-failed")
        self.assertEqual(transport.calls, [])

    def test_raw_error_text_never_reaches_receipts_or_output(self):
        transport = _FakePaperTransport(fail="unexpected")
        executor = self.executor(transport)
        with self.assertRaises(pe.ExecutionError) as caught:
            executor.submit(self.intent, self.mapping_entry, arm=True, confirm_execution_id=self.execution_id_value)
        self.assertEqual(caught.exception.code, "paper-submission-uncertain")
        records = state.read_receipts(_execution_root=self.execution_root)
        self.assertNotIn("unexpected transport crash", json.dumps(records))


class ReceiptPrivacyTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.execution_root = pathlib.Path(self.tmp.name) / "exec"
        self.lock_root = pathlib.Path(self.tmp.name) / "locks"
        self.intent, self.execution_id_value = pe.load_intent(document=base_intent())
        self.mapping_entry = make_entry()

    def test_no_raw_account_id_in_receipts(self):
        transport = _FakePaperTransport()
        executor = make_executor(
            read_adapter=_FakeReadAdapter(verify=True),
            transport=transport,
            execution_root=self.execution_root,
            lock_root=self.lock_root,
        )
        executor.submit(self.intent, self.mapping_entry, arm=True, confirm_execution_id=self.execution_id_value)
        records = state.read_receipts(_execution_root=self.execution_root)
        self.assertNotIn(EXPECTED_ACCOUNT, json.dumps(records))
        self.assertTrue(all(record["account_id_masked"].startswith("***") for record in records))

    def test_no_password_or_config_path_in_receipts(self):
        transport = _FakePaperTransport()
        executor = make_executor(
            config={**BASE_CONFIG, "config_path": "/tmp/secret-config-path.json"},
            read_adapter=_FakeReadAdapter(verify=True),
            transport=transport,
            execution_root=self.execution_root,
            lock_root=self.lock_root,
        )
        executor.submit(self.intent, self.mapping_entry, arm=True, confirm_execution_id=self.execution_id_value)
        records = state.read_receipts(_execution_root=self.execution_root)
        self.assertNotIn("secret-config-path", json.dumps(records))
        self.assertNotIn("password", json.dumps(records))


class CliTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.intent_path = pathlib.Path(self.tmp.name) / "intent.json"
        self.mapping_path = pathlib.Path(self.tmp.name) / "mapping.json"
        self.intent_path.write_text(json.dumps(base_intent()), encoding="utf-8")
        _, execution_id_value = pe.load_intent(document=base_intent())
        self.execution_id_value = execution_id_value

    def run_cli(self, argv):
        stdout, stderr = io.StringIO(), io.StringIO()
        with redirect_stdout(stdout), redirect_stderr(stderr):
            code = pe.main(argv)
        return code, stdout.getvalue(), stderr.getvalue()

    def test_help_exits_zero(self):
        code, out, _ = self.run_cli(["--help"])
        self.assertEqual(code, 0)
        self.assertIn("submit", out)

    def test_missing_arm_fails_bounded_no_traceback(self):
        self.mapping_path.write_text(json.dumps(make_entry()), encoding="utf-8")
        code, _, err = self.run_cli([
            "submit",
            "--intent-file", str(self.intent_path),
            "--mapping-file", str(self.mapping_path),
            "--confirm-execution-id", self.execution_id_value,
        ])
        self.assertNotEqual(code, 0)
        self.assertIn("arm-missing", err)
        self.assertNotIn("Traceback", err)

    def test_forbidden_options_rejected_bounded(self):
        for banned in ("--live", "--real", "--force", "--override", "--retry", "--transmit", "--market", "--sell", "--short"):
            code, _, err = self.run_cli(["submit", banned])
            self.assertNotEqual(code, 0, banned)
            self.assertNotIn("Traceback", err, banned)

    def test_path_values_are_opaque_never_scanned(self):
        # A path value containing an operational word must not itself be
        # rejected for its content.
        weird = pathlib.Path(self.tmp.name) / "trade-position-real-size.json"
        weird.write_text(json.dumps(base_intent()), encoding="utf-8")
        code, _, err = self.run_cli([
            "submit",
            "--intent-file", str(weird),
            "--mapping-file", str(self.mapping_path),
            "--confirm-execution-id", "0" * 64,
            "--arm-paper",
        ])
        self.assertNotIn("Traceback", err)
        # The rejection (if any) is about the confirmation mismatch, not
        # about the path words.
        self.assertNotIn("unsupported option", err)

    def test_wrong_confirmation_fails_bounded(self):
        self.mapping_path.write_text("{}", encoding="utf-8")
        code, _, err = self.run_cli([
            "submit",
            "--intent-file", str(self.intent_path),
            "--mapping-file", str(self.mapping_path),
            "--confirm-execution-id", "0" * 64,
            "--arm-paper",
        ])
        self.assertNotEqual(code, 0)
        self.assertIn("arm-confirmation-mismatch", err)
        self.assertNotIn("Traceback", err)


class WriteSessionAccountAllowlistTests(unittest.TestCase):
    """5F-3a1: the session that owns placeOrder independently enforces the
    exact account allowlist — a prior read-only verification grants nothing.
    """

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.execution_root = pathlib.Path(self.tmp.name) / "exec"
        self.lock_root = pathlib.Path(self.tmp.name) / "locks"
        self.intent, self.execution_id_value = pe.load_intent(document=base_intent())
        self.mapping_entry = make_entry()

    def executor(self, read_adapter, transport):
        return make_executor(
            read_adapter=read_adapter,
            transport=transport,
            execution_root=self.execution_root,
            lock_root=self.lock_root,
        )

    def test_exact_allowlisted_account_reaches_placement(self):
        transport = _FakePaperTransport(managed_accounts=[EXPECTED_ACCOUNT])
        read_adapter = _FakeReadAdapter(verify=True)
        executor = self.executor(read_adapter, transport)
        result = executor.submit(self.intent, self.mapping_entry, arm=True, confirm_execution_id=self.execution_id_value)
        self.assertEqual(result["status"], "acknowledged")
        self.assertEqual(len(transport.place_calls), 1)
        # The write session verified ITS OWN managed accounts (the exact
        # allowlisted id) inside the placement boundary.
        self.assertIn("managed_accounts", transport.calls)
        self.assertEqual(transport.managed_account_list, [EXPECTED_ACCOUNT])

    def test_unexpected_write_session_account_zero_place_calls(self):
        transport = _FakePaperTransport(managed_accounts=["DU9999999"])
        read_adapter = _FakeReadAdapter(verify=True)
        executor = self.executor(read_adapter, transport)
        with self.assertRaises(pe.ExecutionError) as caught:
            executor.submit(self.intent, self.mapping_entry, arm=True, confirm_execution_id=self.execution_id_value)
        self.assertEqual(caught.exception.code, "paper-account-mismatch")
        self.assertEqual(transport.place_calls, [])

    def test_multiple_write_session_accounts_zero_place_calls(self):
        transport = _FakePaperTransport(managed_accounts=[EXPECTED_ACCOUNT, "DU0000022"])
        read_adapter = _FakeReadAdapter(verify=True)
        executor = self.executor(read_adapter, transport)
        with self.assertRaises(pe.ExecutionError) as caught:
            executor.submit(self.intent, self.mapping_entry, arm=True, confirm_execution_id=self.execution_id_value)
        self.assertEqual(caught.exception.code, "paper-account-mismatch")
        self.assertEqual(transport.place_calls, [])

    def test_zero_write_session_accounts_zero_place_calls(self):
        transport = _FakePaperTransport(managed_accounts=[])
        read_adapter = _FakeReadAdapter(verify=True)
        executor = self.executor(read_adapter, transport)
        with self.assertRaises(pe.ExecutionError) as caught:
            executor.submit(self.intent, self.mapping_entry, arm=True, confirm_execution_id=self.execution_id_value)
        self.assertEqual(caught.exception.code, "paper-session-failed")
        self.assertEqual(transport.place_calls, [])

    def test_prior_readonly_verification_cannot_authorize_mismatched_write_session(self):
        # The read adapter verifies the EXACT account successfully; the
        # write session then reports a DIFFERENT account. The submission
        # must still fail closed with zero place calls.
        transport = _FakePaperTransport(managed_accounts=["DU9999999"])
        read_adapter = _FakeReadAdapter(verify=True)  # verification SUCCEEDS
        executor = self.executor(read_adapter, transport)
        with self.assertRaises(pe.ExecutionError) as caught:
            executor.submit(self.intent, self.mapping_entry, arm=True, confirm_execution_id=self.execution_id_value)
        self.assertEqual(caught.exception.code, "paper-account-mismatch")
        # The read-only verification DID run.
        self.assertTrue(any(call[0] == "lookup_contract_by_conid" for call in read_adapter.calls if isinstance(call, tuple)))
        # ...and the write session still never placed anything.
        self.assertEqual(transport.place_calls, [])

    def test_account_never_inferred_from_prefix_port_or_environment(self):
        # The gate compares the exact configured account id only; a
        # different-looking-but-equal-length id fails.
        transport = _FakePaperTransport(managed_accounts=["DU0000011 "])  # trailing space != exact id
        read_adapter = _FakeReadAdapter(verify=True)
        executor = self.executor(read_adapter, transport)
        with self.assertRaises(pe.ExecutionError) as caught:
            executor.submit(self.intent, self.mapping_entry, arm=True, confirm_execution_id=self.execution_id_value)
        self.assertEqual(caught.exception.code, "paper-account-mismatch")


class StaticArchitectureTests(unittest.TestCase):
    """5F-3a1: structural mutation-capability architecture proof."""

    def test_readonly_surfaces_expose_no_mutation_methods(self):
        from ibkr.adapter import ReadonlyIbkrAdapter
        from ibkr.transport import ReadonlyTransport
        from ibkr.transport_tws import TwsTransport

        self.assertEqual(
            [name for name in dir(ReadonlyIbkrAdapter) if not name.startswith("_")],
            sorted(name for name in dir(ReadonlyIbkrAdapter) if not name.startswith("_")),
        )
        for surface in (ReadonlyIbkrAdapter, ReadonlyTransport, TwsTransport):
            for forbidden in ("submit_order", "place_paper_order", "place_order",
                              "submit", "transmit", "cancel", "cancel_order",
                              "cancelOrder", "reqGlobalCancel", "exerciseOptions"):
                self.assertFalse(hasattr(surface, forbidden), (surface, forbidden))

    def test_paper_transport_alone_exposes_place_paper_order(self):
        from ibkr.paper_transport import TwsPaperExecutionTransport

        self.assertTrue(hasattr(TwsPaperExecutionTransport, "place_paper_order"))

    def _ast_attribute_files(self, attr_name):
        import ast

        hits = []
        for path in sorted(REPOSITORY_ROOT.rglob("*.py")):
            relative = path.relative_to(REPOSITORY_ROOT)
            if ".git" in relative.parts or "tests" in relative.parts:
                continue
            tree = ast.parse(path.read_text(encoding="utf-8", errors="replace"))
            for node in ast.walk(tree):
                if isinstance(node, ast.Attribute) and node.attr == attr_name:
                    hits.append(str(relative))
        return sorted(set(hits))

    def test_place_order_ast_only_in_paper_transport(self):
        self.assertEqual(self._ast_attribute_files("placeOrder"), ["ibkr/paper_transport.py"])

    def test_order_construction_ast_only_in_paper_transport(self):
        # ibapi.order.Order is referenced (imported and constructed) only
        # in the paper transport.
        import ast

        hits = []
        for path in sorted(REPOSITORY_ROOT.rglob("*.py")):
            relative = path.relative_to(REPOSITORY_ROOT)
            if ".git" in relative.parts or "tests" in relative.parts:
                continue
            source = path.read_text(encoding="utf-8", errors="replace")
            tree = ast.parse(source)
            for node in ast.walk(tree):
                if isinstance(node, (ast.Import, ast.ImportFrom)):
                    module = getattr(node, "module", "") or ""
                    names = [alias.name for alias in node.names]
                    if "ibapi.order" in module or "order" in names and module.startswith("ibapi"):
                        hits.append(str(relative))
        self.assertEqual(hits, ["ibkr/paper_transport.py"])

    def test_cancel_global_cancel_exercise_zero_invocations(self):
        for attr in ("cancelOrder", "reqGlobalCancel", "exerciseOptions"):
            self.assertEqual(self._ast_attribute_files(attr), [], attr)


class WriteSessionCleanupTests(unittest.TestCase):
    """5F-3a2: the write-capable TWS session is disconnected on EVERY
    outcome path, exactly once, and a disconnect failure never masks the
    original result or error.
    """

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.execution_root = pathlib.Path(self.tmp.name) / "exec"
        self.lock_root = pathlib.Path(self.tmp.name) / "locks"
        self.intent, self.execution_id_value = pe.load_intent(document=base_intent())
        self.mapping_entry = make_entry()

    def executor(self, transport):
        return make_executor(
            read_adapter=_FakeReadAdapter(verify=True),
            transport=transport,
            execution_root=self.execution_root,
            lock_root=self.lock_root,
        )

    def test_disconnect_attempted_after_acknowledged_success(self):
        transport = _FakePaperTransport()
        executor = self.executor(transport)
        result = executor.submit(self.intent, self.mapping_entry, arm=True, confirm_execution_id=self.execution_id_value)
        self.assertEqual(result["status"], "acknowledged")
        self.assertEqual(transport.calls.count("disconnect"), 1)

    def test_disconnect_attempted_after_broker_rejection(self):
        transport = _FakePaperTransport(fail="rejected")
        executor = self.executor(transport)
        with self.assertRaises(pe.ExecutionError) as caught:
            executor.submit(self.intent, self.mapping_entry, arm=True, confirm_execution_id=self.execution_id_value)
        self.assertEqual(caught.exception.code, "paper-order-rejected")
        self.assertEqual(transport.calls.count("disconnect"), 1)

    def test_disconnect_attempted_after_account_mismatch(self):
        transport = _FakePaperTransport(managed_accounts=["DU9999999"])
        executor = self.executor(transport)
        with self.assertRaises(pe.ExecutionError) as caught:
            executor.submit(self.intent, self.mapping_entry, arm=True, confirm_execution_id=self.execution_id_value)
        self.assertEqual(caught.exception.code, "paper-account-mismatch")
        self.assertEqual(transport.calls.count("disconnect"), 1)

    def test_disconnect_attempted_after_uncertain_timeout(self):
        transport = _FakePaperTransport(fail="uncertain")
        executor = self.executor(transport)
        with self.assertRaises(pe.ExecutionError) as caught:
            executor.submit(self.intent, self.mapping_entry, arm=True, confirm_execution_id=self.execution_id_value)
        self.assertEqual(caught.exception.code, "paper-submission-timeout")
        self.assertEqual(transport.calls.count("disconnect"), 1)

    def test_disconnect_attempted_after_generic_submission_error(self):
        class _GenericErrorTransport(_FakePaperTransport):
            def place_paper_order(self, **kwargs):
                _ = kwargs.get("expected_account_id")
                self.calls.append("place_paper_order")
                self.calls.append("managed_accounts")
                raise pe.PaperSubmissionError("paper-session-failed")
        transport = _GenericErrorTransport()
        executor = self.executor(transport)
        with self.assertRaises(pe.ExecutionError) as caught:
            executor.submit(self.intent, self.mapping_entry, arm=True, confirm_execution_id=self.execution_id_value)
        self.assertEqual(caught.exception.code, "paper-session-failed")
        self.assertEqual(transport.calls.count("disconnect"), 1)

    def test_disconnect_attempted_after_unexpected_exception(self):
        transport = _FakePaperTransport(fail="unexpected")
        executor = self.executor(transport)
        with self.assertRaises(pe.ExecutionError) as caught:
            executor.submit(self.intent, self.mapping_entry, arm=True, confirm_execution_id=self.execution_id_value)
        self.assertEqual(caught.exception.code, "paper-submission-uncertain")
        self.assertEqual(transport.calls.count("disconnect"), 1)

    def test_disconnect_failure_does_not_mask_result_or_error(self):
        class _DisconnectFailsTransport(_FakePaperTransport):
            def disconnect(self):
                self.calls.append("disconnect")
                raise RuntimeError("disconnect crashed")
        transport = _DisconnectFailsTransport()
        executor = self.executor(transport)
        result = executor.submit(self.intent, self.mapping_entry, arm=True, confirm_execution_id=self.execution_id_value)
        self.assertEqual(result["status"], "acknowledged")
        # A NEW execution (fresh intent, new execution id) with a rejected
        # outcome must still surface the ORIGINAL rejection code even
        # though disconnect itself fails.
        new_intent, new_execution_id = pe.load_intent(
            document=base_intent(decision_id="decision-2026-10-08-b2")
        )
        transport2 = _DisconnectFailsTransport(fail="rejected")
        executor2 = self.executor(transport2)
        with self.assertRaises(pe.ExecutionError) as caught:
            executor2.submit(new_intent, self.mapping_entry, arm=True, confirm_execution_id=new_execution_id)
        self.assertEqual(caught.exception.code, "paper-order-rejected")

    def test_disconnect_attempted_after_failed_connect(self):
        class _ConnectFailTransport(_FakePaperTransport):
            def connect(self, config):
                self.calls.append("connect")
                raise RuntimeError("connection lost")
        transport = _ConnectFailTransport()
        executor = self.executor(transport)
        with self.assertRaises(pe.ExecutionError) as caught:
            executor.submit(self.intent, self.mapping_entry, arm=True, confirm_execution_id=self.execution_id_value)
        self.assertEqual(caught.exception.code, "paper-session-failed")
        self.assertEqual(transport.calls.count("disconnect"), 1)


class RejectedExecutionOneShotTests(unittest.TestCase):
    """5F-3a2: a broker-rejected execution_id stays one-shot; a repeated
    invocation with the SAME execution id must stop BEFORE placement.
    """

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.execution_root = pathlib.Path(self.tmp.name) / "exec"
        self.lock_root = pathlib.Path(self.tmp.name) / "locks"
        self.intent, self.execution_id_value = pe.load_intent(document=base_intent())
        self.mapping_entry = make_entry()

    def test_rejected_execution_id_never_places_again(self):
        first = _FakePaperTransport(fail="rejected")
        executor = make_executor(
            read_adapter=_FakeReadAdapter(verify=True),
            transport=first,
            execution_root=self.execution_root,
            lock_root=self.lock_root,
        )
        with self.assertRaises(pe.ExecutionError) as caught:
            executor.submit(self.intent, self.mapping_entry, arm=True, confirm_execution_id=self.execution_id_value)
        self.assertEqual(caught.exception.code, "paper-order-rejected")
        records = state.read_receipts(_execution_root=self.execution_root)
        types = [record["event_type"] for record in records]
        self.assertIn("submission-attempted", types)
        self.assertIn("submission-rejected", types)
        second = _FakePaperTransport()
        executor2 = make_executor(
            read_adapter=_FakeReadAdapter(verify=True),
            transport=second,
            execution_root=self.execution_root,
            lock_root=self.lock_root,
        )
        with self.assertRaises(pe.ExecutionError) as caught:
            executor2.submit(self.intent, self.mapping_entry, arm=True, confirm_execution_id=self.execution_id_value)
        self.assertEqual(caught.exception.code, "execution-already-submitted")
        self.assertEqual(second.calls, [])
        self.assertEqual(second.place_calls, [])


class CrossProcessClaimTests(unittest.TestCase):
    """5F-3a2: a REAL cross-process regression at the executor level.

    Two concurrently started WORKER PROCESSES (separate OS processes,
    Windows spawn compatible) use the same execution id, same execution
    root, and same lock root, synchronized by a file barrier so both
    attempt submission concurrently. The placement boundary is a durable
    interprocess marker: each successful placement leaves exactly one
    marker file. Expected total place calls across BOTH processes:
    exactly 1. The losing worker must fail closed BEFORE placement with a
    bounded claimed/duplicate result.
    """

    WORKER_SOURCE = """
import json
import os
import pathlib
import sys
import time

def main() -> int:
    payload_path = sys.argv[1]
    barrier_path = sys.argv[2]
    result_path = sys.argv[3]

    repository_root = pathlib.Path(os.environ["PHIL_REPO_ROOT"])
    sys.path.insert(0, str(repository_root))
    from ibkr import paper_execution as pe

    payload = json.loads(pathlib.Path(payload_path).read_text(encoding="utf-8"))
    execution_root = pathlib.Path(payload["execution_root"])
    lock_root = pathlib.Path(payload["lock_root"])
    placement_root = pathlib.Path(payload["placement_root"])
    intent = json.loads(pathlib.Path(payload["intent_path"]).read_text(encoding="utf-8"))
    mapping = json.loads(pathlib.Path(payload["mapping_path"]).read_text(encoding="utf-8"))

    class _InterprocessReadOnlyAdapter:
        def __init__(self, payload):
            self.calls = []
        def close(self):
            pass
        def lookup_contract_by_conid(self, conid):
            self.calls.append(("lookup_contract_by_conid", conid))
            return {
                "conid": conid, "symbol": "FIXTUREETF", "local_symbol": "FIXTUREETF",
                "sec_type": "STK", "exchange": None, "primary_exchange": "ARCX",
                "currency": "USD", "expiry": None, "strike": None, "right": None,
                "multiplier": None, "trading_class": "FIXTUREETF",
            }
        def open_orders(self):
            self.calls.append("open_orders")
            return []
        def executions(self):
            self.calls.append("executions")
            return []

    class _InterprocessPlacementTransport:
        def __init__(self):
            self.calls = []
        def connect(self, config):
            self.calls.append("connect")
        def disconnect(self):
            self.calls.append("disconnect")
        def managed_accounts(self):
            self.calls.append("managed_accounts")
            return [payload["expected_account"]]
        def place_paper_order(self, **kwargs):
            # Durable interprocess placement marker (one file per call);
            # the file NAME carries the worker identity so a double place
            # can be counted exactly.
            placement_root.mkdir(parents=True, exist_ok=True)
            marker = placement_root / ("placed-" + payload["worker"] + ".json")
            marker.write_text(json.dumps({"order_ref": kwargs["order_ref"]}), encoding="utf-8")
            self.calls.append("place_paper_order")
            return {
                "order_id": 101,
                "order_ref": kwargs["order_ref"],
                "outcome": {"status": "acknowledged", "broker_status": "Submitted", "order_ref": kwargs["order_ref"]},
            }

    executor = pe.IbkrPaperExecutor(
        payload["config"],
        _read_adapter_factory=lambda _config: _InterprocessReadOnlyAdapter(payload),
        _paper_transport_factory=lambda _config: _InterprocessPlacementTransport(),
        _execution_root=execution_root,
        _lock_root=lock_root,
        _now=lambda _t: None,
    )

    # File barrier: announce readiness, then wait until BOTH workers are
    # present before attempting submission concurrently.
    ready = pathlib.Path(barrier_path) / (payload["worker"] + ".ready")
    barrier_dir = pathlib.Path(barrier_path)
    barrier_dir.mkdir(parents=True, exist_ok=True)
    ready.write_text("ready", encoding="utf-8")
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        if len(list(barrier_dir.glob("*.ready"))) >= 2:
            break
        time.sleep(0.01)

    try:
        document = executor.submit(intent, mapping, arm=True, confirm_execution_id=payload["execution_id"])
        outcome = {"status": "success", "result_status": document["status"]}
    except pe.ExecutionError as exc:
        outcome = {"status": "error", "code": exc.code}
    except Exception as exc:
        outcome = {"status": "worker-crash", "error": type(exc).__name__}
    pathlib.Path(result_path).write_text(json.dumps(outcome), encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
"""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.tmp_path = pathlib.Path(self.tmp.name)
        self.execution_root = self.tmp_path / "exec"
        self.lock_root = self.tmp_path / "locks"
        self.placement_root = self.tmp_path / "placements"
        self.barrier_root = self.tmp_path / "barrier"
        self.results_root = self.tmp_path / "results"
        intent, self.execution_id_value = pe.load_intent(document=base_intent())
        self.intent = intent
        self.mapping_entry = make_entry()

    def _run_pair(self, run_name: str) -> list[dict]:
        run_dir = self.tmp_path / run_name
        run_dir.mkdir(parents=True, exist_ok=True)
        intent_path = run_dir / "intent.json"
        mapping_path = run_dir / "mapping.json"
        intent_path.write_text(json.dumps(self.intent), encoding="utf-8")
        mapping_path.write_text(json.dumps(self.mapping_entry), encoding="utf-8")
        worker_path = run_dir / "phil_5f3a2_worker.py"
        worker_path.write_text(self.WORKER_SOURCE, encoding="utf-8")

        environment = dict(os.environ)
        environment["PHIL_REPO_ROOT"] = str(REPOSITORY_ROOT)
        outcomes: list[dict] = []
        for index in range(2):
            result_path = self.results_root / f"{run_name}-{index}.json"
            result_path.parent.mkdir(parents=True, exist_ok=True)
            payload = {
                "worker": f"worker-{index}",
                "execution_root": str(self.execution_root),
                "lock_root": str(self.lock_root),
                "placement_root": str(self.placement_root),
                "intent_path": str(intent_path),
                "mapping_path": str(mapping_path),
                "expected_account": EXPECTED_ACCOUNT,
                "execution_id": self.execution_id_value,
                "config": dict(BASE_CONFIG, config_path=str(run_dir / f"config-{index}.json")),
            }
            payload_path = run_dir / f"payload-{index}.json"
            payload_path.write_text(json.dumps(payload), encoding="utf-8")
            # NOTE: both processes are started as close together as the
            # harness allows; each waits at the file barrier until BOTH
            # are ready, so the two claims genuinely race.
            import subprocess
            completed = subprocess.Popen(
                [sys.executable, str(worker_path), str(payload_path), str(self.barrier_root), str(result_path)],
                env=environment,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
            outcomes.append({"_process": completed, "_result_path": result_path})
        deadline = time.monotonic() + 90
        collected: list[dict] = []
        for entry in outcomes:
            process = entry["_process"]
            try:
                _, stderr = process.communicate(timeout=90)
            except subprocess.TimeoutExpired:
                process.kill()
                process.communicate(timeout=30)
                raise AssertionError(f"worker process timed out; stderr={process.stderr}")
            if process.returncode != 0:
                raise AssertionError(f"worker process failed rc={process.returncode}; stderr={stderr.decode('utf-8', 'replace')}")
            collected.append(json.loads(entry["_result_path"].read_text(encoding="utf-8")))
        return collected

    def test_two_processes_same_execution_id_exactly_one_place(self):
        outcomes = self._run_pair("run1")
        self.assertEqual(len(outcomes), 2)
        successes = [outcome for outcome in outcomes if outcome["status"] == "success"]
        duplicates = [
            outcome for outcome in outcomes
            if outcome["status"] == "error"
            and outcome["code"] in ("execution-already-submitted", "execution-already-claimed", "execution-uncertain")
        ]
        self.assertEqual(len(successes), 1, outcomes)
        self.assertEqual(len(duplicates), 1, outcomes)
        # Exactly ONE placement across BOTH real processes.
        markers = list(self.placement_root.glob("placed-*.json"))
        self.assertEqual(len(markers), 1, markers)

    def test_loser_never_reaches_placement(self):
        outcomes = self._run_pair("run2")
        losers = [outcome for outcome in outcomes if outcome["status"] == "error"]
        self.assertEqual(len(losers), 1)
        # The losing worker returned a bounded duplicate/claimed result.
        self.assertIn(losers[0]["code"], ("execution-already-submitted", "execution-already-claimed", "execution-uncertain"))
        # The durable claim exists exactly once for this execution id.
        records = state.read_receipts(_execution_root=self.execution_root)
        attempted = [record for record in records if record["event_type"] == "submission-attempted"]
        self.assertEqual(len(attempted), 1)


class IsolationTests(unittest.TestCase):
    def test_protected_modules_never_import_the_execution_boundary(self):
        import ast

        for relative in (
            "manus/paper_runner.py",
            "manus/paper_apply.py",
            "scheduled_paper_task.py",
            "manus/scheduled_paper.py",
        ):
            source = (REPOSITORY_ROOT / relative).read_text(encoding="utf-8")
            tree = ast.parse(source)
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        self.assertFalse(
                            alias.name.startswith(("ibkr.paper_execution", "ibkr.paper_transport")),
                            relative,
                        )
                elif isinstance(node, ast.ImportFrom):
                    self.assertFalse(
                        (node.module or "").startswith(("ibkr.paper_execution", "ibkr.paper_transport")),
                        relative,
                    )

    def test_importing_execution_modules_has_no_side_effects(self):
        code = (
            "import sys, json, pathlib\n"
            "sys.path.insert(0, %r)\n"
            "before = set(sys.modules)\n"
            "import ibkr.paper_execution\n"
            "import ibkr.paper_execution_state\n"
            "import ibkr.paper_transport\n"
            "for name in ('ibapi', 'ibapi.client', 'ibapi.wrapper', 'ibapi.contract', 'ibapi.order'):\n"
            "    assert name not in sys.modules, name\n"
        ) % (str(REPOSITORY_ROOT),)
        import subprocess

        result = subprocess.run(
            [sys.executable, "-c", code], capture_output=True, text=True, timeout=60
        )
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_place_order_only_in_paper_transport(self):
        import ast

        offenders = []
        for path in REPOSITORY_ROOT.rglob("*.py"):
            relative = path.relative_to(REPOSITORY_ROOT)
            if any(part.startswith(".git") for part in relative.parts):
                continue
            source = path.read_text(encoding="utf-8", errors="replace")
            tree = ast.parse(source)
            for node in ast.walk(tree):
                if isinstance(node, ast.Attribute) and node.attr == "placeOrder":
                    module_name = str(relative)
                    if module_name != "ibkr/paper_transport.py" and "tests" not in relative.parts:
                        offenders.append(module_name)
        self.assertEqual(offenders, [])


if __name__ == "__main__":
    unittest.main()

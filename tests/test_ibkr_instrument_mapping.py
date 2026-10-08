"""Fully offline regressions for the exact instrument mapping layer (5F-2).

No test in this file contacts IBKR, starts TWS/Gateway, uses credentials,
creates files outside temporary directories, or touches journals or
provenance. The broker adapter is always mocked; the official ``ibapi``
package is never imported here.
"""
from __future__ import annotations

import ast
import copy
import hashlib
import inspect
import json
import pathlib
import sys
import tempfile
import unittest
from unittest.mock import patch

REPOSITORY_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from ibkr import instrument_mapping as im  # noqa: E402
from ibkr.adapter import ADAPTER_VERSION  # noqa: E402
from ibkr.diagnostics import AdapterError  # noqa: E402

REGISTRY_ROOT = REPOSITORY_ROOT / "config" / "ibkr_instrument_mappings.json"


def frozen_source(**changes):
    values = {
        "provider": "polymarket",
        "market_id": "100",
        "event_id": "event-100",
        "outcome": "Yes",
        "expected_outcomes": ["Yes", "No"],
        "expected_end_date": "2026-10-01T00:00:00Z",
    }
    values.update(changes)
    return values


def make_entry(**overrides):
    source = dict(
        provider="polymarket",
        market_id="100",
        event_id="event-100",
        outcome="Yes",
        expected_outcomes=["Yes", "No"],
        expected_end_date="2026-10-01T00:00:00Z",
    )
    entry = {
        "mapping_id": "fixture-btc-etf-yes-long",
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


def registry_with(*entries):
    return im.load_registry(document={"schema_version": im.MAPPING_SCHEMA_VERSION, "mappings": list(entries)})


from contextlib import contextmanager


def _code_only_ast_dump(source_text):
    """AST dump with docstrings removed so prose never trips token scans."""
    import ast as ast_module
    tree = ast_module.parse(source_text)
    for node in ast_module.walk(tree):
        if isinstance(node, (ast_module.FunctionDef, ast_module.AsyncFunctionDef, ast_module.ClassDef, ast_module.Module)):
            if node.body and isinstance(node.body[0], ast_module.Expr) and isinstance(node.body[0].value, ast_module.Constant):
                node.body = node.body[1:] or [ast_module.Pass()]
    return ast_module.dump(tree)


class MappingCodeAssertions:
    """Assert bounded failures by stable code, not message text."""

    @contextmanager
    def expect_code(self, code):
        with self.assertRaises(im.MappingError) as caught:
            yield
        self.assertEqual(caught.exception.code, code, caught.exception)


class FakeAdapter:
    """Fake 5F-1 adapter implementing the same read-only surface used."""

    def __init__(self, contract=None, error_code=None):
        self.contract = contract
        self.error_code = error_code
        self.calls = []

    def lookup_contract(self, symbol, sec_type, *, currency=None, exchange=None):
        self.calls.append(
            {"symbol": symbol, "sec_type": sec_type, "currency": currency, "exchange": exchange}
        )
        if self.error_code is not None:
            raise AdapterError(self.error_code)
        if self.contract is None:
            raise AdapterError("contract-not-found")
        if isinstance(self.contract, list):
            if len(self.contract) == 1:
                return self.contract[0]
            raise AdapterError("contract-ambiguous")
        return dict(self.contract)


def broker_contract(**changes):
    values = {
        "conid": 433489712,
        "symbol": "FIXTUREETF",
        "local_symbol": "FIXTUREETF",
        "sec_type": "STK",
        "exchange": "SMART",
        "primary_exchange": "ARCX",
        "currency": "USD",
        "expiry": None,
        "strike": None,
        "right": None,
        "multiplier": None,
        "trading_class": "FIXTUREETF",
    }
    values.update(changes)
    return values


class PureRegistryTests(MappingCodeAssertions, unittest.TestCase):
    """Instruction items 1-15: pure registry resolution and validation."""

    def test_01_exact_mapping_resolves(self):
        registry = registry_with(make_entry())
        result = im.resolve_mapping(frozen_source(), registry)
        self.assertEqual(result["status"], "instrument-mapped")
        self.assertEqual(result["mapping_id"], "fixture-btc-etf-yes-long")
        self.assertEqual(result["ibkr"]["conid"], 433489712)
        self.assertEqual(result["exposure"]["direction"], "long")
        self.assertEqual(result["exposure"]["relationship"], "POSITIVE_PROXY")

    def test_02_unknown_source_is_unmapped(self):
        result = im.resolve_mapping(frozen_source(market_id="999"), registry_with(make_entry()))
        self.assertEqual(result["status"], "instrument-unmapped")
        self.assertNotIn("ibkr", result)

    def test_02b_empty_registry_is_valid_and_unmapped(self):
        registry = registry_with()
        self.assertEqual(registry["mappings"], [])
        result = im.resolve_mapping(frozen_source(), registry)
        self.assertEqual(result["status"], "instrument-unmapped")

    def test_03_wrong_market_id_is_unmapped(self):
        result = im.resolve_mapping(frozen_source(market_id="999"), registry_with(make_entry()))
        self.assertEqual(result["status"], "instrument-unmapped")

    def test_04_wrong_event_id_is_unmapped(self):
        result = im.resolve_mapping(frozen_source(event_id="event-999"), registry_with(make_entry()))
        self.assertEqual(result["status"], "instrument-unmapped")

    def test_05_wrong_outcome_is_unmapped(self):
        result = im.resolve_mapping(frozen_source(outcome="No"), registry_with(make_entry()))
        self.assertEqual(result["status"], "instrument-unmapped")

    def test_06_duplicate_active_source_route_fails_ambiguous(self):
        first = make_entry()
        second = make_entry(mapping_id="fixture-duplicate-route")
        with self.expect_code("mapping-ambiguous"):
            registry_with(first, second)

    def test_06b_disabled_duplicate_does_not_conflict_and_never_resolves(self):
        disabled = make_entry(status="disabled")
        registry = registry_with(disabled)
        result = im.resolve_mapping(frozen_source(), registry)
        self.assertEqual(result["status"], "instrument-unmapped")

    def test_07_changed_end_date_gives_source_binding_mismatch(self):
        entry = make_entry()
        registry = registry_with(entry)
        # Same exact key, but the frozen end date changed since approval.
        stale = frozen_source(expected_end_date="2026-10-02T00:00:00Z")
        result = im.resolve_mapping(stale, registry)
        self.assertEqual(result["status"], "source-binding-mismatch")
        self.assertNotIn("ibkr", result)

    def test_07b_changed_outcomes_give_source_binding_mismatch(self):
        entry = make_entry()
        registry = registry_with(entry)
        stale = frozen_source(expected_outcomes=["Yes", "No", "Other"])
        result = im.resolve_mapping(stale, registry)
        self.assertEqual(result["status"], "source-binding-mismatch")

    def test_08_key_reordering_does_not_change_hash(self):
        entry = make_entry()
        reordered = json.loads(json.dumps(entry))
        first = im.entry_sha256(
            {k: entry[k] for k in ("mapping_id", "status", "source", "target", "exposure", "operator_note")}
        )
        second = im.entry_sha256(
            {k: reordered[k] for k in ("operator_note", "exposure", "target", "source", "status", "mapping_id")}
        )
        self.assertEqual(first, second)
        self.assertEqual(first, entry["entry_sha256"])

    def test_09_conid_change_changes_hash(self):
        base = make_entry()
        changed = make_entry()
        changed["target"]["conid"] = 999999999
        changed["entry_sha256"] = im.entry_sha256(
            {k: changed[k] for k in ("mapping_id", "status", "source", "target", "exposure", "operator_note")}
        )
        self.assertNotEqual(base["entry_sha256"], changed["entry_sha256"])

    def test_10_direction_change_changes_hash(self):
        base = make_entry()
        changed = make_entry()
        changed["exposure"]["direction"] = "short"
        changed["entry_sha256"] = im.entry_sha256(
            {k: changed[k] for k in ("mapping_id", "status", "source", "target", "exposure", "operator_note")}
        )
        self.assertNotEqual(base["entry_sha256"], changed["entry_sha256"])

    def test_10b_relationship_change_changes_hash(self):
        base = make_entry()
        changed = make_entry()
        changed["exposure"]["relationship"] = "HEDGE"
        changed["entry_sha256"] = im.entry_sha256(
            {k: changed[k] for k in ("mapping_id", "status", "source", "target", "exposure", "operator_note")}
        )
        self.assertNotEqual(base["entry_sha256"], changed["entry_sha256"])

    def test_11_volatile_market_data_never_enters_identity(self):
        entry = make_entry()
        fingerprint_a = im.source_binding_sha256(
            provider="polymarket",
            market_id="100",
            event_id="event-100",
            outcome="Yes",
            expected_outcomes=["Yes", "No"],
            expected_end_date="2026-10-01T00:00:00Z",
        )
        # Changing hypothetical market prices cannot affect any identity hash.
        fingerprint_b = im.source_binding_sha256(
            provider="polymarket",
            market_id="100",
            event_id="event-100",
            outcome="Yes",
            expected_outcomes=["Yes", "No"],
            expected_end_date="2026-10-01T00:00:00Z",
        )
        self.assertEqual(fingerprint_a, fingerprint_b)
        self.assertNotIn("outcome_prices", im.canonical_json(entry))
        self.assertNotIn("volume_24h", im.canonical_json(entry))
        self.assertNotIn("liquidity", im.canonical_json(entry))
        self.assertNotIn("clob_token_ids", im.canonical_json(entry))

    def test_12_malformed_registry_fails_closed(self):
        with self.expect_code("mapping-invalid"):
            im.load_registry(document=None)
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "reg.json"
            path.write_text("{not json", encoding="utf-8")
            with self.expect_code("mapping-invalid"):
                im.load_registry(str(path))

    def test_13_unknown_registry_fields_fail_closed(self):
        document = {"schema_version": im.MAPPING_SCHEMA_VERSION, "mappings": [], "extra": True}
        with self.expect_code("mapping-invalid"):
            im.load_registry(document=document)
        entry = make_entry()
        entry["extra"] = "unknown"
        with self.expect_code("mapping-invalid"):
            registry_with(entry)

    def test_13b_entry_required_field_failures(self):
        with self.expect_code("mapping-invalid"):
            registry_with(make_entry(mapping_id="bad id space"))
        with self.expect_code("mapping-invalid"):
            registry_with(make_entry(status="expired"))
        with self.expect_code("mapping-invalid"):
            bad = make_entry()
            bad["source"]["provider"] = "kalshi"
            registry_with(bad)
        with self.expect_code("mapping-invalid"):
            bad = make_entry()
            bad["source"]["market_id"] = ""
            registry_with(bad)
        with self.expect_code("mapping-invalid"):
            bad = make_entry()
            bad["source"]["event_id"] = None
            registry_with(bad)
        with self.expect_code("mapping-invalid"):
            bad = make_entry()
            bad["source"]["outcome"] = ""
            registry_with(bad)
        with self.expect_code("mapping-invalid"):
            bad = make_entry()
            bad["target"].pop("conid")
            registry_with(bad)
        with self.expect_code("mapping-invalid"):
            registry_with(make_entry(conid=True))
        with self.expect_code("mapping-invalid"):
            registry_with(make_entry(conid=0))
        with self.expect_code("mapping-invalid"):
            registry_with(make_entry(conid=-5))
        with self.expect_code("mapping-invalid"):
            registry_with(make_entry(sec_type="OPT"))
        with self.expect_code("mapping-invalid"):
            registry_with(make_entry(sec_type="FUT"))
        with self.expect_code("mapping-invalid"):
            bad = make_entry()
            bad["target"]["currency"] = ""
            registry_with(bad)
        with self.expect_code("mapping-invalid"):
            bad = make_entry()
            bad["target"]["symbol"] = ""
            registry_with(bad)
        with self.expect_code("mapping-invalid"):
            registry_with(make_entry(exposure={"direction": None, "relationship": "POSITIVE_PROXY"}))
        with self.expect_code("mapping-invalid"):
            registry_with(make_entry(exposure={"direction": "long", "relationship": "INFERRED_GUESS"}))
        with self.expect_code("mapping-invalid"):
            bad = make_entry()
            bad["entry_sha256"] = "deadbeef"
            registry_with(bad)
        with self.expect_code("mapping-invalid"):
            bad = make_entry()
            bad["source"]["source_binding_sha256"] = "deadbeef"
            registry_with(bad)
        # mismatched entry hash (content edited after signing)
        with self.expect_code("mapping-invalid"):
            bad = make_entry()
            bad["target"]["symbol"] = "OTHER"
            registry_with(bad)

    def test_13c_source_binding_hash_must_match_source_facts(self):
        bad = make_entry()
        bad["source"]["source_binding_sha256"] = hashlib.sha256(b"other").hexdigest()
        with self.expect_code("mapping-invalid"):
            registry_with(bad)

    def test_14_duplicate_mapping_id_fails_closed(self):
        with self.expect_code("mapping-invalid"):
            registry_with(make_entry(), make_entry(mapping_id="fixture-btc-etf-yes-long"))

    def test_15_production_registry_contains_zero_mappings(self):
        document = json.loads(REGISTRY_ROOT.read_text(encoding="utf-8"))
        loaded = im.load_registry(document=document)
        self.assertEqual(loaded["schema_version"], im.MAPPING_SCHEMA_VERSION)
        self.assertEqual(loaded["mappings"], [])


class BrokerVerificationTests(MappingCodeAssertions, unittest.TestCase):
    """Instruction items 1-12: verification against a fake 5F-1 adapter only."""

    def entry(self):
        return make_entry()

    def test_01_exactly_one_broker_result_with_exact_fields_verifies(self):
        adapter = FakeAdapter(contract=broker_contract())
        result = im.verify_ibkr_contract(self.entry(), adapter)
        self.assertEqual(result["verification"]["status"], "verified")
        self.assertEqual(result["verification"]["adapter_version"], ADAPTER_VERSION)
        self.assertEqual(result["ibkr"]["conid"], 433489712)
        self.assertEqual(adapter.calls, [
            {"symbol": "FIXTUREETF", "sec_type": "STK", "currency": "USD", "exchange": None}
        ])

    def test_02_no_broker_result_fails_not_found(self):
        with self.expect_code("broker-contract-not-found"):
            im.verify_ibkr_contract(self.entry(), FakeAdapter(contract=None))

    def test_03_multiple_broker_results_fail_ambiguous(self):
        with self.expect_code("broker-contract-ambiguous"):
            im.verify_ibkr_contract(self.entry(), FakeAdapter(contract=[]))

    def test_04_wrong_conid_fails_mismatch(self):
        with self.expect_code("broker-contract-mismatch"):
            im.verify_ibkr_contract(self.entry(), FakeAdapter(contract=broker_contract(conid=1)))

    def test_05_wrong_sec_type_fails_mismatch(self):
        with self.expect_code("broker-contract-mismatch"):
            im.verify_ibkr_contract(self.entry(), FakeAdapter(contract=broker_contract(sec_type="OPT")))

    def test_06_wrong_symbol_fails_mismatch(self):
        with self.expect_code("broker-contract-mismatch"):
            im.verify_ibkr_contract(self.entry(), FakeAdapter(contract=broker_contract(symbol="OTHER")))

    def test_07_wrong_currency_fails_mismatch(self):
        with self.expect_code("broker-contract-mismatch"):
            im.verify_ibkr_contract(self.entry(), FakeAdapter(contract=broker_contract(currency="EUR")))

    def test_08_wrong_configured_primary_exchange_fails_mismatch(self):
        with self.expect_code("broker-contract-mismatch"):
            im.verify_ibkr_contract(
                self.entry(), FakeAdapter(contract=broker_contract(primary_exchange="NASDAQ"))
            )

    def test_08b_configured_optional_fields_are_cross_checked(self):
        entry = make_entry(target={"trading_class": "DIFFERENT"})
        # Re-sign the entry so validation passes and only the broker disagrees.
        entry["entry_sha256"] = im.entry_sha256(
            {k: entry[k] for k in ("mapping_id", "status", "source", "target", "exposure", "operator_note")}
        )
        with self.expect_code("broker-contract-mismatch"):
            im.verify_ibkr_contract(entry, FakeAdapter(contract=broker_contract()))

    def test_09_unconfigured_optional_fields_are_not_cross_checked(self):
        entry = make_entry()
        entry["target"]["exchange"] = None
        entry["target"]["local_symbol"] = None
        entry["target"]["trading_class"] = None
        entry["entry_sha256"] = im.entry_sha256(
            {k: entry[k] for k in ("mapping_id", "status", "source", "target", "exposure", "operator_note")}
        )
        result = im.verify_ibkr_contract(entry, FakeAdapter(contract=broker_contract(exchange="SMART")))
        self.assertEqual(result["verification"]["status"], "verified")

    def test_10_adapter_error_maps_to_bounded_failure(self):
        with self.expect_code("broker-contract-not-found"):
            im.verify_ibkr_contract(self.entry(), FakeAdapter(error_code="contract-not-found"))
        with self.expect_code("broker-contract-ambiguous"):
            im.verify_ibkr_contract(self.entry(), FakeAdapter(error_code="contract-ambiguous"))
        with self.expect_code("broker-contract-mismatch"):
            im.verify_ibkr_contract(self.entry(), FakeAdapter(error_code="broker-data-unavailable"))

    def test_11_account_id_never_enters_result(self):
        result = im.verify_ibkr_contract(self.entry(), FakeAdapter(contract=broker_contract()))
        self.assertNotIn("account", json.dumps(result))
        self.assertNotIn("DU", json.dumps(result))

    def test_12_raw_broker_objects_never_enter_result(self):
        result = im.verify_ibkr_contract(self.entry(), FakeAdapter(contract=broker_contract()))
        text = json.dumps(result)
        self.assertNotIn("Contract", text)
        self.assertNotIn("ibapi", text)
        self.assertNotIn("ContractDetails", text)

    def test_disabled_mapping_cannot_be_verified(self):
        with self.expect_code("mapping-invalid"):
            im.verify_ibkr_contract(make_entry(status="disabled"), FakeAdapter(contract=broker_contract()))


class NoHeuristicsTests(MappingCodeAssertions, unittest.TestCase):
    """Mapping resolution never inspects textual or similarity fields."""

    def test_ticker_suggesting_text_without_mapping_stays_unmapped(self):
        # These fields are NOT part of the frozen binding schema at all;
        # including them must fail the schema check rather than be used.
        decorated = frozen_source()
        decorated["question"] = "Will FIXTUREETF BTC ETF close above 100k?"
        decorated["event_slug"] = "btc-etf-100k"
        decorated["slug"] = "btc-etf-100k-yes"
        decorated["category"] = "crypto"
        with self.expect_code("mapping-invalid"):
            im.resolve_mapping(decorated, registry_with(make_entry()))

    def test_registry_without_entry_ignores_all_suggestive_text(self):
        registry = registry_with()
        result = im.resolve_mapping(frozen_source(), registry)
        self.assertEqual(result["status"], "instrument-unmapped")

    def test_resolver_source_contains_no_text_matching_code(self):
        source = inspect.getsource(im)
        forbidden_tokens = (
            "question",
            "event_slug",
            "slug",
            "description",
            "category",
            "similarity",
            "fuzzy",
            "startswith(",
            ".lower() in",
        )
        resolver_start = source.index("def resolve_mapping")
        resolver_end = source.index("def _verified_target_projection")
        resolver = source[resolver_start:resolver_end]
        code_only = _code_only_ast_dump(resolver)
        for token in forbidden_tokens:
            self.assertNotIn(token, code_only, token)


class SideSemanticsTests(MappingCodeAssertions, unittest.TestCase):
    """Yes/No never implies LONG/SHORT; direction is mapping-only."""

    def test_yes_outcome_does_not_force_long(self):
        yes_short = make_entry(exposure={"direction": "short", "relationship": "HEDGE"})
        registry = registry_with(yes_short)
        result = im.resolve_mapping(frozen_source(outcome="Yes"), registry)
        self.assertEqual(result["exposure"]["direction"], "short")

    def test_no_outcome_does_not_force_short(self):
        no_long = make_entry(
            source={"outcome": "No"},
            exposure={"direction": "long", "relationship": "DIRECT_UNDERLYING"},
        )
        registry = registry_with(no_long)
        result = im.resolve_mapping(frozen_source(outcome="No"), registry)
        self.assertEqual(result["exposure"]["direction"], "long")

    def test_inverse_proxy_is_never_inferred(self):
        # The same source market maps Yes→long POSITIVE_PROXY and No→long
        # DIRECT_UNDERLYING; nothing in the resolver invents an inverse.
        registry = registry_with(
            make_entry(exposure={"direction": "long", "relationship": "POSITIVE_PROXY"}),
            make_entry(
                mapping_id="fixture-no-long",
                source={"outcome": "No"},
                exposure={"direction": "long", "relationship": "DIRECT_UNDERLYING"},
            ),
        )
        yes = im.resolve_mapping(frozen_source(outcome="Yes"), registry)
        no = im.resolve_mapping(frozen_source(outcome="No"), registry)
        self.assertEqual(yes["exposure"]["relationship"], "POSITIVE_PROXY")
        self.assertEqual(no["exposure"]["relationship"], "DIRECT_UNDERLYING")
        self.assertEqual(yes["exposure"]["direction"], "long")
        self.assertEqual(no["exposure"]["direction"], "long")

    def test_direction_and_relationship_come_only_from_the_entry(self):
        registry = registry_with(make_entry())
        result = im.resolve_mapping(frozen_source(), registry)
        self.assertEqual(
            result["exposure"],
            {"direction": "long", "relationship": "POSITIVE_PROXY"},
        )


class SafetyAndIsolationTests(MappingCodeAssertions, unittest.TestCase):
    """Import/runtime safety: no broker writes, no network, no filesystem."""

    def test_module_source_has_no_broker_write_or_order_construction(self):
        import ast as ast_module
        code = _code_only_ast_dump(inspect.getsource(im))
        forbidden_identifiers = (
            "placeOrder",
            "cancelOrder",
            "reqGlobalCancel",
            "exerciseOptions",
            "EClient",
        )
        for identifier in forbidden_identifiers:
            self.assertNotIn(f"Name(id='{identifier}'", code, identifier)
            self.assertNotIn(f"Attribute(attr='{identifier}'", code, identifier)
        # No ibapi import of any kind, and no Order construction.
        self.assertNotIn("ibapi", code)
        for token in ("socket", "urllib", "requests", "httpx", "subprocess"):
            self.assertNotIn(f"'{token}'", code, token)

    def test_module_imports_only_adapter_diagnostics_config(self):
        tree = ast.parse(inspect.getsource(im))
        modules = set()
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                modules.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                modules.add(node.module)
        allowed = {"ibkr.adapter", "ibkr.diagnostics", "ibkr.config", "argparse", "hashlib", "json", "re", "sys", "typing", "__future__"}
        self.assertTrue(modules <= allowed, modules - allowed)
        self.assertFalse(modules & {"socket", "urllib", "requests", "subprocess", "ibapi"})

    def test_verification_only_uses_lookup_contract_surface(self):
        adapter = FakeAdapter(contract=broker_contract())
        im.verify_ibkr_contract(make_entry(), adapter)
        # Only the read-only lookup_contract method was touched.
        self.assertEqual(list(adapter.calls), [
            {"symbol": "FIXTUREETF", "sec_type": "STK", "currency": "USD", "exchange": None}
        ])

    def test_resolution_performs_no_filesystem_or_network_action(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = pathlib.Path(tmp)
            before = sorted(p.name for p in root.iterdir())
            im.resolve_mapping(frozen_source(), registry_with(make_entry()))
            after = sorted(p.name for p in root.iterdir())
        self.assertEqual(before, after)

    def test_load_registry_reads_but_never_writes(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "reg.json"
            document = {"schema_version": im.MAPPING_SCHEMA_VERSION, "mappings": [make_entry()]}
            payload = json.dumps(document)
            path.write_text(payload, encoding="utf-8")
            original_bytes = path.read_bytes()
            im.load_registry(str(path))
            self.assertEqual(path.read_bytes(), original_bytes)

    def test_production_registry_file_is_untouched_by_tests(self):
        document = json.loads(REGISTRY_ROOT.read_text(encoding="utf-8"))
        self.assertEqual(document["mappings"], [])


class CliTests(MappingCodeAssertions, unittest.TestCase):
    """Offline inspection CLI: pure, help-safe, operationally closed."""

    def _files(self, tmp, frozen, registry):
        source_path = pathlib.Path(tmp) / "source.json"
        registry_path = pathlib.Path(tmp) / "registry.json"
        source_path.write_text(json.dumps(frozen), encoding="utf-8")
        registry_path.write_text(json.dumps(registry), encoding="utf-8")
        return str(source_path), str(registry_path)

    def test_inspect_resolves_unmapped_against_empty_production_registry(self):
        import contextlib
        import io
        with tempfile.TemporaryDirectory() as tmp:
            source_path, registry_path = self._files(tmp, frozen_source(), {"schema_version": im.MAPPING_SCHEMA_VERSION, "mappings": []})
            buffer = io.StringIO()
            with contextlib.redirect_stdout(buffer):
                status = im.main(["--source-file", source_path, "--registry", registry_path])
        self.assertEqual(status, 0)
        document = json.loads(buffer.getvalue())
        self.assertEqual(document["status"], "instrument-unmapped")

    def test_inspect_resolves_mapped_entry(self):
        import contextlib
        import io
        with tempfile.TemporaryDirectory() as tmp:
            registry = {"schema_version": im.MAPPING_SCHEMA_VERSION, "mappings": [make_entry()]}
            source_path, registry_path = self._files(tmp, frozen_source(), registry)
            buffer = io.StringIO()
            with contextlib.redirect_stdout(buffer):
                status = im.main(["--source-file", source_path, "--registry", registry_path])
        self.assertEqual(status, 0)
        document = json.loads(buffer.getvalue())
        self.assertEqual(document["status"], "instrument-mapped")

    def test_help_exits_zero(self):
        import contextlib
        import io
        out = io.StringIO()
        try:
            with contextlib.redirect_stdout(out):
                status = im.main(["--help"])
        except SystemExit as exc:
            self.assertIn(exc.code, (None, 0))
            status = 0
        self.assertEqual(status, 0)
        self.assertIn("--source-file", out.getvalue())

    def test_forbidden_operational_arguments_are_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            source_path, registry_path = self._files(tmp, frozen_source(), {"schema_version": im.MAPPING_SCHEMA_VERSION, "mappings": []})
            for option in (
                "--place-order",
                "--order-type",
                "--quantity",
                "--size",
                "--execute",
                "--live",
                "--arm",
                "--cancel",
                "--submit",
                "--transmit",
            ):
                with self.subTest(option=option):
                    with self.assertRaises(im.MappingError) as caught:
                        im.main(["--source-file", source_path, "--registry", registry_path, option])
                    self.assertEqual(caught.exception.code, "mapping-invalid")
                    self.assertIn("forbidden operational", str(caught.exception))

    def test_no_broker_verify_command_exists(self):
        parser = im.build_parser()
        options = {option for action in parser._actions for option in action.option_strings}
        self.assertNotIn("--verify", options)
        self.assertNotIn("--broker", options)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
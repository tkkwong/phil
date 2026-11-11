"""Exact, operator-approved source-market → IBKR instrument mapping (5F-2).

This module is IDENTITY + MAPPING infrastructure only. It never places or
cancels orders, never sizes positions, never requests quotes, and never
constructs an ``ibapi`` object. There is NO heuristic mapping of any kind:
resolution is exact-key lookup against an explicitly operator-approved,
hash-verified registry, and every failure mode is bounded and fail-closed.

Pure registry operations are fully offline: no network, no filesystem write,
no broker call, no journal access. The only broker interaction this package
permits is the 5F-1 read-only contract surface, and verification is
conId-PRIMARY (5F-2a): the sole broker lookup authority is the
operator-approved conId via ``lookup_contract_by_conid``. Symbol, secType,
currency, and exchange are metadata assertions cross-checked AFTER the exact
conId lookup — never a lookup key — and there is no symbol fallback. No
transport is created here and EClient is never exposed.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from typing import Any

from ibkr.adapter import ADAPTER_VERSION, ReadonlyIbkrAdapter
from ibkr.diagnostics import AdapterError
from ibkr.config import AdapterConfigError

MAPPING_SCHEMA_VERSION = "ibkr-instrument-mappings/v1"
MAPPING_RESULT_SCHEMA_VERSION = "ibkr-instrument-mapping-result/v1"

SOURCE_PROVIDER = "polymarket"  # the only source provider this patch knows

# Bounded operator-approved semantic relationship vocabulary.
RELATIONSHIP_DIRECT_UNDERLYING = "DIRECT_UNDERLYING"
RELATIONSHIP_POSITIVE_PROXY = "POSITIVE_PROXY"
RELATIONSHIP_INVERSE_PROXY = "INVERSE_PROXY"
RELATIONSHIP_HEDGE = "HEDGE"
RELATIONSHIP_OTHER_EXPLICIT_PROXY = "OTHER_EXPLICIT_PROXY"
ALLOWED_RELATIONSHIPS = frozenset(
    {
        RELATIONSHIP_DIRECT_UNDERLYING,
        RELATIONSHIP_POSITIVE_PROXY,
        RELATIONSHIP_INVERSE_PROXY,
        RELATIONSHIP_HEDGE,
        RELATIONSHIP_OTHER_EXPLICIT_PROXY,
    }
)

ALLOWED_DIRECTIONS = frozenset({"long", "short"})
ALLOWED_STATUSES = frozenset({"active", "disabled"})

# Intentionally narrow first asset-class scope (risk reduction before 5F-3).
SUPPORTED_SEC_TYPES = frozenset({"STK"})

SUPPORTED_BROKER = "IBKR"

# Closed field sets (unknown fields fail closed).
_REGISTRY_FIELDS = frozenset({"schema_version", "mappings"})
_ENTRY_FIELDS = frozenset(
    {
        "mapping_id",
        "status",
        "source",
        "target",
        "exposure",
        "operator_note",
        "entry_sha256",
    }
)
_SOURCE_FIELDS = frozenset(
    {
        "provider",
        "market_id",
        "event_id",
        "outcome",
        "expected_outcomes",
        "expected_end_date",
        "source_binding_sha256",
    }
)
_TARGET_FIELDS = frozenset(
    {
        "broker",
        "conid",
        "sec_type",
        "symbol",
        "currency",
        "exchange",
        "primary_exchange",
        "local_symbol",
        "trading_class",
    }
)
_EXPOSURE_FIELDS = frozenset({"direction", "relationship"})

# Optional target identity fields retained when the operator supplies them
# and cross-checked against the broker when configured.
_OPTIONAL_TARGET_FIELDS = frozenset(
    {"exchange", "primary_exchange", "local_symbol", "trading_class"}
)

# The exact source binding key: one active mapping per key, never more.
_SOURCE_KEY_FIELDS = ("provider", "market_id", "event_id", "outcome")

# Fields hashed into the source semantic fingerprint. Volatile market data
# (bid/ask/volume/liquidity/probability/scan timestamps) is never hashed.
_SOURCE_BINDING_FIELDS = (
    "provider",
    "market_id",
    "event_id",
    "outcome",
    "expected_outcomes",
    "expected_end_date",
)

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]*$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")

# Bounded text limits.
_MAX_ID = 128
_MAX_OUTCOME = 256
_MAX_SYMBOL = 32
_MAX_CURRENCY = 8
_MAX_NOTE = 512
_MAX_OUTCOMES = 16

class MappingError(ValueError):
    """Raised for any bounded mapping failure.

    ``code`` is a stable, hyphenated identifier from the closed vocabulary;
    the exception message never contains secrets, paths, or broker text.
    """

    def __init__(self, code: str, message: str | None = None) -> None:
        self.code = code
        super().__init__(message or f"mapping failure: {code}")


def canonical_json(document: Any) -> str:
    """Deterministic JSON serialization used for all identity hashing."""
    return json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _require_bounded_text(value: Any, name: str, *, maximum: int) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise MappingError("mapping-invalid", f"{name} must be bounded non-empty text")
    return value


def source_binding_sha256(
    *,
    provider: str,
    market_id: str,
    event_id: str | None,
    outcome: str,
    expected_outcomes: list[str] | None,
    expected_end_date: str | None,
) -> str:
    """Deterministic source semantic fingerprint.

    Hashes exactly the trusted source facts the mapping authorizes — the
    exact key plus the frozen semantic anchors. Never includes question
    text, slugs, prices, volume, liquidity, or timestamps of the scan.
    """
    document = {
        "provider": provider,
        "market_id": market_id,
        "event_id": event_id,
        "outcome": outcome,
        "expected_outcomes": expected_outcomes,
        "expected_end_date": expected_end_date,
    }
    return _sha256(canonical_json(document))


def _validate_source(source: Any) -> dict[str, Any]:
    if not isinstance(source, dict) or set(source) != _SOURCE_FIELDS:
        raise MappingError("mapping-invalid", "source field set is closed and required")
    provider = source["provider"]
    if provider != SOURCE_PROVIDER:
        raise MappingError("mapping-invalid", "unknown source provider fails closed")
    for name in ("market_id", "event_id", "outcome"):
        _require_bounded_text(source[name], f"source {name}", maximum=_MAX_OUTCOME)
    outcomes = source["expected_outcomes"]
    if outcomes is not None:
        if (
            not isinstance(outcomes, list)
            or not outcomes
            or len(outcomes) > _MAX_OUTCOMES
            or any(not isinstance(item, str) or not item or len(item) > _MAX_OUTCOME for item in outcomes)
        ):
            raise MappingError("mapping-invalid", "expected_outcomes must be bounded non-empty strings")
    end_date = source["expected_end_date"]
    if end_date is not None:
        _require_bounded_text(end_date, "expected_end_date", maximum=64)
    binding = source["source_binding_sha256"]
    if not isinstance(binding, str) or not _SHA256_RE.fullmatch(binding):
        raise MappingError("mapping-invalid", "source_binding_sha256 must be a lowercase SHA-256")
    expected = source_binding_sha256(
        provider=provider,
        market_id=source["market_id"],
        event_id=source["event_id"],
        outcome=source["outcome"],
        expected_outcomes=outcomes,
        expected_end_date=end_date,
    )
    if binding != expected:
        raise MappingError("mapping-invalid", "source_binding_sha256 does not match source facts")
    return dict(source)


def _validate_target(target: Any) -> dict[str, Any]:
    if not isinstance(target, dict) or set(target) != _TARGET_FIELDS:
        raise MappingError("mapping-invalid", "target field set is closed and required")
    if target["broker"] != SUPPORTED_BROKER:
        raise MappingError("mapping-invalid", "unsupported broker fails closed")
    conid = target["conid"]
    if isinstance(conid, bool) or not isinstance(conid, int) or conid <= 0:
        raise MappingError("mapping-invalid", "conid must be a positive integer")
    if target["sec_type"] not in SUPPORTED_SEC_TYPES:
        raise MappingError("unsupported-security-type", "only STK mappings are supported in 5F-2")
    _require_bounded_text(target["symbol"], "target symbol", maximum=_MAX_SYMBOL)
    _require_bounded_text(target["currency"], "target currency", maximum=_MAX_CURRENCY)
    for name in _OPTIONAL_TARGET_FIELDS:
        value = target[name]
        if value is not None:
            _require_bounded_text(value, f"target {name}", maximum=_MAX_SYMBOL)
    return dict(target)


def _validate_exposure(exposure: Any) -> dict[str, Any]:
    if not isinstance(exposure, dict) or set(exposure) != _EXPOSURE_FIELDS:
        raise MappingError("mapping-invalid", "exposure field set is closed and required")
    if exposure["direction"] not in ALLOWED_DIRECTIONS:
        raise MappingError("mapping-invalid", "exposure direction must be explicit long or short")
    if exposure["relationship"] not in ALLOWED_RELATIONSHIPS:
        raise MappingError("mapping-invalid", "exposure relationship must be from the bounded vocabulary")
    return dict(exposure)


def validate_entry(entry: Any) -> dict[str, Any]:
    """Validate one mapping entry (closed schema, hash-verified)."""
    if not isinstance(entry, dict) or set(entry) != _ENTRY_FIELDS:
        raise MappingError("mapping-invalid", "mapping entry field set is closed and required")
    mapping_id = entry["mapping_id"]
    if not isinstance(mapping_id, str) or not _IDENTIFIER_RE.fullmatch(mapping_id) or len(mapping_id) > _MAX_ID:
        raise MappingError("mapping-invalid", "mapping_id must be a bounded repository identifier")
    if entry["status"] not in ALLOWED_STATUSES:
        raise MappingError("mapping-invalid", "status must be active or disabled")
    note = entry["operator_note"]
    if note is not None:
        _require_bounded_text(note, "operator_note", maximum=_MAX_NOTE)
    source = _validate_source(entry["source"])
    target = _validate_target(entry["target"])
    exposure = _validate_exposure(entry["exposure"])
    provided = entry["entry_sha256"]
    if not isinstance(provided, str) or not _SHA256_RE.fullmatch(provided):
        raise MappingError("mapping-invalid", "entry_sha256 must be a lowercase SHA-256")
    expected = entry_sha256(
        {
            "mapping_id": mapping_id,
            "status": entry["status"],
            "source": source,
            "target": target,
            "exposure": exposure,
            "operator_note": note,
        }
    )
    if provided != expected:
        raise MappingError("mapping-invalid", "entry_sha256 does not match the entry contents")
    return {
        "mapping_id": mapping_id,
        "status": entry["status"],
        "source": source,
        "target": target,
        "exposure": exposure,
        "operator_note": note,
        "entry_sha256": provided,
    }


def entry_sha256(entry_without_hash: dict[str, Any]) -> str:
    """Deterministic mapping identity: SHA-256 of the canonical entry.

    Same logical mapping → same hash; changing the source identity, conid,
    exposure direction, or semantic relationship changes the hash. No
    timestamps, local paths, account data, or volatile market data are
    ever part of the identity.
    """
    return _sha256(canonical_json(entry_without_hash))


def load_registry(path: str | None = None, *, document: Any = None) -> dict[str, Any]:
    """Load and fully validate a mapping registry document.

    ``document`` is a test seam; ``path`` is read as explicit UTF-8 JSON.
    An empty production registry is a valid state.
    """
    if document is None:
        if path is None:
            raise MappingError("mapping-invalid", "registry path or document is required")
        try:
            with open(path, "r", encoding="utf-8") as handle:
                document = json.load(handle)
        except json.JSONDecodeError as exc:
            raise MappingError("mapping-invalid", "registry is not valid JSON") from exc
        except OSError as exc:
            raise MappingError("mapping-invalid", "registry could not be read") from exc
    if not isinstance(document, dict) or set(document) != _REGISTRY_FIELDS:
        raise MappingError("mapping-invalid", "registry field set is closed and required")
    if document["schema_version"] != MAPPING_SCHEMA_VERSION:
        raise MappingError("mapping-invalid", "registry schema version is incompatible")
    entries = document["mappings"]
    if not isinstance(entries, list):
        raise MappingError("mapping-invalid", "mappings must be a list")
    validated: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    seen_keys: dict[tuple[str, ...], str] = {}
    for raw in entries:
        entry = validate_entry(raw)
        if entry["mapping_id"] in seen_ids:
            raise MappingError("mapping-invalid", f"duplicate mapping_id: {entry['mapping_id']}")
        seen_ids.add(entry["mapping_id"])
        key = tuple(entry["source"][field] for field in _SOURCE_KEY_FIELDS)
        owner = seen_keys.get(key)
        if owner is not None and entry["status"] == "active" and owner != "disabled":
            raise MappingError("mapping-ambiguous", "duplicate active exact source route")
        if entry["status"] == "active":
            seen_keys[key] = entry["mapping_id"]
        elif key not in seen_keys:
            seen_keys[key] = "disabled"
        validated.append(entry)
    return {"schema_version": MAPPING_SCHEMA_VERSION, "mappings": validated}


def resolve_mapping(frozen_source: dict[str, Any], registry: dict[str, Any]) -> dict[str, Any]:
    """Pure, offline exact-key resolution against a validated registry.

    ``frozen_source`` is trusted, frozen source identity (provider,
    market_id, event_id, outcome, expected_outcomes, expected_end_date).
    Returns a bounded resolution document; never guesses, never falls back
    to question text, slugs, category, tickers, or any other heuristic.
    No filesystem write, network call, broker call, or journal access.
    """
    if not isinstance(frozen_source, dict) or set(frozen_source) != set(_SOURCE_BINDING_FIELDS):
        raise MappingError("mapping-invalid", "frozen source field set does not match the binding schema")
    provider = frozen_source["provider"]
    if provider != SOURCE_PROVIDER:
        raise MappingError("mapping-invalid", "unknown source provider fails closed")
    binding = source_binding_sha256(
        provider=provider,
        market_id=frozen_source["market_id"],
        event_id=frozen_source["event_id"],
        outcome=frozen_source["outcome"],
        expected_outcomes=frozen_source["expected_outcomes"],
        expected_end_date=frozen_source["expected_end_date"],
    )
    matches = [
        entry
        for entry in registry["mappings"]
        if entry["status"] == "active"
        and all(entry["source"][field] == frozen_source[field] for field in _SOURCE_KEY_FIELDS)
    ]
    if not matches:
        return _resolution("instrument-unmapped", frozen_source, binding)
    if len(matches) > 1:
        # load_registry already rejects duplicate active routes; this is
        # defense in depth for hand-assembled registries.
        return _resolution("mapping-ambiguous", frozen_source, binding)
    entry = matches[0]
    if entry["source"]["source_binding_sha256"] != binding:
        return _resolution("source-binding-mismatch", frozen_source, binding, mapping=entry)
    return _resolution(
        "instrument-mapped",
        frozen_source,
        binding,
        mapping=entry,
        ibkr=_verified_target_projection(entry),
    )


def _verified_target_projection(entry: dict[str, Any]) -> dict[str, Any]:
    target = entry["target"]
    return {field: target[field] for field in sorted(_TARGET_FIELDS)}


def _resolution(
    status: str,
    frozen_source: dict[str, Any],
    binding: str,
    *,
    mapping: dict[str, Any] | None = None,
    ibkr: dict[str, Any] | None = None,
) -> dict[str, Any]:
    document: dict[str, Any] = {
        "schema_version": MAPPING_RESULT_SCHEMA_VERSION,
        "status": status,
        "source": {
            "provider": frozen_source["provider"],
            "market_id": frozen_source["market_id"],
            "event_id": frozen_source["event_id"],
            "outcome": frozen_source["outcome"],
            "source_binding_sha256": binding,
        },
    }
    if mapping is not None:
        document["mapping_id"] = mapping["mapping_id"]
        document["mapping_sha256"] = mapping["entry_sha256"]
        document["exposure"] = dict(mapping["exposure"])
    if ibkr is not None:
        document["ibkr"] = ibkr
    return document


def verify_ibkr_contract(mapping_entry: dict[str, Any], adapter: Any) -> dict[str, Any]:
    """Read-only conId-primary broker verification (5F-2a).

    The ONLY broker lookup authority is the operator-approved conId via the
    5F-1 read-only ``lookup_contract_by_conid`` surface. Symbol, secType,
    currency, and the configured optional exchange fields are metadata
    ASSERTIONS cross-checked against the single returned contract after the
    exact conId lookup; they are never a lookup key, and there is NO symbol
    fallback: any lookup failure closes without retrying by symbol. Zero or
    ambiguous matches and any mismatch fail closed with bounded codes. No
    transport, EClient, or ibapi object is created, exposed, or returned.
    """
    try:
        entry = validate_entry(mapping_entry)
    except MappingError:
        raise
    if entry["status"] != "active":
        raise MappingError("mapping-invalid", "only active mappings can be broker-verified")
    target = entry["target"]
    try:
        contract = adapter.lookup_contract_by_conid(target["conid"])
    except AdapterError as exc:
        code = {
            "contract-not-found": "broker-contract-not-found",
            "contract-ambiguous": "broker-contract-ambiguous",
        }.get(exc.code, "broker-contract-mismatch")
        raise MappingError(code, f"read-only broker verification failed: {code}") from exc
    except AdapterConfigError as exc:
        raise MappingError("broker-contract-not-found", "read-only broker verification is not configured") from exc
    if contract is None:
        raise MappingError("broker-contract-not-found", "read-only broker verification returned no contract")
    expected_fields = {"conid": target["conid"], "sec_type": target["sec_type"], "symbol": target["symbol"], "currency": target["currency"]}
    for name in sorted(_OPTIONAL_TARGET_FIELDS):
        configured = target[name]
        if configured is not None:
            expected_fields[name] = configured
    for name, expected in sorted(expected_fields.items()):
        if contract.get(name) != expected:
            raise MappingError("broker-contract-mismatch", f"broker contract {name} does not match the approved mapping")
    return {
        "schema_version": MAPPING_RESULT_SCHEMA_VERSION,
        "mapping_id": entry["mapping_id"],
        "mapping_sha256": entry["entry_sha256"],
        "source": {
            "provider": entry["source"]["provider"],
            "market_id": entry["source"]["market_id"],
            "event_id": entry["source"]["event_id"],
            "outcome": entry["source"]["outcome"],
            "source_binding_sha256": entry["source"]["source_binding_sha256"],
        },
        "exposure": dict(entry["exposure"]),
        "ibkr": _verified_target_projection(entry),
        "verification": {"status": "verified", "adapter_version": ADAPTER_VERSION},
    }


# ---------------------------------------------------------------------------
# Read-only inspection CLI (pure, offline). No broker verify command is
# exposed: adding one would widen the interactive surface for no safety gain,
# so broker verification intentionally remains a Python API for this patch.
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    """Closed-option CLI parser (5F-2a).

    The ONLY supported options are ``-h``/``--help``, ``--source-file``,
    and ``--registry``. ``allow_abbrev=False`` keeps prefix abbreviations
    from sneaking through, and argparse rejects every unknown option with
    its bounded usage error. Option VALUES (filesystem paths) are opaque
    data: they are never scanned for words.
    """
    parser = argparse.ArgumentParser(
        prog="python -m ibkr.instrument_mapping",
        description="Offline, read-only exact source-market → IBKR mapping inspector",
        allow_abbrev=False,
    )
    parser.add_argument(
        "--source-file",
        required=True,
        help="JSON file containing the trusted frozen source identity",
    )
    parser.add_argument(
        "--registry",
        required=True,
        help="JSON file containing the operator-approved mapping registry",
    )
    return parser


def _reject_forbidden_options(arguments: list[str]) -> None:
    """Kept for backwards compatibility; now a closed-allowlist check.

    Only exact option NAMES from the closed allowlist are inspected.
    Values of ``--source-file``/``--registry`` are opaque filesystem paths
    and are never scanned. Unknown options raise ``MappingError`` here so
    programmatic callers get the bounded code; ``main`` handles both this
    and argparse's own usage errors with identical bounded hygiene.
    """
    allowlist = {"-h", "--help", "--source-file", "--registry"}
    index = 0
    while index < len(arguments):
        argument = arguments[index]
        if argument in allowlist:
            # Skip a consumed value so an exact-match path can never be
            # mistaken for an option (values are opaque data).
            if argument in ("--source-file", "--registry") and index + 1 < len(arguments):
                index += 2
                continue
            index += 1
            continue
        if argument.startswith("-"):
            raise MappingError("mapping-invalid", "unsupported option; only -h, --help, --source-file, and --registry are available")
        index += 1


def main(argv: list[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    parser = build_parser()
    try:
        _reject_forbidden_options(arguments)
        options = parser.parse_args(arguments)
        with open(options.source_file, "r", encoding="utf-8") as handle:
            frozen_source = json.load(handle)
    except json.JSONDecodeError:
        print("error: source file is not valid JSON", file=sys.stderr)
        return 2
    except OSError:
        print("error: a required input file could not be read", file=sys.stderr)
        return 2
    except MappingError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    except SystemExit as exc:
        return _exit_code(exc.code)
    try:
        registry = load_registry(options.registry)
        document = resolve_mapping(frozen_source, registry)
    except MappingError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(canonical_json(document))
    return 0


def _exit_code(code):
    """Bounded argparse SystemExit translation (never a traceback)."""
    if code is None:
        return 0
    if isinstance(code, int):
        return code
    return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

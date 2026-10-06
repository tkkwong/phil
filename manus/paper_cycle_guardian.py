"""Operator-owned guardian for Manus PAPER candidate packets and forecasts.

``prepare`` and ``validate-intent`` remain data-only. ``record-forecast``
records exactly one fixture-bound forecast. ``record-paper-placement`` is the
only separately authorized PAPER mutation: it requires that existing bound
forecast and calls the narrow protected paper ledger function. Neither route
accepts execution parameters, invokes a real-trading route, broker, Pearl,
or IBKR component, or executes Manus text.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import math
import pathlib
import re
import sys
from typing import Any, Iterable

from core import forecast as forecast_core
from core import ledger as ledger_core
from manus.intent_validator import IntentValidationError, validate_intent


PACKET_VERSION = "paper-candidate-packet/v1"
VALIDATION_VERSION = "paper-guardian-validation/v1"
FORECAST_RECORDING_VERSION = "paper-guardian-forecast-recording/v1"
PAPER_PLACEMENT_VERSION = "paper-guardian-paper-placement/v1"

# These are exactly the fields currently emitted by core/scan.py's keep().
# Prepare accepts the full read-only scan record but emits only its bounded,
# research-identifying subset into the candidate packet. Other protected
# operator-owned modules may import this immutable public contract to project
# scanner records without admitting non-guardian evidence fields into fixtures.
SCAN_SOURCE_FIELDS = frozenset(
    {
        "market_id",
        "question",
        "end_date",
        "event_id",
        "event_slug",
        "outcomes",
        "outcome_prices",
        "clob_token_ids",
        "volume_24h",
        "liquidity",
        "slug",
        "description",
    }
)
_SOURCE_FIXTURE_FIELDS = frozenset({"generated_at", "candidates"})
_SOURCE_REQUIRED_FIELDS = frozenset(
    {"market_id", "question", "end_date", "outcomes", "outcome_prices"}
)

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]*$")
_ISO_UTC_TIMESTAMP_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})$"
)
_FORBIDDEN_CLI_MARKERS = (
    "real",
    "live-trading",
    "execute",
    "order",
    "ibkr",
    "pearl",
    "ledger",
    "journal",
    "supersede",
    "confirm-extreme",
)


class GuardianValidationError(ValueError):
    """Raised when fixture, intent, or guarded forecast data is invalid or unsafe."""

    def __init__(self, message: str, code: str | None = None):
        # ``code`` is optional bounded diagnostic metadata from the protected
        # core's closed vocabularies. It never carries exception text, paths,
        # or provider content, and never changes validation behavior.
        super().__init__(message)
        self.code = code


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _reject_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise GuardianValidationError(f"Duplicate JSON field: {key!r}")
        result[key] = value
    return result


def _reject_nonstandard_json_number(value: str) -> None:
    raise GuardianValidationError(f"Non-standard JSON number is not allowed: {value}")


def _parse_json_document(document: str, label: str) -> Any:
    try:
        return json.loads(
            document,
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=_reject_nonstandard_json_number,
        )
    except GuardianValidationError:
        raise
    except json.JSONDecodeError as exc:
        raise GuardianValidationError(f"Malformed {label} JSON") from exc


def _load_json_path(path_value: str, label: str) -> Any:
    """Read one explicit operator-supplied input path; no arbitrary output path exists."""
    try:
        return _parse_json_document(pathlib.Path(path_value).read_text(encoding="utf-8"), label)
    except OSError as exc:
        raise GuardianValidationError(f"Unable to read {label}") from exc


def _require_exact_fields(value: dict[str, Any], allowed: frozenset[str], label: str) -> None:
    unknown = set(value) - allowed
    missing = allowed - set(value)
    if unknown:
        raise GuardianValidationError(f"Unknown field(s) in {label}: {sorted(unknown)}")
    if missing:
        raise GuardianValidationError(f"Missing required field(s) in {label}: {sorted(missing)}")


def _normalize_market_text(value: Any, label: str, maximum: int) -> str:
    """Canonicalize quoted external market text without interpreting its content.

    Natural-language question, description, and outcome text is untrusted data,
    not a path, command, credential, Git instruction, or execution request. CRLF
    and CR normalize to LF; TAB normalizes to one ASCII space. NUL and all other
    C0/C1 controls are rejected after normalization, while printable Unicode and
    ordinary punctuation remain data.
    """
    if not isinstance(value, str) or not value.strip():
        raise GuardianValidationError(f"{label} must be a non-empty string")
    normalized = value.replace("\r\n", "\n").replace("\r", "\n").replace("\t", " ")
    if len(normalized) > maximum:
        raise GuardianValidationError(f"{label} exceeds {maximum} characters")
    for character in normalized:
        codepoint = ord(character)
        if (codepoint < 32 and character != "\n") or 127 <= codepoint <= 159:
            raise GuardianValidationError(f"Inappropriate control character in {label}")
    return normalized


def _require_identifier(value: Any, label: str, maximum: int = 128) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise GuardianValidationError(f"{label} must be a non-empty identifier up to {maximum} characters")
    if not _IDENTIFIER_RE.fullmatch(value):
        raise GuardianValidationError(f"{label} has an invalid identifier format")
    return value


def _normalize_utc_timestamp(value: Any, label: str) -> str:
    """Normalize one explicit-offset ISO-8601 timestamp to canonical UTC text."""
    if not isinstance(value, str) or not _ISO_UTC_TIMESTAMP_RE.fullmatch(value):
        raise GuardianValidationError(
            f"{label} must be an ISO-8601 timestamp with an explicit UTC offset"
        )
    try:
        parse_value = f"{value[:-1]}+00:00" if value.endswith("Z") else value
        parsed = dt.datetime.fromisoformat(parse_value)
    except ValueError as exc:
        raise GuardianValidationError(f"{label} is not a valid UTC timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise GuardianValidationError(f"{label} must include an explicit UTC offset")

    utc = parsed.astimezone(dt.timezone.utc)
    timestamp = utc.strftime("%Y-%m-%dT%H:%M:%S")
    if utc.microsecond:
        timestamp += f".{utc.microsecond:06d}".rstrip("0")
    return f"{timestamp}Z"


def _require_probability(value: Any, label: str) -> float | int:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise GuardianValidationError(f"{label} must be a JSON number")
    if not math.isfinite(value) or not 0 <= value <= 1:
        raise GuardianValidationError(f"{label} must be between 0 and 1")
    return value


def _normalize_candidate_data(source: dict[str, Any], label: str) -> dict[str, Any]:
    if not isinstance(source, dict):
        raise GuardianValidationError(f"{label} must be an object")
    unknown = set(source) - SCAN_SOURCE_FIELDS
    if unknown:
        raise GuardianValidationError(f"Unknown field(s) in {label}: {sorted(unknown)}")
    missing = _SOURCE_REQUIRED_FIELDS - set(source)
    if missing:
        raise GuardianValidationError(f"Missing required scan field(s) in {label}: {sorted(missing)}")

    market_id = _require_identifier(source["market_id"], f"{label}.market_id")
    question = _normalize_market_text(source["question"], f"{label}.question", 1000)
    end_date = _normalize_utc_timestamp(source["end_date"], f"{label}.end_date")

    outcomes = source["outcomes"]
    if not isinstance(outcomes, list) or not outcomes or len(outcomes) > 20:
        raise GuardianValidationError(f"{label}.outcomes must contain 1 to 20 labels")
    normalized_outcomes = [
        _normalize_market_text(outcome, f"{label}.outcomes[{index}]", 128)
        for index, outcome in enumerate(outcomes)
    ]
    if len(set(normalized_outcomes)) != len(normalized_outcomes):
        raise GuardianValidationError(f"{label}.outcomes must contain unique exact labels")

    prices = source["outcome_prices"]
    if not isinstance(prices, list) or len(prices) != len(normalized_outcomes):
        raise GuardianValidationError(f"{label}.outcome_prices must align one-for-one with outcomes")
    normalized_prices = [
        _require_probability(price, f"{label}.outcome_prices[{index}]")
        for index, price in enumerate(prices)
    ]

    candidate = {
        "market_id": market_id,
        "question": question,
        "outcomes": normalized_outcomes,
        "outcome_prices": normalized_prices,
        "end_date": end_date,
    }
    if "description" in source and source["description"] not in (None, ""):
        candidate["description"] = _normalize_market_text(
            source["description"], f"{label}.description", 500
        )
    return candidate


def _candidate_id(candidate: dict[str, Any]) -> str:
    """Return a deterministic identifier, not a cryptographic authentication tag."""
    digest = hashlib.sha256(_canonical_json(candidate).encode("utf-8")).hexdigest()
    return f"cand-{digest[:24]}"


def _packet_id(generated_at: str, candidates: list[dict[str, Any]]) -> str:
    """Return a deterministic identifier, not a cryptographic authentication tag."""
    seed = {
        "packet_version": PACKET_VERSION,
        "generated_at": generated_at,
        "mode": "PAPER",
        "candidates": candidates,
    }
    digest = hashlib.sha256(_canonical_json(seed).encode("utf-8")).hexdigest()
    return f"packet-{digest[:24]}"


def prepare_packet(fixture: Any) -> dict[str, Any]:
    """Create one canonical PAPER packet from an operator-controlled fixture."""
    if not isinstance(fixture, dict):
        raise GuardianValidationError("Fixture must be an object")
    _require_exact_fields(fixture, _SOURCE_FIXTURE_FIELDS, "fixture")
    generated_at = _normalize_utc_timestamp(fixture["generated_at"], "fixture.generated_at")
    source_candidates = fixture["candidates"]
    if not isinstance(source_candidates, list) or not source_candidates:
        raise GuardianValidationError("fixture.candidates must be a non-empty array")
    if len(source_candidates) > 1000:
        raise GuardianValidationError("fixture.candidates may contain at most 1000 records")

    candidates: list[dict[str, Any]] = []
    market_ids: set[str] = set()
    candidate_ids: set[str] = set()
    for index, source in enumerate(source_candidates):
        candidate = _normalize_candidate_data(source, f"fixture.candidates[{index}]")
        market_id = candidate["market_id"]
        if market_id in market_ids:
            raise GuardianValidationError(f"Duplicate market_id in fixture: {market_id!r}")
        market_ids.add(market_id)
        candidate_id = _candidate_id(candidate)
        if candidate_id in candidate_ids:
            raise GuardianValidationError(f"Duplicate candidate_id in fixture: {candidate_id!r}")
        candidate_ids.add(candidate_id)
        candidates.append({"candidate_id": candidate_id, **candidate})

    candidates.sort(key=lambda candidate: candidate["market_id"])
    return {
        "packet_version": PACKET_VERSION,
        "packet_id": _packet_id(generated_at, candidates),
        "generated_at": generated_at,
        "mode": "PAPER",
        "candidates": candidates,
    }


def _validate_fixture_bound_intent(
    fixture: Any,
    intent_document: str | bytes | bytearray,
    already_applied_intent_ids: Iterable[str] | None = None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    """Validate an intent and bind it to a freshly reconstructed fixture packet."""
    packet = prepare_packet(fixture)
    try:
        intent = validate_intent(intent_document, already_applied_intent_ids)
    except IntentValidationError as exc:
        # Bounded diagnostic metadata only; the message text is unchanged.
        raise GuardianValidationError(str(exc), code="invalid-input") from exc

    candidates = {candidate["candidate_id"]: candidate for candidate in packet["candidates"]}
    candidate = candidates.get(intent["candidate_id"])
    if candidate is None:
        raise GuardianValidationError("intent candidate_id is not present in the trusted fixture", code="invalid-input")
    if intent["market_id"] != candidate["market_id"]:
        raise GuardianValidationError("intent market_id does not exactly match its candidate", code="invalid-input")
    if intent["outcome"] not in candidate["outcomes"]:
        raise GuardianValidationError("intent outcome does not exactly match a trusted outcome", code="invalid-input")
    return packet, intent, candidate


def validate_candidate_intent(
    fixture: Any,
    intent_document: str | bytes | bytearray,
    already_applied_intent_ids: Iterable[str] | None = None,
) -> dict[str, Any]:
    """Validate a fixture-bound intent without performing any mutation or network I/O."""
    packet, intent, _ = _validate_fixture_bound_intent(
        fixture, intent_document, already_applied_intent_ids
    )
    return {
        "validation_version": VALIDATION_VERSION,
        "packet_id": packet["packet_id"],
        "mode": "PAPER",
        "intent": intent,
    }


def record_candidate_forecast(
    fixture: Any,
    intent_document: str | bytes | bytearray,
    already_applied_intent_ids: Iterable[str] | None = None,
    *,
    _forecast_path: pathlib.Path | None = None,
) -> dict[str, Any]:
    """Record exactly one fixture-bound PAPER forecast through protected core.

    All fixture and intent validation completes before public market I/O. The
    only input that can influence the protected forecast call comes from the
    trusted candidate or validated intent. Strategy proposals are deliberately
    discarded; rationale is passed only as the ordinary data-only forecast note.
    ``_forecast_path`` is an internal test seam and has no production CLI flag.
    """
    packet, intent, candidate = _validate_fixture_bound_intent(
        fixture, intent_document, already_applied_intent_ids
    )
    trusted_outcome = next(
        outcome for outcome in candidate["outcomes"] if outcome == intent["outcome"]
    )
    try:
        kwargs: dict[str, Any] = {
            "market_id": candidate["market_id"],
            "outcome": trusted_outcome,
            "est_prob": intent["estimated_probability"],
            "category": intent["category"],
            "skip_reason": intent["forecast_disposition"],
            "note": intent["rationale"],
            "source_intent_id": intent["intent_id"],
            # The Manus route is permanently non-superseding and cannot
            # auto-confirm a protected extreme-disagreement guard.
            "supersede": False,
            "confirm_extreme": False,
        }
        if _forecast_path is not None:
            kwargs["forecast_path"] = _forecast_path
        recorded = forecast_core.record_forecast(**kwargs)
    except forecast_core.ForecastRecordError as exc:
        raise GuardianValidationError(str(exc), code=getattr(exc, "code", None)) from exc
    except OSError as exc:
        raise GuardianValidationError("Forecast write failed") from exc
    except Exception as exc:
        # Unexpected internal failure: no trusted code exists, so the
        # downstream bounded mapping classifies it as unclassified.
        raise GuardianValidationError("Forecast recording failed") from exc

    return {
        "recording_version": FORECAST_RECORDING_VERSION,
        "packet_id": packet["packet_id"],
        "mode": "PAPER",
        "source_intent_id": intent["intent_id"],
        "forecast": recorded,
    }


def _trusted_source_candidate(fixture: Any, candidate: dict[str, Any]) -> dict[str, Any]:
    """Return the original fixture candidate after packet binding succeeds.

    ``event_id`` is intentionally absent from the Manus-visible packet. It is
    recovered only from the same operator-controlled source fixture after the
    selected packet candidate has already bound market, outcome, and identity.
    """
    source_candidates = fixture.get("candidates") if isinstance(fixture, dict) else None
    if not isinstance(source_candidates, list):
        raise GuardianValidationError("Fixture candidates are unavailable")
    matches = [
        source
        for source in source_candidates
        if isinstance(source, dict) and source.get("market_id") == candidate["market_id"]
    ]
    if len(matches) != 1:
        raise GuardianValidationError("Trusted fixture candidate cannot be identified")
    return matches[0]


def record_candidate_paper_placement(
    fixture: Any,
    intent_document: str | bytes | bytearray,
    already_applied_intent_ids: Iterable[str] | None = None,
    *,
    _ledger_path: pathlib.Path | None = None,
    _forecast_path: pathlib.Path | None = None,
    _protected_config: dict[str, Any] | None = None,
    _risk_config: dict[str, Any] | None = None,
    _now: dt.datetime | None = None,
) -> dict[str, Any]:
    """Place one guarded PAPER row after a matching persisted bet forecast.

    All fixture and intent binding completes before the forecast lookup,
    paper-ledger mutation, or public market I/O. The private underscore
    arguments are test seams only; no production CLI option exposes paths,
    prices, stake, forecast id, packet id, event id, token id, or risk limits.
    """
    packet, intent, candidate = _validate_fixture_bound_intent(
        fixture, intent_document, already_applied_intent_ids
    )
    if intent["forecast_disposition"] != "bet":
        raise GuardianValidationError("forecast_disposition must be 'bet' for guarded PAPER placement")

    source = _trusted_source_candidate(fixture, candidate)
    event_id = _require_identifier(source.get("event_id"), "trusted fixture event_id")
    trusted_outcome = next(
        outcome for outcome in candidate["outcomes"] if outcome == intent["outcome"]
    )
    try:
        kwargs: dict[str, Any] = {
            "source_intent_id": intent["intent_id"],
            "source_packet_id": packet["packet_id"],
            "event_id": event_id,
            "market_id": candidate["market_id"],
            "outcome": trusted_outcome,
            "est_prob": intent["estimated_probability"],
            "category": intent["category"],
            "rationale": intent["rationale"],
            # The protected core assigns its own non-real-eligible execution
            # class. This remains research metadata and never selects policy.
            "research_edge_class": intent["edge_class"],
        }
        if _ledger_path is not None:
            kwargs["_ledger_path"] = _ledger_path
        if _forecast_path is not None:
            kwargs["_forecast_path"] = _forecast_path
        if _protected_config is not None:
            kwargs["_protected_config"] = _protected_config
        if _risk_config is not None:
            kwargs["_risk_config"] = _risk_config
        if _now is not None:
            kwargs["_now"] = _now
        placed = ledger_core.record_manus_paper_placement(**kwargs)
    except ledger_core.ManusPlacementError as exc:
        raise GuardianValidationError(str(exc), code=getattr(exc, "code", None)) from exc
    except OSError as exc:
        raise GuardianValidationError("PAPER ledger write failed") from exc
    except Exception as exc:
        # Unexpected internal failure: no trusted code exists, so the
        # downstream bounded mapping classifies it as unclassified.
        raise GuardianValidationError("PAPER placement failed") from exc
    return {
        "placement_version": PAPER_PLACEMENT_VERSION,
        "packet_id": packet["packet_id"],
        "mode": "PAPER",
        "source_intent_id": intent["intent_id"],
        "placement": placed,
    }


def _load_applied_ids(path_value: str | None) -> list[str] | None:
    if path_value is None:
        return None
    value = _load_json_path(path_value, "already-applied intent ids")
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise GuardianValidationError("already-applied intent ids must be a JSON array of strings")
    return value


def _reject_forbidden_cli_options(arguments: list[str], parser: argparse.ArgumentParser) -> None:
    for argument in arguments:
        option = argument.split("=", 1)[0].lower()
        if option.startswith("--") and any(marker in option for marker in _FORBIDDEN_CLI_MARKERS):
            parser.error(f"Forbidden PAPER guardian option: {option}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Guarded Manus PAPER cycle interface")
    subcommands = parser.add_subparsers(dest="operation", required=True)

    prepare = subcommands.add_parser("prepare", help="prepare a PAPER candidate packet from a trusted fixture")
    prepare.add_argument("--fixture", required=True, help="operator-controlled offline JSON fixture")

    validate = subcommands.add_parser(
        "validate-intent", help="validate an intent against a reconstructed trusted fixture packet"
    )
    validate.add_argument("--fixture", required=True, help="operator-controlled offline JSON fixture")
    validate.add_argument("--intent", required=True, help="untrusted Manus intent JSON")
    validate.add_argument("--already-applied", help="optional JSON array of already-applied intent IDs")

    record = subcommands.add_parser(
        "record-forecast", help="record one validated, fixture-bound PAPER forecast"
    )
    record.add_argument("--fixture", required=True, help="operator-controlled offline JSON fixture")
    record.add_argument("--intent", required=True, help="untrusted Manus intent JSON")
    record.add_argument("--already-applied", help="optional JSON array of already-applied intent IDs")

    placement = subcommands.add_parser(
        "record-paper-placement",
        help="place one guarded PAPER row after an existing bound bet forecast",
    )
    placement.add_argument("--fixture", required=True, help="operator-controlled offline JSON fixture")
    placement.add_argument("--intent", required=True, help="untrusted Manus intent JSON")
    placement.add_argument("--already-applied", help="optional JSON array of already-applied intent IDs")
    return parser


def main(argv: list[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    parser = build_parser()
    _reject_forbidden_cli_options(arguments, parser)
    args = parser.parse_args(arguments)
    try:
        fixture = _load_json_path(args.fixture, "fixture")
        if args.operation == "prepare":
            print(_canonical_json(prepare_packet(fixture)))
        else:
            try:
                intent_text = pathlib.Path(args.intent).read_text(encoding="utf-8")
            except OSError as exc:
                raise GuardianValidationError("Unable to read intent") from exc
            applied_ids = _load_applied_ids(args.already_applied)
            if args.operation == "validate-intent":
                result = validate_candidate_intent(fixture, intent_text, applied_ids)
            elif args.operation == "record-forecast":
                result = record_candidate_forecast(fixture, intent_text, applied_ids)
            else:
                result = record_candidate_paper_placement(fixture, intent_text, applied_ids)
            print(_canonical_json(result))
    except GuardianValidationError as exc:
        parser.exit(2, f"REJECTED: {exc}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

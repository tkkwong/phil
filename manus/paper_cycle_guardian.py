"""Operator-owned, data-only guardian for Manus PAPER candidate packets.

The guardian deliberately has no core imports, network calls, credential access,
filesystem writes, or execution path. It turns an operator-controlled offline
fixture following ``core/scan.py``'s public record shape into a packet for
Manus, then reconstructs that packet from the same fixture before validating an
untrusted Manus intent.
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

from manus.intent_validator import IntentValidationError, validate_intent


PACKET_VERSION = "paper-candidate-packet/v1"
VALIDATION_VERSION = "paper-guardian-validation/v1"

# These are exactly the fields currently emitted by core/scan.py's keep().
# Prepare accepts the full read-only scan record but emits only its bounded,
# research-identifying subset into the candidate packet.
_SCAN_SOURCE_FIELDS = frozenset(
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
_FORBIDDEN_CLI_MARKERS = ("real", "live-trading", "execute", "order", "ibkr", "pearl")


class GuardianValidationError(ValueError):
    """Raised when fixture or fixture-bound intent data is malformed or unsafe."""


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
    """Read one explicit operator-supplied input path; this guardian never writes."""
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
    unknown = set(source) - _SCAN_SOURCE_FIELDS
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


def validate_candidate_intent(
    fixture: Any,
    intent_document: str | bytes | bytearray,
    already_applied_intent_ids: Iterable[str] | None = None,
) -> dict[str, Any]:
    """Validate an untrusted intent against a freshly reconstructed trusted packet.

    The fixture, rather than a packet supplied by Manus, is the validation
    authority. This function is data-only: it does not write files, persist ids,
    create forecasts or ledger entries, execute text, or invoke external code.
    """
    packet = prepare_packet(fixture)
    try:
        intent = validate_intent(intent_document, already_applied_intent_ids)
    except IntentValidationError as exc:
        raise GuardianValidationError(str(exc)) from exc

    candidates = {candidate["candidate_id"]: candidate for candidate in packet["candidates"]}
    candidate = candidates.get(intent["candidate_id"])
    if candidate is None:
        raise GuardianValidationError("intent candidate_id is not present in the trusted fixture")
    if intent["market_id"] != candidate["market_id"]:
        raise GuardianValidationError("intent market_id does not exactly match its candidate")
    if intent["outcome"] not in candidate["outcomes"]:
        raise GuardianValidationError("intent outcome does not exactly match a trusted outcome")

    return {
        "validation_version": VALIDATION_VERSION,
        "packet_id": packet["packet_id"],
        "mode": "PAPER",
        "intent": intent,
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
    parser = argparse.ArgumentParser(description="Data-only PAPER cycle guardian")
    subcommands = parser.add_subparsers(dest="operation", required=True)

    prepare = subcommands.add_parser("prepare", help="prepare a PAPER candidate packet from a trusted fixture")
    prepare.add_argument("--fixture", required=True, help="operator-controlled offline JSON fixture")

    validate = subcommands.add_parser(
        "validate-intent", help="validate an intent against a reconstructed trusted fixture packet"
    )
    validate.add_argument("--fixture", required=True, help="operator-controlled offline JSON fixture")
    validate.add_argument("--intent", required=True, help="untrusted Manus intent JSON")
    validate.add_argument("--already-applied", help="optional JSON array of already-applied intent IDs")
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
            result = validate_candidate_intent(
                fixture, intent_text, _load_applied_ids(args.already_applied)
            )
            print(_canonical_json(result))
    except GuardianValidationError as exc:
        parser.exit(2, f"REJECTED: {exc}\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

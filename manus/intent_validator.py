"""Strict data-only validation for Manus paper forecasting intents.

This module intentionally uses only the Python standard library. It parses an
untrusted JSON document, validates the complete contract, and has no I/O or
side effects.
"""

from __future__ import annotations

import json
import math
import re
from typing import Any, Iterable


class IntentValidationError(ValueError):
    """Raised when a Manus intent is malformed, unsafe, or already applied."""


_ALLOWED_FIELDS = frozenset(
    {
        "intent_id",
        "candidate_id",
        "market_id",
        "outcome",
        "estimated_probability",
        "category",
        "rationale",
        "edge_class",
        "mode",
        "forecast_disposition",
        "strategy_proposals",
    }
)
_PROPOSAL_FIELDS = frozenset({"proposal_id", "summary"})

_INTENT_ID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"
)
_EXTERNAL_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]*$")
_LABEL_RE = re.compile(r"^[a-z][a-z0-9_-]*$")
_PROPOSAL_ID_RE = re.compile(r"^proposal-[a-z0-9][a-z0-9-]*$")
_TRAVERSAL_RE = re.compile(r"(?:^|[\\/])\.\.(?:[\\/]|$)")
_WINDOWS_ABSOLUTE_RE = re.compile(r"^[A-Za-z]:[\\/]")

# Field-name tokens are checked before the allowed-field check. This makes
# attempts to introduce an action channel fail explicitly instead of merely
# being treated as ordinary unknown fields.
_FORBIDDEN_FIELD_TOKENS = frozenset(
    {
        # Files and mutation targets.
        "path",
        "filepath",
        "filename",
        "file",
        "directory",
        "dir",
        "write",
        "edit",
        "modify",
        "delete",
        "patch",
        # Commands and execution.
        "command",
        "cmd",
        "shell",
        "executable",
        "execute",
        "exec",
        "argv",
        "script",
        "program",
        "binary",
        "subprocess",
        # Git operations.
        "git",
        "branch",
        "commit",
        "push",
        "pull",
        "pullrequest",
        "merge",
        "rebase",
        "checkout",
        "repository",
        "repo",
        # Real trading and broker execution.
        "trade",
        "trading",
        "order",
        "execution",
        "fill",
        "position",
        "broker",
        "portfolio",
        "account",
        # IBKR / Interactive Brokers.
        "ibkr",
        "interactivebrokers",
        "interactivebroker",
        "conid",
        "contract",
        "orderid",
        "executionid",
        # Pearl.
        "pearl",
        "pearlconnect",
        # Credentials and authorization material.
        "credential",
        "credentials",
        "secret",
        "secrets",
        "token",
        "apikey",
        "password",
        "passwd",
        "authorization",
        "auth",
        "cookie",
        "privatekey",
        "accesskey",
        "clientsecret",
        "key",
    }
)

_COMMAND_WORD_RE = re.compile(
    r"(?:^|\s)(?:bash|sh|zsh|fish|powershell|cmd(?:\.exe)?|python(?:3)?|"
    r"node|npm|pip|git|curl|wget|rm|mv|cp|chmod|make)(?:\s|$)",
    re.IGNORECASE,
)
_SHELL_SYNTAX_RE = re.compile(r"(?:\||&&|;|`|\$\()")
_FILE_MUTATION_RE = re.compile(
    r"\b(?:edit|modify|write|overwrite|delete|create|apply|patch|move|copy|"
    r"rename)\b.*\b(?:file|files|path|directory|folder|config|code|workflow)\b",
    re.IGNORECASE,
)
_GIT_ACTION_RE = re.compile(
    r"\b(?:git\s+(?:add|commit|push|merge|rebase|checkout|reset)|"
    r"(?:create|delete|switch|checkout)\s+(?:a\s+)?branch|"
    r"(?:open|create|merge)\s+(?:a\s+)?pull\s+request|"
    r"(?:commit|push|merge|rebase)\s+(?:the\s+)?(?:changes|branch|repository|repo))\b",
    re.IGNORECASE,
)
_CREDENTIAL_MATERIAL_RE = re.compile(
    r"\b(?:api[ _-]?key|secret|token|password|passwd|authorization|bearer|"
    r"private[ _-]?key|access[ _-]?key)\b\s*(?:[:=]|\b)",
    re.IGNORECASE,
)


def _duplicate_key_rejector(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Build a JSON object while rejecting duplicate object keys."""
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise IntentValidationError(f"Duplicate JSON field: {key!r}")
        result[key] = value
    return result


def _reject_nonstandard_json_constant(value: str) -> None:
    raise IntentValidationError(f"Non-standard JSON number is not allowed: {value}")


def _parse_json(document: str | bytes | bytearray) -> dict[str, Any]:
    if not isinstance(document, (str, bytes, bytearray)):
        raise IntentValidationError("Intent must be a JSON string, bytes, or bytearray")

    try:
        parsed = json.loads(
            document,
            object_pairs_hook=_duplicate_key_rejector,
            parse_constant=_reject_nonstandard_json_constant,
        )
    except IntentValidationError:
        raise
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise IntentValidationError("Malformed JSON intent") from exc

    if not isinstance(parsed, dict):
        raise IntentValidationError("Intent JSON must be an object")
    return parsed


def _field_name_terms(key: str) -> set[str]:
    """Split snake, kebab, spaced, and camel-case field names into terms."""
    camel_separated = re.sub(r"(?<=[a-z0-9])(?=[A-Z])", " ", key)
    return set(re.findall(r"[a-z0-9]+", camel_separated.lower()))


def _validate_keys(value: Any, location: str = "$") -> None:
    """Reject action-bearing field names recursively before shape validation."""
    if isinstance(value, dict):
        for key, child in value.items():
            if not isinstance(key, str):
                raise IntentValidationError(f"Non-string field name at {location}")
            if _field_name_terms(key) & _FORBIDDEN_FIELD_TOKENS:
                raise IntentValidationError(f"Forbidden field {key!r} at {location}")
            _validate_keys(child, f"{location}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _validate_keys(child, f"{location}[{index}]")


def _validate_string_safety(value: Any, location: str = "$") -> None:
    """Reject filesystem addressing and control characters in every string."""
    if isinstance(value, str):
        if any(ord(character) < 32 or ord(character) == 127 for character in value):
            raise IntentValidationError(f"Control character in string at {location}")
        if value.startswith(("/", "\\", "~/")) or _WINDOWS_ABSOLUTE_RE.match(value):
            raise IntentValidationError(f"Absolute path is not allowed at {location}")
        if _TRAVERSAL_RE.search(value):
            raise IntentValidationError(f"Path traversal is not allowed at {location}")
    elif isinstance(value, dict):
        for key, child in value.items():
            _validate_string_safety(child, f"{location}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _validate_string_safety(child, f"{location}[{index}]")


def _require_exact_fields(
    value: dict[str, Any], allowed: frozenset[str], location: str
) -> None:
    unknown = set(value) - allowed
    missing = allowed - set(value)
    if unknown:
        rendered = ", ".join(sorted(repr(field) for field in unknown))
        raise IntentValidationError(f"Unknown field(s) at {location}: {rendered}")
    if missing:
        rendered = ", ".join(sorted(repr(field) for field in missing))
        raise IntentValidationError(f"Missing required field(s) at {location}: {rendered}")


def _require_string(value: Any, field: str, maximum: int) -> str:
    if not isinstance(value, str):
        raise IntentValidationError(f"{field} must be a string")
    if not value or not value.strip():
        raise IntentValidationError(f"{field} must not be empty")
    if len(value) > maximum:
        raise IntentValidationError(f"{field} exceeds {maximum} characters")
    return value


def _require_pattern(value: str, field: str, pattern: re.Pattern[str]) -> str:
    if not pattern.fullmatch(value):
        raise IntentValidationError(f"{field} has an invalid format")
    return value


def _validate_probability(value: Any) -> float | int:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise IntentValidationError("estimated_probability must be a JSON number")
    if not math.isfinite(value) or not 0 < value < 1:
        raise IntentValidationError("estimated_probability must be greater than 0 and less than 1")
    return value


def _validate_proposals(value: Any) -> list[dict[str, str]]:
    if not isinstance(value, list):
        raise IntentValidationError("strategy_proposals must be an array")
    if len(value) > 10:
        raise IntentValidationError("strategy_proposals may contain at most 10 suggestions")

    validated: list[dict[str, str]] = []
    for index, proposal in enumerate(value):
        location = f"strategy_proposals[{index}]"
        if not isinstance(proposal, dict):
            raise IntentValidationError(f"{location} must be an object")
        _require_exact_fields(proposal, _PROPOSAL_FIELDS, location)

        proposal_id = _require_string(proposal["proposal_id"], f"{location}.proposal_id", 64)
        _require_pattern(proposal_id, f"{location}.proposal_id", _PROPOSAL_ID_RE)
        summary = _require_string(proposal["summary"], f"{location}.summary", 500)
        _validate_proposal_summary(summary, location)
        validated.append({"proposal_id": proposal_id, "summary": summary})
    return validated


def _validate_proposal_summary(summary: str, location: str) -> None:
    """Reject executable or mutable instructions while retaining inert strategy text."""
    if _COMMAND_WORD_RE.search(summary) or _SHELL_SYNTAX_RE.search(summary):
        raise IntentValidationError(f"Command or shell content is not allowed in {location}")
    if _FILE_MUTATION_RE.search(summary):
        raise IntentValidationError(f"File mutation is not allowed in {location}")
    if _GIT_ACTION_RE.search(summary):
        raise IntentValidationError(f"Git operation is not allowed in {location}")
    if _CREDENTIAL_MATERIAL_RE.search(summary):
        raise IntentValidationError(f"Credential material is not allowed in {location}")


def _validate_applied_ids(already_applied_intent_ids: Iterable[str] | None) -> set[str]:
    if already_applied_intent_ids is None:
        return set()
    if not isinstance(already_applied_intent_ids, (set, frozenset, list, tuple)):
        raise IntentValidationError(
            "already_applied_intent_ids must be a set, frozenset, list, tuple, or None"
        )
    if not all(isinstance(intent_id, str) for intent_id in already_applied_intent_ids):
        raise IntentValidationError("already_applied_intent_ids must contain only strings")
    return set(already_applied_intent_ids)


def validate_intent(
    document: str | bytes | bytearray,
    already_applied_intent_ids: Iterable[str] | None = None,
) -> dict[str, Any]:
    """Parse and validate one data-only, paper-only Manus intent.

    The function is deliberately pure: it does not persist the accepted id or
    perform any external action. Callers own durable duplicate tracking and
    pass the previously applied ids on each validation attempt.
    """
    intent = _parse_json(document)
    _validate_keys(intent)
    _validate_string_safety(intent)
    _require_exact_fields(intent, _ALLOWED_FIELDS, "intent")

    intent_id = _require_string(intent["intent_id"], "intent_id", 36)
    _require_pattern(intent_id, "intent_id", _INTENT_ID_RE)

    candidate_id = _require_string(intent["candidate_id"], "candidate_id", 128)
    _require_pattern(candidate_id, "candidate_id", _EXTERNAL_ID_RE)

    market_id = _require_string(intent["market_id"], "market_id", 128)
    _require_pattern(market_id, "market_id", _EXTERNAL_ID_RE)

    outcome = _require_string(intent["outcome"], "outcome", 128)

    probability = _validate_probability(intent["estimated_probability"])

    category = _require_string(intent["category"], "category", 64)
    _require_pattern(category, "category", _LABEL_RE)

    rationale = _require_string(intent["rationale"], "rationale", 2000)

    edge_class = _require_string(intent["edge_class"], "edge_class", 64)
    _require_pattern(edge_class, "edge_class", _LABEL_RE)

    mode = _require_string(intent["mode"], "mode", 5)
    if mode != "PAPER":
        raise IntentValidationError("mode must be PAPER")

    forecast_disposition = _require_string(
        intent["forecast_disposition"], "forecast_disposition", 64
    )
    _require_pattern(forecast_disposition, "forecast_disposition", _LABEL_RE)

    proposals = _validate_proposals(intent["strategy_proposals"])

    applied_ids = _validate_applied_ids(already_applied_intent_ids)
    if intent_id in applied_ids:
        raise IntentValidationError("Duplicate intent_id has already been applied")

    return {
        "intent_id": intent_id,
        "candidate_id": candidate_id,
        "market_id": market_id,
        "outcome": outcome,
        "estimated_probability": probability,
        "category": category,
        "rationale": rationale,
        "edge_class": edge_class,
        "mode": mode,
        "forecast_disposition": forecast_disposition,
        "strategy_proposals": proposals,
    }

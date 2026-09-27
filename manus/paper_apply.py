"""Manual operator bridge from fixed validated staging to existing PAPER guards.

This module consumes only a validated intent already staged by
``manus.research_transport``.  It never calls a Manus API, reads a credential,
accepts journal/staging paths from a caller, or implements forecast/placement
policy.  Forecast and simulated PAPER ledger mutations go only through the
existing fixture-bound guardian functions.

The bridge is deliberately single-writer: do not run it concurrently with the
legacy runner or another paper_apply process against the same journals.
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import pathlib
import tempfile
import uuid
from typing import Any, Callable

from core import forecast as forecast_core
from core import ledger as ledger_core
from manus.paper_cycle_guardian import (
    VALIDATION_VERSION,
    GuardianValidationError,
    _canonical_json,
    _parse_json_document,
    prepare_packet,
    record_candidate_forecast,
    record_candidate_paper_placement,
    validate_candidate_intent,
)
from manus.research_transport import TRANSPORT_REQUEST_SCHEMA_VERSION


APPLICATION_VERSION = "paper-apply/v1"
_STAGING_CHILDREN = ("phil-manus", "staging")
_RECEIPT_STATES = frozenset(
    {
        "prepared",
        "forecast-pending",
        "forecast-recorded",
        "placement-pending",
        "completed-no-placement",
        "completed-placement",
        "forecast-rejected",
        "placement-rejected",
        "error",
    }
)
_TERMINAL_FAILURE_STATES = frozenset({"forecast-rejected", "placement-rejected", "error"})
_COMPLETED_STATES = frozenset({"completed-no-placement", "completed-placement"})
_RECEIPT_FIELDS = frozenset(
    {
        "application_version",
        "state",
        "packet_id",
        "intent_id",
        "candidate_id",
        "market_id",
        "fixture_sha256",
        "intent_sha256",
        "forecast_disposition",
        "forecast_id",
        "placement_id",
        "created_at",
        "updated_at",
    }
)
_FORBIDDEN_OPTION_TERMS = frozenset(
    {
        "intent-file",
        "intent",
        "staging",
        "output",
        "forecast",
        "ledger",
        "packet",
        "candidate",
        "market",
        "event",
        "token",
        "stake",
        "price",
        "bid",
        "ask",
        "edge",
        "spread",
        "risk",
        "confirm",
        "supersede",
        "broker",
        "real",
        "live",
        "ibkr",
        "pearl",
        "api",
        "retry",
        "force",
        "override",
        "shell",
        "command",
    }
)
_SHA256_LENGTH = 64


class PaperApplyError(RuntimeError):
    """Raised when staging, reconciliation, or guarded application rejects."""


def _utc_timestamp(now: Callable[[], dt.datetime]) -> str:
    value = now()
    if not isinstance(value, dt.datetime):
        raise PaperApplyError("Protected application clock is invalid")
    if value.tzinfo is None or value.utcoffset() is None:
        raise PaperApplyError("Protected application clock is invalid")
    return value.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _require_canonical_uuid4(value: Any, label: str) -> str:
    if not isinstance(value, str):
        raise PaperApplyError(f"{label} must be a canonical lowercase UUIDv4")
    try:
        parsed = uuid.UUID(value)
    except (AttributeError, ValueError) as exc:
        raise PaperApplyError(f"{label} must be a canonical lowercase UUIDv4") from exc
    if parsed.version != 4 or str(parsed) != value:
        raise PaperApplyError(f"{label} must be a canonical lowercase UUIDv4")
    return value


def _require_sha256(value: Any, label: str) -> str:
    if not isinstance(value, str) or len(value) != _SHA256_LENGTH:
        raise PaperApplyError(f"{label} must be a SHA-256 digest")
    try:
        int(value, 16)
    except ValueError as exc:
        raise PaperApplyError(f"{label} must be a SHA-256 digest") from exc
    if value.lower() != value:
        raise PaperApplyError(f"{label} must be a SHA-256 digest")
    return value


def _require_identifier(value: Any, label: str) -> str:
    if not isinstance(value, str) or not value or len(value) > 128:
        raise PaperApplyError(f"{label} is invalid")
    if any(character not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789._:-" for character in value):
        raise PaperApplyError(f"{label} is invalid")
    return value


def _resolve_staging_root() -> pathlib.Path:
    """Resolve the fixed external staging root without creating it."""
    local_appdata = os.environ.get("LOCALAPPDATA")
    if not local_appdata:
        raise PaperApplyError("Fixed staging root is unavailable")
    root = (pathlib.Path(local_appdata) / _STAGING_CHILDREN[0] / _STAGING_CHILDREN[1]).resolve(strict=False)
    repository_root = pathlib.Path(__file__).resolve().parents[1]
    try:
        root.relative_to(repository_root)
    except ValueError:
        return root
    raise PaperApplyError("Fixed staging root must be outside the repository")


def _load_fixture(fixture_path: str) -> tuple[dict[str, Any], str]:
    """Read exact fixture bytes and apply the guardian's duplicate-key parser."""
    try:
        raw = pathlib.Path(fixture_path).read_bytes()
        fixture = _parse_json_document(raw.decode("utf-8"), "fixture")
    except (OSError, UnicodeDecodeError, GuardianValidationError):
        raise PaperApplyError("Trusted fixture is unavailable or invalid") from None
    if not isinstance(fixture, dict):
        raise PaperApplyError("Trusted fixture is unavailable or invalid")
    return fixture, hashlib.sha256(raw).hexdigest()


def _safe_file(path: pathlib.Path, label: str) -> bytes:
    if path.is_symlink() or not path.is_file():
        raise PaperApplyError(f"Fixed staged {label} is unavailable")
    try:
        return path.read_bytes()
    except OSError:
        raise PaperApplyError(f"Fixed staged {label} is unavailable") from None


def _fixed_paths(staging_root: pathlib.Path, packet_id: str, intent_id: str) -> tuple[pathlib.Path, pathlib.Path, pathlib.Path]:
    """Return only fixed staging and receipt locations; caller inputs cannot alter them."""
    root = pathlib.Path(staging_root)
    try:
        intent_directory = root / _require_identifier(packet_id, "packet_id") / _require_canonical_uuid4(intent_id, "intent_id")
        receipt_path = root / "apply" / packet_id / f"{intent_id}.json"
        intent_directory.relative_to(root)
        receipt_path.relative_to(root)
    except ValueError:
        raise PaperApplyError("Fixed staging path is unavailable") from None
    for directory in (
        root,
        root / packet_id,
        intent_directory,
        root / "apply",
        root / "apply" / packet_id,
    ):
        if directory.is_symlink():
            raise PaperApplyError("Fixed staged intent directory is unavailable")
    return intent_directory / "validated-intent.json", intent_directory / "run-meta.json", receipt_path


def _parse_staged_document(raw: bytes, label: str) -> dict[str, Any]:
    try:
        document = _parse_json_document(raw.decode("utf-8"), label)
    except (UnicodeDecodeError, GuardianValidationError):
        raise PaperApplyError(f"Fixed staged {label} is invalid") from None
    if not isinstance(document, dict):
        raise PaperApplyError(f"Fixed staged {label} is invalid")
    return document


def _matching_candidate(packet: dict[str, Any], candidate_id: Any) -> dict[str, Any]:
    candidates = packet.get("candidates")
    if not isinstance(candidates, list):
        raise PaperApplyError("Trusted packet has no candidates")
    matches = [candidate for candidate in candidates if candidate.get("candidate_id") == candidate_id]
    if len(matches) != 1:
        raise PaperApplyError("Staged candidate_id is not uniquely present in the trusted packet")
    return matches[0]


def _validate_run_meta(
    metadata: dict[str, Any],
    *,
    packet_id: str,
    intent_id: str,
    candidate_id: str,
    fixture_sha256: str,
) -> None:
    required = {
        "packet_id",
        "intent_id",
        "candidate_id",
        "fixture_sha256",
        "transport_schema_version",
        "validation_version",
    }
    if not required.issubset(metadata):
        raise PaperApplyError("Staged run metadata is incomplete")
    if metadata.get("packet_id") != packet_id:
        raise PaperApplyError("Staged run metadata packet_id does not match the trusted fixture")
    if metadata.get("intent_id") != intent_id:
        raise PaperApplyError("Staged run metadata intent_id does not match the requested intent")
    if metadata.get("candidate_id") != candidate_id:
        raise PaperApplyError("Staged run metadata candidate_id does not match the staged intent")
    if metadata.get("fixture_sha256") != fixture_sha256:
        raise PaperApplyError("Staged run metadata fixture hash does not match exact fixture bytes")
    if metadata.get("transport_schema_version") != TRANSPORT_REQUEST_SCHEMA_VERSION:
        raise PaperApplyError("Staged transport schema version is incompatible")
    if metadata.get("validation_version") != VALIDATION_VERSION:
        raise PaperApplyError("Staged validation version is incompatible")


def _load_and_validate_staging(
    fixture_path: str,
    intent_id: str,
    staging_root: pathlib.Path,
) -> dict[str, Any]:
    """Reconstruct fixture authority and revalidate one fixed staged intent."""
    canonical_intent_id = _require_canonical_uuid4(intent_id, "intent_id")
    fixture, fixture_sha256 = _load_fixture(fixture_path)
    try:
        packet = prepare_packet(fixture)
    except GuardianValidationError:
        raise PaperApplyError("Trusted fixture is unavailable or invalid") from None
    packet_id = packet.get("packet_id")
    if not isinstance(packet_id, str):
        raise PaperApplyError("Trusted packet is unavailable")
    intent_path, metadata_path, receipt_path = _fixed_paths(staging_root, packet_id, canonical_intent_id)
    intent_raw = _safe_file(intent_path, "validated intent")
    metadata_raw = _safe_file(metadata_path, "run metadata")
    staged_intent = _parse_staged_document(intent_raw, "validated intent")
    metadata = _parse_staged_document(metadata_raw, "run metadata")

    candidate_id = staged_intent.get("candidate_id")
    candidate = _matching_candidate(packet, candidate_id)
    if staged_intent.get("intent_id") != canonical_intent_id:
        raise PaperApplyError("Staged intent_id does not match the requested intent")
    if staged_intent.get("mode") != "PAPER":
        raise PaperApplyError("Staged intent mode must be PAPER")
    if staged_intent.get("strategy_proposals") != []:
        raise PaperApplyError("Staged intent strategy_proposals must be empty")
    if staged_intent.get("market_id") != candidate.get("market_id"):
        raise PaperApplyError("Staged intent market_id does not match the trusted candidate")
    if staged_intent.get("outcome") not in candidate.get("outcomes", []):
        raise PaperApplyError("Staged intent outcome does not match a trusted candidate outcome")
    _validate_run_meta(
        metadata,
        packet_id=packet_id,
        intent_id=canonical_intent_id,
        candidate_id=candidate_id,
        fixture_sha256=fixture_sha256,
    )
    try:
        validated = validate_candidate_intent(fixture, intent_raw)
    except (GuardianValidationError, TypeError, ValueError):
        raise PaperApplyError("Staged intent failed strict fixture-bound validation") from None
    intent = validated.get("intent")
    if not isinstance(intent, dict) or intent != staged_intent:
        raise PaperApplyError("Staged intent differs from strict fixture-bound validation")
    if validated.get("packet_id") != packet_id or validated.get("validation_version") != VALIDATION_VERSION:
        raise PaperApplyError("Staged fixture-bound validation provenance is incompatible")
    return {
        "fixture": fixture,
        "packet": packet,
        "candidate": candidate,
        "intent": intent,
        "intent_raw": intent_raw,
        "fixture_sha256": fixture_sha256,
        "intent_sha256": hashlib.sha256(intent_raw).hexdigest(),
        "receipt_path": receipt_path,
    }


def _application_plan(intent: dict[str, Any]) -> str:
    return (
        "record-forecast-then-guarded-paper-placement"
        if intent["forecast_disposition"] == "bet"
        else "record-forecast-only"
    )


def _summary(context: dict[str, Any]) -> dict[str, Any]:
    intent = context["intent"]
    packet = context["packet"]
    return {
        "mode": "PAPER",
        "packet_id": packet["packet_id"],
        "intent_id": intent["intent_id"],
        "candidate_id": intent["candidate_id"],
        "market_id": intent["market_id"],
        "forecast_disposition": intent["forecast_disposition"],
        "staging_verified": True,
        "plan": _application_plan(intent),
    }


def _new_receipt(context: dict[str, Any], now: Callable[[], dt.datetime]) -> dict[str, Any]:
    intent = context["intent"]
    timestamp = _utc_timestamp(now)
    return {
        "application_version": APPLICATION_VERSION,
        "state": "prepared",
        "packet_id": context["packet"]["packet_id"],
        "intent_id": intent["intent_id"],
        "candidate_id": intent["candidate_id"],
        "market_id": intent["market_id"],
        "fixture_sha256": context["fixture_sha256"],
        "intent_sha256": context["intent_sha256"],
        "forecast_disposition": intent["forecast_disposition"],
        "forecast_id": None,
        "placement_id": None,
        "created_at": timestamp,
        "updated_at": timestamp,
    }


def _validate_receipt(receipt: dict[str, Any], context: dict[str, Any]) -> dict[str, Any]:
    if set(receipt) != _RECEIPT_FIELDS:
        raise PaperApplyError("Application receipt is malformed")
    expected = _new_receipt(context, lambda: dt.datetime(2000, 1, 1, tzinfo=dt.timezone.utc))
    for field in (
        "application_version",
        "packet_id",
        "intent_id",
        "candidate_id",
        "market_id",
        "fixture_sha256",
        "intent_sha256",
        "forecast_disposition",
    ):
        if receipt.get(field) != expected[field]:
            raise PaperApplyError(f"Application receipt {field} does not match fixed staged provenance")
    if receipt.get("state") not in _RECEIPT_STATES:
        raise PaperApplyError("Application receipt state is invalid")
    for field in ("forecast_id", "placement_id"):
        value = receipt.get(field)
        if value is not None and (not isinstance(value, str) or not value or len(value) > 128):
            raise PaperApplyError(f"Application receipt {field} is invalid")
    for field in ("created_at", "updated_at"):
        if not isinstance(receipt.get(field), str) or not receipt[field].endswith("Z"):
            raise PaperApplyError(f"Application receipt {field} is invalid")
    return receipt


def _read_receipt(path: pathlib.Path, context: dict[str, Any]) -> dict[str, Any] | None:
    if not path.exists() and not path.is_symlink():
        return None
    if path.is_symlink() or not path.is_file():
        raise PaperApplyError("Application receipt is unavailable")
    try:
        receipt = _parse_json_document(path.read_text(encoding="utf-8"), "application receipt")
    except (OSError, UnicodeDecodeError, GuardianValidationError):
        raise PaperApplyError("Application receipt is malformed") from None
    if not isinstance(receipt, dict):
        raise PaperApplyError("Application receipt is malformed")
    return _validate_receipt(receipt, context)


def _atomic_write_receipt(path: pathlib.Path, receipt: dict[str, Any]) -> None:
    """Durably write one fixed receipt before/after guarded journal operations."""
    directory = path.parent
    try:
        directory.mkdir(parents=True, exist_ok=True)
        if not directory.is_dir() or directory.is_symlink() or path.is_symlink():
            raise OSError("invalid application receipt path")
        descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=directory)
    except OSError:
        raise PaperApplyError("Application receipt staging is unavailable") from None
    temporary_path = pathlib.Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(_canonical_json(receipt))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
    except Exception:
        try:
            temporary_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise PaperApplyError("Application receipt write failed") from None


def _transition_receipt(
    path: pathlib.Path,
    receipt: dict[str, Any],
    state: str,
    *,
    now: Callable[[], dt.datetime],
    forecast_id: str | None = None,
    placement_id: str | None = None,
) -> dict[str, Any]:
    if state not in _RECEIPT_STATES:
        raise ValueError("invalid application state")
    updated = {
        **receipt,
        "state": state,
        "forecast_id": receipt["forecast_id"] if forecast_id is None else forecast_id,
        "placement_id": receipt["placement_id"] if placement_id is None else placement_id,
        "updated_at": _utc_timestamp(now),
    }
    _atomic_write_receipt(path, updated)
    return updated


def _read_jsonl(path: pathlib.Path, label: str) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    if path.is_symlink() or not path.is_file():
        raise PaperApplyError(f"Unable to read {label}")
    try:
        values = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        raise PaperApplyError(f"Unable to read {label}") from None
    if not all(isinstance(value, dict) for value in values):
        raise PaperApplyError(f"Unable to read {label}")
    return values


def _recover_forecast(context: dict[str, Any], forecast_path: pathlib.Path) -> dict[str, Any] | None:
    """Find exactly one forecast with exact staged-intent provenance and content."""
    intent = context["intent"]
    matches = [
        row for row in _read_jsonl(forecast_path, "forecast journal")
        if row.get("source_intent_id") == intent["intent_id"]
    ]
    if not matches:
        return None
    if len(matches) != 1:
        raise PaperApplyError("source_intent_id has multiple persisted forecasts")
    forecast = matches[0]
    expected = {
        "market_id": intent["market_id"],
        "outcome": intent["outcome"],
        "est_prob": intent["estimated_probability"],
        "category": intent["category"],
        "skip_reason": intent["forecast_disposition"],
        "note": intent["rationale"],
    }
    for field, value in expected.items():
        if forecast.get(field) != value:
            raise PaperApplyError(f"Recovered forecast {field} does not match staged intent")
    forecast_id = forecast.get("id")
    if not isinstance(forecast_id, str) or not forecast_id or len(forecast_id) > 128:
        raise PaperApplyError("Recovered forecast id is invalid")
    return forecast


def _recover_placement(
    context: dict[str, Any],
    forecast: dict[str, Any],
    ledger_path: pathlib.Path,
) -> dict[str, Any] | None:
    """Find exactly one guarded PAPER row bound to this staged intent and forecast."""
    intent = context["intent"]
    forecast_id = forecast["id"]
    rows = _read_jsonl(ledger_path, "PAPER ledger")
    matches = [
        row for row in rows
        if row.get("source_intent_id") == intent["intent_id"] or row.get("source_forecast_id") == forecast_id
    ]
    if not matches:
        return None
    if len(matches) != 1:
        raise PaperApplyError("Guarded PAPER provenance has multiple matching ledger rows")
    placement = matches[0]
    expected = {
        "source_intent_id": intent["intent_id"],
        "source_forecast_id": forecast_id,
        "source_packet_id": context["packet"]["packet_id"],
        "market_id": intent["market_id"],
        "outcome": intent["outcome"],
        "est_prob": intent["estimated_probability"],
        "category": intent["category"],
        "rationale": intent["rationale"],
        "edge_class": ledger_core.MANUS_PAPER_EDGE_CLASS,
        "research_edge_class": intent["edge_class"],
    }
    for field, value in expected.items():
        if placement.get(field) != value:
            raise PaperApplyError(f"Recovered PAPER placement {field} does not match staged intent")
    placement_id = placement.get("id")
    if not isinstance(placement_id, str) or not placement_id or len(placement_id) > 128:
        raise PaperApplyError("Recovered PAPER placement id is invalid")
    return placement


def _has_ledger_intent_provenance(context: dict[str, Any], ledger_path: pathlib.Path) -> bool:
    """Detect an orphaned guarded row before any forecast retry is considered."""
    intent_id = context["intent"]["intent_id"]
    return any(
        row.get("source_intent_id") == intent_id
        for row in _read_jsonl(ledger_path, "PAPER ledger")
    )


def _record_forecast(
    context: dict[str, Any],
    forecast_path: pathlib.Path | None,
) -> dict[str, Any]:
    kwargs: dict[str, Any] = {}
    if forecast_path is not None:
        kwargs["_forecast_path"] = forecast_path
    try:
        return record_candidate_forecast(context["fixture"], context["intent_raw"], **kwargs)
    except GuardianValidationError as exc:
        raise PaperApplyError(f"Guarded forecast rejected: {exc}") from None


def _record_placement(
    context: dict[str, Any],
    *,
    forecast_path: pathlib.Path | None,
    ledger_path: pathlib.Path | None,
    now: dt.datetime | None,
) -> dict[str, Any]:
    kwargs: dict[str, Any] = {}
    if forecast_path is not None:
        kwargs["_forecast_path"] = forecast_path
    if ledger_path is not None:
        kwargs["_ledger_path"] = ledger_path
    if now is not None:
        kwargs["_now"] = now
    try:
        return record_candidate_paper_placement(context["fixture"], context["intent_raw"], **kwargs)
    except GuardianValidationError as exc:
        raise PaperApplyError(f"Guarded PAPER placement rejected: {exc}") from None


def _result(
    context: dict[str, Any],
    *,
    receipt: dict[str, Any],
    forecast_status: str,
    placement_status: str,
) -> dict[str, Any]:
    result = _summary(context)
    result.update(
        {
            "forecast_id": receipt["forecast_id"],
            "forecast_status": forecast_status,
            "placement_id": receipt["placement_id"],
            "placement_status": placement_status,
            "application_state": receipt["state"],
        }
    )
    return result


def _terminal_result(context: dict[str, Any], receipt: dict[str, Any]) -> dict[str, Any]:
    placement_status = "not-attempted"
    if receipt["state"] == "placement-rejected":
        placement_status = "rejected"
    return _result(
        context,
        receipt=receipt,
        forecast_status="rejected" if receipt["state"] == "forecast-rejected" else "terminal",
        placement_status=placement_status,
    )


def run(
    fixture_path: str,
    intent_id: str,
    *,
    dry_run: bool = False,
    _staging_root: pathlib.Path | None = None,
    _forecast_path: pathlib.Path | None = None,
    _ledger_path: pathlib.Path | None = None,
    _now: Callable[[], dt.datetime] = lambda: dt.datetime.now(dt.timezone.utc),
    _placement_now: dt.datetime | None = None,
) -> dict[str, Any]:
    """Apply one fixed staged intent through existing guarded PAPER functions.

    Underscore arguments are test seams only. The production CLI exposes only
    ``--fixture``, ``--intent-id``, and ``--dry-run``.
    """
    staging_root = _resolve_staging_root() if _staging_root is None else pathlib.Path(_staging_root)
    context = _load_and_validate_staging(fixture_path, intent_id, staging_root)
    if dry_run:
        return _summary(context)

    receipt_path = context["receipt_path"]
    forecast_path = forecast_core.FORECASTS if _forecast_path is None else pathlib.Path(_forecast_path)
    ledger_path = ledger_core.LEDGER if _ledger_path is None else pathlib.Path(_ledger_path)
    receipt = _read_receipt(receipt_path, context)
    if receipt is None:
        receipt = _new_receipt(context, _now)
        _atomic_write_receipt(receipt_path, receipt)

    if receipt["state"] in _TERMINAL_FAILURE_STATES:
        return _terminal_result(context, receipt)

    forecast = _recover_forecast(context, forecast_path)
    disposition = context["intent"]["forecast_disposition"]

    if receipt["state"] in _COMPLETED_STATES:
        if forecast is None:
            raise PaperApplyError("Completed application receipt has no compatible forecast")
        placement = _recover_placement(context, forecast, ledger_path)
        if receipt["state"] == "completed-no-placement":
            if placement is not None:
                raise PaperApplyError("completed-no-placement receipt has an unexpected PAPER ledger row")
            if receipt["forecast_id"] != forecast["id"]:
                raise PaperApplyError("completed-no-placement receipt forecast_id does not match journal")
            return _result(
                context, receipt=receipt, forecast_status="already-completed", placement_status="not-eligible-disposition"
            )
        if placement is None:
            raise PaperApplyError("completed-placement receipt has no compatible PAPER ledger row")
        if receipt["forecast_id"] != forecast["id"] or receipt["placement_id"] != placement["id"]:
            raise PaperApplyError("completed-placement receipt does not match journal provenance")
        return _result(
            context, receipt=receipt, forecast_status="already-completed", placement_status="already-completed"
        )

    if receipt["state"] == "forecast-pending" and forecast is None:
        raise PaperApplyError("Forecast attempt outcome is unknown; operator reconciliation required")
    if receipt["state"] == "placement-pending":
        if forecast is None:
            raise PaperApplyError("Placement attempt has no compatible forecast; operator reconciliation required")
        placement = _recover_placement(context, forecast, ledger_path)
        if placement is None:
            raise PaperApplyError("Placement attempt outcome is unknown; operator reconciliation required")
        receipt = _transition_receipt(
            receipt_path, receipt, "completed-placement", now=_now,
            forecast_id=forecast["id"], placement_id=placement["id"],
        )
        return _result(context, receipt=receipt, forecast_status="recovered", placement_status="recovered")

    if forecast is None:
        if receipt["state"] == "forecast-recorded":
            raise PaperApplyError("forecast-recorded receipt has no compatible forecast")
        if disposition == "bet" and _has_ledger_intent_provenance(context, ledger_path):
            raise PaperApplyError("PAPER ledger row exists without a compatible forecast")
        if receipt["state"] not in {"prepared", "forecast-recorded"}:
            raise PaperApplyError("Application receipt state cannot begin forecast recording")
        receipt = _transition_receipt(receipt_path, receipt, "forecast-pending", now=_now)
        try:
            recorded = _record_forecast(context, _forecast_path)
        except PaperApplyError:
            try:
                _transition_receipt(receipt_path, receipt, "forecast-rejected", now=_now)
            except PaperApplyError:
                pass
            raise
        forecast_id = recorded.get("forecast", {}).get("recorded")
        if not isinstance(forecast_id, str) or not forecast_id:
            raise PaperApplyError("Guarded forecast returned an invalid forecast id")
        try:
            receipt = _transition_receipt(
                receipt_path, receipt, "forecast-recorded", now=_now, forecast_id=forecast_id
            )
        except PaperApplyError:
            # The durable source_intent_id row is authoritative. On rerun the
            # pending state is reconciled before any new forecast attempt.
            raise PaperApplyError("Forecast recorded but application receipt update failed") from None
        forecast = _recover_forecast(context, forecast_path)
        if forecast is None or forecast["id"] != forecast_id:
            raise PaperApplyError("Recorded forecast cannot be reconciled from authoritative journal")
        forecast_status = "recorded"
    else:
        if receipt["forecast_id"] not in (None, forecast["id"]):
            raise PaperApplyError("Application receipt forecast_id does not match journal")
        if receipt["state"] in {"prepared", "forecast-pending"}:
            receipt = _transition_receipt(
                receipt_path, receipt, "forecast-recorded", now=_now, forecast_id=forecast["id"]
            )
        forecast_status = "recovered"

    if disposition != "bet":
        if receipt["state"] != "forecast-recorded":
            raise PaperApplyError("Non-bet application receipt is not ready for completion")
        receipt = _transition_receipt(
            receipt_path, receipt, "completed-no-placement", now=_now, forecast_id=forecast["id"]
        )
        return _result(
            context, receipt=receipt, forecast_status=forecast_status, placement_status="not-eligible-disposition"
        )

    placement = _recover_placement(context, forecast, ledger_path)
    if placement is not None:
        receipt = _transition_receipt(
            receipt_path, receipt, "completed-placement", now=_now,
            forecast_id=forecast["id"], placement_id=placement["id"],
        )
        return _result(context, receipt=receipt, forecast_status=forecast_status, placement_status="recovered")

    if receipt["state"] != "forecast-recorded":
        raise PaperApplyError("Bet application receipt is not ready for guarded placement")
    receipt = _transition_receipt(receipt_path, receipt, "placement-pending", now=_now, forecast_id=forecast["id"])
    try:
        placed = _record_placement(
            context,
            forecast_path=_forecast_path,
            ledger_path=_ledger_path,
            now=_placement_now,
        )
    except PaperApplyError:
        try:
            rejected = _transition_receipt(
                receipt_path, receipt, "placement-rejected", now=_now, forecast_id=forecast["id"]
            )
        except PaperApplyError:
            raise PaperApplyError("Guarded PAPER placement rejected; receipt state requires reconciliation") from None
        return _result(context, receipt=rejected, forecast_status=forecast_status, placement_status="rejected")
    placement_id = placed.get("placement", {}).get("placed")
    if not isinstance(placement_id, str) or not placement_id:
        raise PaperApplyError("Guarded PAPER placement returned an invalid placement id")
    try:
        receipt = _transition_receipt(
            receipt_path, receipt, "completed-placement", now=_now,
            forecast_id=forecast["id"], placement_id=placement_id,
        )
    except PaperApplyError:
        raise PaperApplyError("PAPER placement recorded but application receipt update failed") from None
    return _result(context, receipt=receipt, forecast_status=forecast_status, placement_status="placed")


def _reject_forbidden_options(arguments: list[str], parser: argparse.ArgumentParser) -> None:
    allowed = {"fixture", "intent-id", "dry-run"}
    for argument in arguments:
        if not argument.startswith("--"):
            continue
        option = argument[2:].split("=", 1)[0].lower().replace("_", "-")
        if option in allowed:
            continue
        if option in _FORBIDDEN_OPTION_TERMS or any(term in option.split("-") for term in _FORBIDDEN_OPTION_TERMS):
            parser.error("Forbidden PAPER application option")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Manually apply one fixed validated Manus PAPER intent",
        allow_abbrev=False,
    )
    parser.add_argument("--fixture", required=True, help="operator-controlled trusted JSON fixture")
    parser.add_argument("--intent-id", required=True, help="canonical lowercase UUIDv4 from fixed validated staging")
    parser.add_argument("--dry-run", action="store_true", help="validate fixed staging and print only the application plan")
    return parser


def main(argv: list[str] | None = None) -> int:
    import sys

    arguments = list(sys.argv[1:] if argv is None else argv)
    parser = build_parser()
    _reject_forbidden_options(arguments, parser)
    args = parser.parse_args(arguments)
    try:
        result = run(args.fixture, args.intent_id, dry_run=args.dry_run)
    except PaperApplyError as exc:
        parser.exit(2, f"REJECTED: {exc}\n")
    print(_canonical_json(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

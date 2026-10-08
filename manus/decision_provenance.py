"""Typed, append-only decision provenance for the guarded Manus PAPER route.
This module is observability infrastructure only. It never places, mutates a
journal, mutates operational cycle state, contacts a broker, reads a
credential, or performs network or paid calls. The only write surface is one
append-only JSONL provenance log under the fixed external ``phil-manus``
runtime family, guarded by a fixed OS writer lock. Provenance facts are
recorded only when actually known; unknown identity stays ``null`` and is
never inferred or guessed.

Record identity and integrity:

- ``decision_id`` is deterministic: SHA-256 over the canonical decision
  identity (schema version, execution mode, stage, phase, action, cycle and
  intent identity, and the frozen-input hash). Identical frozen inputs and
  identity therefore always derive the same decision id.
- Appending the byte-identical canonical record for an existing
  ``decision_id`` is an idempotent no-op.
- Appending different canonical content under an existing ``decision_id``
  fails closed with a bounded integrity error and never edits or replaces the
  existing record.

Importing this module has no side effects: no directories, files, threads,
network, credentials, or locks are touched at import time.
"""
from __future__ import annotations

import hashlib
import json
import os
import pathlib
import tempfile
from typing import Any, Callable

from manus import paper_locks
from manus.paper_cycle_guardian import _canonical_json

DECISION_PROVENANCE_SCHEMA_VERSION = "decision-provenance/v1"
FROZEN_INPUT_SCHEMA_VERSION = "decision-frozen-input/v1"

PROVENANCE_CHILDREN = ("phil-manus", "provenance")
PROVENANCE_FILENAME = "decision_provenance.jsonl"

# Bounded decision vocabulary. Existing repository reason codes (safe_reason
# values, rejection codes, and research dispositions) are retained verbatim
# in ``reason_code``; no existing code is renamed or remapped.
DECISION_ACTIONS = frozenset({"trade", "no-trade", "rejected"})
DECISION_STAGES = frozenset(
    {"candidate", "research", "forecast", "decision-policy", "placement"}
)
DECISION_PHASES = frozenset({"attempt", "outcome", "final"})
EXECUTION_MODES = frozenset({"PAPER", "SHADOW", "REPLAY"})
PLACEMENT_RESULTS = frozenset({"pending", "placed", "rejected", "error", None})

_IDENTIFIER_CHARS = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._:-"
)
_SHA256_LENGTH = 64


class ProvenanceWriteError(RuntimeError):
    """Raised when append-only decision provenance cannot be trusted."""


def _require_identifier(value: Any, label: str, *, nullable: bool = False) -> str | None:
    if value is None and nullable:
        return None
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 128
        or not set(value) <= _IDENTIFIER_CHARS
    ):
        raise ProvenanceWriteError(f"Decision provenance {label} is invalid")
    return value


def _require_uuid4(value: Any, label: str, *, nullable: bool = False) -> str | None:
    import uuid

    if value is None and nullable:
        return None
    if not isinstance(value, str):
        raise ProvenanceWriteError(f"Decision provenance {label} is invalid")
    try:
        parsed = uuid.UUID(value)
    except (ValueError, AttributeError) as exc:
        raise ProvenanceWriteError(f"Decision provenance {label} is invalid") from exc
    if parsed.version != 4 or str(parsed) != value:
        raise ProvenanceWriteError(f"Decision provenance {label} is invalid")
    return value


def _require_sha256(value: Any, label: str, *, nullable: bool = False) -> str | None:
    if value is None and nullable:
        return None
    if not isinstance(value, str) or len(value) != _SHA256_LENGTH:
        raise ProvenanceWriteError(f"Decision provenance {label} is invalid")
    try:
        int(value, 16)
    except ValueError as exc:
        raise ProvenanceWriteError(f"Decision provenance {label} is invalid") from exc
    return value


def _require_utc_timestamp(value: Any, label: str, *, nullable: bool = False) -> str | None:
    if value is None and nullable:
        return None
    if not isinstance(value, str) or not value.endswith("Z") or len(value) > 32:
        raise ProvenanceWriteError(f"Decision provenance {label} is invalid")
    return value


def _require_vocabulary(value: Any, vocabulary: frozenset[str], label: str) -> str:
    if not isinstance(value, str) or value not in vocabulary:
        raise ProvenanceWriteError(f"Decision provenance {label} is invalid")
    return value


def _require_reason_code(value: Any, *, nullable: bool = False) -> str | None:
    """Validate one bounded reason code from the existing repository vocabularies."""
    if value is None and nullable:
        return None
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 64
        or not set(value) <= _IDENTIFIER_CHARS
    ):
        raise ProvenanceWriteError("Decision provenance reason_code is invalid")
    return value


# ---------------------------------------------------------------------------
# Frozen decision input
# ---------------------------------------------------------------------------

FROZEN_INPUT_FIELDS = frozenset(
    {
        "schema_version",
        "cycle_id",
        "candidate_id",
        "market_id",
        "event_id",
        "outcome",
        "end_date_utc",
        "best_bid",
        "best_ask",
        "midpoint_probability",
        "liquidity",
        "volume_24h",
        "category",
        "estimated_probability",
        "forecast_disposition",
        "edge_class",
        "rationale",
        "policy",
        "open_positions",
        "eligible_candidate_count",
        "researchable",
        "decision_utc",
    }
)

_POLICY_KEYS = frozenset(
    {
        "stake_usd",
        "max_stake_usd",
        "sim_bankroll_usd",
        "max_open_positions",
        "max_new_positions_per_cycle",
        "max_positions_per_category_per_cycle",
        "min_minutes_to_resolution",
        "min_entry_price",
        "max_entry_price",
        "required_edge",
        "max_spread",
        "max_stake_per_event_usd",
    }
)

_OPEN_POSITION_KEYS = frozenset(
    {
        "market_id",
        "outcome",
        "event_id",
        "stake_usd",
        "shares",
        "status",
    }
)


def _frozen_number(value: Any, label: str) -> float | int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ProvenanceWriteError(f"Frozen decision input {label} is invalid")
    return value


def _frozen_text(value: Any, label: str, maximum: int) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or len(value) > maximum:
        raise ProvenanceWriteError(f"Frozen decision input {label} is invalid")
    return value


def frozen_input(
    *,
    cycle_id: str | None = None,
    candidate_id: str | None = None,
    market_id: str | None = None,
    event_id: str | None = None,
    outcome: str | None = None,
    end_date_utc: str | None = None,
    best_bid: float | None = None,
    best_ask: float | None = None,
    midpoint_probability: float | None = None,
    liquidity: float | None = None,
    volume_24h: float | None = None,
    category: str | None = None,
    estimated_probability: float | None = None,
    forecast_disposition: str | None = None,
    edge_class: str | None = None,
    rationale: str | None = None,
    policy: dict[str, Any] | None = None,
    open_positions: list[dict[str, Any]] | None = None,
    eligible_candidate_count: int | None = None,
    researchable: bool | None = None,
    decision_utc: str | None = None,
) -> dict[str, Any]:
    """Build one bounded, canonical, deterministic frozen decision input.

    Every field of the frozen-input schema is always present (``null`` when
    not applicable), so the canonical serialization and its SHA-256 are
    stable. Timestamps appear only where the decision logic itself consumed
    the clock (``decision_utc``).
    """
    document = {
        "schema_version": FROZEN_INPUT_SCHEMA_VERSION,
        # cycle_id follows the existing cycle owner: the fixture packet's
        # deterministic packet_id (a bounded identifier, not a UUID).
        "cycle_id": _require_identifier(cycle_id, "cycle_id", nullable=True),
        "candidate_id": _require_identifier(candidate_id, "candidate_id", nullable=True),
        "market_id": _require_identifier(market_id, "market_id", nullable=True),
        "event_id": _require_identifier(event_id, "event_id", nullable=True),
        "outcome": _frozen_text(outcome, "outcome", 128),
        "end_date_utc": _require_utc_timestamp(end_date_utc, "end_date_utc", nullable=True),
        "best_bid": _frozen_number(best_bid, "best_bid"),
        "best_ask": _frozen_number(best_ask, "best_ask"),
        "midpoint_probability": _frozen_probability(midpoint_probability, "midpoint_probability"),
        "liquidity": _frozen_number(liquidity, "liquidity"),
        "volume_24h": _frozen_number(volume_24h, "volume_24h"),
        "category": _require_identifier(category, "category", nullable=True),
        "estimated_probability": _frozen_probability(estimated_probability, "estimated_probability"),
        "forecast_disposition": _frozen_text(forecast_disposition, "forecast_disposition", 64),
        "edge_class": _frozen_text(edge_class, "edge_class", 64),
        "rationale": _frozen_text(rationale, "rationale", 2000),
        "policy": _frozen_policy(policy),
        "open_positions": _frozen_open_positions(open_positions),
        "eligible_candidate_count": _frozen_integer(eligible_candidate_count, "eligible_candidate_count"),
        "researchable": researchable if isinstance(researchable, bool) or researchable is None else _invalid("researchable"),
        "decision_utc": _require_utc_timestamp(decision_utc, "decision_utc", nullable=True),
    }
    if set(document) != FROZEN_INPUT_FIELDS:  # defensive schema integrity
        raise ProvenanceWriteError("Frozen decision input schema is invalid")
    return document


def _invalid(label: str):
    raise ProvenanceWriteError(f"Frozen decision input {label} is invalid")


def _frozen_probability(value: Any, label: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not 0.0 < value < 1.0:
        raise ProvenanceWriteError(f"Frozen decision input {label} is invalid")
    return value


def _frozen_integer(value: Any, label: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ProvenanceWriteError(f"Frozen decision input {label} is invalid")
    return value


def _frozen_policy(policy: dict[str, Any] | None) -> dict[str, Any] | None:
    if policy is None:
        return None
    if not isinstance(policy, dict) or not set(policy) <= _POLICY_KEYS:
        raise ProvenanceWriteError("Frozen decision input policy is invalid")
    bounded: dict[str, Any] = {}
    for key in _POLICY_KEYS & set(policy):
        value = policy[key]
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ProvenanceWriteError(f"Frozen decision input policy {key} is invalid")
        bounded[key] = value
    return bounded or None


def _frozen_open_positions(positions: list[dict[str, Any]] | None) -> list[dict[str, Any]] | None:
    if positions is None:
        return None
    if not isinstance(positions, list) or len(positions) > 1000:
        raise ProvenanceWriteError("Frozen decision input open_positions is invalid")
    bounded: list[dict[str, Any]] = []
    for row in positions:
        if not isinstance(row, dict) or not set(row) <= _OPEN_POSITION_KEYS:
            raise ProvenanceWriteError("Frozen decision input open_positions row is invalid")
        bounded.append(
            {
                "market_id": _require_identifier(row.get("market_id"), "open position market_id", nullable=True),
                "outcome": _frozen_text(row.get("outcome"), "open position outcome", 128),
                "event_id": _require_identifier(row.get("event_id"), "open position event_id", nullable=True),
                "stake_usd": _frozen_number(row.get("stake_usd"), "open position stake_usd"),
                "shares": _frozen_number(row.get("shares"), "open position shares"),
                "status": _frozen_text(row.get("status"), "open position status", 16),
            }
        )
    return bounded


def frozen_input_sha256(frozen: dict[str, Any]) -> str:
    """Deterministic SHA-256 over the canonical frozen decision input."""
    if not isinstance(frozen, dict) or set(frozen) != FROZEN_INPUT_FIELDS:
        raise ProvenanceWriteError("Frozen decision input schema is invalid")
    return hashlib.sha256(_canonical_json(frozen).encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Decision records
# ---------------------------------------------------------------------------

_DECISION_RECORD_FIELDS = frozenset(
    {
        "schema_version",
        "decision_id",
        "recorded_at_utc",
        "execution_mode",
        "code_revision",
        "cycle_id",
        "candidate_id",
        "market_id",
        "event_id",
        "source_intent_id",
        "research_task_id",
        "forecast_id",
        "research_provider",
        "research_model_id",
        "research_profile",
        "prompt_version",
        "prompt_sha256",
        "response_schema_version",
        "frozen_input",
        "frozen_input_sha256",
        "decision_action",
        "decision_stage",
        "decision_phase",
        "reason_code",
        "disposition",
        "estimated_probability",
        "market_probability",
        "edge",
        "requested_notional",
        "placement_attempted",
        "placement_result",
        "placement_rejection_code",
        "record_sha256",
    }
)


def decision_identity_sha256(
    *,
    execution_mode: str,
    decision_stage: str,
    decision_phase: str,
    decision_action: str,
    cycle_id: str | None,
    source_intent_id: str | None,
    candidate_id: str | None,
    market_id: str | None,
    input_sha256: str,
) -> str:
    """Deterministic decision id: SHA-256 over the canonical decision identity.

    The identity includes the frozen-input hash, so identical frozen inputs
    under identical identity derive the same decision id, while any mutated
    decision-relevant input derives a different id (both records are then
    retained by the append-only log).
    """
    identity = {
        "schema_version": DECISION_PROVENANCE_SCHEMA_VERSION,
        "execution_mode": _require_vocabulary(execution_mode, EXECUTION_MODES, "execution_mode"),
        "decision_stage": _require_vocabulary(decision_stage, DECISION_STAGES, "decision_stage"),
        "decision_phase": _require_vocabulary(decision_phase, DECISION_PHASES, "decision_phase"),
        "decision_action": _require_vocabulary(decision_action, DECISION_ACTIONS, "decision_action"),
        # cycle_id follows the existing cycle owner: the fixture packet's
        # deterministic packet_id (a bounded identifier, not a UUID).
        "cycle_id": _require_identifier(cycle_id, "cycle_id", nullable=True),
        "source_intent_id": _require_uuid4(source_intent_id, "source_intent_id", nullable=True),
        "candidate_id": _require_identifier(candidate_id, "candidate_id", nullable=True),
        "market_id": _require_identifier(market_id, "market_id", nullable=True),
        "frozen_input_sha256": _require_sha256(input_sha256, "frozen_input_sha256"),
    }
    return hashlib.sha256(_canonical_json(identity).encode("utf-8")).hexdigest()


def build_decision_record(
    *,
    execution_mode: str,
    recorded_at_utc: str,
    code_revision: str | None,
    cycle_id: str | None,
    candidate_id: str | None,
    market_id: str | None,
    event_id: str | None,
    source_intent_id: str | None,
    research_task_id: str | None,
    forecast_id: str | None,
    research_provider: str | None,
    research_model_id: str | None,
    research_profile: str | None,
    prompt_version: str | None,
    prompt_sha256: str | None,
    response_schema_version: str | None,
    frozen: dict[str, Any],
    decision_action: str,
    decision_stage: str,
    decision_phase: str,
    reason_code: str | None,
    disposition: str | None,
    estimated_probability: float | None,
    market_probability: float | None,
    edge: float | None,
    requested_notional: float | None,
    placement_attempted: bool,
    placement_result: str | None,
    placement_rejection_code: str | None,
) -> dict[str, Any]:
    """Build one validated, canonical decision-provenance record.

    Unknown provenance stays ``null``: this function never infers or guesses
    an identity that the existing pipeline did not supply.
    """
    input_sha256 = frozen_input_sha256(frozen)
    decision_id = decision_identity_sha256(
        execution_mode=execution_mode,
        decision_stage=decision_stage,
        decision_phase=decision_phase,
        decision_action=decision_action,
        cycle_id=cycle_id,
        source_intent_id=source_intent_id,
        candidate_id=candidate_id,
        market_id=market_id,
        input_sha256=input_sha256,
    )
    record: dict[str, Any] = {
        "schema_version": DECISION_PROVENANCE_SCHEMA_VERSION,
        "decision_id": decision_id,
        "recorded_at_utc": _require_utc_timestamp(recorded_at_utc, "recorded_at_utc"),
        "execution_mode": _require_vocabulary(execution_mode, EXECUTION_MODES, "execution_mode"),
        "code_revision": _frozen_text(code_revision, "code_revision", 128),
        # cycle_id follows the existing cycle owner: the fixture packet's
        # deterministic packet_id (a bounded identifier, not a UUID).
        "cycle_id": _require_identifier(cycle_id, "cycle_id", nullable=True),
        "candidate_id": _require_identifier(candidate_id, "candidate_id", nullable=True),
        "market_id": _require_identifier(market_id, "market_id", nullable=True),
        "event_id": _require_identifier(event_id, "event_id", nullable=True),
        "source_intent_id": _require_uuid4(source_intent_id, "source_intent_id", nullable=True),
        "research_task_id": _require_identifier(research_task_id, "research_task_id", nullable=True),
        "forecast_id": _require_identifier(forecast_id, "forecast_id", nullable=True),
        "research_provider": _frozen_text(research_provider, "research_provider", 64),
        "research_model_id": _frozen_text(research_model_id, "research_model_id", 128),
        "research_profile": _frozen_text(research_profile, "research_profile", 128),
        "prompt_version": _frozen_text(prompt_version, "prompt_version", 128),
        "prompt_sha256": _require_sha256(prompt_sha256, "prompt_sha256", nullable=True),
        "response_schema_version": _frozen_text(response_schema_version, "response_schema_version", 128),
        "frozen_input": frozen,
        "frozen_input_sha256": input_sha256,
        "decision_action": _require_vocabulary(decision_action, DECISION_ACTIONS, "decision_action"),
        "decision_stage": _require_vocabulary(decision_stage, DECISION_STAGES, "decision_stage"),
        "decision_phase": _require_vocabulary(decision_phase, DECISION_PHASES, "decision_phase"),
        "reason_code": _require_reason_code(reason_code, nullable=True),
        "disposition": _frozen_text(disposition, "disposition", 64),
        "estimated_probability": _frozen_probability(estimated_probability, "estimated_probability"),
        "market_probability": _frozen_probability(market_probability, "market_probability"),
        "edge": _frozen_number(edge, "edge"),
        "requested_notional": _frozen_number(requested_notional, "requested_notional"),
        "placement_attempted": placement_attempted,
        "placement_result": _placement_result(placement_result),
        "placement_rejection_code": _require_reason_code(placement_rejection_code, nullable=True),
    }
    if not isinstance(placement_attempted, bool):
        raise ProvenanceWriteError("Decision provenance placement_attempted is invalid")
    record["record_sha256"] = _record_sha256(record)
    if set(record) != _DECISION_RECORD_FIELDS:  # defensive schema integrity
        raise ProvenanceWriteError("Decision provenance schema is invalid")
    return record


def _placement_result(value: str | None) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str) or value not in PLACEMENT_RESULTS:
        raise ProvenanceWriteError("Decision provenance placement_result is invalid")
    return value


def _record_sha256(record: dict[str, Any]) -> str:
    body = {key: value for key, value in record.items() if key != "record_sha256"}
    return hashlib.sha256(_canonical_json(body).encode("utf-8")).hexdigest()


def verify_record_sha256(record: dict[str, Any]) -> bool:
    """Independently verify one decision record's integrity hash."""
    if not isinstance(record, dict) or "record_sha256" not in record:
        return False
    return _record_sha256(record) == record["record_sha256"]


# ---------------------------------------------------------------------------
# Append-only storage
# ---------------------------------------------------------------------------

def resolve_provenance_root(_provenance_root: pathlib.Path | None = None) -> pathlib.Path:
    """Resolve the fixed external provenance root without creating it.

    Production is fixed at ``%LOCALAPPDATA%\\phil-manus\\provenance`` beside
    the existing runner and staging roots; the private argument is a test
    seam only.
    """
    if _provenance_root is not None:
        root = pathlib.Path(_provenance_root)
    else:
        local_appdata = os.environ.get("LOCALAPPDATA")
        if not local_appdata:
            raise ProvenanceWriteError("Fixed decision provenance root is unavailable")
        root = pathlib.Path(local_appdata).joinpath(*PROVENANCE_CHILDREN)
    for directory in (root.parent, root):
        if paper_locks.path_is_unsafe_indirection(directory):
            raise ProvenanceWriteError("Fixed decision provenance root is unavailable")
    try:
        root.resolve(strict=False).relative_to(_repository_root())
    except ValueError:
        return root
    raise ProvenanceWriteError("Fixed decision provenance root must be outside the repository")


def _repository_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parents[1]


def _provenance_path(root: pathlib.Path) -> pathlib.Path:
    return root / PROVENANCE_FILENAME


def read_decision_records(
    *,
    _provenance_root: pathlib.Path | None = None,
    decision_id: str | None = None,
    cycle_id: str | None = None,
) -> list[dict[str, Any]]:
    """Read-only provenance access; never writes, creates, or repairs."""
    root = resolve_provenance_root(_provenance_root)
    path = _provenance_path(root)
    if not path.exists():
        return []
    if path.is_symlink() or not path.is_file():
        raise ProvenanceWriteError("Decision provenance log is unavailable")
    try:
        text = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as exc:
        raise ProvenanceWriteError("Decision provenance log is unavailable") from exc
    records: list[dict[str, Any]] = []
    for line in text.splitlines():
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ProvenanceWriteError("Decision provenance log is malformed") from exc
        if not isinstance(record, dict) or set(record) != _DECISION_RECORD_FIELDS:
            raise ProvenanceWriteError("Decision provenance record schema is invalid")
        if not verify_record_sha256(record):
            raise ProvenanceWriteError("Decision provenance record integrity failed")
        records.append(record)
    if decision_id is not None:
        records = [record for record in records if record["decision_id"] == decision_id]
    if cycle_id is not None:
        records = [record for record in records if record["cycle_id"] == cycle_id]
    return records


def append_decision_record(
    record: dict[str, Any],
    *,
    now: Callable[[], Any] | None = None,
    _provenance_root: pathlib.Path | None = None,
    _lock_root: pathlib.Path | None = None,
) -> str:
    """Append one provenance record with idempotent, integrity-checked semantics.

    Returns ``"appended"`` or ``"idempotent"``. Raises a bounded
    :class:`ProvenanceWriteError` on malformed input, an integrity conflict,
    or any storage failure; existing records are never edited or replaced
    and no truncate/rewrite behavior exists.
    """
    del now  # ``recorded_at_utc`` is fixed at record-build time for determinism.
    if not isinstance(record, dict) or set(record) != _DECISION_RECORD_FIELDS:
        raise ProvenanceWriteError("Decision provenance record schema is invalid")
    if not verify_record_sha256(record):
        raise ProvenanceWriteError("Decision provenance record integrity failed")
    canonical = _canonical_json(record)
    root = resolve_provenance_root(_provenance_root)
    path = _provenance_path(root)
    lock_root = (
        pathlib.Path(_lock_root)
        if _lock_root is not None
        else root.parent / "locks"
    )
    try:
        with paper_locks.acquire_provenance_writer_lock(
            nonblocking=True, _lock_root=lock_root
        ):
            existing = read_decision_records(_provenance_root=_provenance_root)
            matches = [
                prior
                for prior in existing
                if prior["decision_id"] == record["decision_id"]
            ]
            if len(matches) > 1:
                raise ProvenanceWriteError(
                    "Decision provenance integrity conflict: duplicate decision_id"
                )
            if matches:
                if _canonical_json(matches[0]) == canonical:
                    return "idempotent"
                raise ProvenanceWriteError(
                    "Decision provenance integrity conflict for one decision_id"
                )
            _atomic_append(path, canonical)
    except paper_locks.LockUnavailableError as exc:
        raise ProvenanceWriteError("Decision provenance writer lock is unavailable") from None
    except paper_locks.LockError as exc:
        raise ProvenanceWriteError("Decision provenance writer lock is unavailable") from None
    return "appended"


def _atomic_append(path: pathlib.Path, canonical_line: str) -> None:
    """Append one canonical line; an ordinary failure leaves prior bytes intact."""
    directory = path.parent
    try:
        directory.mkdir(parents=True, exist_ok=True)
        if not directory.is_dir() or paper_locks.path_is_unsafe_indirection(directory):
            raise OSError("unsafe provenance directory")
    except OSError as exc:
        raise ProvenanceWriteError("Decision provenance storage is unavailable") from exc
    try:
        original = path.read_bytes() if path.exists() else b""
    except OSError as exc:
        raise ProvenanceWriteError("Decision provenance storage is unavailable") from exc
    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=directory,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary_book:
            temporary_path = pathlib.Path(temporary_book.name)
            temporary_book.write(original)
            if original and not original.endswith(b"\n"):
                temporary_book.write(b"\n")
            temporary_book.write((canonical_line + "\n").encode("utf-8"))
            temporary_book.flush()
            os.fsync(temporary_book.fileno())
        os.replace(temporary_path, path)
        temporary_path = None
    except Exception as exc:
        if temporary_path is not None:
            try:
                temporary_path.unlink(missing_ok=True)
            except OSError:
                pass
        raise ProvenanceWriteError("Decision provenance write failed") from exc


# ---------------------------------------------------------------------------
# Derived read-only counters
# ---------------------------------------------------------------------------

def decision_counts(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Derived, read-only decision counters from canonical provenance records.

    Only represented decisions are counted. Pending attempt records are
    audit-only and excluded from action counts. Repeated records for the same
    logical decision identity (the same stage/action on the same candidate,
    market, or intent across recovery reruns or later cycles) are counted
    once; the record log itself remains append-only.
    """
    counts = {"trade": 0, "no_trade": 0, "rejected": 0}
    by_stage: dict[str, int] = {}
    by_reason: dict[str, int] = {}
    seen: set[tuple[str, str, str | None, str | None, str | None]] = set()
    for record in records:
        if not isinstance(record, dict) or set(record) != _DECISION_RECORD_FIELDS:
            raise ProvenanceWriteError("Decision provenance record schema is invalid")
        stage = record["decision_stage"]
        action = record["decision_action"]
        if record["decision_phase"] == "attempt" and record["placement_result"] == "pending":
            continue  # audit-only pending attempt: not a represented outcome
        identity = (
            stage,
            action,
            record["source_intent_id"],
            record["candidate_id"],
            record["market_id"],
        )
        by_stage[stage] = by_stage.get(stage, 0) + 1
        reason = record["reason_code"]
        if reason is not None:
            by_reason[reason] = by_reason.get(reason, 0) + 1
        if identity in seen:
            continue
        seen.add(identity)
        key = {"trade": "trade", "no-trade": "no_trade", "rejected": "rejected"}[action]
        counts[key] += 1
    return {
        "decision_counts": counts,
        "by_stage": dict(sorted(by_stage.items())),
        "by_reason": dict(sorted(by_reason.items())),
    }

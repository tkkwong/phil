"""Append-only IBKR PAPER execution receipt store (Patch 5F-3a).

A dedicated operational truth source for Phil's manually armed PAPER
order submissions, stored OUTSIDE the repository under the fixed external
``phil-manus`` runtime family (mirrors the accepted 5E-6 provenance
pattern):

    production root  %LOCALAPPDATA%\\phil-manus\\ibkr-paper-execution
    production file  execution_receipts.jsonl

Properties (all fail-closed):

- append-only: records are never truncated, rewritten, or replaced;
- idempotent: the same ``event_id`` with an identical canonical record is
  a no-op; the same ``event_id`` with conflicting content is a bounded
  integrity error;
- single writer: a fixed cross-process OS writer lock (the accepted
  ``manus.paper_locks`` implementation) serializes appends;
- integrity: every record carries a deterministic ``record_sha256`` over
  its canonical (timestamp-free) content, verified on read and on append;
- private: raw account ids, credentials, config paths, and broker error
  text are structurally rejected before any write.

Importing this module performs no work: no directory is created, no file
is opened, no lock is touched, and no clock is read.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import pathlib
import re
import tempfile
from typing import Any

from manus import paper_locks

STATE_SCHEMA_VERSION = "ibkr-paper-execution-receipts/v1"
RECEIPT_CHILDREN = ("phil-manus", "ibkr-paper-execution")
RECEIPT_FILENAME = "execution_receipts.jsonl"
WRITER_LOCK_NAME = "paper-execution-writer"

# Closed receipt event vocabulary (5F-3a).
EVENT_TYPES = frozenset(
    {
        "submission-attempted",
        "submitted",
        "acknowledged",
        "submission-rejected",
        "submission-uncertain",
    }
)

# Closed bounded reason-code vocabulary for receipts.
REASON_CODES = frozenset(
    {
        "paper-order-rejected",
        "paper-session-failed",
        "paper-account-mismatch",
        "paper-submission-timeout",
        "paper-submission-uncertain",
        "execution-already-submitted",
        "execution-uncertain",
        "receipt-write-failed",
        "arm-missing",
        "arm-confirmation-mismatch",
        "environment-not-paper",
        "account-mismatch",
        "mapping-unverified",
        "paper-short-not-supported",
        "unsupported-order-parameter",
        "notional-cap-exceeded",
        "intent-invalid",
        "duplicate-broker-evidence",
    }
)

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]*$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_ORDERREF_RE = re.compile(r"^phil5f3-[0-9a-f]{32}$")
_MAX_TEXT = 256


class ExecutionStateError(RuntimeError):
    """Raised when execution receipts cannot be trusted (fail closed)."""

    def __init__(self, code: str, message: str | None = None) -> None:
        self.code = code
        super().__init__(message or f"execution state failure: {code}")


def canonical_json(document: Any) -> str:
    """Deterministic JSON serialization (same convention as 5E-6/5F-2)."""
    return json.dumps(
        document, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    )


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _require_identifier(value: Any, label: str, *, nullable: bool = False) -> str | None:
    if value is None and nullable:
        return None
    if (
        not isinstance(value, str)
        or not value
        or len(value) > 128
        or not _IDENTIFIER_RE.fullmatch(value)
    ):
        raise ExecutionStateError("intent-invalid", f"execution receipt {label} is invalid")
    return value


def resolve_execution_root(
    _execution_root: pathlib.Path | None = None,
) -> pathlib.Path:
    """Return the one production execution root outside the repository."""
    if _execution_root is not None:
        root = pathlib.Path(_execution_root)
    else:
        local_appdata = os.environ.get("LOCALAPPDATA")
        if not local_appdata:
            raise ExecutionStateError("receipt-write-failed", "Fixed execution root is unavailable")
        root = pathlib.Path(local_appdata).joinpath(*RECEIPT_CHILDREN)
    try:
        root.resolve(strict=False).relative_to(_repository_root())
        raise ExecutionStateError("receipt-write-failed", "Fixed execution root must be outside the repository")
    except ValueError:
        return root


def _repository_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parents[1]


def _receipt_path(root: pathlib.Path) -> pathlib.Path:
    return root / RECEIPT_FILENAME


def _prepare_root(root: pathlib.Path) -> pathlib.Path:
    try:
        for directory in (root.parent, root):
            if paper_locks.path_is_unsafe_indirection(directory):
                raise OSError("unsafe execution root")
            if directory.exists() and not directory.is_dir():
                raise OSError("invalid execution root")
        root.mkdir(parents=True, exist_ok=True)
        if not root.is_dir() or paper_locks.path_is_unsafe_indirection(root):
            raise OSError("invalid execution root")
    except OSError:
        raise ExecutionStateError("receipt-write-failed", "Execution receipt storage is unavailable") from None
    return root


def _utc_timestamp(now: dt.datetime | None = None) -> str:
    moment = now or dt.datetime.now(dt.timezone.utc)
    return (
        moment.astimezone(dt.timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def record_sha256(record: dict[str, Any]) -> str:
    """Deterministic hash over the canonical record minus its own hash."""
    body = {key: value for key, value in record.items() if key != "record_sha256"}
    return _sha256(canonical_json(body))


def verify_record_sha256(record: dict[str, Any]) -> bool:
    if not isinstance(record, dict) or "record_sha256" not in record:
        return False
    return record_sha256(record) == record["record_sha256"]


def build_receipt(
    *,
    event_type: str,
    execution_id: str,
    intent_sha256: str,
    decision_id: str,
    mapping_id: str,
    mapping_sha256: str,
    source_binding_sha256: str,
    order_ref: str,
    account_id_masked: str,
    target: dict[str, Any],
    order: dict[str, Any],
    broker: dict[str, Any],
    reason_code: str | None = None,
    event_id: str | None = None,
    recorded_at_utc: str | None = None,
    now: dt.datetime | None = None,
) -> dict[str, Any]:
    """Build one bounded, hash-verified receipt record (no I/O)."""
    if event_type not in EVENT_TYPES:
        raise ExecutionStateError("intent-invalid", "execution receipt event type is invalid")
    if reason_code is not None and reason_code not in REASON_CODES:
        raise ExecutionStateError("intent-invalid", "execution receipt reason code is invalid")
    _require_identifier(execution_id, "execution_id")
    _require_identifier(decision_id, "decision_id")
    _require_identifier(mapping_id, "mapping_id")
    for name, value in (
        ("intent_sha256", intent_sha256),
        ("mapping_sha256", mapping_sha256),
        ("source_binding_sha256", source_binding_sha256),
    ):
        if not isinstance(value, str) or not _SHA256_RE.fullmatch(value):
            raise ExecutionStateError("intent-invalid", f"execution receipt {name} is invalid")
    if not isinstance(order_ref, str) or not _ORDERREF_RE.fullmatch(order_ref):
        raise ExecutionStateError("intent-invalid", "execution receipt order_ref is invalid")
    masked = account_id_masked
    # A raw account id must never be persisted: the only accepted
    # representation is the adapter's "***<suffix>" masked form.
    if not isinstance(masked, str) or not masked.startswith("***") or len(masked) > _MAX_TEXT:
        raise ExecutionStateError("intent-invalid", "execution receipt account mask is invalid")
    if not isinstance(target, dict) or set(target) != {"conid", "symbol", "sec_type", "currency", "exchange"}:
        raise ExecutionStateError("intent-invalid", "execution receipt target field set is closed")
    if not isinstance(order, dict) or set(order) != {
        "action", "quantity", "order_type", "limit_price", "tif", "outside_rth"
    }:
        raise ExecutionStateError("intent-invalid", "execution receipt order field set is closed")
    if not isinstance(broker, dict) or set(broker) != {"client_id", "order_id", "perm_id", "status"}:
        raise ExecutionStateError("intent-invalid", "execution receipt broker field set is closed")
    resolved_event_id = event_id or _sha256(canonical_json(
        {
            "event_type": event_type,
            "execution_id": execution_id,
            "intent_sha256": intent_sha256,
            "order_ref": order_ref,
            "broker": broker,
            "reason_code": reason_code,
        }
    ))[:32]
    record: dict[str, Any] = {
        "schema_version": STATE_SCHEMA_VERSION,
        "event_id": _require_identifier(resolved_event_id, "event_id"),
        "event_type": event_type,
        "execution_id": execution_id,
        "recorded_at_utc": recorded_at_utc or _utc_timestamp(now),
        "decision_id": decision_id,
        "mapping_id": mapping_id,
        "mapping_sha256": mapping_sha256,
        "source_binding_sha256": source_binding_sha256,
        "intent_sha256": intent_sha256,
        "order_ref": order_ref,
        "account_id_masked": masked,
        "target": dict(target),
        "order": dict(order),
        "broker": dict(broker),
        "reason_code": reason_code,
    }
    record["record_sha256"] = record_sha256(record)
    return record


def read_receipts(
    _execution_root: pathlib.Path | None = None,
) -> list[dict[str, Any]]:
    """Read and verify all receipts (bounded, never writes)."""
    root = resolve_execution_root(_execution_root)
    path = _receipt_path(root)
    if not path.is_file():
        return []
    records: list[dict[str, Any]] = []
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ExecutionStateError("receipt-write-failed", "Execution receipts are unreadable") from exc
    for line in raw.splitlines():
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError as exc:
            raise ExecutionStateError("receipt-write-failed", "Execution receipts are malformed") from exc
        if not isinstance(record, dict) or not verify_record_sha256(record):
            raise ExecutionStateError("receipt-write-failed", "Execution receipt integrity failed")
        records.append(record)
    return records


def _atomic_append(path: pathlib.Path, canonical_line: str) -> None:
    """Append one canonical line; an ordinary failure leaves prior bytes intact."""
    directory = path.parent
    try:
        directory.mkdir(parents=True, exist_ok=True)
        if not directory.is_dir() or paper_locks.path_is_unsafe_indirection(directory):
            raise OSError("unsafe execution receipt directory")
    except OSError as exc:
        raise ExecutionStateError("receipt-write-failed", "Execution receipt storage is unavailable") from exc
    try:
        original = path.read_bytes() if path.exists() else b""
    except OSError as exc:
        raise ExecutionStateError("receipt-write-failed", "Execution receipt storage is unavailable") from exc
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
        raise ExecutionStateError("receipt-write-failed", "Execution receipt write failed") from exc


def append_receipt(
    record: dict[str, Any],
    *,
    _execution_root: pathlib.Path | None = None,
    _lock_root: pathlib.Path | None = None,
) -> str:
    """Append one receipt under the fixed writer lock; idempotent per event.

    Returns ``"appended"`` or ``"idempotent"``. A conflicting canonical
    record under the same ``event_id`` is a bounded integrity error; prior
    bytes are never modified on any failure.
    """
    if not isinstance(record, dict) or not verify_record_sha256(record):
        raise ExecutionStateError("receipt-write-failed", "Execution receipt integrity failed")
    canonical = canonical_json(record)
    root = resolve_execution_root(_execution_root)
    path = _receipt_path(root)
    lock_root = (
        pathlib.Path(_lock_root)
        if _lock_root is not None
        else root.parent / "locks"
    )
    try:
        with paper_locks._acquire(
            WRITER_LOCK_NAME,
            purpose="ibkr-paper-execution-writer",
            nonblocking=True,
            timeout_seconds=None,
            _lock_root=lock_root,
        ):
            existing = read_receipts(_execution_root=_execution_root)
            matches = [
                prior for prior in existing if prior.get("event_id") == record["event_id"]
            ]
            if len(matches) > 1:
                raise ExecutionStateError(
                    "receipt-write-failed", "Execution receipt integrity conflict: duplicate event_id"
                )
            if matches:
                if canonical_json(matches[0]) == canonical:
                    return "idempotent"
                raise ExecutionStateError(
                    "receipt-write-failed", "Execution receipt integrity conflict for one event_id"
                )
            _atomic_append(path, canonical)
    except paper_locks.LockUnavailableError:
        raise ExecutionStateError("receipt-write-failed", "Execution receipt writer lock is unavailable") from None
    except paper_locks.LockError:
        raise ExecutionStateError("receipt-write-failed", "Execution receipt writer lock is unavailable") from None
    return "appended"

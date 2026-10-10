"""Explicitly armed, idempotent IBKR PAPER order submission boundary (5F-3a).

This module is the ONLY submission orchestrator of Patch 5F-3a and it is
NOT reachable from the automated PAPER pipeline: ``manus.paper_runner``,
``manus.paper_apply``, and ``scheduled_paper_task`` must never import
``ibkr.paper_execution`` or ``ibkr.paper_transport`` (enforced by tests).

What a submission requires — every gate BEFORE any broker I/O where
possible, and ALL of them before ``placeOrder``:

1. current-invocation operator arm (``--arm-paper``; never persisted,
   never an environment variable, never a scheduler surface);
2. exact ``--confirm-execution-id`` match with the intent's deterministic
   execution id;
3. a valid ``ibkr-paper-execution-intent/v1`` execution intent;
4. an operator-approved, hash-verified 5F-2 mapping whose ``entry_sha256``
   equals the intent's ``mapping_sha256``;
5. configuration ``environment == "PAPER"`` (operator-declared label;
   ports, account prefixes, and names are NEVER consulted);
6. the exact configured ``expected_account_id`` with exactly one managed
   account at connect time;
7. no prior receipt state for the same ``execution_id`` (already
   submitted/acknowledged/filled ⇒ duplicate is rejected; a previous
   ``submission-uncertain`` ⇒ fail closed, never auto-retried);
8. immediate read-only conId re-verification (``verify_ibkr_contract``)
   with a fresh adapter session — never cached;
9. no broker evidence of an existing order with the same deterministic
   ``order_ref`` (open orders / executions read first).

Then, and only then: one durable ``submission-attempted`` receipt, ONE
``placeOrder`` through the private PAPER transport, one bounded wait for
exact order evidence, and append-only outcome receipts. Outcome is one of
``acknowledged`` / ``rejected`` / ``uncertain``; there is NO retry of any
kind, ever, in this module.

Importing this module performs no work: no connection, no thread, no
credential access, no file, no lock, no clock, and no ``ibapi`` import.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import sys
from decimal import Decimal, InvalidOperation
from typing import Any, Callable

from ibkr import diagnostics
from ibkr import paper_execution_state as state
from ibkr.adapter import ReadonlyIbkrAdapter, mask_account
from ibkr.config import AdapterConfigError
from ibkr.diagnostics import AdapterError
from ibkr.instrument_mapping import (
    MappingError,
    verify_ibkr_contract,
)
from ibkr.paper_transport import PaperSubmissionError, TwsPaperExecutionTransport

EXECUTION_INTENT_SCHEMA_VERSION = "ibkr-paper-execution-intent/v1"
EXECUTION_RESULT_SCHEMA_VERSION = "ibkr-paper-execution-result/v1"

# Source-controlled ABSOLUTE PAPER notional cap (Decimal; never a float).
ABSOLUTE_MAX_PAPER_ORDER_NOTIONAL_USD = Decimal("100.00")

SUPPORTED_BROKER = "IBKR"
SUPPORTED_ENVIRONMENT = "PAPER"
SUPPORTED_SEC_TYPE = "STK"
SUPPORTED_CURRENCY = "USD"
SUPPORTED_EXCHANGE = "SMART"
SUPPORTED_DIRECTION = "long"
SUPPORTED_ACTION = "BUY"
SUPPORTED_ORDER_TYPE = "LMT"
SUPPORTED_TIF = "DAY"
SUPPORTED_OUTSIDE_RTH = False

_EXECUTION_ID_RE = re.compile(r"^[0-9a-f]{64}$")
_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]*$")

_CLOSED_INTENT_FIELDS = frozenset(
    {
        "schema_version",
        "decision_id",
        "mapping_id",
        "mapping_sha256",
        "source_binding_sha256",
        "target",
        "exposure",
        "order",
        "created_at",
    }
)
_CLOSED_TARGET_FIELDS = frozenset(
    {"conid", "sec_type", "symbol", "currency", "exchange", "primary_exchange", "local_symbol", "trading_class"}
)
_CLOSED_EXPOSURE_FIELDS = frozenset({"direction", "relationship"})
_CLOSED_ORDER_FIELDS = frozenset(
    {"action", "quantity", "order_type", "limit_price", "tif", "outside_rth"}
)


class ExecutionError(RuntimeError):
    """Bounded PAPER submission failure; ``code`` is from the closed set."""

    CODES = frozenset(
        {
            "arm-missing",
            "arm-confirmation-mismatch",
            "intent-invalid",
            "intent-schema-mismatch",
            "mapping-hash-mismatch",
            "environment-not-paper",
            "paper-short-not-supported",
            "unsupported-order-parameter",
            "notional-cap-exceeded",
            "execution-already-claimed",
            "execution-already-submitted",
            "execution-uncertain",
            "duplicate-broker-evidence",
            "receipt-write-failed",
            "verification-failed",
            "connection-unavailable",
            "session-unavailable",
            "unexpected-account",
            "multiple-accounts",
            "account-mismatch",
            "paper-order-rejected",
            "paper-account-mismatch",
            "paper-session-failed",
            "paper-submission-timeout",
            "paper-submission-uncertain",
            "paper-transport-unavailable",
        }
    )

    def __init__(self, code: str, message: str | None = None) -> None:
        if code not in self.CODES:
            raise ValueError("unbounded execution error code")
        self.code = code
        super().__init__(message or f"execution failure: {code}")


def canonical_json(document: Any) -> str:
    """Deterministic JSON serialization (same convention as 5F-2)."""
    return json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _sha256(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _require_bounded_text(value: Any, name: str, *, maximum: int = 128) -> str:
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise ExecutionError("intent-invalid", f"execution intent {name} must be bounded non-empty text")
    return value


def parse_limit_price(value: Any) -> str:
    """Strictly parse one positive Decimal limit price (exact, no float).

    Returns the normalized string form used for identity and order
    construction; floats and malformed Decimals are rejected.
    """
    if isinstance(value, float):
        raise ExecutionError("intent-invalid", "execution intent limit_price must be an exact decimal string")
    if not isinstance(value, (str, int, Decimal)):
        raise ExecutionError("intent-invalid", "execution intent limit_price must be an exact decimal string")
    try:
        parsed = Decimal(str(value))
    except InvalidOperation:
        raise ExecutionError("intent-invalid", "execution intent limit_price is not a valid decimal") from None
    if not parsed.is_finite() or parsed <= 0:
        raise ExecutionError("intent-invalid", "execution intent limit_price must be positive")
    text = format(parsed, "f")
    if text.startswith("."):
        text = "0" + text
    return text


def parse_quantity(value: Any) -> int:
    """Parse one whole-share quantity (integer >= 1; no float, no Decimal)."""
    if isinstance(value, bool) or not isinstance(value, int):
        raise ExecutionError("intent-invalid", "execution intent quantity must be a whole number of shares")
    if value < 1:
        raise ExecutionError("intent-invalid", "execution intent quantity must be at least 1")
    return value


def execution_id(intent: dict[str, Any]) -> str:
    """Deterministic identity over the complete canonical execution intent.

    Changing ANY identity field — decision_id, mapping hash, source
    binding, conId, action, quantity, limit price, TIF, outside-RTH —
    changes the execution id. Timestamps and metadata are excluded.
    """
    return _sha256(canonical_json(intent_identity(intent)))


def intent_identity(intent: dict[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": intent["schema_version"],
        "decision_id": intent["decision_id"],
        "mapping_id": intent["mapping_id"],
        "mapping_sha256": intent["mapping_sha256"],
        "source_binding_sha256": intent["source_binding_sha256"],
        "target": {field: intent["target"][field] for field in sorted(_CLOSED_TARGET_FIELDS)},
        "exposure": {field: intent["exposure"][field] for field in sorted(_CLOSED_EXPOSURE_FIELDS)},
        "order": {field: intent["order"][field] for field in sorted(_CLOSED_ORDER_FIELDS)},
    }


def order_ref(execution_id_value: str) -> str:
    """Bounded deterministic broker-visible orderRef for one execution id."""
    if not isinstance(execution_id_value, str) or not _EXECUTION_ID_RE.fullmatch(execution_id_value):
        raise ExecutionError("intent-invalid", "execution id must be a lowercase SHA-256")
    return f"phil5f3-{execution_id_value[:32]}"


def validate_intent(document: Any) -> dict[str, Any]:
    """Validate one execution intent against the closed v1 schema."""
    if not isinstance(document, dict) or set(document) != _CLOSED_INTENT_FIELDS:
        raise ExecutionError("intent-schema-mismatch", "execution intent field set is closed and required")
    if document["schema_version"] != EXECUTION_INTENT_SCHEMA_VERSION:
        raise ExecutionError("intent-schema-mismatch", "execution intent schema version is incompatible")
    _require_bounded_text(document["decision_id"], "decision_id")
    _require_bounded_text(document["mapping_id"], "mapping_id")
    for name in ("mapping_sha256", "source_binding_sha256"):
        value = document[name]
        if not isinstance(value, str) or not _EXECUTION_ID_RE.fullmatch(value):
            raise ExecutionError("intent-invalid", f"execution intent {name} must be a lowercase SHA-256")
    target = document["target"]
    if not isinstance(target, dict) or set(target) != _CLOSED_TARGET_FIELDS:
        raise ExecutionError("intent-invalid", "execution intent target field set is closed")
    conid = target["conid"]
    if isinstance(conid, bool) or not isinstance(conid, int) or conid <= 0:
        raise ExecutionError("intent-invalid", "execution intent conid must be a positive integer")
    if target["sec_type"] != SUPPORTED_SEC_TYPE:
        raise ExecutionError("unsupported-order-parameter", "only STK targets are supported in 5F-3a")
    if target["currency"] != SUPPORTED_CURRENCY:
        raise ExecutionError("unsupported-order-parameter", "only USD targets are supported in 5F-3a")
    if target["exchange"] != SUPPORTED_EXCHANGE:
        raise ExecutionError("unsupported-order-parameter", "only SMART routing is supported in 5F-3a")
    for name in ("symbol", "primary_exchange", "local_symbol", "trading_class"):
        value = target[name]
        if value is not None and (not isinstance(value, str) or not value or len(value) > 32):
            raise ExecutionError("intent-invalid", f"execution intent target {name} is invalid")
    exposure = document["exposure"]
    if not isinstance(exposure, dict) or set(exposure) != _CLOSED_EXPOSURE_FIELDS:
        raise ExecutionError("intent-invalid", "execution intent exposure field set is closed")
    direction = exposure["direction"]
    if direction == "short":
        raise ExecutionError("paper-short-not-supported", "5F-3a supports only long exposure")
    if direction != SUPPORTED_DIRECTION:
        raise ExecutionError("intent-invalid", "exposure direction must be explicit long or short")
    relationship = exposure["relationship"]
    if not isinstance(relationship, str) or not _IDENTIFIER_RE.fullmatch(relationship) or len(relationship) > 64:
        raise ExecutionError("intent-invalid", "exposure relationship must be a bounded identifier")
    order = document["order"]
    if not isinstance(order, dict) or set(order) != _CLOSED_ORDER_FIELDS:
        raise ExecutionError("intent-invalid", "execution intent order field set is closed")
    if order["action"] != SUPPORTED_ACTION:
        raise ExecutionError("unsupported-order-parameter", "only BUY is supported in 5F-3a")
    quantity = parse_quantity(order["quantity"])
    limit_price_text = parse_limit_price(order["limit_price"])
    if order["order_type"] != SUPPORTED_ORDER_TYPE:
        raise ExecutionError("unsupported-order-parameter", "only LMT orders are supported in 5F-3a")
    if order["tif"] != SUPPORTED_TIF:
        raise ExecutionError("unsupported-order-parameter", "only DAY time in force is supported in 5F-3a")
    if order["outside_rth"] is not SUPPORTED_OUTSIDE_RTH:
        raise ExecutionError("unsupported-order-parameter", "outside regular hours trading is not supported in 5F-3a")
    notional = Decimal(quantity) * Decimal(limit_price_text)
    if notional > ABSOLUTE_MAX_PAPER_ORDER_NOTIONAL_USD:
        raise ExecutionError("notional-cap-exceeded", "order notional exceeds the absolute USD 100.00 paper cap")
    validated: dict[str, Any] = {
        "schema_version": EXECUTION_INTENT_SCHEMA_VERSION,
        "decision_id": document["decision_id"],
        "mapping_id": document["mapping_id"],
        "mapping_sha256": document["mapping_sha256"],
        "source_binding_sha256": document["source_binding_sha256"],
        "target": dict(target),
        "exposure": dict(exposure),
        "order": {
            "action": SUPPORTED_ACTION,
            "quantity": quantity,
            "order_type": SUPPORTED_ORDER_TYPE,
            "limit_price": limit_price_text,
            "tif": SUPPORTED_TIF,
            "outside_rth": SUPPORTED_OUTSIDE_RTH,
        },
    }
    created_at = document["created_at"]
    if created_at is not None:
        if not isinstance(created_at, str) or len(created_at) > 32:
            raise ExecutionError("intent-invalid", "created_at must be informational text")
        validated["created_at"] = created_at
    return validated


def load_intent(path: str | None = None, *, document: Any = None) -> tuple[dict[str, Any], str]:
    """Load and validate one execution intent; return (intent, execution_id)."""
    if document is None:
        if path is None:
            raise ExecutionError("intent-invalid", "intent path or document is required")
        try:
            with open(path, "r", encoding="utf-8") as handle:
                document = json.load(handle)
        except json.JSONDecodeError:
            raise ExecutionError("intent-invalid", "execution intent is not valid JSON") from None
        except OSError:
            raise ExecutionError("intent-invalid", "execution intent file could not be read") from None
    validated = validate_intent(document)
    return validated, execution_id(validated)


def _paper_environment_gate(config: dict[str, Any]) -> None:
    environment = config.get("environment")
    if environment != SUPPORTED_ENVIRONMENT:
        raise ExecutionError("environment-not-paper", "execution requires an explicitly PAPER configuration")


class IbkrPaperExecutor:
    """Manually armed PAPER-only submission boundary (never automated).

    One instance performs at most the submissions explicitly invoked
    through :meth:`submit`; it holds no persistent arm, no persistent
    enablement, and no scheduler surface.
    """

    def __init__(
        self,
        configuration: dict[str, Any] | None = None,
        *,
        _read_adapter_factory: Callable[[dict[str, Any]], Any] | None = None,
        _paper_transport_factory: Callable[[dict[str, Any]], Any] | None = None,
        _execution_root: Any = None,
        _lock_root: Any = None,
        _now: Callable[[Any], Any] | None = None,
    ) -> None:
        self._configuration = configuration
        self._read_adapter_factory = _read_adapter_factory
        self._paper_transport_factory = _paper_transport_factory
        self._execution_root = _execution_root
        self._lock_root = _lock_root
        self._now = _now

    # -- configuration -------------------------------------------------------

    def _config(self) -> dict[str, Any]:
        if self._configuration is None:
            from ibkr.config import load_config

            self._configuration = load_config()
        return self._configuration

    # -- public submission ---------------------------------------------------

    def submit(
        self,
        intent_document: Any,
        mapping_entry: dict[str, Any],
        *,
        arm: bool,
        confirm_execution_id: str,
    ) -> dict[str, Any]:
        """Run the full gated PAPER submission flow for ONE execution."""
        intent, computed_execution_id = load_intent(document=intent_document)
        if not arm:
            raise ExecutionError("arm-missing", "execution requires the explicit current-invocation arm")
        if confirm_execution_id != computed_execution_id:
            raise ExecutionError("arm-confirmation-mismatch", "execution id confirmation does not match the intent")
        config = self._config()
        _paper_environment_gate(config)
        self._verify_mapping_hash(intent, mapping_entry)
        # Early optimization only; the authoritative idempotency boundary
        # is the atomic claim below, which re-checks state under the claim
        # lock before any placement can proceed.
        self._reject_known_execution_state(computed_execution_id)
        verification = self._reverify_contract(mapping_entry)
        self._reject_broker_evidence_duplicate(order_ref(computed_execution_id))
        receipt_seed = self._claim_submission_attempt(
            intent=intent,
            execution_id=computed_execution_id,
            order_ref=order_ref(computed_execution_id),
        )
        return self._submit_through_transport(
            intent=intent,
            execution_id=computed_execution_id,
            order_ref=order_ref(computed_execution_id),
            verification=verification,
            receipt_seed=receipt_seed,
        )

    # -- gates ---------------------------------------------------------------

    def _verify_mapping_hash(self, intent: dict[str, Any], mapping_entry: dict[str, Any]) -> None:
        if mapping_entry.get("entry_sha256") != intent["mapping_sha256"]:
            raise ExecutionError("mapping-hash-mismatch", "approved mapping hash does not match the execution intent")

    def _reject_known_execution_state(self, computed_execution_id: str) -> None:
        try:
            records = state.read_receipts(_execution_root=self._execution_root)
        except state.ExecutionStateError as exc:
            raise ExecutionError("receipt-write-failed", str(exc)) from None
        # Uncertain history takes precedence: it must never be silently
        # retried nor surfaced as a plain duplicate.
        if any(
            record.get("execution_id") == computed_execution_id
            and record.get("event_type") == "submission-uncertain"
            for record in records
        ):
            raise ExecutionError("execution-uncertain", "prior submission outcome is uncertain; no auto-retry")
        for record in records:
            if record.get("execution_id") != computed_execution_id:
                continue
            event_type = record.get("event_type")
            if event_type in ("submission-attempted", "submitted", "acknowledged"):
                raise ExecutionError("execution-already-submitted", "execution already submitted; never re-place")

    def _reverify_contract(self, mapping_entry: dict[str, Any]) -> dict[str, Any]:
        factory = self._read_adapter_factory or (lambda config: ReadonlyIbkrAdapter(config))
        adapter = factory(self._config())
        try:
            return verify_ibkr_contract(mapping_entry, adapter)
        except MappingError as exc:
            raise ExecutionError("verification-failed", f"broker verification failed: {exc.code}") from None
        except AdapterError as exc:
            raise ExecutionError("verification-failed", f"broker verification failed: {exc.code}") from None
        except AdapterConfigError:
            raise ExecutionError("verification-failed", "broker verification is not configured") from None
        finally:
            close = getattr(adapter, "close", None)
            if callable(close):
                close()

    def _reject_broker_evidence_duplicate(self, computed_order_ref: str) -> None:
        factory = self._read_adapter_factory or (lambda config: ReadonlyIbkrAdapter(config))
        adapter = factory(self._config())
        try:
            open_orders = adapter.open_orders()
            executions = adapter.executions()
        except AdapterError as exc:
            raise ExecutionError("connection-unavailable", f"broker evidence read failed: {exc.code}") from None
        finally:
            close = getattr(adapter, "close", None)
            if callable(close):
                close()
        for row in open_orders:
            if row.get("order_ref") == computed_order_ref:
                raise ExecutionError("duplicate-broker-evidence", "an open order with this order_ref already exists")
        for row in executions:
            if row.get("order_ref") == computed_order_ref:
                raise ExecutionError("duplicate-broker-evidence", "an execution with this order_ref already exists")

    # -- receipts ------------------------------------------------------------

    def _record_submission_attempt(
        self,
        *,
        intent: dict[str, Any],
        execution_id: str,
        order_ref: str,
    ) -> dict[str, Any]:
        config = self._config()
        target = intent["target"]
        order = intent["order"]
        try:
            record = state.build_receipt(
                event_type="submission-attempted",
                execution_id=execution_id,
                intent_sha256=intent["mapping_sha256"] and _sha256(state.canonical_json(intent)),
                decision_id=intent["decision_id"],
                mapping_id=intent["mapping_id"],
                mapping_sha256=intent["mapping_sha256"],
                source_binding_sha256=intent["source_binding_sha256"],
                order_ref=order_ref,
                account_id_masked=mask_account(str(config["expected_account_id"])),
                target={
                    "conid": target["conid"],
                    "symbol": target["symbol"],
                    "sec_type": target["sec_type"],
                    "currency": target["currency"],
                    "exchange": target["exchange"],
                },
                order={
                    "action": order["action"],
                    "quantity": order["quantity"],
                    "order_type": order["order_type"],
                    "limit_price": order["limit_price"],
                    "tif": order["tif"],
                    "outside_rth": order["outside_rth"],
                },
                broker={"client_id": config["client_id"], "order_id": None, "perm_id": None, "status": None},
                reason_code=None,
                recorded_at_utc=None,
            )
            state.append_receipt(
                record,
                _execution_root=self._execution_root,
                _lock_root=self._lock_root,
            )
        except state.ExecutionStateError as exc:
            raise ExecutionError("receipt-write-failed", str(exc)) from None
        return record

    def _claim_submission_attempt(
        self,
        *,
        intent: dict[str, Any],
        execution_id: str,
        order_ref: str,
    ) -> dict[str, Any]:
        """Atomically claim this execution_id (5F-3a2) and return the
        durable ``submission-attempted`` receipt.

        Under one fixed cross-process lock the state module re-checks
        EVERY prior record for this exact execution id and, only if the
        execution is unclaimed, appends one durable ``submission-attempted``
        record. Only this process receives ``claimed`` and may continue
        toward placement; a concurrent process receives a bounded
        already-claimed/uncertain result and stops BEFORE any placement.
        Ownership is by execution_id alone, never by event_id idempotency
        or timestamp coincidence.
        """
        config = self._config()
        target = intent["target"]
        order = intent["order"]
        try:
            record = state.build_receipt(
                event_type="submission-attempted",
                execution_id=execution_id,
                intent_sha256=_sha256(state.canonical_json(intent)),
                decision_id=intent["decision_id"],
                mapping_id=intent["mapping_id"],
                mapping_sha256=intent["mapping_sha256"],
                source_binding_sha256=intent["source_binding_sha256"],
                order_ref=order_ref,
                account_id_masked=mask_account(str(config["expected_account_id"])),
                target={
                    "conid": target["conid"],
                    "symbol": target["symbol"],
                    "sec_type": target["sec_type"],
                    "currency": target["currency"],
                    "exchange": target["exchange"],
                },
                order={
                    "action": order["action"],
                    "quantity": order["quantity"],
                    "order_type": order["order_type"],
                    "limit_price": order["limit_price"],
                    "tif": order["tif"],
                    "outside_rth": order["outside_rth"],
                },
                broker={"client_id": config["client_id"], "order_id": None, "perm_id": None, "status": None},
                reason_code=None,
                recorded_at_utc=None,
            )
            outcome = state.claim_submission_attempt(
                record,
                _execution_root=self._execution_root,
                _lock_root=self._lock_root,
            )
        except state.ExecutionStateError as exc:
            if exc.code == "execution-already-claimed":
                raise ExecutionError(
                    "execution-already-submitted", "execution already claimed by another attempt; never re-place"
                ) from None
            if exc.code == "execution-uncertain":
                raise ExecutionError(
                    "execution-uncertain", "prior submission outcome is uncertain; no auto-retry"
                ) from None
            raise ExecutionError("receipt-write-failed", str(exc)) from None
        if outcome != "claimed":  # pragma: no cover - defensive
            raise ExecutionError("execution-already-submitted", "execution claim was not granted")
        return record

    def _record_outcome(
        self,
        *,
        attempt_record: dict[str, Any],
        event_type: str,
        reason_code: str | None,
        broker: dict[str, Any],
    ) -> None:
        try:
            record = state.build_receipt(
                event_type=event_type,
                execution_id=attempt_record["execution_id"],
                intent_sha256=attempt_record["intent_sha256"],
                decision_id=attempt_record["decision_id"],
                mapping_id=attempt_record["mapping_id"],
                mapping_sha256=attempt_record["mapping_sha256"],
                source_binding_sha256=attempt_record["source_binding_sha256"],
                order_ref=attempt_record["order_ref"],
                account_id_masked=attempt_record["account_id_masked"],
                target=dict(attempt_record["target"]),
                order=dict(attempt_record["order"]),
                broker=broker,
                reason_code=reason_code,
            )
            state.append_receipt(
                record,
                _execution_root=self._execution_root,
                _lock_root=self._lock_root,
            )
        except state.ExecutionStateError as exc:
            raise ExecutionError("receipt-write-failed", str(exc)) from None

    # -- transport submission -------------------------------------------------

    def _submit_through_transport(
        self,
        *,
        intent: dict[str, Any],
        execution_id: str,
        order_ref: str,
        verification: dict[str, Any],
        receipt_seed: dict[str, Any],
    ) -> dict[str, Any]:
        target = verification["ibkr"]
        order = intent["order"]
        try:
            transport = self._paper_transport_factory(self._config())
        except Exception:
            raise ExecutionError("paper-transport-unavailable", "paper execution transport is unavailable") from None
        disconnect_attempted = False
        try:
            transport.connect(self._config())
            try:
                evidence = transport.place_paper_order(
                    target=target,
                    action=order["action"],
                    quantity=order["quantity"],
                    limit_price=order["limit_price"],
                    order_ref=order_ref,
                    expected_account_id=str(self._config()["expected_account_id"]),
                )
            except PaperSubmissionError as exc:
                if exc.code == "paper-order-rejected":
                    self._record_outcome(
                        attempt_record=receipt_seed,
                        event_type="submission-rejected",
                        reason_code="paper-order-rejected",
                        broker={
                            "client_id": self._config()["client_id"],
                            "order_id": None,
                            "perm_id": None,
                            "status": "Rejected",
                        },
                    )
                    raise ExecutionError("paper-order-rejected", "paper order was rejected by the broker") from None
                if exc.code == "paper-account-mismatch":
                    # The write session's account allowlist failed BEFORE
                    # any order-id allocation or placement: the broker
                    # never saw the order, so there is no broker-state
                    # uncertainty. Fail closed with the bounded code and
                    # no outcome receipt.
                    raise ExecutionError("paper-account-mismatch", "write session account does not match the expected account") from None
                self._record_outcome(
                    attempt_record=receipt_seed,
                    event_type="submission-uncertain",
                    reason_code=exc.code,
                    broker={"client_id": self._config()["client_id"], "order_id": None, "perm_id": None, "status": None},
                )
                raise ExecutionError(exc.code, "paper submission outcome is uncertain") from None
            except Exception:
                self._record_outcome(
                    attempt_record=receipt_seed,
                    event_type="submission-uncertain",
                    reason_code="paper-submission-uncertain",
                    broker={"client_id": self._config()["client_id"], "order_id": None, "perm_id": None, "status": None},
                )
                raise ExecutionError("paper-submission-uncertain", "paper submission outcome is uncertain") from None
            order_id = evidence["order_id"]
            outcome = evidence["outcome"]
            status = outcome.get("status")
            broker_evidence = {
                "client_id": self._config()["client_id"],
                "order_id": order_id,
                "perm_id": None,
                "status": outcome.get("broker_status"),
            }
            if status in ("acknowledged", "done"):
                self._record_outcome(
                    attempt_record=receipt_seed,
                    event_type="acknowledged",
                    reason_code=None,
                    broker=broker_evidence,
                )
                result_status = "acknowledged"
            else:
                self._record_outcome(
                    attempt_record=receipt_seed,
                    event_type="submission-uncertain",
                    reason_code=outcome.get("code", "paper-submission-uncertain"),
                    broker={
                        "client_id": self._config()["client_id"],
                        "order_id": order_id,
                        "perm_id": None,
                        "status": None,
                    },
                )
                raise ExecutionError(
                    outcome.get("code", "paper-submission-uncertain"), "paper submission outcome is uncertain"
                )
        except ExecutionError:
            # Inner placement handling already produced the bounded,
            # final outcome; cleanup happens in ``finally``.
            raise
        except AdapterError:
            self._record_outcome(
                attempt_record=receipt_seed,
                event_type="submission-uncertain",
                reason_code="paper-session-failed",
                broker={"client_id": self._config()["client_id"], "order_id": None, "perm_id": None, "status": None},
            )
            raise
        except Exception:
            # The inner placement handler already translated
            # PaperSubmissionError and unexpected placement crashes into
            # bounded ExecutionErrors; only a bare connect-path failure
            # reaches this handler.
            self._record_outcome(
                attempt_record=receipt_seed,
                event_type="submission-uncertain",
                reason_code="paper-session-failed",
                broker={"client_id": self._config()["client_id"], "order_id": None, "perm_id": None, "status": None},
            )
            raise ExecutionError("paper-session-failed", "paper transport connection failed") from None
        finally:
            # 5F-3a2: the write-capable TWS session is disconnected on
            # EVERY outcome path (acknowledged, done, rejected, account
            # mismatch, timeout, uncertain, unexpected exception, and a
            # partial connection), exactly once, best effort. A cleanup
            # failure never masks the original result or error.
            if not disconnect_attempted:
                disconnect_attempted = True
                try:
                    transport.disconnect()
                except Exception:
                    pass
        return {
            "schema_version": EXECUTION_RESULT_SCHEMA_VERSION,
            "execution_id": execution_id,
            "intent_sha256": _sha256(state.canonical_json(intent)),
            "order_ref": order_ref,
            "status": result_status,
            "order_id": order_id,
            "reason_code": None,
        }


# ---------------------------------------------------------------------------
# CLI (closed options; operator manual use only)
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    """Closed-option CLI parser for the armed PAPER submission boundary."""
    parser = argparse.ArgumentParser(
        prog="python -m ibkr.paper_execution",
        description="Manually armed IBKR PAPER order submission boundary",
        allow_abbrev=False,
    )
    operations = parser.add_subparsers(dest="operation", required=True, metavar="{submit}")
    submit = operations.add_parser("submit", help="submit one bounded PAPER order", allow_abbrev=False)
    submit.add_argument("--intent-file", required=True, help="execution intent JSON file")
    submit.add_argument("--mapping-file", required=True, help="approved mapping entry JSON file")
    submit.add_argument("--confirm-execution-id", required=True, help="exact execution id confirmation")
    submit.add_argument("--arm-paper", action="store_true", help="explicit current-invocation paper arm")
    return parser


def main(argv: list[str] | None = None) -> int:
    arguments = list(sys.argv[1:] if argv is None else argv)
    parser = build_parser()
    try:
        options = parser.parse_args(arguments)
    except SystemExit as exc:
        return _exit_code(exc.code)
    if options.operation != "submit":
        print("error: operation must be submit", file=sys.stderr)
        return 2
    try:
        with open(options.mapping_file, "r", encoding="utf-8") as handle:
            mapping_entry = json.load(handle)
        with open(options.intent_file, "r", encoding="utf-8") as handle:
            raw_intent = json.load(handle)
        # Decode the intent file HERE (symmetric with the mapping file);
        # load_intent receives the decoded JSON document, never the path
        # string. A path string would be schema-rejected as
        # intent-schema-mismatch by validate_intent.
        intent_document, computed_execution_id = load_intent(document=raw_intent)
    except json.JSONDecodeError:
        print("error: a required input file is not valid JSON", file=sys.stderr)
        return 2
    except OSError:
        print("error: a required input file could not be read", file=sys.stderr)
        return 2
    except ExecutionError as exc:
        print(f"error: {exc.code}", file=sys.stderr)
        return 2
    if options.confirm_execution_id != computed_execution_id:
        print("error: arm-confirmation-mismatch", file=sys.stderr)
        return 2
    executor = IbkrPaperExecutor()
    try:
        document = executor.submit(
            intent_document,
            mapping_entry,
            arm=options.arm_paper,
            confirm_execution_id=options.confirm_execution_id,
        )
    except ExecutionError as exc:
        print(f"error: {exc.code}", file=sys.stderr)
        return 1
    print(canonical_json(document))
    return 0


def _exit_code(code: Any) -> int:
    if code is None:
        return 0
    if isinstance(code, int):
        return code
    return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

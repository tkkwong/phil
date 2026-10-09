"""Private, explicitly bounded IBKR PAPER order submission transport (5F-3a).

This module is the ONLY place in the repository where the official broker
mutation ``EClient.placeOrder`` is reachable and the ONLY place where an
``ibapi.order.Order`` is constructed. No adapter, no mapping module, no
``manus/*`` or ``core/*`` module may import this module (enforced by test).

Architecture: ``TwsPaperExecutionTransport`` extends the accepted 5F-1
``TwsTransport`` by overriding exactly one private seam
(``_build_wrapper``) to install a PAPER-execution collector that also
observes the official order callbacks (``openOrder``/``openOrderEnd``/
``orderStatus``) and retains the broker-issued ``nextValidId`` seed. The
proven connection, run-loop, readiness, account, and request-id machinery
is reused unchanged; there is deliberately NO second IBKR session
implementation.

The paper wrapper is READ-OBSERVATION-ONLY on top of the read-only
collector: it adds storage of officially delivered order-evidence rows and
per-order completion events. It never mutates broker state itself and
never fabricates outcomes; only ``place_order`` — called exactly once per
bounded submission by :class:`ibkr.paper_execution.IbkrPaperExecutor` —
performs the single permitted mutation.

Importing this module performs no work: no connection, no thread, no
credential access, no file, no lock, and the official ``ibapi`` import is
deferred to connection/submit time exactly as in the read-only transport.
"""
from __future__ import annotations

import threading
from typing import Any

from ibkr import diagnostics
from ibkr.transport import TransportError
from ibkr.transport_tws import (
    _CollectingWrapper,
    _FATAL_CONNECTION_CODES,
    _FATAL_SESSION_CODES,
    TwsTransport,
)

# Bounded time the paper transport waits, per submission, for exact order
# evidence (openOrder/orderStatus) after placeOrder. Notional budget only;
# no retry is ever performed on timeout.
ACKNOWLEDGEMENT_TIMEOUT_SECONDS = 5.0

# Official status vocabulary that counts as acknowledged/done evidence.
_DONE_STATUS_PREFIXES = ("Filled", "Cancelled", "Inactive")

# Numeric IBKR error codes that bound a specific order's rejection.
_ORDER_REJECTION_CODES = frozenset(
    {
        101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114,
        115, 116, 117, 118, 119, 120,
    }
)


class PaperSubmissionError(RuntimeError):
    """Bounded PAPER submission failure; ``code`` is from the closed set.

    Message is stable operator text only — never broker error prose, never
    an account id, never a path. The numeric broker code (if delivered) is
    retained explicitly as ``broker_code`` because it is non-sensitive.
    """

    def __init__(self, code: str, *, broker_code: int | None = None) -> None:
        self.code = code
        self.broker_code = broker_code
        super().__init__(code)


class _PaperExecutionWrapper(_CollectingWrapper):
    """Collector + bounded order-evidence observation (paper only).

    Read-only behavior is inherited unchanged. Additions:

    - ``next_valid_order_id`` retains the official handshake seed so the
      broker-authoritative order-id lifecycle can be honored (the
      read-only collector still discards it — this subclass is used only
      by the paper execution transport).
    - ``openOrder``/``orderStatus``/order-scoped ``error`` append
      normalized evidence rows and wake ONLY the completion event of the
      exact submitted order id.
    """

    def __init__(self, timeout: float) -> None:
        super().__init__(timeout)
        self.next_valid_order_id: int | None = None
        self.order_events: dict[int, dict[str, Any]] = {}
        self._order_lock = threading.Lock()

    # -- order evidence observation (paper wrapper only) --------------------

    def nextValidId(self, orderId: int) -> None:  # noqa: N802 (ibapi naming)
        """Retain the official seed for order-id authority (paper only)."""
        with self._order_lock:
            self.next_valid_order_id = orderId
        self.ready.set()

    def openOrder(self, _orderId: int, contract: Any, order: Any, orderState: Any) -> None:  # noqa: N802
        self._observe_order_row(orderId, order, orderState)
        super().openOrder(_orderId, contract, order, orderState)

    def orderStatus(
        self,
        orderId: int,
        status: str,
        filled: float,
        remaining: float,
        _avgFillPrice: float,
        _permId: int,
        _parentId: int,
        _lastFillPrice: float,
        _clientId: int,
        _whyHeld: str,
    ) -> None:  # noqa: N802 (ibapi naming)
        """Official orderStatus callback: bounded evidence for ONE order."""
        self._observe_order_row(orderId, None, None, status=status)

    def _observe_order_row(
        self,
        order_id: int,
        order: Any,
        order_state: Any,
        *,
        status: str | None = None,
    ) -> None:
        with self._order_lock:
            event_row = self.order_events.get(order_id)
            if event_row is None:
                return
            if status is not None:
                event_row["status"] = status
            else:
                row_status = getattr(order_state, "status", None)
                if row_status:
                    event_row.setdefault("status", row_status)
                ref = getattr(order, "orderRef", None)
                if ref:
                    event_row.setdefault("order_ref", ref)
            event_row["event"].set()

    def begin_order_watch(self, order_id: int) -> None:
        """Register the per-order completion event BEFORE placeOrder."""
        with self._order_lock:
            self.order_events[order_id] = {
                "event": threading.Event(),
                "status": None,
                "order_ref": None,
            }

    def order_outcome(self, order_id: int) -> dict[str, Any]:
        with self._order_lock:
            row = self.order_events.get(order_id)
            if row is None:
                return {"status": None, "order_ref": None, "broker_code": None}
            return {
                "status": row["status"],
                "order_ref": row["order_ref"],
                "broker_code": row.get("broker_code"),
            }

    def finish_order_watch(self, order_id: int) -> None:
        with self._order_lock:
            self.order_events.pop(order_id, None)

    def error(
        self,
        reqId: Any,
        errorTime: int,
        errorCode: int,
        errorString: str,
        advancedOrderRejectJson: str = "",
    ) -> None:  # noqa: N802 (ibapi naming)
        """Route order-scoped errors to the exact watched order (paper).

        Non-order-scoped errors keep the inherited read-only classification
        unchanged. Broker text is always discarded; only the numeric code
        is retained on the exact order event.
        """
        watched = isinstance(reqId, int) and reqId > 0
        if watched:
            with self._order_lock:
                row = self.order_events.get(reqId)
                if row is not None:
                    if errorCode in _ORDER_REJECTION_CODES:
                        row["status"] = "Rejected"
                    row["broker_code"] = errorCode
                    row["event"].set()
                    return
        # Inherited read-only classification. The broker text and time are
        # deliberately discarded here, so bounded empties are forwarded.
        del errorTime, errorString, advancedOrderRejectJson
        super().error(reqId, 0, errorCode, "", "")


class TwsPaperExecutionTransport(TwsTransport):
    """Private PAPER-only write transport; subclasses the proven session.

    Adds exactly one mutation primitive (``place_order``) with a hard
    construction envelope. Everything else (connect, readiness, managed
    accounts, reads, disconnect) is inherited unchanged.
    """

    def _build_wrapper(self, timeout: float) -> _CollectingWrapper:
        return _PaperExecutionWrapper(timeout)

    # -- the single permitted mutation primitive ----------------------------

    def place_paper_order(
        self,
        *,
        target: dict[str, Any],
        action: str,
        quantity: int,
        limit_price: str,
        order_ref: str,
    ) -> dict[str, Any]:
        """Submit ONE bounded PAPER order and return bounded evidence.

        Contract identity is the exact verified conId (5F-2); no contract
        search, no symbol inference. The constructed Order is exactly the
        5F-3a envelope: BUY/LMT/DAY, whole shares, outsideRth=False,
        transmit=True, deterministic orderRef. Returns a bounded outcome
        document; never a raw broker object, never broker error text.
        """
        client, wrapper = self._require_connected()
        if not isinstance(wrapper, _PaperExecutionWrapper):
            raise TransportError("paper transport wrapper is not armed")
        if self._wrapper is not wrapper:  # pragma: no cover - defensive
            raise TransportError("paper transport wrapper mismatch")

        from ibapi import contract as ibapi_contract  # type: ignore
        from ibapi.order import Order  # type: ignore

        contract = ibapi_contract.Contract()
        contract.conId = int(target["conid"])
        contract.exchange = "SMART"

        order = Order()
        order.action = "BUY"
        order.orderType = "LMT"
        order.totalQuantity = int(quantity)
        order.lmtPrice = float(limit_price)
        order.tif = "DAY"
        order.outsideRth = False
        order.transmit = True
        order.orderRef = order_ref

        # Broker-authoritative order id; never invented by Phil.
        next_order_id = wrapper.next_valid_order_id
        if not isinstance(next_order_id, int) or next_order_id <= 0:
            raise PaperSubmissionError("paper-session-failed")

        wrapper.begin_order_watch(next_order_id)
        client.placeOrder(next_order_id, contract, order)
        event_row = wrapper.order_events.get(next_order_id)
        if event_row is None:  # pragma: no cover - defensive
            raise PaperSubmissionError("paper-session-failed")
        try:
            outcome = self._await_order_outcome(wrapper, next_order_id, event_row)
        finally:
            wrapper.finish_order_watch(next_order_id)
        return {
            "order_id": next_order_id,
            "order_ref": order_ref,
            "outcome": outcome,
        }

    @staticmethod
    def _await_order_outcome(
        wrapper: _PaperExecutionWrapper,
        order_id: int,
        event_row: dict[str, Any],
    ) -> dict[str, Any]:
        """Bounded wait for exact order evidence; no retry, no fabrication."""
        outcome: dict[str, Any] = {"status": "uncertain", "order_ref": None}
        if not event_row["event"].wait(ACKNOWLEDGEMENT_TIMEOUT_SECONDS):
            outcome["code"] = "paper-submission-timeout"
            return outcome
        status = event_row.get("status")
        if status == "Rejected":
            outcome["code"] = "paper-order-rejected"
            outcome["broker_code"] = event_row.get("broker_code")
            return outcome
        if isinstance(status, str) and status.startswith(_DONE_STATUS_PREFIXES):
            outcome["status"] = "done"
            outcome["broker_status"] = status
            outcome["order_ref"] = event_row.get("order_ref")
            return outcome
        if isinstance(status, str) and status:
            outcome["status"] = "acknowledged"
            outcome["broker_status"] = status
            outcome["order_ref"] = event_row.get("order_ref")
            return outcome
        outcome["code"] = "paper-submission-uncertain"
        return outcome

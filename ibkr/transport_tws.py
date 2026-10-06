"""Official TWS / IB Gateway read-only transport (Patch 5F-1a).

Uses the official IBKR Python TWS API client (``ibapi``) with the official
Python connection lifecycle:

    client.connect(host, port, clientId)   # creates and starts the reader
    client.run()                           # processes the incoming message
                                           # queue and invokes EWrapper
                                           # callbacks (one bounded daemon
                                           # thread per connected transport)
    client.disconnect()                    # ends the session and the loop

The ``ibapi`` import is deferred to :meth:`TwsTransport.connect` so that
importing this module performs no work and has no side effects: no network,
no threads, no subprocesses, no credential access, no files created.

CRITICAL BOUNDARY: the underlying ``ibapi.EClient`` exposes order-mutation
methods (``placeOrder``, ``cancelOrder``, ...). It is held only in a private
attribute of this private class and is never returned, exposed, or wrapped.
The public adapter never receives this object.

READINESS: a successful TCP connect alone is not sufficient. The transport
waits, bounded, for the official initial-handshake callback
(:meth:`_CollectingWrapper.nextValidId`) to arrive through the run/message
processing path before any read request is issued. The nextValidId payload
is discarded; it is a readiness indication only and never an order-id
authority. No Order object is ever constructed anywhere in this module.
"""
from __future__ import annotations

import threading
from typing import Any

from ibkr.transport import ReadonlyTransport, TransportError

# Bounded shutdown budget for the message-processing thread.
_RUN_JOIN_TIMEOUT_SECONDS = 5.0

# Documented initial-status/inactive-but-normal TWS notifications that must
# NOT make a healthy read-only session unusable (numeric-code policy only;
# no text parsing, no broad 21xx suppression, unknown codes stay fatal).
_BENIGN_INFORMATIONAL_CODES = frozenset({2104, 2106, 2107, 2108, 2158})


class _CollectingWrapper:
    """Bounded request collector for read-only snapshots.

    Mirrors the official ``EWrapper`` callback surface used by this
    transport. Callbacks are invoked by the message-processing run loop,
    never directly by this transport's own code.
    """

    def __init__(self, timeout: float) -> None:
        self.timeout = timeout
        self.done = threading.Event()
        self.ready = threading.Event()
        self.failure: str | None = None
        self.accounts: list[str] = []
        self.account_summary: dict[str, Any] = {}
        self.positions: list[dict[str, Any]] = []
        self.open_orders: list[dict[str, Any]] = []
        self.executions: list[dict[str, Any]] = []
        self.contract_matches: list[dict[str, Any]] = []
        self.request_id = 9000

    def next_id(self) -> int:
        self.request_id += 1
        return self.request_id

    # -- EWrapper callbacks (read-only data collection only) ----------------

    def nextValidId(self, orderId: int) -> None:  # noqa: N802 (ibapi naming)
        """Official initial-handshake callback; readiness signal ONLY.

        The delivered identifier is deliberately discarded: it is never
        stored as an order id and never authorizes any order action.
        """
        del orderId
        self.ready.set()

    def managedAccounts(self, accountsList: str) -> None:  # noqa: N802
        self.accounts = [item for item in accountsList.split(",") if item]
        self.done.set()

    def error(
        self,
        reqId: Any,
        errorTime: int,
        errorCode: int,
        errorString: str,
        advancedOrderRejectJson: str = "",
    ) -> None:  # noqa: N802 (ibapi naming)
        """Official current Python callback shape (ibapi > 10.33).

        The exact official signature is
        ``error(reqId, errorTime, errorCode, errorString,
        advancedOrderRejectJson="")``; ``errorTime`` is an epoch-millisecond
        timestamp that precedes the numeric code and must never be
        interpreted as an error code. Broker text (``errorString``) and
        ``advancedOrderRejectJson`` are discarded entirely: never logged,
        persisted, or printed. Classification uses only the numeric code.
        """
        del errorTime, errorString, advancedOrderRejectJson
        if errorCode in _BENIGN_INFORMATIONAL_CODES:
            # Documented connection-status notifications; not fatal, not
            # readiness-ending, processing continues.
            return
        if errorCode in (502, 504, 1100, 1101, 1102, 1300):
            self.failure = "connection"
            self.done.set()
            self.ready.set()
        elif errorCode in (508, 510, 511, 540, 542):
            self.failure = "session"
            self.done.set()
            self.ready.set()

    def connectionClosed(self) -> None:  # noqa: N802
        """Official callback when the socket is closed by the peer/API."""
        self.failure = "connection"
        self.done.set()
        self.ready.set()

    def accountSummary(self, _req: int, account: str, tag: str, value: str, _currency: str) -> None:  # noqa: N802
        self.account_summary.setdefault(account, {})[tag] = value

    def accountSummaryEnd(self, _req: int) -> None:  # noqa: N802
        self.done.set()

    def position(self, account: str, contract: Any, pos: float, avgCost: float) -> None:  # noqa: N802
        self.positions.append(
            {
                "account": account,
                "conid": getattr(contract, "conId", None),
                "symbol": getattr(contract, "symbol", None),
                "sec_type": getattr(contract, "secType", None),
                "exchange": getattr(contract, "exchange", None),
                "currency": getattr(contract, "currency", None),
                "quantity": pos,
                "average_cost": avgCost,
            }
        )

    def positionEnd(self) -> None:  # noqa: N802
        self.done.set()

    def openOrder(self, _orderId: int, contract: Any, order: Any, _orderState: Any) -> None:  # noqa: N802
        # Read-only observation of an existing order object delivered by the
        # broker; no Order is ever constructed by this module.
        self.open_orders.append(
            {
                "order_id": getattr(order, "orderId", _orderId),
                "conid": getattr(contract, "conId", None),
                "symbol": getattr(contract, "symbol", None),
                "sec_type": getattr(contract, "secType", None),
                "exchange": getattr(contract, "exchange", None),
                "currency": getattr(contract, "currency", None),
                "action": getattr(order, "action", None),
                "total_quantity": getattr(order, "totalQuantity", None),
                "cash_quantity": getattr(order, "cashQty", None),
                "limit_price": getattr(order, "lmtPrice", None),
                "order_type": getattr(order, "orderType", None),
                "status": getattr(_orderState, "status", None),
            }
        )

    def openOrderEnd(self) -> None:  # noqa: N802
        self.done.set()

    def execDetails(self, _reqId: int, contract: Any, execution: Any) -> None:  # noqa: N802
        self.executions.append(
            {
                "exec_id": getattr(execution, "execId", None),
                "order_id": getattr(execution, "orderId", None),
                "conid": getattr(contract, "conId", None),
                "symbol": getattr(contract, "symbol", None),
                "sec_type": getattr(contract, "secType", None),
                "exchange": getattr(execution, "exchange", None),
                "side": getattr(execution, "side", None),
                "quantity": getattr(execution, "shares", None),
                "price": getattr(execution, "price", None),
                "time": getattr(execution, "time", None),
            }
        )

    def execDetailsEnd(self, _reqId: int) -> None:  # noqa: N802
        self.done.set()

    def contractDetails(self, _reqId: int, contractDetails: Any) -> None:  # noqa: N802
        contract = getattr(contractDetails, "contract", None)
        self.contract_matches.append(
            {
                "conid": getattr(contract, "conId", None),
                "symbol": getattr(contract, "symbol", None),
                "local_symbol": getattr(contract, "localSymbol", None),
                "sec_type": getattr(contract, "secType", None),
                "exchange": getattr(contract, "exchange", None),
                "primary_exchange": getattr(contract, "primaryExchange", None),
                "currency": getattr(contract, "currency", None),
                "expiry": getattr(contract, "lastTradeDateOrContractMonth", None),
                "strike": getattr(contract, "strike", None),
                "right": getattr(contract, "right", None),
                "multiplier": getattr(contract, "multiplier", None),
                "trading_class": getattr(contract, "tradingClass", None),
            }
        )

    def contractDetailsEnd(self, _reqId: int) -> None:  # noqa: N802
        self.done.set()


class TwsTransport(ReadonlyTransport):
    """Private official-API transport. Never exposed through the adapter."""

    def __init__(self, config: dict[str, Any]) -> None:
        self._config = config
        self._client: Any | None = None
        self._wrapper: _CollectingWrapper | None = None
        self._run_thread: threading.Thread | None = None

    # -- lifecycle -------------------------------------------------------------

    def connect(self, config: dict[str, Any]) -> None:
        self._config = config
        if self._client is not None:
            # Reconnect without an intervening disconnect is not supported;
            # treat as already-connected state (no duplicate run loops).
            return
        # Deferred official import: no side effects at module import time.
        try:
            from ibapi import client as ibapi_client  # type: ignore
            from ibapi import wrapper as ibapi_wrapper  # type: ignore
        except Exception as exc:  # pragma: no cover - environment-specific
            raise TransportError("official ibapi package is unavailable") from exc

        class _BoundWrapper(_CollectingWrapper, ibapi_wrapper.EWrapper):
            pass

        timeout = float(config.get("read_only_timeout_seconds", 10.0))
        self._wrapper = _BoundWrapper(timeout)
        self._client = ibapi_client.EClient(self._wrapper)
        try:
            # Official Python API connection: creates and starts the reader.
            self._client.connect(
                config["host"], int(config["port"]), int(config["client_id"])
            )
        except Exception as exc:
            self._client = None
            self._wrapper = None
            raise TransportError("connect failed") from exc
        if not self._client.isConnected():
            self._client = None
            self._wrapper = None
            raise TransportError("connection is not established")
        # Start exactly one bounded message-processing loop. Callbacks can
        # only arrive through this path; no thread exists before connect().
        self._run_thread = threading.Thread(
            target=self._process_messages,
            name="phil-ibkr-readonly-messages",
            daemon=True,
        )
        self._run_thread.start()
        try:
            self._await_readiness()
        except TransportError:
            self.disconnect()
            raise

    def _process_messages(self) -> None:
        """Official message-processing loop (bounded daemon thread)."""
        assert self._client is not None
        try:
            self._client.run()
        except Exception:
            # The loop died; surface a bounded failure state.
            if self._wrapper is not None:
                self._wrapper.failure = "connection"
                self._wrapper.done.set()
                self._wrapper.ready.set()

    def _await_readiness(self) -> None:
        """Wait, bounded, for the official handshake callback."""
        assert self._wrapper is not None and self._client is not None
        if not self._wrapper.ready.wait(self._wrapper.timeout):
            raise TransportError("connection readiness timed out")
        if self._wrapper.failure is not None:
            raise TransportError("connection error reported")

    def disconnect(self) -> None:
        thread = self._run_thread
        self._run_thread = None
        if self._client is not None:
            try:
                # Official Python API disconnect; ends the run() loop.
                self._client.disconnect()
            except Exception:  # pragma: no cover - best effort only
                pass
        if thread is not None and thread.is_alive():
            thread.join(_RUN_JOIN_TIMEOUT_SECONDS)
        self._client = None
        self._wrapper = None

    # -- internal helpers ----------------------------------------------------

    def _shutdown_on_error(self) -> None:
        self.disconnect()

    def _wait(self) -> None:
        assert self._wrapper is not None
        if not self._wrapper.done.wait(self._wrapper.timeout):
            self._shutdown_on_error()
            raise TransportError("read-only request timed out")
        if self._wrapper.failure == "connection":
            self._shutdown_on_error()
            raise TransportError("connection error reported")
        if self._wrapper.failure == "session":
            self._shutdown_on_error()
            raise TransportError("session error reported")

    def _require_connected(self) -> tuple[Any, _CollectingWrapper]:
        if self._client is None or self._wrapper is None:
            raise TransportError("transport is not connected")
        return self._client, self._wrapper

    # -- read-only operations --------------------------------------------------

    def managed_accounts(self) -> list[str]:
        client, wrapper = self._require_connected()
        wrapper.done.clear()
        wrapper.accounts = []
        wrapper.failure = None
        client.reqManagedAccts()
        self._wait()
        return list(wrapper.accounts)

    def account_summary(self, account_id: str) -> dict[str, Any]:
        client, wrapper = self._require_connected()
        request_id = wrapper.next_id()
        wrapper.done.clear()
        wrapper.account_summary = {}
        wrapper.failure = None
        tags = "AccountType,NetLiquidation,AvailableFunds,BuyingPower,Currency"
        client.reqAccountSummary(request_id, "All", tags)
        self._wait()
        summary = wrapper.account_summary.get(account_id)
        if summary is None:
            raise TransportError("account summary is unavailable")
        return dict(summary)

    def positions(self) -> list[dict[str, Any]]:
        client, wrapper = self._require_connected()
        wrapper.done.clear()
        wrapper.positions = []
        wrapper.failure = None
        client.reqPositions()
        self._wait()
        return [dict(row) for row in wrapper.positions]

    def open_orders(self) -> list[dict[str, Any]]:
        client, wrapper = self._require_connected()
        wrapper.done.clear()
        wrapper.open_orders = []
        wrapper.failure = None
        client.reqOpenOrders()
        self._wait()
        return [dict(row) for row in wrapper.open_orders]

    def executions(self) -> list[dict[str, Any]]:
        client, wrapper = self._require_connected()
        request_id = wrapper.next_id()
        wrapper.done.clear()
        wrapper.executions = []
        wrapper.failure = None
        client.reqExecutions(request_id, _ExecutionFilter())
        self._wait()
        return [dict(row) for row in wrapper.executions]

    def contract_details(
        self,
        symbol: str,
        sec_type: str,
        *,
        currency: str | None = None,
        exchange: str | None = None,
    ) -> list[dict[str, Any]]:
        client, wrapper = self._require_connected()
        request_id = wrapper.next_id()
        wrapper.done.clear()
        wrapper.contract_matches = []
        wrapper.failure = None
        from ibapi import contract as ibapi_contract  # type: ignore

        contract = ibapi_contract.Contract()
        contract.symbol = symbol
        contract.secType = sec_type
        contract.currency = currency or ""
        contract.exchange = exchange or "SMART"
        client.reqContractDetails(request_id, contract)
        self._wait()
        return [dict(row) for row in wrapper.contract_matches]


class _ExecutionFilter:  # pragma: no cover - trivial data holder
    """Bounded execution filter (all recent executions, no side effects)."""

    def __init__(self) -> None:
        self.clientId = ""
        self.acctCode = ""
        self.time = ""
        self.symbol = ""
        self.secType = ""
        self.exchange = ""
        self.side = ""

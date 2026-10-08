"""Official TWS / IB Gateway read-only transport (Patch 5F-1c).

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
The public adapter never receives this object. No Order object is ever
constructed anywhere in this module.

COMPLETION SYNCHRONIZATION (5F-1c): there is deliberately NO generic shared
completion event. Each callback stream owns a distinct completion signal:

    ready                    nextValidId only (connection readiness)
    managed_accounts_ready   managedAccounts only
    positions_done           positionEnd only
    open_orders_done         openOrderEnd only
    per-request events       accountSummaryEnd(reqId) / execDetailsEnd(reqId)
                             / contractDetailsEnd(reqId), matched on the
                             exact request id

A late or duplicate callback from one stream can therefore never satisfy
another read's completion condition. Empty results are returned only after
the genuine terminal callback for that exact stream/request was observed;
timeouts and fatal failures tear the session down and fail closed — empty
data is never fabricated.

READINESS: a successful TCP connect alone is not sufficient. The transport
waits, bounded, for the official initial-handshake callback
(:meth:`_CollectingWrapper.nextValidId`) to arrive through the run/message
processing path before any read request is issued. The nextValidId payload
is discarded; it is a readiness indication only and never an order-id
authority.
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


class _ScopedOperation:
    """Completion state for exactly one request-id-scoped read.

    ``event`` is set only by the genuine terminal callback for the exact
    request id, or by a fatal failure (waiters then detect the failure and
    fail closed). ``end_observed`` proves the terminal callback arrived, so
    a wake-up without the genuine END callback can never be read as
    success and empty data is never fabricated.
    """

    __slots__ = ("event", "data", "end_observed")

    def __init__(self, data: Any) -> None:
        self.event = threading.Event()
        self.data = data
        self.end_observed = False


class _CollectingWrapper:
    """Bounded request collector for read-only snapshots.

    Mirrors the official ``EWrapper`` callback surface used by this
    transport. Callbacks are invoked by the message-processing run loop,
    never directly by this transport's own code.

    Completion synchronization is per callback stream (see module
    docstring): no unrelated callback can complete another read, and
    request-id-scoped completions require the exact matching request id.
    """

    def __init__(self, timeout: float) -> None:
        self.timeout = timeout
        # Distinct per-stream completion events. There is deliberately no
        # generic shared ``done`` event (removed in Patch 5F-1c).
        self.ready = threading.Event()  # nextValidId only
        self.managed_accounts_ready = threading.Event()  # managedAccounts only
        self.positions_done = threading.Event()  # positionEnd only
        self.open_orders_done = threading.Event()  # openOrderEnd only
        # Per-stream data buffers (reset by the transport before each read).
        self.accounts: list[str] = []
        self.positions: list[dict[str, Any]] = []
        self.open_orders: list[dict[str, Any]] = []
        self.positions_end_observed = False
        self.open_orders_end_observed = False
        self.failure: str | None = None
        self.request_id = 9000
        # Request-id-scoped operations: completion requires the exact id.
        self._account_summary_ops: dict[int, _ScopedOperation] = {}
        self._execution_ops: dict[int, _ScopedOperation] = {}
        self._contract_ops: dict[int, _ScopedOperation] = {}
        # Private state lock for request lifecycle state (buffer resets,
        # operation registration, id allocation). It provides no broker
        # authority, performs no import-time work, and is never held while
        # waiting on a completion event.
        self._state_lock = threading.Lock()

    def next_id(self) -> int:
        with self._state_lock:
            self.request_id += 1
            return self.request_id

    # -- request lifecycle helpers (transport side, serialized) -------------

    def begin_positions(self) -> None:
        """Reset the positions stream for one read.

        A fresh completion event per request guarantees that a late END
        from a previous positions subscription cannot complete this one.
        """
        with self._state_lock:
            self.positions = []
            self.positions_end_observed = False
            self.positions_done = threading.Event()

    def begin_open_orders(self) -> None:
        """Reset the open-orders stream for one read (fresh event)."""
        with self._state_lock:
            self.open_orders = []
            self.open_orders_end_observed = False
            self.open_orders_done = threading.Event()

    def begin_account_summary(self, request_id: int) -> None:
        with self._state_lock:
            self._account_summary_ops[request_id] = _ScopedOperation({})

    def begin_executions(self, request_id: int) -> None:
        with self._state_lock:
            self._execution_ops[request_id] = _ScopedOperation([])

    def begin_contract_details(self, request_id: int) -> None:
        with self._state_lock:
            self._contract_ops[request_id] = _ScopedOperation([])

    def account_summary_operation(self, request_id: int) -> _ScopedOperation:
        with self._state_lock:
            return self._account_summary_ops[request_id]

    def executions_operation(self, request_id: int) -> _ScopedOperation:
        with self._state_lock:
            return self._execution_ops[request_id]

    def contract_details_operation(self, request_id: int) -> _ScopedOperation:
        with self._state_lock:
            return self._contract_ops[request_id]

    def finish_account_summary(self, request_id: int) -> None:
        """Drop the completed operation so late/stale ids are ignored."""
        with self._state_lock:
            self._account_summary_ops.pop(request_id, None)

    def finish_executions(self, request_id: int) -> None:
        with self._state_lock:
            self._execution_ops.pop(request_id, None)

    def finish_contract_details(self, request_id: int) -> None:
        with self._state_lock:
            self._contract_ops.pop(request_id, None)

    def accounts_snapshot(self) -> list[str]:
        with self._state_lock:
            return list(self.accounts)

    def positions_snapshot(self) -> list[dict[str, Any]]:
        with self._state_lock:
            return [dict(row) for row in self.positions]

    def open_orders_snapshot(self) -> list[dict[str, Any]]:
        with self._state_lock:
            return [dict(row) for row in self.open_orders]

    def account_summary_value(self, request_id: int, account_id: str) -> dict[str, str] | None:
        with self._state_lock:
            operation = self._account_summary_ops.get(request_id)
            if operation is None:
                return None
            value = operation.data.get(account_id)
            return dict(value) if value else None

    def executions_snapshot(self, request_id: int) -> list[dict[str, Any]]:
        with self._state_lock:
            operation = self._execution_ops.get(request_id)
            if operation is None:
                return []
            return [dict(row) for row in operation.data]

    def contract_matches_snapshot(self, request_id: int) -> list[dict[str, Any]]:
        with self._state_lock:
            operation = self._contract_ops.get(request_id)
            if operation is None:
                return []
            return [dict(row) for row in operation.data]

    def _signal_fatal(self, kind: str) -> None:
        """Record a fatal failure and wake every pending waiter.

        Waiters re-check ``failure`` before accepting any result, so being
        woken here always fails closed; this never reports success.
        """
        with self._state_lock:
            if self.failure is None:
                self.failure = kind
            events = [
                self.ready,
                self.managed_accounts_ready,
                self.positions_done,
                self.open_orders_done,
            ]
            for table in (self._account_summary_ops, self._execution_ops, self._contract_ops):
                events.extend(operation.event for operation in table.values())
        for event in events:
            event.set()

    # -- EWrapper callbacks (read-only data collection only) ----------------

    def nextValidId(self, orderId: int) -> None:  # noqa: N802 (ibapi naming)
        """Official initial-handshake callback; readiness signal ONLY.

        The delivered identifier is deliberately discarded: it is never
        stored as an order id and never authorizes any order action.
        """
        del orderId
        self.ready.set()

    def managedAccounts(self, accountsList: str) -> None:  # noqa: N802
        """managedAccounts stream only: stores accounts, signals its own event.

        IBKR emits this automatically at connection establishment and may
        deliver duplicates; the event is idempotent and touches no other
        read's completion state.
        """
        with self._state_lock:
            self.accounts = [item for item in accountsList.split(",") if item]
            self.managed_accounts_ready.set()

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
            self._signal_fatal("connection")
        elif errorCode in (508, 510, 511, 540, 542):
            self._signal_fatal("session")

    def connectionClosed(self) -> None:  # noqa: N802
        """Official callback when the socket is closed by the peer/API."""
        self._signal_fatal("connection")

    def accountSummary(self, req: int, account: str, tag: str, value: str, _currency: str) -> None:  # noqa: N802
        with self._state_lock:
            operation = self._account_summary_ops.get(req)
            if operation is not None:
                operation.data.setdefault(account, {})[tag] = value

    def accountSummaryEnd(self, req: int) -> None:  # noqa: N802
        """Completes ONLY the account-summary request with the exact id."""
        with self._state_lock:
            operation = self._account_summary_ops.get(req)
            if operation is not None:
                operation.end_observed = True
                operation.event.set()

    def position(self, account: str, contract: Any, pos: float, avgCost: float) -> None:  # noqa: N802
        row = {
            "account": account,
            "conid": getattr(contract, "conId", None),
            "symbol": getattr(contract, "symbol", None),
            "sec_type": getattr(contract, "secType", None),
            "exchange": getattr(contract, "exchange", None),
            "currency": getattr(contract, "currency", None),
            "quantity": pos,
            "average_cost": avgCost,
        }
        with self._state_lock:
            self.positions.append(row)

    def positionEnd(self) -> None:  # noqa: N802
        """Completes ONLY the positions stream."""
        with self._state_lock:
            self.positions_end_observed = True
            self.positions_done.set()

    def openOrder(self, _orderId: int, contract: Any, order: Any, _orderState: Any) -> None:  # noqa: N802
        # Read-only observation of an existing order object delivered by the
        # broker; no Order is ever constructed by this module.
        row = {
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
        with self._state_lock:
            self.open_orders.append(row)

    def openOrderEnd(self) -> None:  # noqa: N802
        """Completes ONLY the open-orders stream."""
        with self._state_lock:
            self.open_orders_end_observed = True
            self.open_orders_done.set()

    def execDetails(self, reqId: int, contract: Any, execution: Any) -> None:  # noqa: N802
        row = {
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
        with self._state_lock:
            operation = self._execution_ops.get(reqId)
            if operation is not None:
                operation.data.append(row)

    def execDetailsEnd(self, reqId: int) -> None:  # noqa: N802
        """Completes ONLY the executions request with the exact id."""
        with self._state_lock:
            operation = self._execution_ops.get(reqId)
            if operation is not None:
                operation.end_observed = True
                operation.event.set()

    def contractDetails(self, reqId: int, contractDetails: Any) -> None:  # noqa: N802
        contract = getattr(contractDetails, "contract", None)
        row = {
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
        with self._state_lock:
            operation = self._contract_ops.get(reqId)
            if operation is not None:
                operation.data.append(row)

    def contractDetailsEnd(self, reqId: int) -> None:  # noqa: N802
        """Completes ONLY the contract-details request with the exact id."""
        with self._state_lock:
            operation = self._contract_ops.get(reqId)
            if operation is not None:
                operation.end_observed = True
                operation.event.set()


class TwsTransport(ReadonlyTransport):
    """Private official-API transport. Never exposed through the adapter."""

    def __init__(self, config: dict[str, Any]) -> None:
        self._config = config
        self._client: Any | None = None
        self._wrapper: _CollectingWrapper | None = None
        self._run_thread: threading.Thread | None = None
        # Private request-lifecycle serialization: two threads can never
        # concurrently reuse/reset the same stream buffers. Bounded by the
        # existing per-read timeout semantics; no broker authority; no
        # import-time work.
        self._request_lock = threading.Lock()

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
            # The loop died; surface a bounded failure state that wakes
            # every pending waiter (which then fail closed).
            if self._wrapper is not None:
                self._wrapper._signal_fatal("connection")

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

    def _raise_failure(self) -> None:
        """Raise the bounded transport failure if one is recorded."""
        wrapper = self._wrapper
        assert wrapper is not None
        if wrapper.failure == "connection":
            self._shutdown_on_error()
            raise TransportError("connection error reported")
        if wrapper.failure == "session":
            self._shutdown_on_error()
            raise TransportError("session error reported")

    def _complete_or_fail(
        self,
        event: threading.Event,
        end_observed: Any,
        what: str,
    ) -> None:
        """Wait bounded for one operation's genuine terminal callback.

        Only the operation-specific terminal callback ends the wait as
        success. A fatal failure (which wakes the event) or a timeout tears
        the session down and fails closed; empty data is never fabricated.
        """
        wrapper = self._wrapper
        assert wrapper is not None
        if not event.wait(wrapper.timeout):
            # Bounded timeout: report a recorded failure first, otherwise
            # fail closed on the timeout itself.
            self._raise_failure()
            self._shutdown_on_error()
            raise TransportError(f"{what} timed out")
        # The event fired: either the genuine terminal callback or a fatal
        # failure wake-up. Only a failure-free, END-proven wake is success.
        self._raise_failure()
        if not end_observed():
            self._shutdown_on_error()
            raise TransportError(f"{what} completion was not observed")

    def _require_connected(self) -> tuple[Any, _CollectingWrapper]:
        if self._client is None or self._wrapper is None:
            raise TransportError("transport is not connected")
        return self._client, self._wrapper

    # -- read-only operations --------------------------------------------------

    def managed_accounts(self) -> list[str]:
        client, wrapper = self._require_connected()
        with self._request_lock:
            # IBKR automatically emits managedAccounts once the API
            # connection is established. That automatic callback is the
            # sole authority here: no duplicate reqManagedAccts() is
            # issued, so a late/duplicate delivery can only satisfy this
            # read (its own idempotent event) and never any other
            # operation's completion.
            self._complete_or_fail(
                wrapper.managed_accounts_ready, lambda: True, "managed accounts"
            )
            accounts = wrapper.accounts_snapshot()
        if not accounts:
            raise TransportError("managed accounts are unavailable")
        return accounts

    def account_summary(self, account_id: str) -> dict[str, Any]:
        client, wrapper = self._require_connected()
        with self._request_lock:
            request_id = wrapper.next_id()
            wrapper.begin_account_summary(request_id)
            tags = "AccountType,NetLiquidation,AvailableFunds,BuyingPower,Currency"
            client.reqAccountSummary(request_id, "All", tags)
            operation = wrapper.account_summary_operation(request_id)
            self._complete_or_fail(
                operation.event, lambda: operation.end_observed, "account summary"
            )
            summary = wrapper.account_summary_value(request_id, account_id)
            wrapper.finish_account_summary(request_id)
        if not summary:
            raise TransportError("account summary is unavailable")
        return summary

    def positions(self) -> list[dict[str, Any]]:
        client, wrapper = self._require_connected()
        with self._request_lock:
            wrapper.begin_positions()
            client.reqPositions()
            self._complete_or_fail(
                wrapper.positions_done,
                lambda: wrapper.positions_end_observed,
                "positions",
            )
            return wrapper.positions_snapshot()

    def open_orders(self) -> list[dict[str, Any]]:
        client, wrapper = self._require_connected()
        with self._request_lock:
            wrapper.begin_open_orders()
            client.reqOpenOrders()
            self._complete_or_fail(
                wrapper.open_orders_done,
                lambda: wrapper.open_orders_end_observed,
                "open orders",
            )
            return wrapper.open_orders_snapshot()

    def executions(self) -> list[dict[str, Any]]:
        client, wrapper = self._require_connected()
        with self._request_lock:
            request_id = wrapper.next_id()
            wrapper.begin_executions(request_id)
            client.reqExecutions(request_id, _ExecutionFilter())
            operation = wrapper.executions_operation(request_id)
            self._complete_or_fail(
                operation.event, lambda: operation.end_observed, "executions"
            )
            rows = wrapper.executions_snapshot(request_id)
            wrapper.finish_executions(request_id)
            return rows

    def contract_details(
        self,
        symbol: str,
        sec_type: str,
        *,
        currency: str | None = None,
        exchange: str | None = None,
    ) -> list[dict[str, Any]]:
        client, wrapper = self._require_connected()
        with self._request_lock:
            request_id = wrapper.next_id()
            wrapper.begin_contract_details(request_id)
            from ibapi import contract as ibapi_contract  # type: ignore

            contract = ibapi_contract.Contract()
            contract.symbol = symbol
            contract.secType = sec_type
            contract.currency = currency or ""
            contract.exchange = exchange or "SMART"
            client.reqContractDetails(request_id, contract)
            operation = wrapper.contract_details_operation(request_id)
            self._complete_or_fail(
                operation.event, lambda: operation.end_observed, "contract details"
            )
            rows = wrapper.contract_matches_snapshot(request_id)
            wrapper.finish_contract_details(request_id)
            return rows

    def contract_details_by_conid(self, conid: int) -> list[dict[str, Any]]:
        """Read-only contract-details request addressed by exact conId.

        5F-2a addition alongside (not instead of) ``contract_details``:
        the official ``ibapi.Contract`` is used privately exactly as the
        symbol-based method does, with the positive conId as the sole
        lookup identity (``conId`` set; ``secType``/``exchange`` left
        unconstrained so the broker resolves exactly the delivered conId).
        Same scoped request-id operation, same bounded completion, same
        normalized row shape; no Order, no EClient exposure, no raw
        Contract/ContractDetails escape, no unrelated refactor.
        """
        client, wrapper = self._require_connected()
        with self._request_lock:
            request_id = wrapper.next_id()
            wrapper.begin_contract_details(request_id)
            from ibapi import contract as ibapi_contract  # type: ignore

            contract = ibapi_contract.Contract()
            contract.conId = conid
            client.reqContractDetails(request_id, contract)
            operation = wrapper.contract_details_operation(request_id)
            self._complete_or_fail(
                operation.event, lambda: operation.end_observed, "contract details"
            )
            rows = wrapper.contract_matches_snapshot(request_id)
            wrapper.finish_contract_details(request_id)
            return rows


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

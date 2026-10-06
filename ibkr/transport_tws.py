"""Official TWS / IB Gateway read-only transport (Patch 5F-1).

Uses the official IBKR TWS API client (``ibapi``) synchronously with a
short-lived EClient connection and no reader thread beyond the bounded
request loop. The ``ibapi`` import is deferred to :meth:`TwsTransport.connect`
so that importing this module performs no work and has no side effects.

CRITICAL BOUNDARY: the underlying ``ibapi.EClient`` exposes order-mutation
methods (``placeOrder``, ``cancelOrder``, ...). It is held only in a private
attribute of this private class and is never returned, exposed, or wrapped.
The public adapter never receives this object.
"""
from __future__ import annotations

import threading
from typing import Any

from ibkr.transport import ReadonlyTransport, TransportError


class _CollectingWrapper:
    """Bounded request collector for synchronous read-only snapshots."""

    def __init__(self, timeout: float) -> None:
        self.timeout = timeout
        self.done = threading.Event()
        self.error: str | None = None
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

    def managedAccounts(self, accountsList: str) -> None:  # noqa: N802 (ibapi naming)
        self.accounts = [item for item in accountsList.split(",") if item]
        self.done.set()

    def error(self, reqId: Any, errorCode: int, errorString: str) -> None:  # noqa: N802
        # Keep only the numeric category; discard the broker text entirely.
        del errorString
        if errorCode in (502, 504, 1100, 1102, 1300):
            self.error = "connection"
            self.done.set()
        elif errorCode in (508, 510, 511, 540, 542):
            self.error = "session"
            self.done.set()

    def accountSummary(self, _req: int, account: str, tag: str, value: str, _currency: str) -> None:  # noqa: N802
        self.account_summary.setdefault(account, {})[tag] = value

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

    def connect(self, config: dict[str, Any]) -> None:
        self._config = config
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
            self._client.eConnect(config["host"], int(config["port"]), int(config["client_id"]))
        except Exception as exc:
            raise TransportError("eConnect failed") from exc
        if not self._client.isConnected():
            raise TransportError("connection is not established")

    def disconnect(self) -> None:
        if self._client is not None:
            try:
                self._client.eDisconnect()
            except Exception:  # pragma: no cover - best effort only
                pass
            self._client = None
            self._wrapper = None

    # -- internal helpers ----------------------------------------------------

    def _wait(self) -> None:
        assert self._wrapper is not None and self._client is not None
        if not self._wrapper.done.wait(self._wrapper.timeout):
            self._client.eDisconnect()
            raise TransportError("read-only request timed out")
        if self._wrapper.error == "connection":
            self._client.eDisconnect()
            raise TransportError("connection error reported")
        if self._wrapper.error == "session":
            self._client.eDisconnect()
            raise TransportError("session error reported")

    def _require_connected(self) -> tuple[Any, _CollectingWrapper]:
        if self._client is None or self._wrapper is None:
            raise TransportError("transport is not connected")
        return self._client, self._wrapper

    # -- read-only operations --------------------------------------------------

    def managed_accounts(self) -> list[str]:
        _client, wrapper = self._require_connected()
        wrapper.done.clear()
        wrapper.accounts = []
        wrapper.error = None
        _client.reqManagedAccts()
        self._wait()
        return list(wrapper.accounts)

    def account_summary(self, account_id: str) -> dict[str, Any]:
        _client, wrapper = self._require_connected()
        request_id = wrapper.next_id()
        wrapper.done.clear()
        wrapper.account_summary = {}
        wrapper.error = None
        tags = "AccountType,NetLiquidation,AvailableFunds,BuyingPower,Currency"
        _client.reqAccountSummary(request_id, "All", tags)
        self._wait()
        summary = wrapper.account_summary.get(account_id)
        if summary is None:
            raise TransportError("account summary is unavailable")
        return dict(summary)

    def positions(self) -> list[dict[str, Any]]:
        client, wrapper = self._require_connected()
        wrapper.done.clear()
        wrapper.positions = []
        wrapper.error = None
        client.reqPositions()
        self._wait()
        return [dict(row) for row in wrapper.positions]

    def open_orders(self) -> list[dict[str, Any]]:
        client, wrapper = self._require_connected()
        wrapper.done.clear()
        wrapper.open_orders = []
        wrapper.error = None
        client.reqOpenOrders()
        self._wait()
        return [dict(row) for row in wrapper.open_orders]

    def executions(self) -> list[dict[str, Any]]:
        client, wrapper = self._require_connected()
        request_id = wrapper.next_id()
        wrapper.done.clear()
        wrapper.executions = []
        wrapper.error = None
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
        wrapper.error = None
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

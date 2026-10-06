"""Public read-only IBKR adapter boundary (Patch 5F-1).

The :class:`ReadonlyIbkrAdapter` is the only public surface. It is
technically incapable of mutating broker state:

- it exposes no order-mutation method of any kind;
- the underlying mutation-capable transport (and through it the official
  ``EClient``) is a private attribute whose name is randomized per
  instance and which is never returned by any public member;
- every read path validates the connected account against the operator
  allowlist and fails closed on any mismatch or ambiguity;
- broker failures are reduced to bounded diagnostic codes; broker
  exception text is discarded, never logged or persisted.

Importing this module performs no I/O of any kind. A connection happens
only inside an explicit read-only operation.
"""
from __future__ import annotations

import datetime as dt
from typing import Any, Callable

from ibkr import config as config_module
from ibkr import diagnostics
from ibkr.diagnostics import AdapterError
from ibkr.transport import INTERFACE_NAME, default_transport_factory

ADAPTER_VERSION = "ibkr-readonly/v1"

_SUMMARY_FIELD_MAP = {
    "AccountType": "account_type",
    "NetLiquidation": "net_liquidation",
    "AvailableFunds": "available_funds",
    "BuyingPower": "buying_power",
    "Currency": "base_currency",
}
_POSITION_FIELDS = (
    "conid", "symbol", "sec_type", "exchange", "currency", "quantity", "average_cost",
)
_ORDER_FIELDS = (
    "order_id", "conid", "symbol", "sec_type", "exchange", "currency",
    "action", "total_quantity", "limit_price", "order_type", "status",
)
_EXECUTION_FIELDS = (
    "exec_id", "order_id", "conid", "symbol", "sec_type", "exchange",
    "side", "quantity", "price", "time",
)
_CONTRACT_FIELDS = (
    "conid", "symbol", "local_symbol", "sec_type", "exchange", "primary_exchange",
    "currency", "expiry", "strike", "right", "multiplier", "trading_class",
)


def mask_account(account_id: str) -> str:
    """Mask an account identifier for operator-facing output."""
    if not account_id:
        return "***"
    if len(account_id) <= 4:
        return "***" + account_id
    return "***" + account_id[-4:]


class ReadonlyIbkrAdapter:
    """Fail-closed, read-only IBKR broker-state boundary."""

    def __init__(
        self,
        configuration: dict[str, Any] | None = None,
        *,
        _transport_factory: Callable[[dict[str, Any]], Any] | None = None,
        _now: Callable[[], dt.datetime] | None = None,
    ) -> None:
        self._configuration = configuration  # None -> lazy load at first use
        self._transport_factory = _transport_factory or default_transport_factory
        self._now = _now or (lambda: dt.datetime.now(dt.timezone.utc))
        # Name-mangled private slot: reachable only through this class's own
        # methods, and never returned by any public member.
        self.__transport = None

    # -- internal helpers ------------------------------------------------------

    def _config(self) -> dict[str, Any]:
        if self._configuration is None:
            self._configuration = config_module.load_config()
        return self._configuration

    def _raise(self, code: str) -> None:
        raise AdapterError(code)

    def _session(self):
        """Open (or reuse) the private read-only session. No allowlist check."""
        if self.__transport is not None:
            return self.__transport
        config = self._config()
        try:
            transport = self._transport_factory(config)
            transport.connect(config)
        except diagnostics.AdapterError:
            raise
        except Exception as exc:
            raise AdapterError(diagnostics.classify_exception(exc), cause=exc) from None
        self.__transport = transport
        return transport

    def _verified_session(self):
        """Return (transport, connected_account) with the allowlist enforced.

        Any failure closes the session before propagating a bounded error,
        so a provenance failure never leaves a live session behind.
        """
        transport = self._session()
        try:
            accounts = transport.managed_accounts()
            return transport, self._verify_accounts(accounts)
        except diagnostics.AdapterError:
            # _verify_accounts already closed the session.
            raise
        except Exception as exc:
            self._close_quietly()
            raise AdapterError(diagnostics.classify_exception(exc), cause=exc) from None

    def _close_quietly(self) -> None:
        transport = self.__transport
        self.__transport = None
        if transport is not None:
            try:
                transport.disconnect()
            except Exception:
                pass

    def _verify_accounts(self, accounts: list[str]) -> str:
        expected = str(self._config()["expected_account_id"])
        if not accounts:
            self._close_quietly()
            self._raise("session-unavailable")
        if len(accounts) > 1:
            self._close_quietly()
            self._raise("multiple-accounts")
        connected = str(accounts[0])
        if connected != expected:
            self._close_quietly()
            self._raise("unexpected-account")
        return connected

    def _read(self, operation_name: str, reader) -> Any:
        """Run one read-only operation with a fresh verified session."""
        del operation_name  # diagnostics only; no dynamic dispatch on it
        transport, connected = self._verified_session()
        try:
            result = reader(transport, connected)
        except diagnostics.AdapterError:
            self._close_quietly()
            raise
        except Exception as exc:
            self._close_quietly()
            raise AdapterError(diagnostics.classify_exception(exc), cause=exc) from None
        self._close_quietly()
        return result

    # -- public read-only surface ---------------------------------------------

    @property
    def interface(self) -> str:
        return INTERFACE_NAME

    def status(self) -> dict[str, Any]:
        """Read-only connection/account snapshot; never mutates broker state.

        Infrastructure/configuration failures return a bounded diagnostic
        document; provenance failures (wrong or ambiguous account) raise,
        consistent with the fail-closed allowlist boundary.
        """
        config = self._config()
        masked = mask_account(str(config["expected_account_id"]))
        base: dict[str, Any] = {
            "adapter_version": ADAPTER_VERSION,
            "timestamp": self._now().astimezone(dt.timezone.utc)
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z"),
            "interface": self.interface,
            "connected": False,
            "environment": config["environment"],
            "account_match": False,
            "account_id_masked": masked,
            "positions_count": None,
            "open_orders_count": None,
            "diagnostic_code": None,
        }
        try:
            transport, connected = self._verified_session()
        except AdapterError as exc:
            if exc.code in ("unexpected-account", "multiple-accounts", "session-unavailable"):
                raise
            base["diagnostic_code"] = exc.code
            return base
        try:
            summary = transport.account_summary(connected)
            positions = transport.positions()
            open_orders = transport.open_orders()
        except AdapterError as exc:
            self._close_quietly()
            base["diagnostic_code"] = exc.code
            return base
        except Exception as exc:
            self._close_quietly()
            base["diagnostic_code"] = diagnostics.classify_exception(exc)
            return base
        base.update(
            {
                "connected": True,
                "account_match": True,
                "base_currency": _as_number(summary.get("Currency"), str),
                "net_liquidation": _as_number(summary.get("NetLiquidation"), float),
                "available_funds": _as_number(summary.get("AvailableFunds"), float),
                "positions_count": len(positions),
                "open_orders_count": len(open_orders),
            }
        )
        self._close_quietly()
        return base

    def account_summary(self) -> dict[str, Any]:
        """Read-only normalized account summary for the allowlisted account."""
        config = self._config()
        masked = mask_account(str(config["expected_account_id"]))

        def reader(transport, connected) -> dict[str, Any]:
            summary = transport.account_summary(connected)
            return {
                "adapter_version": ADAPTER_VERSION,
                "environment": config["environment"],
                "account_id_masked": mask_account(connected),
                "account_type": _as_number(summary.get("AccountType"), str),
                "base_currency": _as_number(summary.get("Currency"), str),
                "net_liquidation": _as_number(summary.get("NetLiquidation"), float),
                "available_funds": _as_number(summary.get("AvailableFunds"), float),
                "buying_power": _as_number(summary.get("BuyingPower"), float),
                "delivered_account_masked": masked,
            }

        return self._read("account_summary", reader)

    def positions(self) -> list[dict[str, Any]]:
        """Read-only normalized positions; deterministic ordering."""

        def reader(transport, connected) -> list[dict[str, Any]]:
            rows = transport.positions()
            normalized = []
            for row in rows:
                if str(row.get("account")) != connected:
                    continue
                record = {"account_id_masked": mask_account(connected)}
                for field in _POSITION_FIELDS:
                    record[field] = row.get(field)
                normalized.append(record)
            return sorted(normalized, key=lambda r: (str(r.get("conid")), str(r.get("symbol"))))

        return self._read("positions", reader)

    def open_orders(self) -> list[dict[str, Any]]:
        """Read-only normalized open orders; never altered."""

        def reader(transport, connected) -> list[dict[str, Any]]:
            rows = transport.open_orders()
            normalized = []
            for row in rows:
                record = {"account_id_masked": mask_account(connected)}
                for field in _ORDER_FIELDS:
                    record[field] = row.get(field)
                normalized.append(record)
            return sorted(normalized, key=lambda r: (str(r.get("order_id")), str(r.get("conid"))))

        return self._read("open_orders", reader)

    def executions(self) -> list[dict[str, Any]]:
        """Read-only normalized recent executions/fills; never altered."""

        def reader(transport, connected) -> list[dict[str, Any]]:
            rows = transport.executions()
            normalized = []
            for row in rows:
                record = {"account_id_masked": mask_account(connected)}
                for field in _EXECUTION_FIELDS:
                    record[field] = row.get(field)
                normalized.append(record)
            return sorted(normalized, key=lambda r: (str(r.get("time")), str(r.get("exec_id"))))

        return self._read("executions", reader)

    def lookup_contract(
        self,
        symbol: str,
        sec_type: str,
        *,
        currency: str | None = None,
        exchange: str | None = None,
        require_unique: bool = True,
    ) -> dict[str, Any] | None:
        """Read-only contract discovery; returns canonical metadata or None.

        Zero matches raise ``contract-not-found``; multiple matches with
        ``require_unique=True`` raise ``contract-ambiguous``. No order
        object is ever constructed and no automatic mapping is performed.
        """

        def reader(transport, connected) -> dict[str, Any] | None:
            del connected  # allowlist already enforced by _verified_session
            matches = transport.contract_details(
                symbol, sec_type, currency=currency, exchange=exchange
            )
            if not matches:
                self._raise("contract-not-found")
            if require_unique and len(matches) > 1:
                self._raise("contract-ambiguous")
            best = matches[0]
            record: dict[str, Any] = {}
            for field in _CONTRACT_FIELDS:
                record[field] = best.get(field)
            return record

        return self._read("lookup_contract", reader)

    def close(self) -> None:
        """Close the private session, if one is open."""
        self._close_quietly()

    def __enter__(self) -> "ReadonlyIbkrAdapter":
        return self

    def __exit__(self, *_exc: Any) -> None:
        self.close()


def _as_number(value: Any, caster):
    if value is None:
        return None
    try:
        return caster(value)
    except (TypeError, ValueError):
        return None

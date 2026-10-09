"""Private read-only transport seam for the IBKR adapter (Patch 5F-1).

The transport layer is deliberately private. The default TWS/Gateway
transport lazily imports the official ``ibapi`` package only inside
``connect()`` — importing this module has no side effects: no connection,
no threads, no subprocesses, no credential access, and no files created.

No transport object is ever reachable through the public adapter API.
"""
from __future__ import annotations

from typing import Any

INTERFACE_NAME = "tws-gateway"


class TransportError(RuntimeError):
    """Raised by transports on connectivity or data problems.

    The adapter converts this into bounded diagnostic codes; transport
    implementations must never embed broker exception text in this error.
    """


class ReadonlyTransport:
    """Minimal synchronous read-only transport interface."""

    def connect(self, config: dict[str, Any]) -> None:  # pragma: no cover - interface
        raise NotImplementedError

    def disconnect(self) -> None:  # pragma: no cover - interface
        raise NotImplementedError

    def managed_accounts(self) -> list[str]:  # pragma: no cover - interface
        raise NotImplementedError

    def account_summary(self, account_id: str) -> dict[str, Any]:  # pragma: no cover
        raise NotImplementedError

    def positions(self) -> list[dict[str, Any]]:  # pragma: no cover - interface
        raise NotImplementedError

    def open_orders(self) -> list[dict[str, Any]]:  # pragma: no cover - interface
        raise NotImplementedError

    def executions(self) -> list[dict[str, Any]]:  # pragma: no cover - interface
        raise NotImplementedError

    def contract_details(
        self,
        symbol: str,
        sec_type: str,
        *,
        currency: str | None = None,
        exchange: str | None = None,
    ) -> list[dict[str, Any]]:  # pragma: no cover - interface
        raise NotImplementedError

    def contract_details_by_conid(self, conid: int) -> list[dict[str, Any]]:
        """Read-only exact-conId contract details (5F-2a)."""
        raise NotImplementedError

    def submit_order(self, **_kwargs: Any) -> dict[str, Any]:  # pragma: no cover - interface
        """Deliberately NOT part of the read-only transport interface (5F-3a).

        This stub exists only so the interface's negative boundary can be
        asserted in tests: the read-only transport surface has no order
        submission capability. It always raises; the PAPER execution
        transport is a separate private class in ``ibkr.paper_transport``
        and never replaces ``ReadonlyTransport`` on the adapter.
        """
        raise NotImplementedError


def default_transport_factory(config: dict[str, Any]) -> ReadonlyTransport:
    """Return the official-interface transport for the configured endpoint.

    The import of ``ibkr.transport_tws`` (and its internal ``ibapi``
    import) happens only here, i.e. only when an explicit read-only
    operation requests a connection — never at module import time.
    """
    from ibkr.transport_tws import TwsTransport

    return TwsTransport(config)

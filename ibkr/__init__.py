"""Fail-closed read-only IBKR adapter (Patch 5F-1), with a narrowly isolated,
explicitly armed PAPER execution boundary (Patch 5F-3a).

This package is an infrastructure discovery / read-only broker-state
boundary. It is deliberately NOT imported by, or connected to, the Manus
research transport, the PAPER runner, ``manus.paper_apply``, the scheduled
wrapper, or any scheduler administration surface. The existing IBKR
forbidlists in those protected PAPER modules remain correct and unchanged.

The read-only surface (:class:`ibkr.adapter.ReadonlyIbkrAdapter` and its
private transports) has no order capability of any kind: no submit, no
modify, no replace, no cancel, no exercise, no transfer, no configuration
change, and no LIVE/PAPER arming. Read-only means read-only.

Patch 5F-3a adds exactly ONE deliberate, strictly isolated exception:
``ibkr.paper_execution`` / ``ibkr.paper_execution_state`` /
``ibkr.paper_transport`` implement a manually armed, PAPER-only,
hard-capped IBKR order submission boundary. ``ReadonlyIbkrAdapter`` itself
remains incapable of broker mutation, and ``placeOrder`` exists only inside
the private PAPER execution transport. There is still no cancel, no modify,
no exercise, no transfer, and no LIVE execution path anywhere.
"""
ADAPTER_VERSION = "ibkr-readonly/v1"

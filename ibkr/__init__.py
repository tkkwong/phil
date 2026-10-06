"""Fail-closed read-only IBKR adapter (Patch 5F-1).

This package is an infrastructure discovery / read-only broker-state
boundary. It is deliberately NOT imported by, or connected to, the Manus
research transport, the PAPER runner, ``manus.paper_apply``, the scheduled
wrapper, or any scheduler administration surface. The existing IBKR
forbidlists in those protected PAPER modules remain correct and unchanged.

There is no order capability of any kind in this package: no submit, no
modify, no replace, no cancel, no exercise, no transfer, no configuration
change, and no LIVE/PAPER arming. Read-only means read-only.
"""

ADAPTER_VERSION = "ibkr-readonly/v1"

"""Read-only operator CLI for the IBKR adapter (Patch 5F-1).

Usage:

    python -m ibkr.readonly status

The only operation is ``status``: compact canonical JSON with bounded
diagnostics and masked account identifiers. No operation mutates broker
state, creates files, or touches Phil's journals or PAPER runtime state.
"""
from __future__ import annotations

import argparse
import json
import sys

from ibkr import diagnostics
from ibkr.adapter import ADAPTER_VERSION, ReadonlyIbkrAdapter
from ibkr.config import AdapterConfigError

_READ_ONLY_OPERATIONS = ("status",)

_FORBIDDEN_OPTION_MARKERS = (
    "order", "place", "cancel", "modify", "replace", "submit", "transmit",
    "exercise", "transfer", "account-number", "secret", "credential", "password",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m ibkr.readonly",
        description="Fail-closed read-only IBKR broker-state inspection.",
    )
    subparsers = parser.add_subparsers(dest="operation", required=True, metavar="{status}")
    subparsers.add_parser("status", help="read-only connection and account snapshot")
    return parser


def _reject_forbidden_options(arguments: list[str], parser: argparse.ArgumentParser) -> None:
    for option in arguments:
        if not option.startswith("--"):
            continue
        lowered = option.lower()
        if any(marker in lowered for marker in _FORBIDDEN_OPTION_MARKERS):
            parser.error("Forbidden IBKR adapter option")


def _canonical_json(document: dict) -> str:
    return json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    arguments = sys.argv[1:] if argv is None else argv
    _reject_forbidden_options(arguments, parser)
    parsed = parser.parse_args(arguments)
    if parsed.operation not in _READ_ONLY_OPERATIONS:
        parser.error("operation must be a read-only inspection")
    try:
        adapter = ReadonlyIbkrAdapter()
    except AdapterConfigError as exc:
        print(_canonical_json({
            "adapter_version": ADAPTER_VERSION,
            "connected": False,
            "diagnostic_code": exc.code,
        }))
        return 1
    try:
        document = adapter.status()
    except (diagnostics.AdapterError, AdapterConfigError) as exc:
        code = getattr(exc, "code", "unclassified")
        print(_canonical_json({
            "adapter_version": ADAPTER_VERSION,
            "connected": False,
            "diagnostic_code": code,
        }))
        return 1
    print(_canonical_json(document))
    return 0 if document.get("diagnostic_code") is None else 1


if __name__ == "__main__":
    raise SystemExit(main())

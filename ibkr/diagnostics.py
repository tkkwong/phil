"""Bounded diagnostic codes for the read-only IBKR adapter (Patch 5F-1).

Follows the Patch 5E-5a pattern: a closed vocabulary of stable codes, an
operator-facing classification, and never any broker exception text, path,
token, or secret in persisted or printed diagnostics.
"""
from __future__ import annotations

# Closed vocabulary, based on the adapter's actual code paths.
DIAGNOSTIC_CODES = frozenset(
    {
        "not-configured",
        "connection-unavailable",
        "session-unavailable",
        "unexpected-account",
        "multiple-accounts",
        "environment-ambiguous",
        "unsupported-environment",
        "broker-data-unavailable",
        "invalid-broker-response",
        "contract-not-found",
        "contract-ambiguous",
        "unclassified",
    }
)

# Operator-facing failure classification.
DIAGNOSTIC_CLASSIFICATIONS = {
    "not-configured": "configuration",
    "connection-unavailable": "infrastructure",
    "session-unavailable": "infrastructure",
    "unexpected-account": "provenance",
    "multiple-accounts": "provenance",
    "environment-ambiguous": "configuration",
    "unsupported-environment": "configuration",
    "broker-data-unavailable": "infrastructure",
    "invalid-broker-response": "infrastructure",
    "contract-not-found": "provenance",
    "contract-ambiguous": "provenance",
    "unclassified": "internal",
}

# Bounded human-readable messages, one per code. Never includes broker text.
_DIAGNOSTIC_MESSAGES = {
    "not-configured": "IBKR adapter configuration is missing or invalid",
    "connection-unavailable": "IBKR endpoint is unreachable",
    "session-unavailable": "IBKR read-only session is unavailable",
    "unexpected-account": "connected IBKR account is not the expected account",
    "multiple-accounts": "multiple connected IBKR accounts cannot be distinguished",
    "environment-ambiguous": "IBKR trading environment could not be determined",
    "unsupported-environment": "IBKR trading environment is not supported",
    "broker-data-unavailable": "IBKR broker data is unavailable",
    "invalid-broker-response": "IBKR broker response is malformed",
    "contract-not-found": "IBKR contract lookup returned no match",
    "contract-ambiguous": "IBKR contract lookup returned multiple matches",
    "unclassified": "IBKR adapter failure could not be classified",
}


class AdapterError(RuntimeError):
    """Read-only adapter failure carrying a bounded diagnostic code."""

    def __init__(self, code: str, *, cause: BaseException | None = None) -> None:
        if code not in DIAGNOSTIC_CODES:
            # Fail closed: never promote arbitrary text into a code.
            code = "unclassified"
        self.code = code
        self.classification = DIAGNOSTIC_CLASSIFICATIONS[code]
        self.cause = cause
        super().__init__(_DIAGNOSTIC_MESSAGES[code])


def classify_exception(exc: BaseException) -> str:
    """Map one broker exception to a bounded code without keeping its text."""
    del exc  # the exception text is deliberately discarded
    return "broker-data-unavailable"

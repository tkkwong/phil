"""Operator-owned, fail-closed adapter configuration (Patch 5F-1).

The configuration file is intentionally NOT part of the repository. It is
resolved from, in order:

1. ``PHIL_IBKR_CONFIG`` environment variable (a file path), or
2. ``<repo>/config/ibkr.local.json`` if it exists (gitignored), or
3. ``~/.config/phil/ibkr.json``.

Operators create it locally; no real account number, host, port, or secret
is ever committed. Expected shape (all fields required unless noted):

{
  "expected_account_id": "DU1234567",          # the single allowed account
  "environment": "PAPER",                       # "PAPER" or "LIVE"
  "host": "127.0.0.1",
  "port": 7497,                                 # 7497 paper / 4001 gateway etc.
  "client_id": 19,
  "read_only_timeout_seconds": 10               # optional, default 10
}

Missing file, missing fields, unknown fields, or an unknown environment
string fail closed before any connection attempt.
"""
from __future__ import annotations

import json
import os
import pathlib
from typing import Any

CONFIG_VERSION = "ibkr-config/v1"

_REQUIRED_FIELDS = ("expected_account_id", "environment", "host", "port", "client_id")
_ALLOWED_FIELDS = frozenset(_REQUIRED_FIELDS + ("read_only_timeout_seconds",))
_ALLOWED_ENVIRONMENTS = frozenset({"PAPER", "LIVE"})


class AdapterConfigError(RuntimeError):
    """Raised when adapter configuration is missing or invalid (fail closed)."""

    def __init__(self, message: str, *, code: str) -> None:
        super().__init__(message)
        self.code = code


def candidate_config_paths(repository_root: pathlib.Path | None = None) -> list[pathlib.Path]:
    """Return the operator-owned config search paths (never created)."""
    root = repository_root if repository_root is not None else _default_repository_root()
    paths = []
    override = os.environ.get("PHIL_IBKR_CONFIG")
    if override:
        paths.append(pathlib.Path(override))
    paths.append(root / "config" / "ibkr.local.json")
    home = os.environ.get("HOME") or str(pathlib.Path.home())
    paths.append(pathlib.Path(home) / ".config" / "phil" / "ibkr.json")
    return paths


def _default_repository_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parents[1]


def load_config(
    repository_root: pathlib.Path | None = None,
    *,
    environ: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Load and strictly validate the operator-owned adapter configuration."""
    env = os.environ if environ is None else environ
    override = env.get("PHIL_IBKR_CONFIG")
    candidates: list[pathlib.Path] = []
    if override:
        candidates.append(pathlib.Path(override))
    else:
        candidates = candidate_config_paths(repository_root)
    config_path = next((path for path in candidates if path.is_file()), None)
    if config_path is None:
        raise AdapterConfigError(
            "IBKR adapter configuration is not present", code="not-configured"
        )
    try:
        raw = config_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise AdapterConfigError(
            "IBKR adapter configuration cannot be read", code="not-configured"
        ) from exc
    try:
        document = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise AdapterConfigError(
            "IBKR adapter configuration is malformed", code="not-configured"
        ) from exc
    if not isinstance(document, dict):
        raise AdapterConfigError(
            "IBKR adapter configuration must be a JSON object", code="not-configured"
        )
    unknown = sorted(set(document) - _ALLOWED_FIELDS)
    if unknown:
        raise AdapterConfigError(
            "IBKR adapter configuration has unknown fields", code="not-configured"
        )
    missing = [field for field in _REQUIRED_FIELDS if field not in document]
    if missing:
        raise AdapterConfigError(
            "IBKR adapter configuration is missing required fields", code="not-configured"
        )
    environment = document["environment"]
    if not isinstance(environment, str) or environment not in _ALLOWED_ENVIRONMENTS:
        raise AdapterConfigError(
            "IBKR adapter environment must be PAPER or LIVE", code="environment-ambiguous"
        )
    account = document["expected_account_id"]
    if not isinstance(account, str) or not (1 <= len(account) <= 16):
        raise AdapterConfigError(
            "IBKR expected account id has an invalid shape", code="not-configured"
        )
    host = document["host"]
    if not isinstance(host, str) or not host or len(host) > 253:
        raise AdapterConfigError(
            "IBKR adapter host is invalid", code="not-configured"
        )
    port = document["port"]
    if isinstance(port, bool) or not isinstance(port, int) or not (1 <= port <= 65535):
        raise AdapterConfigError(
            "IBKR adapter port is invalid", code="not-configured"
        )
    client_id = document["client_id"]
    if isinstance(client_id, bool) or not isinstance(client_id, int) or not (0 <= client_id <= 2**31 - 1):
        raise AdapterConfigError(
            "IBKR adapter client id is invalid", code="not-configured"
        )
    timeout = document.get("read_only_timeout_seconds", 10)
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not (0 < timeout <= 120):
        raise AdapterConfigError(
            "IBKR read_only_timeout_seconds is invalid", code="not-configured"
        )
    return {
        "config_version": CONFIG_VERSION,
        "config_path": str(config_path),
        "expected_account_id": account,
        "environment": environment,
        "host": host,
        "port": port,
        "client_id": client_id,
        "read_only_timeout_seconds": float(timeout),
    }

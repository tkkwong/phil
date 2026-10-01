"""Fixed, disabled-by-default entrypoint for one unattended PAPER runner call.

This module does not install or configure a scheduler.  A future external
scheduler may invoke this module once, but the fixed local marker below is the
only authority that permits the one protected runner invocation.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import pathlib
import re
import tempfile
from typing import Any, Callable

from manus import paper_locks, paper_runner


SCHEDULER_VERSION = "scheduled-paper/v1"
_MARKER_NAME = "enabled.json"
_LAST_RUN_NAME = "last-run.json"
_MARKER_MAX_BYTES = 1024
_SUMMARY_MAX_BYTES = 2048
_SCHEDULER_CHILDREN = ("phil-manus", "scheduler")
_MARKER_FIELDS = frozenset({"scheduler_version", "enabled"})
_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_SAFE_LABEL_RE = re.compile(r"^[a-z][a-z0-9-]{0,127}$")
_SAFE_OUTCOMES = frozenset({"disabled", "enabled", "runner-result", "runner-failed-closed", "scheduler-safety-failure"})
_FORBIDDEN_OPTION_TERMS = frozenset(
    {
        "enable",
        "disable",
        "budget",
        "credit",
        "dry",
        "real",
        "live",
        "broker",
        "ibkr",
        "pearl",
        "runner",
        "root",
        "staging",
        "journal",
        "schedule",
        "interval",
        "retry",
        "loop",
        "daemon",
        "service",
        "credential",
        "token",
        "key",
        "shell",
        "command",
    }
)


class ScheduledPaperError(RuntimeError):
    """Raised when the fixed scheduled PAPER boundary fails closed."""


class ScheduledPaperSafetyError(ScheduledPaperError):
    """Raised when a runner result conflicts with permanent unattended limits."""


def _repository_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parents[1]


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _utc_timestamp(now: Callable[[], dt.datetime]) -> str:
    value = now()
    if not isinstance(value, dt.datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ScheduledPaperError("Fixed scheduler clock is unavailable")
    return value.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _resolve_scheduler_root() -> pathlib.Path:
    local_appdata = os.environ.get("LOCALAPPDATA")
    if not local_appdata:
        raise ScheduledPaperError("Fixed scheduler root is unavailable")
    return _validate_scheduler_root(pathlib.Path(local_appdata) / _SCHEDULER_CHILDREN[0] / _SCHEDULER_CHILDREN[1])


def _validate_scheduler_root(root: pathlib.Path) -> pathlib.Path:
    root = pathlib.Path(root)
    for directory in (root.parent, root):
        if paper_locks.path_is_unsafe_indirection(directory):
            raise ScheduledPaperError("Fixed scheduler root is unavailable")
    try:
        root.resolve(strict=False).relative_to(_repository_root())
    except ValueError:
        return root
    except OSError:
        raise ScheduledPaperError("Fixed scheduler root is unavailable") from None
    raise ScheduledPaperError("Fixed scheduler root must be outside the repository")


def _parse_json(raw: str, label: str) -> Any:
    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ScheduledPaperError(f"{label} is malformed")
            result[key] = value
        return result

    try:
        return json.loads(
            raw,
            object_pairs_hook=reject_duplicates,
            parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()),
        )
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError):
        raise ScheduledPaperError(f"{label} is malformed") from None


def _marker_path(root: pathlib.Path) -> pathlib.Path:
    path = root / _MARKER_NAME
    try:
        path.relative_to(root)
    except ValueError:
        raise ScheduledPaperError("Fixed scheduler marker is unavailable") from None
    return path


def _last_run_path(root: pathlib.Path) -> pathlib.Path:
    path = root / _LAST_RUN_NAME
    try:
        path.relative_to(root)
    except ValueError:
        raise ScheduledPaperError("Fixed scheduler result is unavailable") from None
    return path


def _read_enable_marker(root: pathlib.Path) -> bool:
    """Read one exact marker without creating any scheduler state."""
    root = _validate_scheduler_root(root)
    try:
        if not root.exists():
            return False
        if paper_locks.path_is_unsafe_indirection(root) or not root.is_dir():
            raise OSError("unsafe fixed scheduler root")
        marker = _marker_path(root)
        if paper_locks.path_is_unsafe_indirection(marker):
            raise OSError("unsafe fixed scheduler marker")
        if not marker.exists():
            return False
        if not marker.is_file():
            raise OSError("invalid fixed scheduler marker")
        with marker.open("rb") as handle:
            raw = handle.read(_MARKER_MAX_BYTES + 1)
    except OSError:
        raise ScheduledPaperError("Fixed scheduler marker is unavailable") from None
    if len(raw) > _MARKER_MAX_BYTES:
        raise ScheduledPaperError("Fixed scheduler marker is malformed")
    try:
        document = _parse_json(raw.decode("utf-8"), "Fixed scheduler marker")
    except UnicodeDecodeError:
        raise ScheduledPaperError("Fixed scheduler marker is malformed") from None
    if not isinstance(document, dict) or set(document) != _MARKER_FIELDS:
        raise ScheduledPaperError("Fixed scheduler marker is malformed")
    if document.get("scheduler_version") != SCHEDULER_VERSION or type(document.get("enabled")) is not bool:
        raise ScheduledPaperError("Fixed scheduler marker is malformed")
    return document["enabled"]


def _status_document(enabled: bool) -> dict[str, Any]:
    return {
        "scheduler_version": SCHEDULER_VERSION,
        "outcome": "enabled" if enabled else "disabled",
        "enabled": enabled,
    }


def status(*, _scheduler_root: pathlib.Path | None = None) -> dict[str, Any]:
    """Return fixed marker status only; this never invokes the PAPER runner."""
    root = _resolve_scheduler_root() if _scheduler_root is None else pathlib.Path(_scheduler_root)
    return _status_document(_read_enable_marker(root))


def _safe_identifier(value: Any, label: str, *, nullable: bool = True) -> str | None:
    if value is None and nullable:
        return None
    if not isinstance(value, str) or not _IDENTIFIER_RE.fullmatch(value):
        raise ScheduledPaperError(f"Scheduled runner {label} is invalid")
    return value


def _safe_label(value: Any, label: str, *, nullable: bool = True) -> str | None:
    if value is None and nullable:
        return None
    if not isinstance(value, str) or not _SAFE_LABEL_RE.fullmatch(value):
        raise ScheduledPaperError(f"Scheduled runner {label} is invalid")
    return value


def _safe_count(value: Any, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 1:
        raise ScheduledPaperError(f"Scheduled runner {label} is invalid")
    return value


def _safe_runner_summary(result: Any, now: Callable[[], dt.datetime]) -> dict[str, Any]:
    """Validate and project only bounded, non-sensitive runner result scalars."""
    required = {
        "mode",
        "dry_run",
        "cycle_id",
        "cycle_state",
        "market_id",
        "task_id",
        "intent_id",
        "application_state",
        "forecast_id",
        "placement_id",
        "new_manus_tasks",
        "logical_applications",
        "forecasts_recorded",
        "placements_recorded",
        "safe_reason",
    }
    if not isinstance(result, dict) or not required.issubset(result):
        raise ScheduledPaperError("Scheduled runner result is invalid")
    if result.get("mode") != "PAPER" or result.get("dry_run") is not False:
        raise ScheduledPaperError("Scheduled runner result is invalid")
    new_tasks = _safe_count(result.get("new_manus_tasks"), "new_manus_tasks")
    if new_tasks != 0:
        raise ScheduledPaperSafetyError("Scheduled runner reported prohibited new task authority")
    summary = {
        "scheduler_version": SCHEDULER_VERSION,
        "invocation_time": _utc_timestamp(now),
        "outcome": "runner-result",
        "cycle_id": _safe_identifier(result.get("cycle_id"), "cycle_id", nullable=False),
        "cycle_state": _safe_label(result.get("cycle_state"), "cycle_state", nullable=False),
        "market_id": _safe_identifier(result.get("market_id"), "market_id"),
        "task_id": _safe_identifier(result.get("task_id"), "task_id"),
        "intent_id": _safe_identifier(result.get("intent_id"), "intent_id"),
        "application_state": _safe_label(result.get("application_state"), "application_state"),
        "forecast_id": _safe_identifier(result.get("forecast_id"), "forecast_id"),
        "placement_id": _safe_identifier(result.get("placement_id"), "placement_id"),
        "new_manus_tasks": new_tasks,
        "logical_applications": _safe_count(result.get("logical_applications"), "logical_applications"),
        "forecasts_recorded": _safe_count(result.get("forecasts_recorded"), "forecasts_recorded"),
        "placements_recorded": _safe_count(result.get("placements_recorded"), "placements_recorded"),
        "safe_reason": _safe_label(result.get("safe_reason"), "safe_reason", nullable=False),
    }
    return summary


def _failure_summary(now: Callable[[], dt.datetime], outcome: str) -> dict[str, Any]:
    if outcome not in {"runner-failed-closed", "scheduler-safety-failure"}:
        raise ScheduledPaperError("Scheduled failure outcome is invalid")
    return {
        "scheduler_version": SCHEDULER_VERSION,
        "invocation_time": _utc_timestamp(now),
        "outcome": outcome,
    }


def _atomic_write_last_run(root: pathlib.Path, summary: dict[str, Any]) -> None:
    """Atomically replace the one fixed bounded result document."""
    try:
        encoded = (_canonical_json(summary) + "\n").encode("utf-8")
    except (TypeError, ValueError):
        raise ScheduledPaperError("Scheduled result is invalid") from None
    if len(encoded) > _SUMMARY_MAX_BYTES:
        raise ScheduledPaperError("Scheduled result is too large")
    path = _last_run_path(root)
    try:
        if paper_locks.path_is_unsafe_indirection(root) or not root.is_dir():
            raise OSError("unsafe fixed scheduler root")
        if paper_locks.path_is_unsafe_indirection(path):
            raise OSError("unsafe fixed scheduler result")
        descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=root)
    except OSError:
        raise ScheduledPaperError("Fixed scheduler result is unavailable") from None
    temporary_path = pathlib.Path(temporary_name)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
    except Exception:
        try:
            temporary_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise ScheduledPaperError("Fixed scheduler result write failed") from None


def _write_failure(root: pathlib.Path, now: Callable[[], dt.datetime], outcome: str) -> None:
    _atomic_write_last_run(root, _failure_summary(now, outcome))


def run(
    *,
    _scheduler_root: pathlib.Path | None = None,
    _runner_run: Callable[..., dict[str, Any]] = paper_runner.run,
    _now: Callable[[], dt.datetime] = lambda: dt.datetime.now(dt.timezone.utc),
) -> dict[str, Any]:
    """Invoke the protected manual runner once only when exact local enablement exists.

    Underscore parameters are isolated offline-test seams.  The production CLI
    accepts no root, runner, budget, credit, or scheduling configuration.
    """
    root = _resolve_scheduler_root() if _scheduler_root is None else pathlib.Path(_scheduler_root)
    enabled = _read_enable_marker(root)
    if not enabled:
        return _status_document(False)
    try:
        result = _runner_run(
            dry_run=False,
            manus_task_budget=0,
            manus_soft_credit_ceiling=None,
        )
    except Exception:
        _write_failure(root, _now, "runner-failed-closed")
        raise ScheduledPaperError("Protected PAPER runner failed closed") from None
    try:
        summary = _safe_runner_summary(result, _now)
    except ScheduledPaperSafetyError:
        _write_failure(root, _now, "scheduler-safety-failure")
        raise
    except ScheduledPaperError:
        _write_failure(root, _now, "runner-failed-closed")
        raise
    _atomic_write_last_run(root, summary)
    return summary


def _reject_forbidden_options(arguments: list[str], parser: argparse.ArgumentParser) -> None:
    for argument in arguments:
        if not argument.startswith("--"):
            continue
        option = argument[2:].split("=", 1)[0].lower().replace("_", "-")
        if option == "status":
            continue
        if option in _FORBIDDEN_OPTION_TERMS or any(term in option.split("-") for term in _FORBIDDEN_OPTION_TERMS):
            parser.error("Forbidden scheduled PAPER option")
        parser.error("Unsupported scheduled PAPER option")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Report or invoke one fixed disabled-by-default PAPER wrapper",
        allow_abbrev=False,
    )
    parser.add_argument("--status", action="store_true", help="read fixed local enable state only")
    return parser


def main(argv: list[str] | None = None) -> int:
    import sys

    arguments = list(sys.argv[1:] if argv is None else argv)
    parser = build_parser()
    _reject_forbidden_options(arguments, parser)
    args = parser.parse_args(arguments)
    try:
        result = status() if args.status else run()
    except ScheduledPaperError:
        parser.exit(2, "REJECTED: scheduled PAPER operation failed closed\n")
    print(_canonical_json(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

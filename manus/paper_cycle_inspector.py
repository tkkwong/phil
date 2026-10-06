#!/usr/bin/env python3
"""Strictly read-only local inspection of persisted Manus PAPER runner state.

The inspector reuses the existing fixed path-safety and validation primitives
from :mod:`manus.paper_runner` and never calls any function that persists,
repairs, or transitions state. It acquires no locks, creates no files, performs
no network or scheduler action, and exposes no filesystem paths through its
CLI. Inspection is advisory only: a reported ``researchable_now`` value
describes the persisted candidate against the exact Patch 5E-4a freshness rule
and never mutates the cycle.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import pathlib
import sys
from typing import Any, Callable

from manus import paper_apply, paper_locks, paper_runner

INSPECTOR_VERSION = "paper-cycle-inspector/v1"

_READ_ONLY_OPERATIONS = ("status", "cycle")


class PaperCycleInspectorError(RuntimeError):
    """Raised when persisted PAPER state cannot be inspected safely."""


def _frozen_clock(now: Callable[[], dt.datetime] | None) -> Callable[[], dt.datetime]:
    """Return a callable returning one frozen timestamp for a whole inspection."""
    if now is None:
        return lambda: dt.datetime.now(dt.timezone.utc)
    frozen = now()
    if not isinstance(frozen, dt.datetime) or frozen.tzinfo is None or frozen.utcoffset() is None:
        raise PaperCycleInspectorError("Inspector clock is unavailable")
    return lambda: frozen.astimezone(dt.timezone.utc)


def _canonical_timestamp(value: dt.datetime) -> str:
    return value.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _resolve_inspection_root(_runner_root: pathlib.Path | None) -> pathlib.Path:
    """Resolve the fixed runner root without creating or preparing anything."""
    try:
        if _runner_root is None:
            return paper_runner._resolve_runner_root()
        return paper_runner._validate_runner_root(pathlib.Path(_runner_root))
    except paper_runner.PaperRunnerError as exc:
        raise PaperCycleInspectorError(str(exc)) from None


def _read_cycle_state(root: pathlib.Path, cycle_id: str) -> dict[str, Any]:
    """Read and validate one persisted cycle file without any mutation."""
    try:
        path = paper_runner._cycle_path(root, cycle_id)
        raw = paper_runner._read_regular_file(path, "Cycle state")
    except paper_runner.PaperRunnerError as exc:
        raise PaperCycleInspectorError(str(exc)) from None
    if raw is None:
        raise PaperCycleInspectorError("Cycle state is unavailable")
    try:
        return paper_runner._validate_cycle(paper_runner._parse_json(raw.decode("utf-8"), "Cycle state"))
    except paper_runner.PaperRunnerError as exc:
        raise PaperCycleInspectorError(str(exc)) from None


def _bound_fixture_candidate(
    root: pathlib.Path,
    cycle: dict[str, Any],
) -> dict[str, Any]:
    """Verify the cycle/fixture binding and return the frozen candidate.

    Uses the exact invariants the runner itself enforces before research: the
    persisted fixture bytes must hash to ``fixture_sha256`` and the
    reconstructed packet must carry the persisted ``packet_id``. The requested
    candidate must exist in the packet with the persisted market_id. No rescan,
    substitution, or repair is ever attempted.
    """
    if cycle["fixture_sha256"] is None or cycle["packet_id"] is None:
        raise PaperCycleInspectorError("Cycle fixture provenance is incomplete")
    try:
        _fixture, fixture_sha256, packet = paper_runner._load_fixture(
            paper_runner._fixture_path(root, cycle["cycle_id"])
        )
    except paper_runner.PaperRunnerError as exc:
        raise PaperCycleInspectorError(str(exc)) from None
    if cycle["fixture_sha256"] != fixture_sha256 or cycle["packet_id"] != packet["packet_id"]:
        raise PaperCycleInspectorError("Cycle fixture provenance conflicts with cycle state")
    matches = [
        candidate
        for candidate in packet["candidates"]
        if candidate["candidate_id"] == cycle["candidate_id"]
        and candidate["market_id"] == cycle["market_id"]
    ]
    if len(matches) != 1:
        raise PaperCycleInspectorError("Persisted candidate is absent from the frozen fixture")
    return matches[0]


def _empty_status(clock: Callable[[], dt.datetime]) -> dict[str, Any]:
    return {
        "inspector_version": INSPECTOR_VERSION,
        "active_cycle_id": None,
        "cycle_state": None,
        "safe_reason": None,
        "candidate_id": None,
        "market_id": None,
        "candidate_end_date": None,
        "current_time_utc": _canonical_timestamp(clock()),
        "minimum_researchable_end_date": _canonical_timestamp(
            clock() + dt.timedelta(minutes=paper_runner.core_scan.PROTECTED["min_minutes_to_resolution"])
        ),
        "researchable_now": None,
        "intent_id": None,
        "task_id": None,
        "new_manus_tasks": None,
        "logical_applications": None,
        "forecast_id": None,
        "placement_id": None,
    }


def _receipt_path_for(cycle: dict[str, Any], staging_root: pathlib.Path | None) -> pathlib.Path | None:
    """Resolve one canonical receipt path without creating or mutating anything."""
    if staging_root is None:
        try:
            staging_root = paper_apply._resolve_staging_root()
        except paper_apply.PaperApplyError:
            return None
    try:
        return paper_apply._fixed_paths(staging_root, cycle["packet_id"], cycle["intent_id"])[2]
    except paper_apply.PaperApplyError:
        return None


def _persisted_rejection_code(receipt_path: pathlib.Path) -> str | None:
    """Read the bounded rejection code from one canonical application receipt.

    Strictly read-only. Only values from the closed paper_apply vocabulary are
    surfaced; historical receipts without the field report ``None``. The
    inspector never infers a code from arbitrary text.
    """
    try:
        if receipt_path.is_symlink() or not receipt_path.is_file():
            return None
        document = json.loads(receipt_path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return None
    if not isinstance(document, dict):
        return None
    code = document.get("rejection_code")
    if isinstance(code, str) and code in paper_apply.REJECTION_CODES:
        return code
    return None


def _status_document(
    root: pathlib.Path,
    cycle: dict[str, Any],
    clock: Callable[[], dt.datetime],
    *,
    cycle_id: str | None = None,
    staging_root: pathlib.Path | None = None,
) -> dict[str, Any]:
    """Build the safe operational summary for one already-validated cycle."""
    selected_or_later = cycle["fixture_sha256"] is not None
    candidate_end_date = None
    researchable_now = None
    if selected_or_later:
        candidate = _bound_fixture_candidate(root, cycle)
        candidate_end_date = candidate["end_date"]
        researchable_now = paper_runner._candidate_is_researchable(candidate, clock)
    counters = cycle["counters"]
    document = {
        "inspector_version": INSPECTOR_VERSION,
        "active_cycle_id": cycle["cycle_id"],
        "cycle_state": cycle["state"],
        "safe_reason": cycle["safe_reason"],
        "candidate_id": cycle["candidate_id"],
        "market_id": cycle["market_id"],
        "candidate_end_date": candidate_end_date,
        "current_time_utc": _canonical_timestamp(clock()),
        "minimum_researchable_end_date": _canonical_timestamp(
            clock() + dt.timedelta(minutes=paper_runner.core_scan.PROTECTED["min_minutes_to_resolution"])
        ),
        "researchable_now": researchable_now,
        "intent_id": cycle["intent_id"],
        "task_id": cycle["task_id"],
        "new_manus_tasks": counters["new_manus_tasks"],
        "logical_applications": counters["logical_applications"],
        "forecast_id": cycle["forecast_id"],
        "placement_id": cycle["placement_id"],
        # Persisted bounded diagnostic from the canonical application receipt.
        # Historical receipts without the field report null; the inspector
        # never infers a rejection code from arbitrary text.
        "rejection_code": (
            _persisted_rejection_code(
                _receipt_path_for(cycle, staging_root)
            )
            if staging_root is not None
            and cycle["state"] == "failed-terminal"
            and cycle["application_state"] in {"placement-rejected", "forecast-rejected", "error"}
            and cycle["packet_id"] is not None
            and cycle["intent_id"] is not None
            else None
        ),
    }
    if cycle_id is not None:
        document["requested_cycle_id"] = cycle_id
        document["is_active_cycle"] = document["active_cycle_id"] == cycle_id
    return document


def status(
    *,
    now: Callable[[], dt.datetime] | None = None,
    _runner_root: pathlib.Path | None = None,
    _staging_root: pathlib.Path | None = None,
) -> dict[str, Any]:
    """Return the safe operational status of the persisted active cycle.

    ``_staging_root`` is an internal test seam pointing at the fixed external
    application staging root; inspection never writes to it.
    """
    clock = _frozen_clock(now)
    root = _resolve_inspection_root(_runner_root)
    try:
        active = paper_runner._read_active_cycle(root)
    except paper_runner.PaperRunnerError as exc:
        raise PaperCycleInspectorError(str(exc)) from None
    if active is None:
        return _empty_status(clock)
    cycle, _cycle_directory_path = active
    return _status_document(root, cycle, clock, staging_root=_staging_root)


def cycle_status(
    cycle_id: str,
    *,
    now: Callable[[], dt.datetime] | None = None,
    _runner_root: pathlib.Path | None = None,
    _staging_root: pathlib.Path | None = None,
) -> dict[str, Any]:
    """Return the safe operational status of one explicitly requested cycle."""
    clock = _frozen_clock(now)
    root = _resolve_inspection_root(_runner_root)
    try:
        paper_runner._cycle_directory(root, cycle_id)
    except paper_runner.PaperRunnerError as exc:
        raise PaperCycleInspectorError(str(exc)) from None
    cycle = _read_cycle_state(root, cycle_id)
    try:
        active = paper_runner._read_active_cycle(root)
    except paper_runner.PaperRunnerError:
        active = None  # a malformed active pointer does not block explicit inspection
    if active is not None and active[0]["cycle_id"] == cycle_id:
        cycle = active[0]
    return _status_document(root, cycle, clock, cycle_id=cycle_id, staging_root=_staging_root)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="manus.paper_cycle_inspector",
        description="Read-only inspection of persisted Manus PAPER runner state.",
    )
    subparsers = parser.add_subparsers(dest="operation", required=True, metavar="{status,cycle}")
    subparsers.add_parser("status", help="inspect the persisted active cycle")
    cycle_parser = subparsers.add_parser("cycle", help="inspect one persisted cycle by id")
    cycle_parser.add_argument("cycle_id", help="cycle id (UUIDv4)")
    return parser


def _canonical_json(document: dict[str, Any]) -> str:
    return json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    if arguments.operation not in _READ_ONLY_OPERATIONS:
        parser.error("operation must be a read-only inspection")
    try:
        if arguments.operation == "status":
            document = status()
        else:
            document = cycle_status(arguments.cycle_id)
    except PaperCycleInspectorError as exc:
        print(f"inspector: {exc}", file=sys.stderr)
        return 1
    print(_canonical_json(document))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

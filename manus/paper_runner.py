"""Bounded manual orchestration for one protected Manus PAPER cycle.

The runner owns only a fixed outer cycle receipt and lock. Protected scanning,
research transport, intent validation, forecast recording, and PAPER placement
remain in their existing modules. This manual module has no scheduler, service,
broker, credential reader, direct journal writer, or HTTP client.
"""
from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import pathlib
import re
import tempfile
import uuid
from typing import Any, Callable

from core import scan as core_scan
from manus import paper_apply, paper_locks, research_transport
from manus.paper_cycle_guardian import SCAN_SOURCE_FIELDS, GuardianValidationError, prepare_packet


RUNNER_VERSION = "paper-runner/v2"
MAX_SELECTED_CANDIDATES_PER_CYCLE = 1
MAX_NEW_MANUS_TASKS_PER_CYCLE = 1
MAX_LOGICAL_APPLICATIONS_PER_CYCLE = 1
SCAN_MAX_CANDIDATES = 20

_RUNNER_CHILDREN = ("phil-manus", "runner")
_CYCLE_STATES = frozenset(
    {
        "prepared",
        "scanned",
        "selected",
        "research-pending",
        "research-completed",
        "application-pending",
        "completed",
        "completed-no-candidate",
        "failed-terminal",
    }
)
_TERMINAL_STATES = frozenset({"completed", "completed-no-candidate", "failed-terminal"})
_SAFE_REASON_VALUES = frozenset(
    {
        "none",
        "no-provider-tag-eligible-candidate",
        "current-invocation-research-authorization-required",
        "research-transport-reconciliation-required",
        "research-transport-failed",
        "application-reconciliation-required",
        "paper-placement-rejected",
        "paper-forecast-rejected",
        "paper-application-error",
        "selected-candidate-no-longer-researchable",
    }
)
_ALLOWED_PROVIDER_TAG_IDS = frozenset({"1", "21", "64"})
_EXCLUDED_PROVIDER_TAG_IDS = frozenset(
    {
        "2",
        "1597",
        "101206",
        "101252",
        "104743",
        "100265",
        "126",
        "100285",
        "102305",
        "104010",
        "104039",
        "104608",
        "415",
    }
)
_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_DECIMAL_IDENTIFIER_RE = re.compile(r"^[0-9]{1,64}$")
_TAG_SLUG_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_APPLICATION_TERMINAL_STATES = frozenset(
    {
        "completed-no-placement",
        "completed-placement",
        "placement-rejected",
        "forecast-rejected",
        "error",
    }
)
_CYCLE_FIELDS = frozenset(
    {
        "cycle_version",
        "cycle_id",
        "state",
        "created_at",
        "updated_at",
        "fixture_sha256",
        "packet_id",
        "candidate_id",
        "market_id",
        "selection_evidence",
        "selection_evidence_sha256",
        "task_id",
        "intent_id",
        "application_state",
        "forecast_id",
        "placement_id",
        "safe_reason",
        "hard_limits",
        "counters",
        "advisory_soft_credit_ceiling",
    }
)
_ACTIVE_POINTER_FIELDS = frozenset({"runner_version", "cycle_id", "state"})
_FORBIDDEN_OPTION_TERMS = frozenset(
    {
        "real",
        "live",
        "broker",
        "ibkr",
        "pearl",
        "shell",
        "command",
        "path",
        "root",
        "staging",
        "journal",
        "ledger",
        "forecast",
        "output",
        "schedule",
        "daemon",
        "service",
        "loop",
        "credential",
        "token",
        "key",
    }
)


class PaperRunnerError(RuntimeError):
    """Raised when a bounded manual cycle cannot safely proceed."""


def _repository_root() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parents[1]


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def _utc_timestamp(now: Callable[[], dt.datetime]) -> str:
    value = now()
    if not isinstance(value, dt.datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise PaperRunnerError("Runner clock is unavailable")
    return value.astimezone(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _require_uuid4(value: Any, label: str) -> str:
    if not isinstance(value, str):
        raise PaperRunnerError(f"{label} is invalid")
    try:
        parsed = uuid.UUID(value)
    except (ValueError, AttributeError) as exc:
        raise PaperRunnerError(f"{label} is invalid") from exc
    if parsed.version != 4 or str(parsed) != value:
        raise PaperRunnerError(f"{label} is invalid")
    return value


def _require_identifier(value: Any, label: str, *, nullable: bool = False) -> str | None:
    if value is None and nullable:
        return None
    if not isinstance(value, str) or not _IDENTIFIER_RE.fullmatch(value):
        raise PaperRunnerError(f"{label} is invalid")
    return value


def _require_sha256(value: Any, label: str, *, nullable: bool = False) -> str | None:
    if value is None and nullable:
        return None
    if not isinstance(value, str) or not _SHA256_RE.fullmatch(value):
        raise PaperRunnerError(f"{label} is invalid")
    return value


def _resolve_runner_root() -> pathlib.Path:
    local_appdata = os.environ.get("LOCALAPPDATA")
    if not local_appdata:
        raise PaperRunnerError("Fixed PAPER runner root is unavailable")
    return _validate_runner_root(pathlib.Path(local_appdata) / _RUNNER_CHILDREN[0] / _RUNNER_CHILDREN[1])


def _validate_runner_root(root: pathlib.Path) -> pathlib.Path:
    root = pathlib.Path(root)
    for directory in (root.parent, root):
        if paper_locks.path_is_unsafe_indirection(directory):
            raise PaperRunnerError("Fixed PAPER runner root is unavailable")
    try:
        root.resolve(strict=False).relative_to(_repository_root())
    except ValueError:
        return root
    raise PaperRunnerError("Fixed PAPER runner root must be outside the repository")


def _prepare_runner_root(root: pathlib.Path) -> pathlib.Path:
    root = _validate_runner_root(root)
    cycles = root / "cycles"
    try:
        root.mkdir(parents=True, exist_ok=True)
        for directory in (root, cycles):
            directory.mkdir(exist_ok=True)
            if paper_locks.path_is_unsafe_indirection(directory) or not directory.is_dir():
                raise OSError("unsafe runner directory")
    except OSError:
        raise PaperRunnerError("Fixed PAPER runner root is unavailable") from None
    return root


def _safe_child(parent: pathlib.Path, component: str, label: str) -> pathlib.Path:
    if not isinstance(component, str) or not component or "/" in component or "\\" in component or component in {".", ".."}:
        raise PaperRunnerError(f"{label} path is invalid")
    path = parent / component
    try:
        path.relative_to(parent)
    except ValueError:
        raise PaperRunnerError(f"{label} path is invalid") from None
    return path


def _parse_json(raw: str, label: str) -> Any:
    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise PaperRunnerError(f"{label} is malformed")
            result[key] = value
        return result

    try:
        return json.loads(raw, object_pairs_hook=reject_duplicates, parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()))
    except (UnicodeDecodeError, ValueError, json.JSONDecodeError):
        raise PaperRunnerError(f"{label} is malformed") from None


def _read_regular_file(path: pathlib.Path, label: str) -> bytes | None:
    if (
        paper_locks.path_is_unsafe_indirection(path)
        or paper_locks.path_is_unsafe_indirection(path.parent)
    ):
        raise PaperRunnerError(f"{label} is unavailable")
    if not path.exists():
        return None
    if not path.is_file():
        raise PaperRunnerError(f"{label} is unavailable")
    try:
        return path.read_bytes()
    except OSError:
        raise PaperRunnerError(f"{label} is unavailable") from None


def _atomic_write_json(path: pathlib.Path, document: dict[str, Any], label: str) -> None:
    directory = path.parent
    try:
        if (
            paper_locks.path_is_unsafe_indirection(directory)
            or not directory.is_dir()
            or paper_locks.path_is_unsafe_indirection(path)
        ):
            raise OSError("unsafe runner path")
        descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=directory)
    except OSError:
        raise PaperRunnerError(f"{label} is unavailable") from None
    temporary_path = pathlib.Path(temporary_name)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(_canonical_json(document))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
    except Exception:
        try:
            temporary_path.unlink(missing_ok=True)
        except OSError:
            pass
        raise PaperRunnerError(f"{label} write failed") from None


def _limits() -> dict[str, int]:
    return {
        "max_selected_candidates": MAX_SELECTED_CANDIDATES_PER_CYCLE,
        "max_new_manus_tasks": MAX_NEW_MANUS_TASKS_PER_CYCLE,
        "max_logical_applications": MAX_LOGICAL_APPLICATIONS_PER_CYCLE,
    }


def _new_cycle(cycle_id: str, now: Callable[[], dt.datetime], ceiling: int | None) -> dict[str, Any]:
    _require_uuid4(cycle_id, "cycle_id")
    timestamp = _utc_timestamp(now)
    return {
        "cycle_version": RUNNER_VERSION,
        "cycle_id": cycle_id,
        "state": "prepared",
        "created_at": timestamp,
        "updated_at": timestamp,
        "fixture_sha256": None,
        "packet_id": None,
        "candidate_id": None,
        "market_id": None,
        "selection_evidence": None,
        "selection_evidence_sha256": None,
        "task_id": None,
        "intent_id": None,
        "application_state": None,
        "forecast_id": None,
        "placement_id": None,
        "safe_reason": "none",
        "hard_limits": _limits(),
        # This is a logical identity counter. It is persisted before the first
        # application attempt and remains one while paper_apply reconciles the
        # same fixed intent on later runner invocations.
        "counters": {"candidates_selected": 0, "new_manus_tasks": 0, "logical_applications": 0},
        # Advisory metadata only. It is never read to grant task creation.
        "advisory_soft_credit_ceiling": ceiling,
    }


def _normalize_provider_metadata(value: Any) -> dict[str, Any] | None:
    """Validate the exact normalized scanner evidence without classifying text."""
    if not isinstance(value, dict) or set(value) != {"market_tags_status", "market_tags"}:
        return None
    if value.get("market_tags_status") != "ok" or not isinstance(value.get("market_tags"), list):
        return None
    tags = value["market_tags"]
    if len(tags) > 32:
        return None
    normalized: list[dict[str, str]] = []
    seen_ids: set[str] = set()
    for tag in tags:
        if not isinstance(tag, dict) or set(tag) != {"id", "slug", "label"}:
            return None
        tag_id, slug, label = tag.get("id"), tag.get("slug"), tag.get("label")
        if not isinstance(tag_id, str) or not _DECIMAL_IDENTIFIER_RE.fullmatch(tag_id):
            return None
        if not isinstance(slug, str) or not _TAG_SLUG_RE.fullmatch(slug):
            return None
        if not isinstance(label, str) or not label or len(label) > 256 or not label.isprintable():
            return None
        if tag_id in seen_ids:
            return None
        seen_ids.add(tag_id)
        normalized.append({"id": tag_id, "slug": slug, "label": label})
    # Round trip through canonical JSON freezes the protected normalized envelope
    # rather than retaining a caller-provided mutable object.
    return json.loads(_canonical_json({"market_tags_status": "ok", "market_tags": normalized}))


def _provider_metadata_is_eligible(value: Any) -> bool:
    metadata = _normalize_provider_metadata(value)
    if metadata is None:
        return False
    tag_ids = {tag["id"] for tag in metadata["market_tags"]}
    if tag_ids & _EXCLUDED_PROVIDER_TAG_IDS:
        return False
    return bool(tag_ids & _ALLOWED_PROVIDER_TAG_IDS)


def _parse_utc_timestamp(value: Any) -> dt.datetime | None:
    """Return a timezone-aware UTC datetime for a fixture end_date, else None."""
    if not isinstance(value, str) or not value:
        return None
    text = value.strip()
    if text[-1:] in ("Z", "z"):
        text = text[:-1] + "+00:00"
    try:
        parsed = dt.datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return None
    return parsed.astimezone(dt.timezone.utc)


def _candidate_is_researchable(candidate: dict[str, Any], now: Callable[[], dt.datetime]) -> bool:
    """Independent runner freshness gate using the injected runner clock.

    Scanner filtering is not trusted enforcement. A frozen candidate may only
    consume research authority while its trusted fixture end_date still leaves
    the protected minimum time to resolution; boundary equality is valid.
    """
    end = _parse_utc_timestamp(candidate.get("end_date"))
    if end is None:
        return False
    floor = now() + dt.timedelta(minutes=core_scan.PROTECTED["min_minutes_to_resolution"])
    return end >= floor


def _selection_evidence(market_id: str, provider_metadata: Any) -> dict[str, Any]:
    _require_identifier(market_id, "selection evidence market_id")
    metadata = _normalize_provider_metadata(provider_metadata)
    if metadata is None or not _provider_metadata_is_eligible(metadata):
        raise PaperRunnerError("Selection evidence is invalid or ineligible")
    return {"market_id": market_id, "provider_metadata": metadata}


def _selection_evidence_sha256(evidence: dict[str, Any]) -> str:
    return hashlib.sha256(_canonical_json(evidence).encode("utf-8")).hexdigest()


def _validate_cycle(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != _CYCLE_FIELDS:
        raise PaperRunnerError("Cycle state is malformed")
    if value.get("cycle_version") != RUNNER_VERSION:
        raise PaperRunnerError("Cycle state version is incompatible")
    _require_uuid4(value.get("cycle_id"), "cycle_id")
    state = value.get("state")
    if state not in _CYCLE_STATES:
        raise PaperRunnerError("Cycle state is invalid")
    for field in ("created_at", "updated_at"):
        timestamp = value.get(field)
        if not isinstance(timestamp, str) or not timestamp.endswith("Z"):
            raise PaperRunnerError("Cycle timestamp is invalid")
    _require_sha256(value.get("fixture_sha256"), "fixture_sha256", nullable=True)
    _require_identifier(value.get("packet_id"), "packet_id", nullable=True)
    _require_identifier(value.get("candidate_id"), "candidate_id", nullable=True)
    market_id = _require_identifier(value.get("market_id"), "market_id", nullable=True)
    _require_sha256(value.get("selection_evidence_sha256"), "selection_evidence_sha256", nullable=True)
    evidence = value.get("selection_evidence")
    if evidence is not None:
        if not isinstance(evidence, dict) or set(evidence) != {"market_id", "provider_metadata"}:
            raise PaperRunnerError("Cycle selection evidence is malformed")
        if evidence.get("market_id") != market_id:
            raise PaperRunnerError("Cycle selection evidence market_id conflicts")
        normalized = _normalize_provider_metadata(evidence.get("provider_metadata"))
        if normalized is None or normalized != evidence.get("provider_metadata") or not _provider_metadata_is_eligible(normalized):
            raise PaperRunnerError("Cycle selection evidence is invalid or ineligible")
        if value["selection_evidence_sha256"] != _selection_evidence_sha256(evidence):
            raise PaperRunnerError("Cycle selection evidence hash conflicts")
    elif value.get("selection_evidence_sha256") is not None:
        raise PaperRunnerError("Cycle selection evidence hash conflicts")
    _require_identifier(value.get("task_id"), "task_id", nullable=True)
    intent_id = value.get("intent_id")
    if intent_id is not None:
        _require_uuid4(intent_id, "intent_id")
    for field in ("application_state", "forecast_id", "placement_id"):
        _require_identifier(value.get(field), field, nullable=True)
    if value.get("safe_reason") not in _SAFE_REASON_VALUES:
        raise PaperRunnerError("Cycle safe reason is invalid")
    if value.get("hard_limits") != _limits():
        raise PaperRunnerError("Cycle hard limits are incompatible")
    counters = value.get("counters")
    expected_counter_keys = {"candidates_selected", "new_manus_tasks", "logical_applications"}
    if not isinstance(counters, dict) or set(counters) != expected_counter_keys:
        raise PaperRunnerError("Cycle counters are malformed")
    for key, maximum in (
        ("candidates_selected", MAX_SELECTED_CANDIDATES_PER_CYCLE),
        ("new_manus_tasks", MAX_NEW_MANUS_TASKS_PER_CYCLE),
        ("logical_applications", MAX_LOGICAL_APPLICATIONS_PER_CYCLE),
    ):
        count = counters.get(key)
        if isinstance(count, bool) or not isinstance(count, int) or not 0 <= count <= maximum:
            raise PaperRunnerError("Cycle counter is invalid")
    ceiling = value.get("advisory_soft_credit_ceiling")
    if ceiling is not None and (isinstance(ceiling, bool) or not isinstance(ceiling, int) or not 1 <= ceiling <= 100):
        raise PaperRunnerError("Cycle advisory credit ceiling is invalid")

    preselection = {"prepared", "scanned"}
    selected_or_later = {"selected", "research-pending", "research-completed", "application-pending", "completed", "failed-terminal"}
    if state == "prepared":
        if any(value[field] is not None for field in ("fixture_sha256", "packet_id", "candidate_id", "market_id", "selection_evidence", "intent_id")):
            raise PaperRunnerError("Prepared cycle has conflicting provenance")
        if any(counters.values()):
            raise PaperRunnerError("Prepared cycle has conflicting counters")
    if state in _CYCLE_STATES - {"prepared", "completed-no-candidate"}:
        if value["fixture_sha256"] is None or value["packet_id"] is None:
            raise PaperRunnerError("Cycle fixture provenance is incomplete")
    if state == "scanned":
        if any(value[field] is not None for field in ("candidate_id", "market_id", "selection_evidence", "intent_id")):
            raise PaperRunnerError("Scanned cycle has conflicting selection provenance")
        if any(counters.values()):
            raise PaperRunnerError("Scanned cycle has conflicting counters")
    if state in selected_or_later:
        if value["candidate_id"] is None or market_id is None or evidence is None or counters["candidates_selected"] != 1:
            raise PaperRunnerError("Cycle selection provenance is incomplete")
    pre_research_stale = state == "failed-terminal" and value["safe_reason"] == "selected-candidate-no-longer-researchable"
    if pre_research_stale:
        # Narrow exception for the pre-research stale terminal only: research
        # never ran, so no research/application provenance may exist.
        if any(value[field] is not None for field in ("intent_id", "task_id", "application_state", "forecast_id", "placement_id")):
            raise PaperRunnerError("Pre-research stale terminal has conflicting provenance")
        if counters != {"candidates_selected": 1, "new_manus_tasks": 0, "logical_applications": 0}:
            raise PaperRunnerError("Pre-research stale terminal has conflicting counters")
    elif state in {"research-completed", "application-pending", "completed", "failed-terminal"} and value["intent_id"] is None:
        raise PaperRunnerError("Cycle research provenance is incomplete")
    if state in {"prepared", "scanned", "selected", "research-pending", "research-completed"} and counters["logical_applications"] != 0:
        raise PaperRunnerError("Cycle logical application counter conflicts with cycle state")
    if state in {"application-pending", "completed", "failed-terminal"} and not pre_research_stale and counters["logical_applications"] != 1:
        raise PaperRunnerError("Cycle logical application counter conflicts with cycle state")
    if state in {"completed", "failed-terminal"} and not pre_research_stale and value["application_state"] not in _APPLICATION_TERMINAL_STATES:
        raise PaperRunnerError("Cycle terminal application state is invalid")
    return value


def _cycle_directory(root: pathlib.Path, cycle_id: str) -> pathlib.Path:
    _require_uuid4(cycle_id, "cycle_id")
    cycles = _safe_child(root, "cycles", "runner cycles")
    return _safe_child(cycles, cycle_id, "cycle")


def _cycle_path(root: pathlib.Path, cycle_id: str) -> pathlib.Path:
    return _safe_child(_cycle_directory(root, cycle_id), "cycle.json", "cycle state")


def _fixture_path(root: pathlib.Path, cycle_id: str) -> pathlib.Path:
    return _safe_child(_cycle_directory(root, cycle_id), "fixture.json", "cycle fixture")


def _active_pointer_path(root: pathlib.Path) -> pathlib.Path:
    return _safe_child(root, "active-cycle.json", "active cycle")


def _read_active_cycle(root: pathlib.Path) -> tuple[dict[str, Any], pathlib.Path] | None:
    raw = _read_regular_file(_active_pointer_path(root), "Active cycle pointer")
    if raw is None:
        return None
    value = _parse_json(raw.decode("utf-8"), "Active cycle pointer")
    if not isinstance(value, dict) or set(value) != _ACTIVE_POINTER_FIELDS or value.get("runner_version") != RUNNER_VERSION:
        raise PaperRunnerError("Active cycle pointer is malformed")
    try:
        cycle_id = _require_uuid4(value.get("cycle_id"), "active cycle id")
    except PaperRunnerError:
        raise PaperRunnerError("Active cycle pointer is malformed") from None
    if value.get("state") not in _CYCLE_STATES:
        raise PaperRunnerError("Active cycle pointer state is invalid")
    path = _cycle_path(root, cycle_id)
    cycle_raw = _read_regular_file(path, "Cycle state")
    if cycle_raw is None:
        raise PaperRunnerError("Active cycle state is unavailable")
    cycle = _validate_cycle(_parse_json(cycle_raw.decode("utf-8"), "Cycle state"))
    if cycle["cycle_id"] != cycle_id or cycle["state"] != value["state"]:
        raise PaperRunnerError("Active cycle pointer conflicts with cycle state")
    return cycle, _cycle_directory(root, cycle_id)


def _persist_cycle(root: pathlib.Path, cycle: dict[str, Any], now: Callable[[], dt.datetime]) -> dict[str, Any]:
    cycle = dict(cycle)
    cycle["updated_at"] = _utc_timestamp(now)
    cycle = _validate_cycle(cycle)
    directory = _cycle_directory(root, cycle["cycle_id"])
    try:
        directory.mkdir(parents=True, exist_ok=True)
        if paper_locks.path_is_unsafe_indirection(directory) or not directory.is_dir():
            raise OSError("unsafe cycle directory")
    except OSError:
        raise PaperRunnerError("Cycle state is unavailable") from None
    _atomic_write_json(_cycle_path(root, cycle["cycle_id"]), cycle, "Cycle state")
    _atomic_write_json(
        _active_pointer_path(root),
        {"runner_version": RUNNER_VERSION, "cycle_id": cycle["cycle_id"], "state": cycle["state"]},
        "Active cycle pointer",
    )
    return cycle


def _load_fixture(path: pathlib.Path) -> tuple[dict[str, Any], str, dict[str, Any]]:
    raw = _read_regular_file(path, "Cycle fixture")
    if raw is None:
        raise PaperRunnerError("Cycle fixture is unavailable")
    fixture = _parse_json(raw.decode("utf-8"), "Cycle fixture")
    try:
        packet = prepare_packet(fixture)
    except GuardianValidationError:
        raise PaperRunnerError("Cycle fixture is invalid") from None
    return fixture, hashlib.sha256(raw).hexdigest(), packet


def _write_fixture(path: pathlib.Path, fixture: dict[str, Any]) -> str:
    try:
        prepare_packet(fixture)
    except GuardianValidationError:
        raise PaperRunnerError("Protected scan output cannot form a trusted fixture") from None
    _atomic_write_json(path, fixture, "Cycle fixture")
    raw = _read_regular_file(path, "Cycle fixture")
    if raw is None:
        raise PaperRunnerError("Cycle fixture is unavailable")
    return hashlib.sha256(raw).hexdigest()


def _fixture_from_scan(candidates: list[dict[str, Any]], now: Callable[[], dt.datetime]) -> tuple[dict[str, Any], dict[str, dict[str, Any] | None]]:
    """Project protected scanner records into guardian data and separate evidence."""
    if not isinstance(candidates, list):
        raise PaperRunnerError("Protected scan output is invalid")
    fixture_candidates: list[dict[str, Any]] = []
    metadata_by_market: dict[str, dict[str, Any] | None] = {}
    for record in candidates:
        if not isinstance(record, dict):
            raise PaperRunnerError("Protected scan output is invalid")
        market_id = _require_identifier(record.get("market_id"), "scan market_id")
        if market_id in metadata_by_market:
            raise PaperRunnerError("Protected scan output has duplicate market_id")
        # SCAN_SOURCE_FIELDS is the guardian's public immutable contract. This
        # deliberate projection excludes provider_metadata from the fixture.
        fixture_candidates.append({key: record[key] for key in SCAN_SOURCE_FIELDS if key in record})
        metadata_by_market[market_id] = _normalize_provider_metadata(record.get("provider_metadata"))
    return {"generated_at": _utc_timestamp(now), "candidates": fixture_candidates}, metadata_by_market


def _call_protected_scan(scan_candidates: Callable[..., list[dict[str, Any]]]) -> list[dict[str, Any]]:
    return scan_candidates(include_provider_metadata=True, max_candidates=SCAN_MAX_CANDIDATES)


def _ensure_fixture(
    root: pathlib.Path,
    cycle: dict[str, Any],
    now: Callable[[], dt.datetime],
    scan_candidates: Callable[..., list[dict[str, Any]]],
) -> tuple[dict[str, Any], dict[str, Any], pathlib.Path, dict[str, dict[str, Any] | None] | None]:
    fixture_path = _fixture_path(root, cycle["cycle_id"])
    fixture_raw = _read_regular_file(fixture_path, "Cycle fixture")
    if fixture_raw is not None:
        fixture, fixture_sha256, packet = _load_fixture(fixture_path)
        if cycle["fixture_sha256"] is None:
            if cycle["state"] != "prepared":
                raise PaperRunnerError("Cycle fixture provenance conflicts with cycle state")
            # A fixture-only crash window has no corresponding provider evidence.
            # It is adopted rather than replaced; later selection fails closed.
            cycle = dict(cycle)
            cycle.update({"state": "scanned", "fixture_sha256": fixture_sha256, "packet_id": packet["packet_id"], "safe_reason": "none"})
            cycle = _persist_cycle(root, cycle, now)
        elif cycle["fixture_sha256"] != fixture_sha256 or cycle["packet_id"] != packet["packet_id"]:
            raise PaperRunnerError("Cycle fixture hash or packet provenance conflicts with cycle state")
        return cycle, packet, fixture_path, None
    if cycle["state"] != "prepared" or cycle["fixture_sha256"] is not None:
        raise PaperRunnerError("Cycle fixture is missing")

    scanned = _call_protected_scan(scan_candidates)
    if not scanned:
        cycle = dict(cycle)
        cycle.update({"state": "completed-no-candidate", "safe_reason": "no-provider-tag-eligible-candidate"})
        return _persist_cycle(root, cycle, now), {"candidates": []}, fixture_path, {}
    fixture, metadata_by_market = _fixture_from_scan(scanned, now)
    fixture_sha256 = _write_fixture(fixture_path, fixture)
    try:
        packet = prepare_packet(fixture)
    except GuardianValidationError:
        raise PaperRunnerError("Cycle fixture is invalid") from None
    cycle = dict(cycle)
    cycle.update({"state": "scanned", "fixture_sha256": fixture_sha256, "packet_id": packet["packet_id"], "safe_reason": "none"})
    return _persist_cycle(root, cycle, now), packet, fixture_path, metadata_by_market


def _select_or_recover(
    root: pathlib.Path,
    cycle: dict[str, Any],
    fixture_path: pathlib.Path,
    packet: dict[str, Any],
    metadata_by_market: dict[str, dict[str, Any] | None] | None,
    now: Callable[[], dt.datetime],
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    packet_by_market = {candidate["market_id"]: candidate for candidate in packet["candidates"]}
    if cycle["candidate_id"] is not None:
        candidate = next((item for item in packet["candidates"] if item["candidate_id"] == cycle["candidate_id"]), None)
        if candidate is None or candidate["market_id"] != cycle["market_id"]:
            raise PaperRunnerError("Selected candidate conflicts with frozen fixture")
        # _validate_cycle already verifies evidence integrity and its market bind.
        return cycle, candidate
    if cycle["state"] != "scanned":
        raise PaperRunnerError("Cycle selection state is invalid")
    if metadata_by_market is None:
        # The durable fixture deliberately excludes provider metadata. A crash
        # after fixture publication but before selection is never recovered by a
        # silent rescan or candidate substitution.
        raise PaperRunnerError("Provider selection evidence is unavailable; operator reconciliation is required")
    fixture, _, _ = _load_fixture(fixture_path)
    source_candidates = fixture.get("candidates")
    if not isinstance(source_candidates, list) or set(metadata_by_market) != set(packet_by_market):
        raise PaperRunnerError("Provider selection evidence conflicts with frozen fixture")
    selected: dict[str, Any] | None = None
    selected_evidence: dict[str, Any] | None = None
    # Source fixture order is protected scanner order. No probability, edge,
    # label, slug, question, category, or model rank influences this decision.
    for source in source_candidates:
        if not isinstance(source, dict):
            raise PaperRunnerError("Cycle fixture is invalid")
        market_id = source.get("market_id")
        if market_id not in packet_by_market:
            raise PaperRunnerError("Provider selection evidence conflicts with frozen fixture")
        metadata = metadata_by_market[market_id]
        if _provider_metadata_is_eligible(metadata) and _candidate_is_researchable(packet_by_market[market_id], now):
            selected = packet_by_market[market_id]
            selected_evidence = _selection_evidence(selected["market_id"], metadata)
            break
    if selected is None or selected_evidence is None:
        cycle = dict(cycle)
        cycle.update({"state": "completed-no-candidate", "safe_reason": "no-provider-tag-eligible-candidate"})
        return _persist_cycle(root, cycle, now), None
    cycle = dict(cycle)
    cycle.update(
        {
            "state": "selected",
            "candidate_id": selected["candidate_id"],
            "market_id": selected["market_id"],
            "selection_evidence": selected_evidence,
            "selection_evidence_sha256": _selection_evidence_sha256(selected_evidence),
            "counters": {**cycle["counters"], "candidates_selected": 1},
            "safe_reason": "none",
        }
    )
    return _persist_cycle(root, cycle, now), selected


def _safe_research_reason(error: research_transport.ResearchTransportError, budget: int) -> str:
    message = str(error).lower()
    if budget == 0 and "not authorized to create" in message:
        return "current-invocation-research-authorization-required"
    if "reconciliation" in message or "ambiguous" in message:
        return "research-transport-reconciliation-required"
    return "research-transport-failed"


def _validate_budget(budget: Any, ceiling: Any) -> tuple[int, int | None]:
    if type(budget) is not int or budget not in {0, 1}:
        raise PaperRunnerError("manus task budget must be 0 or 1")
    if ceiling is not None and (type(ceiling) is not int or not 1 <= ceiling <= 100):
        raise PaperRunnerError("manus soft credit ceiling must be an integer from 1 to 100")
    if budget == 1 and ceiling is None:
        raise PaperRunnerError("manus task budget 1 requires a soft credit ceiling")
    return budget, ceiling


def _summary(cycle: dict[str, Any], *, dry_run: bool = False, rejection_code: str | None = None) -> dict[str, Any]:
    # The persisted cycle schema intentionally stays unchanged: the application
    # receipt is the authoritative rejection source, and the runner summary is
    # a per-invocation view of that bounded code. safe_reason is not overloaded.
    if rejection_code is not None and rejection_code not in paper_apply.REJECTION_CODES:
        rejection_code = "unclassified"
    return {
        "mode": "PAPER",
        "dry_run": dry_run,
        "cycle_id": cycle["cycle_id"],
        "cycle_state": cycle["state"],
        "candidate_id": cycle["candidate_id"],
        "market_id": cycle["market_id"],
        "task_id": cycle["task_id"],
        "intent_id": cycle["intent_id"],
        "application_state": cycle["application_state"],
        "forecast_id": cycle["forecast_id"],
        "placement_id": cycle["placement_id"],
        "new_manus_tasks": cycle["counters"]["new_manus_tasks"],
        "logical_applications": cycle["counters"]["logical_applications"],
        "forecasts_recorded": int(cycle["forecast_id"] is not None),
        "placements_recorded": int(cycle["placement_id"] is not None),
        "safe_reason": cycle["safe_reason"],
        "rejection_code": rejection_code,
    }


def _dry_run(
    scan_candidates: Callable[..., list[dict[str, Any]]],
    now: Callable[[], dt.datetime],
    new_uuid: Callable[[], uuid.UUID],
) -> dict[str, Any]:
    cycle = _new_cycle(str(new_uuid()), now, None)
    scanned = _call_protected_scan(scan_candidates)
    if not scanned:
        cycle.update({"state": "completed-no-candidate", "safe_reason": "no-provider-tag-eligible-candidate"})
        return _summary(cycle, dry_run=True)
    fixture, metadata_by_market = _fixture_from_scan(scanned, now)
    try:
        packet = prepare_packet(fixture)
    except GuardianValidationError:
        raise PaperRunnerError("Protected scan output cannot form a trusted fixture") from None
    packet_by_market = {candidate["market_id"]: candidate for candidate in packet["candidates"]}
    for source in fixture["candidates"]:
        market_id = source["market_id"]
        metadata = metadata_by_market[market_id]
        if _provider_metadata_is_eligible(metadata) and _candidate_is_researchable(packet_by_market[market_id], now):
            candidate = packet_by_market[market_id]
            evidence = _selection_evidence(market_id, metadata)
            cycle.update(
                {
                    "state": "selected",
                    "fixture_sha256": hashlib.sha256((_canonical_json(fixture) + "\n").encode("utf-8")).hexdigest(),
                    "packet_id": packet["packet_id"],
                    "candidate_id": candidate["candidate_id"],
                    "market_id": candidate["market_id"],
                    "selection_evidence": evidence,
                    "selection_evidence_sha256": _selection_evidence_sha256(evidence),
                    "counters": {**cycle["counters"], "candidates_selected": 1},
                }
            )
            return _summary(cycle, dry_run=True)
    cycle.update({"state": "completed-no-candidate", "safe_reason": "no-provider-tag-eligible-candidate"})
    return _summary(cycle, dry_run=True)


def _require_result_identifier(value: Any, label: str, *, nullable: bool = False) -> str | None:
    return _require_identifier(value, label, nullable=nullable)


def _bounded_rejection_code(application: Any) -> str | None:
    """Validate one bounded rejection code from a durable application result.

    Only the closed paper_apply vocabulary is accepted; any other value —
    including arbitrary text — maps to ``unclassified``. This mirrors, never
    weakens, the receipt-side validation and adds no runner authority.
    """
    code = application.get("rejection_code") if isinstance(application, dict) else None
    if code is None:
        return None
    if isinstance(code, str) and code in paper_apply.REJECTION_CODES:
        return code
    return "unclassified"


def _terminal_application_update(application: Any) -> tuple[str, str, str | None, str | None, str]:
    """Validate a durable paper_apply terminal result and map runner authority.

    Transient status strings are checked only for result consistency. The durable
    ``application_state`` is the exclusive source for runner terminal state.
    """
    if not isinstance(application, dict):
        raise PaperRunnerError("PAPER application returned an invalid result")
    required = {"application_state", "forecast_id", "placement_id", "forecast_status", "placement_status"}
    if not required.issubset(application):
        raise PaperRunnerError("PAPER application returned an invalid result")
    application_state = application.get("application_state")
    if application_state not in _APPLICATION_TERMINAL_STATES:
        raise PaperRunnerError("PAPER application state is missing, nonterminal, or unknown")
    forecast_id = _require_result_identifier(application.get("forecast_id"), "application forecast_id", nullable=True)
    placement_id = _require_result_identifier(application.get("placement_id"), "application placement_id", nullable=True)
    forecast_status = application.get("forecast_status")
    placement_status = application.get("placement_status")
    if not isinstance(forecast_status, str) or not isinstance(placement_status, str):
        raise PaperRunnerError("PAPER application result statuses are invalid")

    if application_state == "completed-no-placement":
        if forecast_id is None or placement_id is not None or placement_status != "not-eligible-disposition":
            raise PaperRunnerError("PAPER completed-no-placement result is inconsistent")
        if forecast_status not in {"recorded", "already-completed", "recovered"}:
            raise PaperRunnerError("PAPER completed-no-placement result is inconsistent")
        return "completed", "none", forecast_id, None, application_state
    if application_state == "completed-placement":
        if forecast_id is None or placement_id is None:
            raise PaperRunnerError("PAPER completed-placement result is inconsistent")
        if forecast_status not in {"recorded", "already-completed", "recovered"} or placement_status not in {"placed", "already-completed", "recovered"}:
            raise PaperRunnerError("PAPER completed-placement result is inconsistent")
        return "completed", "none", forecast_id, placement_id, application_state
    if application_state == "placement-rejected":
        if forecast_id is None or placement_id is not None or placement_status != "rejected":
            raise PaperRunnerError("PAPER placement-rejected result is inconsistent")
        if forecast_status not in {"recorded", "terminal"}:
            raise PaperRunnerError("PAPER placement-rejected result is inconsistent")
        return "failed-terminal", "paper-placement-rejected", forecast_id, None, application_state
    if application_state == "forecast-rejected":
        if forecast_id is not None or placement_id is not None or forecast_status != "rejected" or placement_status != "not-attempted":
            raise PaperRunnerError("PAPER forecast-rejected result is inconsistent")
        return "failed-terminal", "paper-forecast-rejected", None, None, application_state
    # Durable receipt state ``error`` is terminal; no transient value grants it
    # authority, but the receipt's terminal summary must remain internally sane.
    if placement_id is not None or forecast_status != "terminal" or placement_status != "not-attempted":
        raise PaperRunnerError("PAPER error result is inconsistent")
    return "failed-terminal", "paper-application-error", forecast_id, None, application_state


def run(
    *,
    dry_run: bool = False,
    manus_task_budget: int = 0,
    manus_soft_credit_ceiling: int | None = None,
    _runner_root: pathlib.Path | None = None,
    _lock_root: pathlib.Path | None = None,
    _scan_candidates: Callable[..., list[dict[str, Any]]] = core_scan.scan_candidates,
    _research_run: Callable[..., dict[str, Any]] = research_transport.run,
    _apply_run: Callable[..., dict[str, Any]] = paper_apply.run,
    _now: Callable[[], dt.datetime] = lambda: dt.datetime.now(dt.timezone.utc),
    _new_uuid: Callable[[], uuid.UUID] = uuid.uuid4,
) -> dict[str, Any]:
    """Run one bounded manual cycle; no scheduler or background loop exists.

    ``manus_task_budget`` is current-invocation authority only. It maps directly
    to ``research_transport.run(..., allow_new_task=...)`` and is not persisted
    or inferred from prior runner, reservation, or staging state.
    """
    budget, ceiling = _validate_budget(manus_task_budget, manus_soft_credit_ceiling)
    if type(dry_run) is not bool:
        raise PaperRunnerError("dry_run must be a bool")
    if dry_run:
        return _dry_run(_scan_candidates, _now, _new_uuid)

    root = _resolve_runner_root() if _runner_root is None else _validate_runner_root(pathlib.Path(_runner_root))
    lock_root = root.parent / "locks" if _lock_root is None else pathlib.Path(_lock_root)
    try:
        with paper_locks.acquire_cycle_lock(nonblocking=True, _lock_root=lock_root):
            root = _prepare_runner_root(root)
            active = _read_active_cycle(root)
            if active is None or active[0]["state"] in _TERMINAL_STATES:
                cycle = _new_cycle(str(_new_uuid()), _now, ceiling)
                cycle = _persist_cycle(root, cycle, _now)
            else:
                cycle, _ = active
                if ceiling is not None and cycle["advisory_soft_credit_ceiling"] != ceiling:
                    cycle = dict(cycle)
                    cycle["advisory_soft_credit_ceiling"] = ceiling
                    cycle = _persist_cycle(root, cycle, _now)

            cycle, packet, fixture_path, metadata_by_market = _ensure_fixture(root, cycle, _now, _scan_candidates)
            if cycle["state"] in _TERMINAL_STATES:
                return _summary(cycle)
            cycle, candidate = _select_or_recover(root, cycle, fixture_path, packet, metadata_by_market, _now)
            if candidate is None or cycle["state"] in _TERMINAL_STATES:
                return _summary(cycle)

            if cycle["state"] == "selected":
                cycle = dict(cycle)
                cycle.update({"state": "research-pending", "safe_reason": "none"})
                cycle = _persist_cycle(root, cycle, _now)

            if cycle["state"] == "research-pending":
                # Defense in depth: a candidate valid at selection may age past
                # the protected resolution floor while persisted as
                # selected/research-pending (stop, wait, later budget-1 resume).
                # Re-check immediately before spending research authority; no
                # substitute candidate is silently selected.
                if not _candidate_is_researchable(candidate, _now):
                    cycle = dict(cycle)
                    cycle.update(
                        {
                            "state": "failed-terminal",
                            "safe_reason": "selected-candidate-no-longer-researchable",
                        }
                    )
                    cycle = _persist_cycle(root, cycle, _now)
                    return _summary(cycle)
                try:
                    research = _research_run(str(fixture_path), candidate["candidate_id"], allow_new_task=budget == 1)
                except research_transport.ResearchTransportError as exc:
                    cycle = dict(cycle)
                    cycle["safe_reason"] = _safe_research_reason(exc, budget)
                    cycle = _persist_cycle(root, cycle, _now)
                    return _summary(cycle)
                if not isinstance(research, dict) or research.get("validated") is not True:
                    raise PaperRunnerError("Research transport returned an invalid validated staging summary")
                intent_id = _require_uuid4(research.get("intent_id"), "research intent_id")
                task_id = _require_identifier(research.get("task_id"), "research task_id", nullable=True)
                created = research.get("task_created_this_invocation")
                if type(created) is not bool:
                    raise PaperRunnerError("Research transport task audit is invalid")
                new_tasks = cycle["counters"]["new_manus_tasks"] + int(created)
                if new_tasks > MAX_NEW_MANUS_TASKS_PER_CYCLE:
                    raise PaperRunnerError("Cycle new Manus task cap would be exceeded")
                cycle = dict(cycle)
                cycle.update(
                    {
                        "state": "research-completed",
                        "intent_id": intent_id,
                        "task_id": task_id,
                        "counters": {**cycle["counters"], "new_manus_tasks": new_tasks},
                        "safe_reason": "none",
                    }
                )
                cycle = _persist_cycle(root, cycle, _now)

            if cycle["state"] == "research-completed":
                cycle = dict(cycle)
                cycle.update(
                    {
                        "state": "application-pending",
                        "counters": {**cycle["counters"], "logical_applications": 1},
                        "safe_reason": "none",
                    }
                )
                cycle = _persist_cycle(root, cycle, _now)

            if cycle["state"] != "application-pending":
                raise PaperRunnerError("Cycle application state is invalid")
            try:
                # Exactly one call in this runner invocation. A later invocation
                # may call the same fixed identity only to reconcile paper_apply.
                application = _apply_run(str(fixture_path), cycle["intent_id"])
            except paper_apply.PaperApplyError:
                cycle = dict(cycle)
                cycle["safe_reason"] = "application-reconciliation-required"
                cycle = _persist_cycle(root, cycle, _now)
                return _summary(cycle)
            terminal_state, reason, forecast_id, placement_id, application_state = _terminal_application_update(application)
            rejection_code = _bounded_rejection_code(application)
            cycle = dict(cycle)
            cycle.update(
                {
                    "state": terminal_state,
                    "application_state": application_state,
                    "forecast_id": forecast_id,
                    "placement_id": placement_id,
                    "safe_reason": reason,
                }
            )
            cycle = _persist_cycle(root, cycle, _now)
            return _summary(cycle, rejection_code=rejection_code)
    except paper_locks.LockUnavailableError:
        raise PaperRunnerError("PAPER cycle is already running; no mutation was performed") from None
    except paper_locks.LockError:
        raise PaperRunnerError("Fixed PAPER cycle lock is unavailable; no mutation was performed") from None


def _reject_forbidden_options(arguments: list[str], parser: argparse.ArgumentParser) -> None:
    for argument in arguments:
        if not argument.startswith("--"):
            continue
        option = argument[2:].split("=", 1)[0].lower().replace("_", "-")
        if option in {"dry-run", "manus-task-budget", "manus-soft-credit-ceiling"}:
            continue
        if any(term in option.split("-") for term in _FORBIDDEN_OPTION_TERMS):
            parser.error("Forbidden PAPER runner option")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run one bounded manual Manus PAPER cycle", allow_abbrev=False)
    parser.add_argument("--dry-run", action="store_true", help="scan and show one bounded local plan without persistent mutation")
    parser.add_argument("--manus-task-budget", type=int, choices=(0, 1), default=0, help="current invocation only: 0 permits resume only; 1 may create one task")
    parser.add_argument("--manus-soft-credit-ceiling", type=int, help="advisory current-invocation ceiling, 1 through 100")
    return parser


def main(argv: list[str] | None = None) -> int:
    import sys

    arguments = list(sys.argv[1:] if argv is None else argv)
    parser = build_parser()
    _reject_forbidden_options(arguments, parser)
    args = parser.parse_args(arguments)
    try:
        result = run(dry_run=args.dry_run, manus_task_budget=args.manus_task_budget, manus_soft_credit_ceiling=args.manus_soft_credit_ceiling)
    except PaperRunnerError as exc:
        parser.exit(2, f"REJECTED: {exc}\n")
    print(_canonical_json(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

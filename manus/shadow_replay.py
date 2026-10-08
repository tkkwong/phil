"""Read-only offline replay and shadow comparison for guarded Manus decisions.
This module is observability infrastructure only. It performs zero operational
mutation: it never appends provenance, never writes journals or operational
cycle state, never places anything, and never calls the scanner network APIs,
research transport, ``paper_apply``, the broker adapter, or any model provider.
Every shadow engine is a pluggable callable boundary; the repository ships
none, so no JEV/AI implementation or key handling can occur accidentally.
"""
from __future__ import annotations

import json
import pathlib
from typing import Any, Callable

from manus import decision_provenance
from manus.decision_provenance import ProvenanceWriteError
from manus.paper_cycle_guardian import _canonical_json

SHADOW_REPLAY_VERSION = "shadow-replay/v1"
REPLAY_INPUT_SCHEMA_VERSION = "decision-frozen-input/v1"
REPLAY_INCOMPLETE = "replay-input-incomplete"
REPLAYABLE = "replayable"
PROVENANCE_UNAVAILABLE = "provenance-unavailable"

# Engines are typed as callables taking the canonical frozen input dict and
# returning a bounded decision document. This is the entire engine boundary:
# a future JEV-style engine plugs in here behind explicit operator
# authorization, with fake implementations used in all offline tests.
ShadowEngine = Callable[[dict[str, Any]], dict[str, Any]]

_ENGINE_DECISION_FIELDS = frozenset(
    {"action", "reason_code", "estimated_probability", "requested_notional"}
)


class ShadowReplayError(RuntimeError):
    """Raised when read-only replay or shadow comparison cannot safely proceed."""


def _require_frozen_input(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict) or set(value) != decision_provenance.FROZEN_INPUT_FIELDS:
        raise ShadowReplayError("Replay frozen input schema is invalid")
    if value.get("schema_version") != REPLAY_INPUT_SCHEMA_VERSION:
        raise ShadowReplayError("Replay frozen input schema version is incompatible")
    return value


def load_frozen_input_document(raw: str | bytes) -> dict[str, Any]:
    """Parse and validate one frozen-input JSON fixture without side effects."""
    if isinstance(raw, bytes):
        try:
            raw = raw.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ShadowReplayError("Replay frozen input is unavailable") from exc
    if not isinstance(raw, str):
        raise ShadowReplayError("Replay frozen input is unavailable")
    try:
        document = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ShadowReplayError("Replay frozen input is malformed") from exc
    return _require_frozen_input(document)


def evaluate_frozen_decision(frozen: dict[str, Any]) -> dict[str, Any]:
    """Deterministically evaluate one decision from a frozen input.

    This is the extracted pure form of the existing decision lifecycle logic
    for replay/shadow purposes only. It performs identical checks in the
    identical order as the live guarded path and derives the identical
    actions and reason codes from identical inputs. It performs no I/O, no
    mutation, no placement, and no network call.
    """
    frozen = _require_frozen_input(frozen)
    disposition = frozen["forecast_disposition"]
    estimated = frozen["estimated_probability"]
    policy = frozen["policy"]
    if (
        disposition is None
        or estimated is None
        or policy is None
        or frozen["market_id"] is None
        or frozen["outcome"] is None
        or frozen["best_ask"] is None
        or frozen["best_bid"] is None
        or frozen["researchable"] is False
    ):
        raise ShadowReplayError(REPLAY_INCOMPLETE)
    price = frozen["best_ask"]
    bid = frozen["best_bid"]
    if (
        isinstance(price, bool) or not isinstance(price, (int, float))
        or isinstance(bid, bool) or not isinstance(bid, (int, float))
        or isinstance(estimated, bool) or not isinstance(estimated, (int, float))
    ):
        raise ShadowReplayError(REPLAY_INCOMPLETE)
    if price <= 0 or not (0.0 < estimated < 1.0):
        raise ShadowReplayError(REPLAY_INCOMPLETE)
    stake = policy.get("stake_usd")
    max_stake = policy.get("max_stake_usd")
    if (
        isinstance(stake, bool) or not isinstance(stake, (int, float))
        or isinstance(max_stake, bool) or not isinstance(max_stake, (int, float))
    ):
        raise ShadowReplayError(REPLAY_INCOMPLETE)
    required = (
        "min_entry_price", "max_entry_price", "required_edge", "max_spread",
        "max_open_positions", "max_stake_per_event_usd",
        "max_new_positions_per_cycle", "max_positions_per_category_per_cycle",
    )
    for name in required:
        value = policy.get(name)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ShadowReplayError(REPLAY_INCOMPLETE)
    min_entry = policy["min_entry_price"]
    max_entry = policy["max_entry_price"]
    required_edge = policy["required_edge"]
    max_spread = policy["max_spread"]
    max_open = policy["max_open_positions"]
    max_stake_per_event = policy["max_stake_per_event_usd"]
    max_new_per_cycle = policy["max_new_positions_per_cycle"]
    category_cap = policy["max_positions_per_category_per_cycle"]
    if not isinstance(frozen["category"], str) or not frozen["category"]:
        raise ShadowReplayError(REPLAY_INCOMPLETE)
    market_probability = (price + bid) / 2.0
    spread = price - bid
    edge = estimated - price
    open_positions = frozen["open_positions"] or []
    if not isinstance(open_positions, list):
        raise ShadowReplayError(REPLAY_INCOMPLETE)
    relevant = [
        row
        for row in open_positions
        if isinstance(row, dict)
        and row.get("market_id") == frozen["market_id"]
        and row.get("outcome") == frozen["outcome"]
        and row.get("status") == "open"
    ]
    if stake > max_stake:
        return _decision_document(
            frozen, "no-trade", "decision-policy", "insufficient-cash",
            estimated, market_probability, edge, stake, attempt=False,
        )
    open_event_stake = 0.0
    for row in open_positions:
        if not isinstance(row, dict):
            raise ShadowReplayError(REPLAY_INCOMPLETE)
        event_id = row.get("event_id")
        row_stake = row.get("stake_usd")
        if event_id is None:
            continue
        if not isinstance(event_id, str) or not event_id:
            raise ShadowReplayError(REPLAY_INCOMPLETE)
        if isinstance(row_stake, bool) or not isinstance(row_stake, (int, float)):
            raise ShadowReplayError(REPLAY_INCOMPLETE)
        if event_id == frozen["event_id"]:
            open_event_stake += row_stake
    if stake + open_event_stake > max_stake_per_event:
        return _decision_document(
            frozen, "no-trade", "decision-policy", "risk-cap-event",
            estimated, market_probability, edge, stake, attempt=False,
        )
    if len(open_positions) >= max_open:
        return _decision_document(
            frozen, "no-trade", "decision-policy", "max-open-positions",
            estimated, market_probability, edge, stake, attempt=False,
        )
    if any(
        isinstance(row, dict)
        and row.get("market_id") == frozen["market_id"]
        and row.get("outcome") == frozen["outcome"]
        and row.get("status") == "open"
        for row in open_positions
    ):
        return _decision_document(
            frozen, "no-trade", "decision-policy", "duplicate-market-outcome",
            estimated, market_probability, edge, stake, attempt=False,
        )
    if frozen["eligible_candidate_count"] is not None and frozen["eligible_candidate_count"] >= max_new_per_cycle:
        return _decision_document(
            frozen, "no-trade", "decision-policy", "packet-position-cap",
            estimated, market_probability, edge, stake, attempt=False,
        )
    if (
        frozen["eligible_candidate_count"] is not None
        and frozen["eligible_candidate_count"] >= category_cap
        and any(isinstance(row, dict) and row.get("category") == frozen["category"] for row in open_positions)
    ):
        return _decision_document(
            frozen, "no-trade", "decision-policy", "category-position-cap",
            estimated, market_probability, edge, stake, attempt=False,
        )
    if not min_entry <= price <= max_entry:
        return _decision_document(
            frozen, "no-trade", "decision-policy", "entry-price-out-of-bounds",
            estimated, market_probability, edge, stake, attempt=False,
        )
    if spread > max_spread:
        return _decision_document(
            frozen, "no-trade", "decision-policy", "spread-too-wide",
            estimated, market_probability, edge, stake, attempt=False,
        )
    if edge < required_edge:
        return _decision_document(
            frozen, "no-trade", "decision-policy", "edge-below-threshold",
            estimated, market_probability, edge, stake, attempt=False,
        )
    if disposition != "bet":
        return _decision_document(
            frozen, "no-trade", "decision-policy", disposition,
            estimated, market_probability, edge, stake, attempt=False,
        )
    return _decision_document(
        frozen, "trade", "placement", None, estimated, market_probability,
        edge, stake, attempt=True, result="placed",
    )


def _decision_document(
    frozen: dict[str, Any],
    action: str,
    stage: str,
    reason_code: str | None,
    estimated: float,
    market_probability: float,
    edge: float,
    stake: float,
    *,
    attempt: bool,
    result: str | None = None,
) -> dict[str, Any]:
    return {
        "action": action,
        "decision_stage": stage,
        "decision_phase": "attempt" if attempt else "final",
        "decision": {
            "action": action,
            "reason_code": reason_code,
            "estimated_probability": estimated,
            "market_probability": market_probability,
            "edge": edge,
            "requested_notional": stake,
        },
        "placement_attempted": attempt,
        "placement_result": result,
    }


def _decision_fields(record: dict[str, Any]) -> dict[str, Any]:
    return {
        "action": record.get("decision_action"),
        "reason_code": record.get("reason_code"),
        "estimated_probability": record.get("estimated_probability"),
        "requested_notional": record.get("requested_notional"),
    }


def replay(
    frozen_input: dict[str, Any],
    *,
    engine: ShadowEngine | None = None,
) -> dict[str, Any]:
    """Deterministically re-derive one decision from a frozen input.

    ``engine`` defaults to the repository's own decision evaluator. A supplied
    engine receives the identical frozen input and is treated as a shadow
    candidate; the baseline document is always produced by the repository
    evaluator. No mutation of any kind occurs in either path.
    """
    frozen = _require_frozen_input(frozen_input)
    input_sha256 = decision_provenance.frozen_input_sha256(frozen)
    try:
        baseline = evaluate_frozen_decision(frozen)
    except ShadowReplayError as exc:
        raise ShadowReplayError(
            f"{REPLAY_INCOMPLETE}: {exc}"
        ) from None
    document: dict[str, Any] = {
        "shadow_replay_version": SHADOW_REPLAY_VERSION,
        "frozen_input_sha256": input_sha256,
        "baseline": baseline,
    }
    if engine is not None:
        document["shadow"] = _shadow_document(frozen, engine, baseline, input_sha256)
    return document


def _shadow_document(
    frozen: dict[str, Any],
    engine: ShadowEngine,
    baseline: dict[str, Any],
    input_sha256: str,
) -> dict[str, Any]:
    """Run one shadow engine against the identical frozen input, read-only."""
    name = getattr(engine, "__name__", None)
    if not isinstance(name, str) or not name:
        name = "shadow-engine"
    try:
        raw = engine(frozen)
    except ShadowReplayError:
        raise
    except BaseException as exc:
        raise ShadowReplayError(
            "shadow-engine-failed: shadow engine raised an unexpected error"
        ) from exc
    if not isinstance(raw, dict):
        raise ShadowReplayError("shadow-engine-failed: shadow result is invalid")
    decision = raw.get("decision")
    if not isinstance(decision, dict) or not _ENGINE_DECISION_FIELDS.issubset(decision):
        raise ShadowReplayError("shadow-engine-failed: shadow decision is invalid")
    shadow = {key: decision.get(key) for key in _ENGINE_DECISION_FIELDS}
    baseline_decision = baseline["decision"]
    action_match = shadow["action"] == baseline_decision["action"]
    reason_match = shadow["reason_code"] == baseline_decision["reason_code"]
    delta = None
    if (
        isinstance(shadow["estimated_probability"], (int, float))
        and not isinstance(shadow["estimated_probability"], bool)
        and isinstance(baseline_decision["estimated_probability"], (int, float))
        and not isinstance(baseline_decision["estimated_probability"], bool)
    ):
        delta = round(shadow["estimated_probability"] - baseline_decision["estimated_probability"], 6)
    return {
        "engine": name,
        "engine_schema_version": raw.get("schema_version"),
        "action": shadow["action"],
        "action_match": action_match,
        "reason_code": shadow["reason_code"],
        "reason_match": reason_match,
        "estimated_probability": shadow["estimated_probability"],
        "baseline_estimated_probability": baseline_decision["estimated_probability"],
        "probability_delta": delta,
        "requested_notional": shadow["requested_notional"],
        "frozen_input_sha256": input_sha256,
    }


def replay_provenance_record(
    record: dict[str, Any],
    *,
    engine: ShadowEngine | None = None,
) -> dict[str, Any]:
    """Replay one existing decision-provenance record, read-only.

    Fails closed with the bounded incomplete status when the record's frozen
    input is missing or structurally unusable, or when the record integrity
    hash does not verify.
    """
    if not isinstance(record, dict) or set(record) != decision_provenance._DECISION_RECORD_FIELDS:
        raise ShadowReplayError(f"{PROVENANCE_UNAVAILABLE}: decision record schema is invalid")
    if not decision_provenance.verify_record_sha256(record):
        raise ShadowReplayError(f"{PROVENANCE_UNAVAILABLE}: decision record integrity failed")
    frozen = record.get("frozen_input")
    if frozen is None:
        raise ShadowReplayError(f"{REPLAY_INPUT_INCOMPLETE}: record has no frozen input")
    document = replay(frozen, engine=engine)
    document["decision_id"] = record["decision_id"]
    baseline = document["baseline"]
    recorded = _decision_fields(record)
    document["baseline_match"] = {
        "action_match": baseline["decision"]["action"] == recorded["action"],
        "reason_match": baseline["decision"]["reason_code"] == recorded["reason_code"],
    }
    return document


def replayability_status(record: dict[str, Any] | None) -> str:
    """Bounded replayability status for one provenance record (or absence)."""
    if record is None:
        return PROVENANCE_UNAVAILABLE
    if not isinstance(record, dict) or set(record) != decision_provenance._DECISION_RECORD_FIELDS:
        return PROVENANCE_UNAVAILABLE
    if not decision_provenance.verify_record_sha256(record):
        return PROVENANCE_UNAVAILABLE
    frozen = record.get("frozen_input")
    if not isinstance(frozen, dict) or set(frozen) != decision_provenance.FROZEN_INPUT_FIELDS:
        return REPLAY_INCOMPLETE
    try:
        evaluate_frozen_decision(frozen)
    except ShadowReplayError:
        return REPLAY_INCOMPLETE
    return REPLAYABLE


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def build_parser() -> "argparse.ArgumentParser":
    import argparse

    parser = argparse.ArgumentParser(
        description="Read-only offline replay and shadow comparison for guarded Manus decisions",
        allow_abbrev=False,
    )
    sub = parser.add_subparsers(dest="command", required=True)
    replay_parser = sub.add_parser("replay", help="deterministically re-derive one frozen decision")
    replay_parser.add_argument("--frozen-input", required=True, help="one frozen-input JSON fixture file")
    shadow_parser = sub.add_parser("shadow", help="compare a baseline and one shadow engine on one frozen input")
    shadow_parser.add_argument("--frozen-input", required=True, help="one frozen-input JSON fixture file")
    return parser


def _reject_forbidden_options(arguments: list[str], parser: "argparse.ArgumentParser") -> None:
    import argparse

    for argument in arguments:
        # Help is not an operational mutation capability: argparse's own help
        # flags (including after a subcommand) must reach argparse untouched.
        if argument in {"-h", "--help"}:
            continue
        if not argument.startswith("--"):
            continue
        option = argument[2:].split("=", 1)[0].lower().replace("_", "-")
        if option not in {"frozen-input"} and not option.startswith(("replay", "shadow")):
            parser.error("Forbidden shadow/replay option")
        if any(
            term in option.split("-")
            for term in ("real", "live", "broker", "ibkr", "pearl", "place", "order", "trade", "task", "journal", "ledger")
        ):
            parser.error("Forbidden shadow/replay option")


def main(argv: list[str] | None = None) -> int:
    import sys

    arguments = list(sys.argv[1:] if argv is None else argv)
    parser = build_parser()
    _reject_forbidden_options(arguments, parser)
    args = parser.parse_args(arguments)
    try:
        raw = pathlib.Path(args.frozen_input).read_bytes()
    except OSError:
        parser.exit(2, "REJECTED: replay frozen input is unavailable\n")
    try:
        frozen = load_frozen_input_document(raw)
    except ShadowReplayError as exc:
        parser.exit(2, f"REJECTED: {exc}\n")
    try:
        if args.command == "replay":
            document = replay(frozen)
        else:
            raise ShadowReplayError("No offline shadow engine is bundled; supply one through the Python API")
    except ShadowReplayError as exc:
        parser.exit(2, f"REJECTED: {exc}\n")
    print(_canonical_json(document))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


import argparse  # noqa: E402  (kept at bottom: CLI-only, no import side effects)

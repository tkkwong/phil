#!/usr/bin/env python3
"""Paper broker: place simulated positions and track the bankroll.

The legacy ``place`` CLI preserves Phil's existing paper-placement semantics.
``record_manus_paper_placement`` is a separate, Python-only, operator-owned
route for a forecast already bound to a validated Manus PAPER intent. It has no
CLI surface and never calls real-trading, broker, Pearl, or IBKR components.
"""
import argparse
import datetime as dt
from decimal import Decimal
import json
import math
import os
import pathlib
import re
import sys
import tempfile
import uuid

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import pmapi  # noqa: E402

ROOT = pathlib.Path(__file__).resolve().parent.parent
LEDGER = ROOT / "journal" / "ledger.jsonl"
FORECASTS = ROOT / "journal" / "forecasts.jsonl"
RISK = ROOT / "strategy" / "risk.json"
PROTECTED = json.loads((ROOT / "config" / "protected.json").read_text())

_IDENTIFIER_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]*$")
_ISO_UTC_TIMESTAMP_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})$"
)
MANUS_PAPER_EDGE_CLASS = "manus-paper-only"


PLACEMENT_REJECTION_CODES = frozenset(
    {
        "duplicate-source-intent",
        "duplicate-source-forecast",
        "duplicate-market-outcome",
        "forecast-provenance-invalid",
        "market-data-unavailable",
        "market-closed",
        "too-close-to-resolution",
        "outcome-token-invalid",
        "orderbook-unavailable",
        "entry-price-out-of-bounds",
        "spread-too-wide",
        "edge-below-threshold",
        "event-identity-unresolved-current-market",
        "event-identity-unresolved-existing-position",
        "event-identity-mismatch",
        "risk-cap-event",
        "max-open-positions",
        "insufficient-cash",
        "packet-position-cap",
        "category-position-cap",
        "invalid-policy",
        "ledger-write-failed",
        "unclassified",
    }
)


class ManusPlacementError(ValueError):
    """Raised when guarded Manus PAPER placement rejects before ledger mutation.

    ``code`` is a stable, non-sensitive diagnostic identifier drawn from
    ``PLACEMENT_REJECTION_CODES``. It carries no exception text, filesystem
    path, or provider response content, and is diagnostic metadata only:
    guard order, policy, and thresholds are untouched.
    """

    def __init__(self, message: str, code: str = "unclassified"):
        if code not in PLACEMENT_REJECTION_CODES:
            code = "unclassified"
        super().__init__(message)
        self.code = code


def read_ledger():
    if not LEDGER.exists():
        return []
    return [json.loads(line) for line in LEDGER.read_text().splitlines() if line.strip()]


def bankroll(entries):
    cash = PROTECTED["sim_bankroll_usd"]
    for e in entries:
        if e["status"] in ("open", "won", "lost", "void"):
            cash -= e["stake_usd"]
        if e["status"] == "won":
            cash += e["shares"]  # $1 per share
        elif e["status"] == "void":
            cash += e["stake_usd"]
    return cash


def cmd_status(entries):
    open_pos = [e for e in entries if e["status"] == "open"]
    settled = [e for e in entries if e["status"] in ("won", "lost")]
    pnl = sum((e["shares"] - e["stake_usd"]) if e["status"] == "won" else -e["stake_usd"]
              for e in settled)
    print(json.dumps({
        "cash": round(bankroll(entries), 2),
        "open_positions": len(open_pos),
        "settled": len(settled),
        "wins": sum(1 for e in settled if e["status"] == "won"),
        "realized_pnl": round(pnl, 2),
        "open": [{"id": e["id"], "q": e["question"][:70], "outcome": e["outcome"],
                  "entry": e["entry_price"], "est": e["est_prob"], "ends": e["end_date"]}
                 for e in open_pos],
    }, indent=2))


def _require_identifier(value, label, maximum=128):
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise ManusPlacementError(f"{label} must be a non-empty identifier up to {maximum} characters", "unclassified")
    if not _IDENTIFIER_RE.fullmatch(value):
        raise ManusPlacementError(f"{label} has an invalid identifier format", "unclassified")
    return value


def _require_text(value, label, maximum):
    if not isinstance(value, str) or not value or len(value) > maximum:
        raise ManusPlacementError(f"{label} must be a non-empty string up to {maximum} characters", "unclassified")
    return value


def _require_probability(value, label):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ManusPlacementError(f"{label} must be a number", "unclassified")
    if not math.isfinite(value) or not 0.0 < value < 1.0:
        raise ManusPlacementError(f"{label} must be in (0,1)", "unclassified")
    return float(value)


def _require_nonnegative_number(value, label, *, positive=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ManusPlacementError(f"{label} must be a number", "unclassified")
    if not math.isfinite(value) or (value <= 0 if positive else value < 0):
        comparison = "positive" if positive else "non-negative"
        raise ManusPlacementError(f"{label} must be {comparison}", "unclassified")
    return float(value)


def _exact_decimal(value, label):
    """Return one finite numeric input as a Decimal without float arithmetic."""
    try:
        exact = Decimal(str(value))
    except Exception as exc:  # noqa: BLE001 -- preserve the guarded rejection boundary
        raise ManusPlacementError(f"{label} must be a decimal number", "unclassified") from exc
    if not exact.is_finite():
        raise ManusPlacementError(f"{label} must be a finite decimal number", "unclassified")
    return exact


def _require_limit(value, label):
    number = _require_nonnegative_number(value, label)
    if not number.is_integer():
        raise ManusPlacementError(f"{label} must be an integer", "unclassified")
    return int(number)


def _read_jsonl(path, label):
    path = pathlib.Path(path)
    if not path.exists():
        return []
    try:
        return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    except (OSError, json.JSONDecodeError) as exc:
        raise ManusPlacementError(f"Unable to read {label}", "market-data-unavailable") from exc


def _read_manus_policy(protected_config=None, risk_config=None):
    """Read only existing policy files; optional values are private test seams."""
    protected = PROTECTED if protected_config is None else protected_config
    if risk_config is None:
        try:
            risk = json.loads(RISK.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise ManusPlacementError("Unable to read guarded PAPER risk policy", "invalid-policy") from exc
    else:
        risk = risk_config
    if not isinstance(protected, dict) or not isinstance(risk, dict):
        raise ManusPlacementError("Guarded PAPER policy must be an object", "invalid-policy")
    real_policy = protected.get("real")
    if not isinstance(real_policy, dict):
        raise ManusPlacementError("Protected real policy must be an object", "invalid-policy")
    real_allowlist = real_policy.get("allowed_edge_classes")
    if not isinstance(real_allowlist, list) or not all(isinstance(item, str) for item in real_allowlist):
        raise ManusPlacementError("Protected real allowed_edge_classes must be a list of strings", "invalid-policy")
    if MANUS_PAPER_EDGE_CLASS in real_allowlist:
        raise ManusPlacementError("manus-paper-only must never be real-eligible", "invalid-policy")

    stake = _require_nonnegative_number(risk.get("default_stake_usd"), "default_stake_usd", positive=True)
    max_stake = _require_nonnegative_number(protected.get("max_stake_usd"), "max_stake_usd", positive=True)
    if stake > max_stake:
        raise ManusPlacementError("default_stake_usd exceeds protected max_stake_usd", "invalid-policy")

    min_edge = _require_nonnegative_number(risk.get("min_edge"), "min_edge")
    min_edge_book_devig = _require_nonnegative_number(
        risk.get("min_edge_book_devig"), "min_edge_book_devig"
    )
    if min_edge >= 1 or min_edge_book_devig >= 1:
        raise ManusPlacementError("Guarded PAPER edge threshold must be below 1", "invalid-policy")
    max_spread = _require_nonnegative_number(risk.get("max_spread"), "max_spread")
    if max_spread >= 1:
        raise ManusPlacementError("max_spread must be below 1", "invalid-policy")

    return {
        "stake": stake,
        "max_stake": max_stake,
        "sim_bankroll": _require_nonnegative_number(
            protected.get("sim_bankroll_usd"), "sim_bankroll_usd", positive=True
        ),
        "max_open_positions": _require_limit(
            protected.get("max_open_positions"), "max_open_positions"
        ),
        "max_new_positions_per_cycle": _require_limit(
            protected.get("max_new_positions_per_cycle"), "max_new_positions_per_cycle"
        ),
        "min_minutes_to_resolution": _require_nonnegative_number(
            protected.get("min_minutes_to_resolution"), "min_minutes_to_resolution"
        ),
        "min_entry_price": _require_nonnegative_number(
            protected.get("min_entry_price"), "min_entry_price"
        ),
        "max_entry_price": _require_nonnegative_number(
            protected.get("max_entry_price"), "max_entry_price"
        ),
        # The operator-approved guarded policy applies the stricter existing
        # floor without using untrusted research_edge_class for authorization.
        "required_edge": max(min_edge, min_edge_book_devig),
        "max_spread": max_spread,
        "max_positions_per_category_per_cycle": _require_limit(
            risk.get("max_positions_per_category_per_cycle"),
            "max_positions_per_category_per_cycle",
        ),
        "max_stake_per_event_usd": _require_nonnegative_number(
            risk.get("max_stake_per_event_usd"), "max_stake_per_event_usd", positive=True
        ),
    }


def _guarded_bankroll(entries, sim_bankroll):
    """Mirror legacy bankroll accounting using only existing ledger semantics."""
    cash = sim_bankroll
    for entry in entries:
        status = entry.get("status")
        stake = _require_nonnegative_number(entry.get("stake_usd"), "existing ledger stake", positive=True)
        if status in ("open", "won", "lost", "void"):
            cash -= stake
        if status == "won":
            cash += _require_nonnegative_number(entry.get("shares"), "existing ledger shares")
        elif status == "void":
            cash += stake
    return cash


def _read_live_event_id(market_id, unresolved_code="event-identity-unresolved-current-market"):
    """Use the same Gamma list-record event shape that core/scan.py reads.

    ``unresolved_code`` distinguishes which lookup failed (the current
    candidate market vs an existing legacy open position) without any change
    in resolution behavior or additional network calls.
    """
    try:
        records = pmapi.gamma_markets(id=market_id)
    except Exception as exc:  # noqa: BLE001 -- stable fail-closed boundary
        raise ManusPlacementError("Unable to resolve live event identity", unresolved_code) from exc
    if not isinstance(records, list):
        raise ManusPlacementError("Unable to resolve live event identity", unresolved_code)
    matches = [record for record in records if isinstance(record, dict) and str(record.get("id")) == market_id]
    if len(matches) != 1:
        raise ManusPlacementError("Unable to resolve live event identity", unresolved_code)
    events = matches[0].get("events")
    if not isinstance(events, list) or not events or not isinstance(events[0], dict):
        raise ManusPlacementError("Unable to resolve live event identity", unresolved_code)
    return _require_identifier(events[0].get("id"), "live event_id")


def _parse_live_end_date(value):
    if not isinstance(value, str) or not _ISO_UTC_TIMESTAMP_RE.fullmatch(value):
        raise ManusPlacementError("live market endDate is malformed or timezone-naive", "too-close-to-resolution")
    try:
        parsed = dt.datetime.fromisoformat(f"{value[:-1]}+00:00" if value.endswith("Z") else value)
    except ValueError as exc:
        raise ManusPlacementError("live market endDate is malformed", "too-close-to-resolution") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ManusPlacementError("live market endDate is malformed or timezone-naive", "too-close-to-resolution")
    return parsed.astimezone(dt.timezone.utc)


def _matching_source_forecast(rows, source_intent_id, *, market_id, outcome, est_prob, category, rationale):
    matches = [row for row in rows if row.get("source_intent_id") == source_intent_id]
    if not matches:
        raise ManusPlacementError("No persisted forecast matches source_intent_id", "forecast-provenance-invalid")
    if len(matches) != 1:
        raise ManusPlacementError("source_intent_id has multiple persisted forecasts", "forecast-provenance-invalid")
    forecast = matches[0]
    if forecast.get("status") != "open" or forecast.get("superseded_by"):
        raise ManusPlacementError("source forecast must be open and not superseded", "forecast-provenance-invalid")
    expected = {
        "market_id": market_id,
        "outcome": outcome,
        "est_prob": est_prob,
        "category": category,
        "skip_reason": "bet",
        "note": rationale,
    }
    for field, required in expected.items():
        if forecast.get(field) != required:
            raise ManusPlacementError(f"source forecast {field} does not match the validated intent", "forecast-provenance-invalid")
    return _require_identifier(forecast.get("id"), "source_forecast_id")


def _atomic_append_source_record(ledger_path, row):
    """Atomically append a guarded provenance-bearing ledger row.

    This retains the documented single-writer assumption. Before replacement,
    an ordinary Python failure leaves the original ledger bytes untouched.
    """
    ledger_path = pathlib.Path(ledger_path)
    original = ledger_path.read_bytes() if ledger_path.exists() else b""
    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=ledger_path.parent,
            prefix=f".{ledger_path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary_book:
            temporary_path = pathlib.Path(temporary_book.name)
            temporary_book.write(original)
            if original and not original.endswith(b"\n"):
                temporary_book.write(b"\n")
            temporary_book.write((json.dumps(row) + "\n").encode("utf-8"))
            temporary_book.flush()
            os.fsync(temporary_book.fileno())
        # The temporary file is closed before os.replace for Windows compatibility.
        os.replace(temporary_path, ledger_path)
        temporary_path = None
    except Exception:
        if temporary_path is not None:
            try:
                temporary_path.unlink()
            except OSError:
                pass
        raise


def _validate_source_intent_id(source_intent_id):
    if not isinstance(source_intent_id, str):
        raise ManusPlacementError("source_intent_id must be a canonical UUIDv4", "unclassified")
    try:
        parsed = uuid.UUID(source_intent_id)
    except (AttributeError, ValueError) as exc:
        raise ManusPlacementError("source_intent_id must be a canonical UUIDv4", "unclassified") from exc
    if parsed.version != 4 or str(parsed) != source_intent_id:
        raise ManusPlacementError("source_intent_id must be a canonical UUIDv4", "unclassified")
    return source_intent_id


def record_manus_paper_placement(
    *,
    source_intent_id,
    source_packet_id,
    event_id,
    market_id,
    outcome,
    est_prob,
    category,
    rationale,
    research_edge_class=None,
    _ledger_path=LEDGER,
    _forecast_path=FORECASTS,
    _protected_config=None,
    _risk_config=None,
    _now=None,
):
    """Place one guarded PAPER row after a fixture-bound forecast exists.

    This Python-only function deliberately exposes no CLI. All path and policy
    substitutions are private test seams. Production callers receive no route
    to a stake, ledger path, forecast id, event id, live mode, real execution,
    broker operation, or strategy proposal.
    """
    source_intent_id = _validate_source_intent_id(source_intent_id)
    source_packet_id = _require_identifier(source_packet_id, "source_packet_id")
    event_id = _require_identifier(event_id, "event_id")
    market_id = _require_identifier(market_id, "market_id")
    outcome = _require_text(outcome, "outcome", 128)
    est_prob = _require_probability(est_prob, "estimated_probability")
    category = _require_identifier(category, "category", 64)
    rationale = _require_text(rationale, "rationale", 2000)
    if research_edge_class is not None:
        research_edge_class = _require_identifier(research_edge_class, "research_edge_class", 64)

    policy = _read_manus_policy(_protected_config, _risk_config)
    ledger_path = pathlib.Path(_ledger_path)
    forecast_path = pathlib.Path(_forecast_path)
    entries = _read_jsonl(ledger_path, "paper ledger")

    # Durable replay protection precedes every public Gamma/CLOB market read,
    # including when the prior row is already settled.
    if any(row.get("source_intent_id") == source_intent_id for row in entries):
        raise ManusPlacementError("source_intent_id has already placed a PAPER position", "duplicate-source-intent")

    forecast_rows = _read_jsonl(forecast_path, "forecast book")
    source_forecast_id = _matching_source_forecast(
        forecast_rows,
        source_intent_id,
        market_id=market_id,
        outcome=outcome,
        est_prob=est_prob,
        category=category,
        rationale=rationale,
    )
    if any(row.get("source_forecast_id") == source_forecast_id for row in entries):
        raise ManusPlacementError("source_forecast_id has already placed a PAPER position", "duplicate-source-forecast")

    open_positions = [row for row in entries if row.get("status") == "open"]
    if len(open_positions) >= policy["max_open_positions"]:
        raise ManusPlacementError("max_open_positions reached", "max-open-positions")
    if policy["stake"] > policy["max_stake"]:
        raise ManusPlacementError("default_stake_usd exceeds protected max_stake_usd", "insufficient-cash")
    if policy["stake"] > _guarded_bankroll(entries, policy["sim_bankroll"]):
        raise ManusPlacementError("insufficient simulated cash", "insufficient-cash")
    if any(
        row.get("market_id") == market_id and row.get("outcome") == outcome
        for row in open_positions
    ):
        raise ManusPlacementError("already have an open position on this market+outcome", "duplicate-market-outcome")
    if sum(1 for row in entries if row.get("source_packet_id") == source_packet_id) >= policy[
        "max_new_positions_per_cycle"
    ]:
        raise ManusPlacementError("max_new_positions_per_cycle reached", "packet-position-cap")
    if sum(
        1
        for row in entries
        if row.get("source_packet_id") == source_packet_id and row.get("category") == category
    ) >= policy["max_positions_per_category_per_cycle"]:
        raise ManusPlacementError("max_positions_per_category_per_cycle reached", "category-position-cap")

    # Existing public read-only Phil paper-fill semantics: Gamma market,
    # exact outcome/token mapping, then a simulated taker fill at CLOB best ask.
    try:
        market = pmapi.gamma_market(market_id)
    except Exception as exc:  # noqa: BLE001 -- public read failure is a rejection
        raise ManusPlacementError("market-data lookup failed", "market-data-unavailable") from exc
    if not isinstance(market, dict) or market.get("closed"):
        raise ManusPlacementError("market is closed", "market-closed")
    live_end = _parse_live_end_date(market.get("endDate"))
    now = dt.datetime.now(dt.timezone.utc) if _now is None else _now
    if not isinstance(now, dt.datetime) or now.tzinfo is None or now.utcoffset() is None:
        raise ManusPlacementError("protected clock is invalid", "unclassified")
    if live_end <= now.astimezone(dt.timezone.utc) + dt.timedelta(minutes=policy["min_minutes_to_resolution"]):
        raise ManusPlacementError("market is too close to resolution", "too-close-to-resolution")
    try:
        tokens = pmapi.market_tokens(market)
    except Exception as exc:  # noqa: BLE001
        raise ManusPlacementError("market-data lookup failed", "market-data-unavailable") from exc
    if outcome not in tokens:
        raise ManusPlacementError(f"outcome {outcome!r} not in live market outcomes", "outcome-token-invalid")
    try:
        bid, ask = pmapi.best_prices(tokens[outcome])
    except Exception as exc:  # noqa: BLE001
        raise ManusPlacementError("market-data lookup failed", "orderbook-unavailable") from exc
    if bid is None or ask is None:
        raise ManusPlacementError("guarded PAPER placement requires both best bid and best ask", "orderbook-unavailable")
    raw_bid, raw_ask, raw_est_prob = bid, ask, est_prob
    bid = _require_nonnegative_number(bid, "live best_bid")
    ask = _require_nonnegative_number(ask, "live best_ask", positive=True)
    if not policy["min_entry_price"] <= ask <= policy["max_entry_price"]:
        raise ManusPlacementError("fill price is outside protected entry bounds", "entry-price-out-of-bounds")
    exact_bid = _exact_decimal(raw_bid, "live best_bid")
    exact_ask = _exact_decimal(raw_ask, "live best_ask")
    exact_est_prob = _exact_decimal(raw_est_prob, "estimated_probability")
    exact_spread = exact_ask - exact_bid
    if exact_spread < Decimal("0") or exact_spread > _exact_decimal(policy["max_spread"], "max_spread"):
        raise ManusPlacementError("live spread exceeds guarded PAPER max_spread", "spread-too-wide")
    exact_edge = exact_est_prob - exact_ask
    if exact_edge < _exact_decimal(policy["required_edge"], "required_edge"):
        raise ManusPlacementError("ask_edge is below the guarded PAPER required_edge", "edge-below-threshold")
    # Rounding remains a legacy/reporting representation only; all guarded
    # risk decisions above use the exact Decimal values from the inputs.
    spread = round(float(exact_spread), 4)
    edge = round(float(exact_edge), 4)

    # Gamma's single-market endpoint omits events. The list-query event shape
    # below is exactly the extraction semantics used by core/scan.py.
    live_event_id = _read_live_event_id(market_id)
    if live_event_id != event_id:
        raise ManusPlacementError("trusted fixture event_id does not match live market event identity", "event-identity-mismatch")

    # Every open legacy row without an event id must be resolved before this
    # event's exposure can be proven safe. Unknown identity fails closed.
    open_event_stake = 0.0
    for row in open_positions:
        existing_event_id = row.get("event_id")
        if existing_event_id is None:
            existing_event_id = _read_live_event_id(
                _require_identifier(row.get("market_id"), "legacy market_id"),
                unresolved_code="event-identity-unresolved-existing-position",
            )
        else:
            existing_event_id = _require_identifier(existing_event_id, "existing event_id")
        if existing_event_id == event_id:
            open_event_stake += _require_nonnegative_number(
                row.get("stake_usd"), "existing event stake", positive=True
            )
    if open_event_stake + policy["stake"] > policy["max_stake_per_event_usd"]:
        raise ManusPlacementError("max_stake_per_event_usd reached", "risk-cap-event")

    row = {
        "id": uuid.uuid4().hex[:12],
        "ts": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "market_id": market_id,
        "question": market.get("question"),
        "slug": market.get("slug"),
        "end_date": market.get("endDate"),
        "outcome": outcome,
        "token_id": tokens[outcome],
        "entry_price": ask,
        "best_bid_at_entry": bid,
        "market_prob_at_entry": ask,
        "est_prob": est_prob,
        "edge": edge,
        "stake_usd": policy["stake"],
        "shares": round(policy["stake"] / ask, 4),
        "category": category,
        # This protected class is outside the legacy real-twin allowlist;
        # research_edge_class is inert metadata only.
        "edge_class": MANUS_PAPER_EDGE_CLASS,
        "rationale": rationale,
        "strategy_rev": "",
        "status": "open",
        "source_intent_id": source_intent_id,
        "source_forecast_id": source_forecast_id,
        "source_packet_id": source_packet_id,
        "event_id": event_id,
    }
    if research_edge_class is not None:
        row["research_edge_class"] = research_edge_class
    try:
        _atomic_append_source_record(ledger_path, row)
    except OSError as exc:
        raise ManusPlacementError("guarded PAPER ledger write failed", "ledger-write-failed") from exc
    except Exception as exc:  # noqa: BLE001 -- no partial source-row mutation
        raise ManusPlacementError("guarded PAPER ledger write failed", "ledger-write-failed") from exc
    return {
        "placed": row["id"],
        "filled_at": ask,
        "edge": edge,
        "spread": spread,
        "shares": row["shares"],
        "stake_usd": row["stake_usd"],
        "source_intent_id": source_intent_id,
        "source_forecast_id": source_forecast_id,
        "source_packet_id": source_packet_id,
        "event_id": event_id,
    }


def cmd_place(args, entries):
    open_pos = [e for e in entries if e["status"] == "open"]
    if len(open_pos) >= PROTECTED["max_open_positions"]:
        sys.exit(f"REJECTED: max_open_positions ({PROTECTED['max_open_positions']}) reached")
    if args.stake > PROTECTED["max_stake_usd"]:
        sys.exit(f"REJECTED: stake {args.stake} > max_stake_usd {PROTECTED['max_stake_usd']}")
    if args.stake > bankroll(entries):
        sys.exit("REJECTED: insufficient sim cash")
    if not 0.0 < args.est_prob < 1.0:
        sys.exit("REJECTED: est-prob must be in (0,1)")
    if any(e["market_id"] == args.market_id and e["outcome"] == args.outcome
           for e in open_pos):
        sys.exit("REJECTED: already have an open position on this market+outcome")
    m = pmapi.gamma_market(args.market_id)
    if m.get("closed"):
        sys.exit("REJECTED: market is closed")
    tokens = pmapi.market_tokens(m)
    if args.outcome not in tokens:
        sys.exit(f"REJECTED: outcome {args.outcome!r} not in {list(tokens)}")
    bid, ask = pmapi.best_prices(tokens[args.outcome])
    if ask is None:
        sys.exit("REJECTED: no asks in the book (cannot fill)")
    if not PROTECTED["min_entry_price"] <= ask <= PROTECTED["max_entry_price"]:
        sys.exit(f"REJECTED: fill price {ask} outside protected bounds")
    entry = {
        "id": uuid.uuid4().hex[:12],
        "ts": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "market_id": args.market_id,
        "question": m.get("question"),
        "slug": m.get("slug"),
        "end_date": m.get("endDate"),
        "outcome": args.outcome,
        "token_id": tokens[args.outcome],
        "entry_price": ask,
        "best_bid_at_entry": bid,
        "market_prob_at_entry": ask,
        "est_prob": args.est_prob,
        "edge": round(args.est_prob - ask, 4),
        "stake_usd": args.stake,
        "shares": round(args.stake / ask, 4),
        "category": args.category,
        "edge_class": args.edge_class,
        "rationale": args.rationale,
        "strategy_rev": args.strategy_rev,
        "status": "open",
    }
    with LEDGER.open("a") as f:
        f.write(json.dumps(entry) + "\n")
    print(json.dumps({"placed": entry["id"], "filled_at": ask, "edge": entry["edge"],
                      "shares": entry["shares"], "question": entry["question"]}, indent=2))


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("place")
    p.add_argument("--market-id", required=True)
    p.add_argument("--outcome", required=True, help="exact outcome name, e.g. Yes")
    p.add_argument("--est-prob", type=float, required=True,
                   help="agent's probability that this outcome wins")
    p.add_argument("--stake", type=float, required=True)
    p.add_argument("--category", required=True,
                   help="agent-assigned category, e.g. earnings/soccer/esports/news")
    p.add_argument("--edge-class", required=True,
                   choices=["info-race", "cross-market", "book-devig", "other"],
                   help="playbook edge class this bet claims (scored separately)")
    p.add_argument("--rationale", required=True, help="one-line reason (for the retro)")
    p.add_argument("--strategy-rev", default="", help="git rev of strategy/ used")
    sub.add_parser("status")
    args = ap.parse_args()
    entries = read_ledger()
    if args.cmd == "status":
        cmd_status(entries)
    else:
        cmd_place(args, entries)


if __name__ == "__main__":
    main()

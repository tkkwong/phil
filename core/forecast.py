#!/usr/bin/env python3
"""Forecast ledger: score every researched estimate, not just bets.

PROTECTED CORE — the trading agent must not edit files under core/.
This is the only writer of journal/forecasts.jsonl (resolve.py settles rows).

The experiment's honest metric is brier_delta — is the agent's probability a
better forecast than the market's own price? — and measuring that needs no
stake. Research that ends in a skip (no-edge, market-agrees) still produced
an estimate; recording it here turns ~10-30 researched candidates/day into
scored calibration feedback instead of ~0-1 settled bets/day.

No caps, no edge floor, no fill: the market baseline is the MID at record
time ((bid+ask)/2), not the ask a bet would fill at — a forecast has no
transaction, and the mid is the stricter benchmark. Forecast brier_delta is
therefore NOT comparable to bet brier_delta; score.py reports them in
separate sections. est_prob is the agent's honest belief, formed before
anchoring on the price, exactly as for bets.

One live forecast per market+outcome: recurring re-checks of the same market
must not flood the stats with correlated rows. A materially changed read
(|delta est_prob| >= 0.05, or a changed funnel decision) may replace the live
row via record --supersede: the rows are linked (supersedes / superseded_by),
only the latest row counts in headline scoring, and the superseded row still
settles into score.py's separate revised-away slice - whether revisions
actually improve estimates is measured, not assumed (PLBY 2026-08-10: the
revision was worse than the original).

Usage:
  record: python3 core/forecast.py record --market-id 123 --outcome Yes \
            --est-prob 0.62 --category econ --skip-reason no-edge \
            [--fit-score 4] [--note "..."] [--strategy-rev abc1234] \
            [--confirm-extreme] [--supersede]
  status: python3 core/forecast.py status
"""
import argparse
import datetime as dt
import json
import os
import pathlib
import sys
import tempfile
import uuid
from collections import Counter

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import pmapi  # noqa: E402

ROOT = pathlib.Path(__file__).resolve().parent.parent
FORECASTS = ROOT / "journal" / "forecasts.jsonl"

# A supersede must change something material: a re-record of an unchanged
# estimate is the correlated-row flooding the one-live-row rule exists to
# prevent, not a revision.
MIN_REVISION_DELTA = 0.05

# An |est_prob - mid| gap this large is either a deliberate extreme
# disagreement (rare: the outside-view-veto class) or an inverted outcome
# side (fe954ed9f325: --outcome No with the Yes-side est_prob, silently
# recording the opposite of the researched belief). The flag costs one
# keystroke exactly when the agent should be pausing anyway; the typo gets
# caught at the only moment it is fixable.
EXTREME_DISAGREEMENT = 0.40


FORECAST_REJECTION_CODES = frozenset(
    {
        "duplicate-forecast",
        "market-closed",
        "outcome-token-invalid",
        "market-data-unavailable",
        "orderbook-unavailable",
        "extreme-disagreement",
        "invalid-input",
        "unclassified",
    }
)


class ForecastRecordError(ValueError):
    """Raised when protected forecast recording rejects an input before write.

    ``code`` is a stable, non-sensitive diagnostic identifier drawn from
    ``FORECAST_REJECTION_CODES``. It carries no exception text, filesystem
    path, or provider response content, and is diagnostic metadata only:
    guard order and policy are untouched.
    """

    def __init__(self, message, code="unclassified"):
        if code not in FORECAST_REJECTION_CODES:
            code = "unclassified"
        super().__init__(message)
        self.code = code


def read_forecasts(forecast_path=FORECASTS):
    """Read one forecast book; the optional path exists solely as an internal test seam."""
    forecast_path = pathlib.Path(forecast_path)
    if not forecast_path.exists():
        return []
    return [json.loads(line) for line in forecast_path.read_text().splitlines() if line.strip()]


def cmd_status(rows):
    print(json.dumps({
        "total": len(rows),
        "by_status": dict(Counter(r["status"] for r in rows)),
        "by_skip_reason": dict(Counter(r.get("skip_reason") or "?" for r in rows)),
        "settled_wins": sum(1 for r in rows if r["status"] == "won"),
        "revised_away": sum(1 for r in rows if r.get("superseded_by")),
    }, indent=2))


def _num(value):
    """gamma numeric field -> float rounded to cents, or None if absent/unparseable."""
    if value in (None, ""):
        return None
    try:
        return round(float(value), 2)
    except (TypeError, ValueError):
        return None


def _validate_source_intent_id(source_intent_id):
    """Validate optional durable Manus provenance without changing old rows."""
    if source_intent_id is None:
        return None
    if not isinstance(source_intent_id, str):
        raise ForecastRecordError("source_intent_id must be a canonical UUIDv4", "invalid-input")
    try:
        parsed = uuid.UUID(source_intent_id)
    except (AttributeError, ValueError) as exc:
        raise ForecastRecordError("source_intent_id must be a canonical UUIDv4", "invalid-input") from exc
    if parsed.version != 4 or str(parsed) != source_intent_id:
        raise ForecastRecordError("source_intent_id must be a canonical UUIDv4", "invalid-input")
    return source_intent_id


def _atomic_append_source_record(forecast_path, row):
    """Atomically append a provenance-bearing row under the single-writer assumption."""
    original = forecast_path.read_bytes() if forecast_path.exists() else b""
    temporary_path = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=forecast_path.parent,
            prefix=f".{forecast_path.name}.",
            suffix=".tmp",
            delete=False,
        ) as temporary_book:
            temporary_path = pathlib.Path(temporary_book.name)
            # Preserve every original byte before adding the new JSONL record.
            temporary_book.write(original)
            if original and not original.endswith(b"\n"):
                temporary_book.write(b"\n")
            temporary_book.write((json.dumps(row) + "\n").encode("utf-8"))
            temporary_book.flush()
            os.fsync(temporary_book.fileno())
        # The file is closed before replacement for Windows compatibility.
        os.replace(temporary_path, forecast_path)
        temporary_path = None
    except Exception:
        if temporary_path is not None:
            try:
                temporary_path.unlink()
            except OSError:
                pass
        raise


def _write_record(forecast_path, rows, row, old):
    """Perform the single forecast-book mutation after all guards have passed."""
    if row.get("source_intent_id") is not None:
        _atomic_append_source_record(forecast_path, row)
        return
    if old is not None:
        old["superseded_by"] = row["id"]
        old["superseded_ts"] = row["ts"]
        forecast_path.write_text("".join(json.dumps(r) + "\n" for r in rows + [row]))
    else:
        with forecast_path.open("a") as forecast_book:
            forecast_book.write(json.dumps(row) + "\n")


def record_forecast(
    *,
    market_id,
    outcome,
    est_prob,
    category,
    skip_reason,
    fit_score=None,
    note="",
    strategy_rev="",
    confirm_extreme=False,
    supersede=False,
    source_intent_id=None,
    forecast_path=FORECASTS,
    existing_rows=None,
):
    """Record one guarded forecast using the existing Gamma/CLOB benchmark flow.

    ``source_intent_id`` is optional for the legacy CLI and required by the
    Manus guardian. When present, it is canonical UUIDv4 provenance and is
    rejected if it has ever appeared in this forecast book, regardless of
    settlement or supersession. This initial implementation assumes the
    existing single-writer forecast-book discipline; it intentionally adds no
    multi-process locking or recovery protocol.

    ``forecast_path`` and ``existing_rows`` are internal test seams. They are
    not exposed by the production CLI or the Manus guardian CLI.
    """
    forecast_path = pathlib.Path(forecast_path)
    rows = list(existing_rows) if existing_rows is not None else read_forecasts(forecast_path)
    source_intent_id = _validate_source_intent_id(source_intent_id)
    if source_intent_id is not None and supersede:
        raise ForecastRecordError("source_intent_id records may not use --supersede", "invalid-input")

    # Durable provenance comes before market I/O and all normal duplicate or
    # supersede logic. A successful append followed by a caller crash remains
    # detectable on retry, even after settlement or supersession.
    if source_intent_id is not None and any(
        row.get("source_intent_id") == source_intent_id for row in rows
    ):
        raise ForecastRecordError("source_intent_id has already recorded a forecast", "duplicate-forecast")

    if not isinstance(est_prob, (int, float)) or isinstance(est_prob, bool) or not 0.0 < est_prob < 1.0:
        raise ForecastRecordError("est-prob must be in (0,1)", "invalid-input")

    live = [
        row
        for row in rows
        if row["market_id"] == market_id
        and row["outcome"] == outcome
        and row["status"] == "open"
        and not row.get("superseded_by")
    ]
    if live and not supersede:
        raise ForecastRecordError(
            "already have an open forecast on this market+outcome "
            "(a materially changed read may supersede it: --supersede)",
            "duplicate-forecast",
        )
    old = None
    if supersede:
        if not live:
            raise ForecastRecordError("--supersede, but no live open forecast on this market+outcome to supersede", "duplicate-forecast")
        old = live[0]
        # Round like ledger.py's edge field so an exactly-boundary revision
        # (0.33 - 0.28 = 0.049999...) does not float-drop below the gate.
        if (
            round(abs(est_prob - old["est_prob"]), 4) < MIN_REVISION_DELTA
            and skip_reason == old.get("skip_reason")
        ):
            raise ForecastRecordError(
                "supersede needs a material change — "
                f"|delta est_prob| >= {MIN_REVISION_DELTA} "
                f"(old {old['est_prob']}, new {est_prob}) or a changed "
                f"skip-reason (old {old.get('skip_reason')!r})",
                "duplicate-forecast",
            )

    # Public, read-only market data preserves the existing forecast semantics.
    # All market, outcome, and price checks complete before the only write.
    try:
        market = pmapi.gamma_market(market_id)
    except Exception as exc:  # noqa: BLE001 — surface a stable pre-write rejection
        raise ForecastRecordError("market-data lookup failed before recording", "market-data-unavailable") from exc
    if market.get("closed"):
        raise ForecastRecordError("market is closed", "market-closed")
    try:
        tokens = pmapi.market_tokens(market)
    except Exception as exc:  # noqa: BLE001 — malformed public market data
        raise ForecastRecordError("market-data lookup failed before recording", "market-data-unavailable") from exc
    if outcome not in tokens:
        raise ForecastRecordError(f"outcome {outcome!r} not in {list(tokens)}", "outcome-token-invalid")
    try:
        bid, ask = pmapi.best_prices(tokens[outcome])
    except Exception as exc:  # noqa: BLE001 — public CLOB read failure
        raise ForecastRecordError("market-data lookup failed before recording", "orderbook-unavailable") from exc
    if bid is None and ask is None:
        raise ForecastRecordError("empty book — no market probability to benchmark against", "orderbook-unavailable")
    mid = (bid + ask) / 2 if bid is not None and ask is not None else bid or ask

    gap = abs(est_prob - mid)
    if gap > EXTREME_DISAGREEMENT and not confirm_extreme:
        raise ForecastRecordError(
            f"est_prob {est_prob} vs market mid {round(mid, 4)} "
            f"for outcome {outcome!r} differs by {round(gap, 4)} "
            f"(> {EXTREME_DISAGREEMENT}). If this extreme disagreement is your "
            "researched belief, re-run with --confirm-extreme; if not, you "
            "probably inverted the outcome side (est_prob must be for the "
            f"named outcome {outcome!r}, not its complement).",
            "extreme-disagreement",
        )

    row = {
        "id": uuid.uuid4().hex[:12],
        "ts": dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "market_id": market_id,
        "question": market.get("question"),
        "slug": market.get("slug"),
        "end_date": market.get("endDate"),
        "outcome": outcome,
        "token_id": tokens[outcome],
        "est_prob": est_prob,
        "best_bid_at_record": bid,
        "best_ask_at_record": ask,
        "market_prob_at_record": round(mid, 4),
        # Book size at record time, from the same gamma record scan.py reads.
        # Added 2026-09-09 so research edge can be split by liquidity later;
        # gnhf run 5 had to drop that feature for lack of it. None when gamma
        # omits the field - never a guess, never a later re-read.
        "liquidity_at_record": _num(market.get("liquidityNum")),
        "volume_24h_at_record": _num(market.get("volume24hr")),
        "category": category,
        "skip_reason": skip_reason,
        "fit_score": fit_score,
        "note": note,
        "strategy_rev": strategy_rev,
        "status": "open",
    }
    if source_intent_id is not None:
        row["source_intent_id"] = source_intent_id
    if old is not None:
        row["supersedes"] = old["id"]

    _write_record(forecast_path, rows, row, old)
    out = {
        "recorded": row["id"],
        "mid": row["market_prob_at_record"],
        "bid": bid,
        "ask": ask,
        "delta_vs_mid": round(est_prob - mid, 4),
        "question": row["question"],
    }
    if source_intent_id is not None:
        out["source_intent_id"] = source_intent_id
    if old is not None:
        out["supersedes"] = old["id"]
        out["delta_est_prob"] = round(est_prob - old["est_prob"], 4)
    return out


def cmd_record(args, rows):
    try:
        out = record_forecast(
            market_id=args.market_id,
            outcome=args.outcome,
            est_prob=args.est_prob,
            category=args.category,
            skip_reason=args.skip_reason,
            fit_score=args.fit_score,
            note=args.note,
            strategy_rev=args.strategy_rev,
            confirm_extreme=args.confirm_extreme,
            supersede=args.supersede,
            existing_rows=rows,
        )
    except ForecastRecordError as exc:
        sys.exit(f"REJECTED: {exc}")
    print(json.dumps(out, indent=2))


def main():
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("record")
    p.add_argument("--market-id", required=True)
    p.add_argument("--outcome", required=True, help="exact outcome name, e.g. Yes")
    p.add_argument("--est-prob", type=float, required=True,
                   help="agent's honest probability, formed before reading the price")
    p.add_argument("--category", required=True,
                   help="agent-assigned category, e.g. earnings/soccer/esports/news")
    p.add_argument("--skip-reason", required=True,
                   help="funnel disposition: bet|no-edge|market-agrees|... "
                        "(use 'bet' when a place follows this forecast)")
    p.add_argument("--supersede", action="store_true",
                   help="replace this market+outcome's live open forecast with a "
                        "materially changed read (links the rows; the old one is "
                        "graded in the revised-away slice, not the headline stats)")
    p.add_argument("--confirm-extreme", action="store_true",
                   help="required when |est_prob - mid| > "
                        f"{EXTREME_DISAGREEMENT}: confirms the extreme "
                        "disagreement is deliberate, not an inverted outcome side")
    p.add_argument("--fit-score", type=int, default=None, help="playbook fit score 0-5")
    p.add_argument("--note", default="", help="one line of context (optional)")
    p.add_argument("--strategy-rev", default="", help="git rev of strategy/ used")
    sub.add_parser("status")
    args = ap.parse_args()

    rows = read_forecasts()
    if args.cmd == "status":
        cmd_status(rows)
    else:
        cmd_record(args, rows)


if __name__ == "__main__":
    main()

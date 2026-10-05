#!/usr/bin/env python3
"""Scan Polymarket for candidate short-term markets.

PROTECTED CORE — the trading agent must not edit files under core/.

Split of responsibilities (changed 2026-08-03):
  * WHAT TO LOOK FOR is the agent's: `strategy/discovery.py` supplies the
    gamma queries (windows, volume/liquidity floors, ordering, tags). The
    agent owns its own sensing and can widen or retarget it when its evidence
    says the candidate pool is an artifact rather than the market.
  * WHAT IS ALLOWED stays here: banned market classes, minimum time to
    resolution, entry-price bounds and the output contract are enforced after
    discovery, on every candidate, and cannot be bypassed by a query.

If `strategy/discovery.py` is missing, raises, or returns nothing usable, this
falls back to the built-in default query and says so on stderr — a broken
discovery module degrades sensing, it never silently returns an empty market.

Usage: python3 core/scan.py [--hours 168] [--min-volume-24h 0] [--limit 400]
Output: JSON lines, one candidate market per line.
"""
import argparse
import datetime as dt
import json
import pathlib
import re
import sys

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))
import pmapi  # noqa: E402

ROOT = pathlib.Path(__file__).resolve().parent.parent
PROTECTED = json.loads((ROOT / "config" / "protected.json").read_text())

_MAX_PROVIDER_TAG_RECORDS = 32
_MAX_PROVIDER_TAG_ID_LENGTH = 64
_MAX_PROVIDER_TAG_SLUG_LENGTH = 128
_MAX_PROVIDER_TAG_LABEL_LENGTH = 256
_PROVIDER_TAG_ID_RE = re.compile(r"^[0-9]+$")
_PROVIDER_TAG_SLUG_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]*$")


def utcnow():
    return dt.datetime.now(dt.timezone.utc)


def iso(ts):
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")


def default_queries(args, min_end, horizon):
    q = {"_label": "default", "closed": "false", "order": "endDate",
         "ascending": "true", "end_date_min": iso(min_end),
         "end_date_max": iso(horizon)}
    if args.min_total_volume > 0:
        q["volume_num_min"] = args.min_total_volume
    return [q]


def discovery_queries(args, min_end, horizon):
    """Ask the agent-owned discovery module what to look for."""
    try:
        sys.path.insert(0, str(ROOT / "strategy"))
        import discovery  # noqa: PLC0415 — optional, agent-owned
        qs = discovery.queries(now=utcnow(), min_end=min_end, horizon=horizon,
                               args=vars(args), protected=PROTECTED)
        qs = [dict(q) for q in qs if isinstance(q, dict)]
        if not qs:
            raise ValueError("discovery.queries() returned no usable queries")
        for q in qs:
            q.setdefault("closed", "false")
            # a query may not reach past the protected resolution floor
            q["end_date_min"] = max(str(q.get("end_date_min") or ""), iso(min_end))
        print(f"scan: discovery.py supplied {len(qs)} quer{'y' if len(qs) == 1 else 'ies'}",
              file=sys.stderr)
        return qs
    except Exception as e:  # noqa: BLE001 — any failure falls back, loudly
        print(f"scan: discovery.py unusable ({type(e).__name__}: {e}); "
              f"falling back to default query", file=sys.stderr)
        return default_queries(args, min_end, horizon)


def keep(m, seen, banned, args):
    """Protected admissibility filter. Returns the output record or None."""
    q = m.get("question") or ""
    if m.get("id") in seen:
        return None
    seen.add(m.get("id"))
    if any(p.search(q) for p in banned):
        return None
    if float(m.get("volume24hr") or 0) < args.min_volume_24h:
        return None
    try:
        prices = [float(p) for p in json.loads(m.get("outcomePrices", "[]"))]
    except (ValueError, TypeError):
        return None
    if not prices:
        return None
    # skip effectively-decided markets (in-play blowouts, resolved-in-waiting)
    if max(prices) > PROTECTED["max_entry_price"] or min(prices) < PROTECTED["min_entry_price"]:
        return None
    event = (m.get("events") or [{}])[0]
    return {
        "market_id": m.get("id"),
        "question": q,
        "end_date": m.get("endDate"),
        "event_id": event.get("id"),
        "event_slug": event.get("slug"),
        "outcomes": json.loads(m.get("outcomes", "[]")),
        "outcome_prices": prices,
        "clob_token_ids": json.loads(m.get("clobTokenIds", "[]")),
        "volume_24h": float(m.get("volume24hr") or 0),
        "liquidity": float(m.get("liquidityNum") or 0),
        "slug": m.get("slug"),
        "description": (m.get("description") or "")[:500],
    }


def _parse_utc_end(value):
    """Return a timezone-aware UTC datetime for an ISO-8601 endDate, else None."""
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


def _time_fresh(end_date, closed, min_end, horizon):
    """Authoritative local freshness gate using the one frozen scan clock.

    Provider query parameters (closed=false, end_date_min) are advisory input
    filtering, not enforcement. Every returned market must independently pass
    this local time-bound check before becoming a candidate.
    """
    if closed is True:
        return False
    end = _parse_utc_end(end_date)
    if end is None:
        return False
    return min_end <= end <= horizon


def _validate_scan_controls(include_provider_metadata, max_candidates):
    """Reject non-default API controls that would make scanner bounds ambiguous."""
    if type(include_provider_metadata) is not bool:
        raise TypeError("include_provider_metadata must be a bool")
    if max_candidates is not None:
        if type(max_candidates) is not int:
            raise TypeError("max_candidates must be a positive int or None")
        if max_candidates <= 0:
            raise ValueError("max_candidates must be a positive int or None")


def _normalize_provider_tag_text(value, maximum, pattern=None, require_printable=False):
    if not isinstance(value, str) or not value or len(value) > maximum:
        return None
    if any(ord(character) < 32 or 127 <= ord(character) <= 159 for character in value):
        return None
    if require_printable and not value.isprintable():
        return None
    if pattern is not None and not pattern.fullmatch(value):
        return None
    return value


def _normalize_provider_tag(value):
    """Return one bounded provider-owned tag record or ``None`` if invalid."""
    if not isinstance(value, dict):
        return None
    raw_id = value.get("id")
    if isinstance(raw_id, int) and not isinstance(raw_id, bool):
        tag_id = str(raw_id)
    elif isinstance(raw_id, str):
        tag_id = raw_id
    else:
        return None
    if len(tag_id) > _MAX_PROVIDER_TAG_ID_LENGTH or not _PROVIDER_TAG_ID_RE.fullmatch(tag_id):
        return None
    slug = _normalize_provider_tag_text(
        value.get("slug"), _MAX_PROVIDER_TAG_SLUG_LENGTH, _PROVIDER_TAG_SLUG_RE
    )
    label = _normalize_provider_tag_text(
        value.get("label"), _MAX_PROVIDER_TAG_LABEL_LENGTH, require_printable=True
    )
    if slug is None or label is None:
        return None
    return {"id": tag_id, "slug": slug, "label": label}


def _provider_metadata(market_id):
    """Return a bounded non-authoritative tag envelope for one kept market."""
    try:
        response = pmapi.gamma_market_tags(market_id)
    except Exception:  # noqa: BLE001 — emit the fixed safe unavailable status
        return {"market_tags_status": "unavailable", "market_tags": []}
    if not isinstance(response, list) or len(response) > _MAX_PROVIDER_TAG_RECORDS:
        return {"market_tags_status": "invalid", "market_tags": []}

    tags_by_id = {}
    for raw_tag in response:
        tag = _normalize_provider_tag(raw_tag)
        if tag is None:
            return {"market_tags_status": "invalid", "market_tags": []}
        existing = tags_by_id.get(tag["id"])
        if existing is not None and existing != tag:
            return {"market_tags_status": "invalid", "market_tags": []}
        tags_by_id[tag["id"]] = tag
    return {
        "market_tags_status": "ok",
        "market_tags": sorted(tags_by_id.values(), key=lambda tag: (len(tag["id"]), tag["id"])),
    }


def _iter_candidates(
    *,
    hours=168,
    min_volume_24h=0,
    limit=400,
    min_total_volume=50000,
    include_provider_metadata: bool = False,
    max_candidates: int | None = None,
):
    """Yield normal protected scan candidates in existing deterministic order."""
    _validate_scan_controls(include_provider_metadata, max_candidates)
    args = argparse.Namespace(
        hours=hours,
        min_volume_24h=min_volume_24h,
        limit=limit,
        min_total_volume=min_total_volume,
    )
    banned = [re.compile(p, re.I) for p in PROTECTED["banned_question_patterns"]]
    now = utcnow()
    horizon = now + dt.timedelta(hours=args.hours)
    min_end = now + dt.timedelta(minutes=PROTECTED["min_minutes_to_resolution"])

    seen, kept = set(), 0
    for base in discovery_queries(args, min_end, horizon):
        label = base.pop("_label", "unlabeled")
        got = 0
        offset = 0
        while offset < args.limit:
            try:
                batch = pmapi.gamma_markets(**dict(base, limit=100, offset=offset))
            except RuntimeError as e:
                print(f"scan: query {label!r} failed: {e}", file=sys.stderr)
                break
            if not batch:
                break
            offset += len(batch)
            for m in batch:
                if max_candidates is not None and kept >= max_candidates:
                    return
                rec = keep(m, seen, banned, args)
                if rec and not _time_fresh(rec["end_date"], m.get("closed"), min_end, horizon):
                    rec = None
                if rec:
                    if include_provider_metadata:
                        rec["provider_metadata"] = _provider_metadata(rec["market_id"])
                    got += 1
                    kept += 1
                    yield rec
                    if max_candidates is not None and kept >= max_candidates:
                        return
        print(f"scan: query {label!r} -> {got} candidates", file=sys.stderr)
    print(f"scan: {kept} candidates total within {args.hours}h", file=sys.stderr)


def scan_candidates(
    *,
    hours=168,
    min_volume_24h=0,
    limit=400,
    min_total_volume=50000,
    include_provider_metadata: bool = False,
    max_candidates: int | None = None,
):
    """Return normal protected scan candidates in existing deterministic order."""
    return list(_iter_candidates(
        hours=hours,
        min_volume_24h=min_volume_24h,
        limit=limit,
        min_total_volume=min_total_volume,
        include_provider_metadata=include_provider_metadata,
        max_candidates=max_candidates,
    ))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--hours", type=float, default=168)
    ap.add_argument("--min-volume-24h", type=float, default=0)
    ap.add_argument("--limit", type=int, default=400,
                    help="max markets paged per query")
    ap.add_argument("--min-total-volume", type=float, default=50000,
                    help="lifetime-volume floor (gamma volume_num_min) for the "
                         "DEFAULT query. Results page in endDate order and the "
                         "near-term universe is thousands of sub-daily markets "
                         "deep, so without a floor the scan never escapes today "
                         "regardless of --hours. strategy/discovery.py may set "
                         "its own per-query floors.")
    args = ap.parse_args()
    for record in _iter_candidates(
        hours=args.hours,
        min_volume_24h=args.min_volume_24h,
        limit=args.limit,
        min_total_volume=args.min_total_volume,
    ):
        print(json.dumps(record))


if __name__ == "__main__":
    main()

"""Offline parity tests for the protected core scan API extraction."""
import argparse
import datetime as dt
import io
import json
import sys
import types
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest.mock import patch

from core import scan


def market(market_id, *, question=None, volume=10, prices="[0.5, 0.5]"):
    """Return the smallest Gamma-style record accepted by scan.keep()."""
    return {
        "id": market_id,
        "question": question or f"Will {market_id} happen?",
        "volume24hr": volume,
        "outcomePrices": prices,
        "endDate": "2026-10-01T00:00:00Z",
        "events": [{"id": f"event-{market_id}", "slug": f"event-{market_id}"}],
        "outcomes": json.dumps(["Yes", "No"]),
        "clobTokenIds": json.dumps([f"yes-{market_id}", f"no-{market_id}"]),
        "liquidityNum": 25,
        "slug": f"slug-{market_id}",
        "description": f"Description for {market_id}",
    }


class ScanCandidatesTests(unittest.TestCase):
    def scan_with(self, queries, batches, **kwargs):
        with patch.object(scan, "discovery_queries", return_value=[dict(query) for query in queries]), \
             patch.object(scan.pmapi, "gamma_markets", side_effect=batches):
            return scan.scan_candidates(**kwargs)

    def test_returns_existing_candidate_shape_in_query_and_market_order(self):
        records = self.scan_with(
            [{"_label": "first"}],
            [[market("a"), market("b"), market("c")], []],
        )
        self.assertIsInstance(records, list)
        self.assertEqual([record["market_id"] for record in records], ["a", "b", "c"])
        self.assertEqual(
            set(records[0]),
            {
                "market_id", "question", "end_date", "event_id", "event_slug",
                "outcomes", "outcome_prices", "clob_token_ids", "volume_24h",
                "liquidity", "slug", "description",
            },
        )

    def test_deduplication_and_existing_keep_filter_remain_authoritative(self):
        records = self.scan_with(
            [{"_label": "one"}, {"_label": "two"}],
            [
                [market("kept"), market("banned", question="Up or Down - BTC 10:30AM-11:30AM")],
                [],
                [market("kept"), market("also-kept")],
                [],
            ],
        )
        self.assertEqual([record["market_id"] for record in records], ["kept", "also-kept"])

    def test_existing_page_offset_limit_semantics_are_preserved(self):
        calls = []

        def gamma_markets(**params):
            calls.append(params)
            return [market("first")] if params["offset"] == 0 else [market("second")]

        with patch.object(scan, "discovery_queries", return_value=[{"_label": "one"}]), \
             patch.object(scan.pmapi, "gamma_markets", side_effect=gamma_markets):
            records = scan.scan_candidates(limit=2)
        self.assertEqual([record["market_id"] for record in records], ["first", "second"])
        self.assertEqual([call["offset"] for call in calls], [0, 1])
        self.assertTrue(all(call["limit"] == 100 for call in calls))

    def test_existing_discovery_fallback_is_used_by_scan_candidates(self):
        failing_discovery = types.SimpleNamespace(
            queries=lambda **_kwargs: (_ for _ in ()).throw(RuntimeError("fixture failure"))
        )
        now = dt.datetime(2026, 9, 28, 12, tzinfo=dt.timezone.utc)
        calls = []

        def gamma_markets(**params):
            calls.append(params)
            return [market("fallback")] if params["offset"] == 0 else []

        with patch.dict(sys.modules, {"discovery": failing_discovery}), \
             patch.object(scan, "utcnow", return_value=now), \
             patch.object(scan.pmapi, "gamma_markets", side_effect=gamma_markets), \
             redirect_stderr(io.StringIO()):
            records = scan.scan_candidates(hours=168, min_total_volume=12345)
        self.assertEqual([record["market_id"] for record in records], ["fallback"])
        min_minutes = scan.PROTECTED["min_minutes_to_resolution"]
        expected = scan.default_queries(
            argparse.Namespace(hours=168, min_volume_24h=0, limit=400, min_total_volume=12345),
            now + dt.timedelta(minutes=min_minutes),
            now + dt.timedelta(hours=168),
        )[0]
        expected.pop("_label")
        self.assertEqual({key: calls[0][key] for key in expected}, expected)

    def test_main_streams_iterator_records_as_json_lines(self):
        records = [{"market_id": "a"}, {"market_id": "b"}]
        stdout = io.StringIO()
        with patch.object(scan, "_iter_candidates", return_value=iter(records)) as candidates, \
             patch.object(sys, "argv", ["scan.py"]), redirect_stdout(stdout):
            scan.main()
        candidates.assert_called_once_with(
            hours=168,
            min_volume_24h=0,
            limit=400,
            min_total_volume=50000,
        )
        self.assertEqual(stdout.getvalue(), "".join(json.dumps(record) + "\n" for record in records))

    def test_main_keeps_earlier_jsonl_output_when_a_later_page_raises(self):
        calls = 0

        def gamma_markets(**_params):
            nonlocal calls
            calls += 1
            if calls == 1:
                return [market("a")]
            raise ValueError("deterministic later-page failure")

        stdout = io.StringIO()
        with patch.object(scan, "discovery_queries", return_value=[{"_label": "one"}]), \
             patch.object(scan.pmapi, "gamma_markets", side_effect=gamma_markets), \
             patch.object(sys, "argv", ["scan.py", "--limit", "2"]), \
             redirect_stdout(stdout), redirect_stderr(io.StringIO()):
            with self.assertRaisesRegex(ValueError, "deterministic later-page failure"):
                scan.main()
        self.assertEqual(stdout.getvalue(), json.dumps(scan.keep(
            market("a"), set(), [], argparse.Namespace(min_volume_24h=0)
        )) + "\n")


if __name__ == "__main__":
    unittest.main()

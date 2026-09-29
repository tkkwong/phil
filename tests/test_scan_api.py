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

from core import pmapi, scan


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


def tag(tag_id="1", slug="sports", label="Sports"):
    """Return the smallest supported public Gamma taxonomy object."""
    return {"id": tag_id, "slug": slug, "label": label}


class GammaMarketTagHelperTests(unittest.TestCase):
    def test_market_tag_helper_uses_one_decimal_market_id_path_segment_without_auth(self):
        with patch.object(pmapi, "get_json", return_value=[]) as get_json:
            self.assertEqual(pmapi.gamma_market_tags("4609832"), [])
        get_json.assert_called_once_with(
            "https://gamma-api.polymarket.com/markets/4609832/tags", retries=1
        )

    def test_market_tag_helper_rejects_route_altering_market_ids_before_get(self):
        malformed = (
            "",
            "4609832/../events",
            "4609832?closed=false",
            "4609832#fragment",
            "4609832%2Ftags",
            "../4609832",
            4609832,
        )
        for market_id in malformed:
            with self.subTest(market_id=market_id), patch.object(pmapi, "get_json") as get_json:
                with self.assertRaises(ValueError):
                    pmapi.gamma_market_tags(market_id)
                get_json.assert_not_called()


class ScanCandidatesTests(unittest.TestCase):
    def scan_with(self, queries, batches, **kwargs):
        with patch.object(scan, "discovery_queries", return_value=[dict(query) for query in queries]), \
             patch.object(scan.pmapi, "gamma_markets", side_effect=batches):
            return scan.scan_candidates(**kwargs)

    def test_default_api_parity_keeps_existing_shape_order_and_avoids_tag_lookup(self):
        with patch.object(scan, "discovery_queries", return_value=[{"_label": "first"}]), \
             patch.object(scan.pmapi, "gamma_markets", side_effect=[[market("a"), market("b")], []]), \
             patch.object(scan.pmapi, "gamma_market_tags") as market_tags:
            records = scan.scan_candidates()
        self.assertIsInstance(records, list)
        self.assertEqual([record["market_id"] for record in records], ["a", "b"])
        expected_seen = set()
        expected = [
            scan.keep(candidate, expected_seen, [], argparse.Namespace(min_volume_24h=0))
            for candidate in (market("a"), market("b"))
        ]
        self.assertEqual(records, expected)
        self.assertEqual(
            set(records[0]),
            {
                "market_id", "question", "end_date", "event_id", "event_slug",
                "outcomes", "outcome_prices", "clob_token_ids", "volume_24h",
                "liquidity", "slug", "description",
            },
        )
        market_tags.assert_not_called()

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

    def test_max_candidates_is_global_across_queries_and_bounds_tag_lookups(self):
        first = [market(f"first-{index:02d}") for index in range(15)]
        second = [market(f"second-{index:02d}") for index in range(15)]
        with patch.object(
            scan,
            "discovery_queries",
            return_value=[{"_label": "first"}, {"_label": "second"}],
        ), patch.object(scan.pmapi, "gamma_markets", side_effect=[first, [], second]), \
             patch.object(scan.pmapi, "gamma_market_tags", return_value=[tag()]) as market_tags:
            records = scan.scan_candidates(include_provider_metadata=True, max_candidates=20)
        self.assertEqual(
            [record["market_id"] for record in records],
            [f"first-{index:02d}" for index in range(15)]
            + [f"second-{index:02d}" for index in range(5)],
        )
        self.assertEqual(market_tags.call_count, 20)

    def test_metadata_lookup_runs_only_after_keep_and_deduplication(self):
        rejected = market("rejected", question="Up or Down - BTC 10:30AM-11:30AM")
        with patch.object(scan, "discovery_queries", return_value=[{"_label": "one"}]), \
             patch.object(
                 scan.pmapi,
                 "gamma_markets",
                 side_effect=[[rejected, market("kept"), market("kept"), market("other")], []],
             ), patch.object(scan.pmapi, "gamma_market_tags", return_value=[tag()]) as market_tags:
            records = scan.scan_candidates(include_provider_metadata=True)
        self.assertEqual([record["market_id"] for record in records], ["kept", "other"])
        self.assertEqual([call.args[0] for call in market_tags.call_args_list], ["kept", "other"])

    def test_metadata_mode_preserves_existing_candidate_order(self):
        ordered = [market("third"), market("first"), market("second")]
        with patch.object(scan, "discovery_queries", return_value=[{"_label": "one"}]), \
             patch.object(scan.pmapi, "gamma_markets", side_effect=[ordered, []]), \
             patch.object(scan.pmapi, "gamma_market_tags", return_value=[tag()]):
            records = scan.scan_candidates(include_provider_metadata=True)
        self.assertEqual([record["market_id"] for record in records], ["third", "first", "second"])

    def test_valid_multi_tag_response_is_normalized_and_sorted(self):
        tags = [tag("64", "esports", "Esports"), tag("1", "sports", "Sports"), tag(21, "crypto", "Crypto")]
        tags[0]["untrusted_provider_field"] = "discarded"
        with patch.object(scan, "discovery_queries", return_value=[{"_label": "one"}]), \
             patch.object(scan.pmapi, "gamma_markets", side_effect=[[market("a")], []]), \
             patch.object(scan.pmapi, "gamma_market_tags", return_value=tags):
            record = scan.scan_candidates(include_provider_metadata=True)[0]
        self.assertEqual(
            record["provider_metadata"],
            {
                "market_tags_status": "ok",
                "market_tags": [
                    tag("1", "sports", "Sports"),
                    tag("21", "crypto", "Crypto"),
                    tag("64", "esports", "Esports"),
                ],
            },
        )
        self.assertNotIn("eligible", record["provider_metadata"])

    def test_empty_tag_response_is_valid_non_authoritative_data(self):
        with patch.object(scan, "discovery_queries", return_value=[{"_label": "one"}]), \
             patch.object(scan.pmapi, "gamma_markets", side_effect=[[market("a")], []]), \
             patch.object(scan.pmapi, "gamma_market_tags", return_value=[]):
            record = scan.scan_candidates(include_provider_metadata=True)[0]
        self.assertEqual(record["provider_metadata"], {"market_tags_status": "ok", "market_tags": []})

    def test_lookup_failure_is_safe_and_candidate_remains(self):
        with patch.object(scan, "discovery_queries", return_value=[{"_label": "one"}]), \
             patch.object(scan.pmapi, "gamma_markets", side_effect=[[market("a")], []]), \
             patch.object(scan.pmapi, "gamma_market_tags", side_effect=RuntimeError("server-body-secret")):
            record = scan.scan_candidates(include_provider_metadata=True)[0]
        self.assertEqual(
            record["provider_metadata"],
            {"market_tags_status": "unavailable", "market_tags": []},
        )
        self.assertNotIn("server-body-secret", json.dumps(record))

    def test_malformed_tag_response_is_invalid_without_classification(self):
        with patch.object(scan, "discovery_queries", return_value=[{"_label": "one"}]), \
             patch.object(scan.pmapi, "gamma_markets", side_effect=[[market("a")], []]), \
             patch.object(scan.pmapi, "gamma_market_tags", return_value=[{"id": "not-decimal"}]):
            record = scan.scan_candidates(include_provider_metadata=True)[0]
        self.assertEqual(record["provider_metadata"], {"market_tags_status": "invalid", "market_tags": []})
        self.assertNotIn("category", record["provider_metadata"])

    def test_duplicate_provider_id_canonicalizes_only_when_identical(self):
        with patch.object(scan, "discovery_queries", return_value=[{"_label": "one"}]), \
             patch.object(scan.pmapi, "gamma_markets", side_effect=[[market("a")], []]), \
             patch.object(scan.pmapi, "gamma_market_tags", return_value=[tag(), tag()]):
            record = scan.scan_candidates(include_provider_metadata=True)[0]
        self.assertEqual(record["provider_metadata"], {"market_tags_status": "ok", "market_tags": [tag()]})

        with patch.object(scan, "discovery_queries", return_value=[{"_label": "one"}]), \
             patch.object(scan.pmapi, "gamma_markets", side_effect=[[market("a")], []]), \
             patch.object(scan.pmapi, "gamma_market_tags", return_value=[tag(), tag("1", "sports", "Changed")]):
            record = scan.scan_candidates(include_provider_metadata=True)[0]
        self.assertEqual(record["provider_metadata"], {"market_tags_status": "invalid", "market_tags": []})

    def test_tag_count_and_string_bounds_fail_closed(self):
        oversized_count = [tag(str(index), f"tag-{index}", f"Tag {index}") for index in range(33)]
        oversized_slug = [tag("1", "x" * 129, "Sports")]
        oversized_label = [tag("1", "sports", "x" * 257)]
        non_printable_label = [tag("1", "sports", "Sports\u200b")]
        for response in (oversized_count, oversized_slug, oversized_label, non_printable_label):
            with self.subTest(response=response):
                with patch.object(scan, "discovery_queries", return_value=[{"_label": "one"}]), \
                     patch.object(scan.pmapi, "gamma_markets", side_effect=[[market("a")], []]), \
                     patch.object(scan.pmapi, "gamma_market_tags", return_value=response):
                    record = scan.scan_candidates(include_provider_metadata=True)[0]
                self.assertEqual(
                    record["provider_metadata"],
                    {"market_tags_status": "invalid", "market_tags": []},
                )

    def test_extension_controls_require_strict_types(self):
        invalid = (
            ({"include_provider_metadata": 1}, TypeError),
            ({"include_provider_metadata": "true"}, TypeError),
            ({"max_candidates": True}, TypeError),
            ({"max_candidates": 0}, ValueError),
            ({"max_candidates": -1}, ValueError),
            ({"max_candidates": "20"}, TypeError),
        )
        for kwargs, exception_type in invalid:
            with self.subTest(kwargs=kwargs):
                with self.assertRaises(exception_type):
                    scan.scan_candidates(**kwargs)

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

    def test_main_default_jsonl_is_unchanged_and_does_not_lookup_tags(self):
        stdout = io.StringIO()
        with patch.object(scan, "discovery_queries", return_value=[{"_label": "one"}]), \
             patch.object(scan.pmapi, "gamma_markets", side_effect=[[market("a")], []]), \
             patch.object(scan.pmapi, "gamma_market_tags") as market_tags, \
             patch.object(sys, "argv", ["scan.py"]), redirect_stdout(stdout), redirect_stderr(io.StringIO()):
            scan.main()
        expected = scan.keep(market("a"), set(), [], argparse.Namespace(min_volume_24h=0))
        self.assertEqual(stdout.getvalue(), json.dumps(expected) + "\n")
        market_tags.assert_not_called()

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

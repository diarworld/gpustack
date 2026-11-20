"""Model-card utilization metrics: query building, Prometheus response
parsing, and the per-route aggregation the my-models cards render.

The collector is plumbing over ``prometheus_query``; the semantic decisions
under test are which series feed which field, what "no data" means (None,
never zero), and how the several targets of one route combine into the
single value a card shows.
"""

import math

import pytest

from gpustack.server.model_metrics import (
    RATE_WINDOW,
    aggregate_route_metrics,
    build_utilization_queries,
    model_id_selector,
    parse_instant_results,
)


def test_selector_joins_model_ids_into_one_regex_matcher():
    assert model_id_selector([1, 2]) == 'model_id=~"1|2"'


def test_queries_cover_every_card_field_over_the_shared_rate_window():
    queries = build_utilization_queries([1, 2])
    assert set(queries) == {
        "requests_running",
        "requests_waiting",
        "prompt_tokens_per_second",
        "generation_tokens_per_second",
        "kv_cache_usage_ratio",
        "prefix_cache_hit_rate",
        "ttft_sum_rate",
        "ttft_count_rate",
    }
    assert (
        queries["requests_running"]
        == 'sum by (model_id) (gpustack:num_requests_running{model_id=~"1|2"})'
    )
    assert queries["requests_waiting"].startswith(
        'sum by (model_id) (gpustack:num_requests_waiting{'
    )
    assert (
        f"rate(gpustack:prompt_tokens_total{{model_id=~\"1|2\"}}[{RATE_WINDOW}])"
        in (queries["prompt_tokens_per_second"])
    )
    assert (
        f"rate(gpustack:generation_tokens_total{{model_id=~\"1|2\"}}[{RATE_WINDOW}])"
        in queries["generation_tokens_per_second"]
    )
    assert queries["kv_cache_usage_ratio"] == (
        'max by (model_id) (gpustack:kv_cache_usage_ratio{model_id=~"1|2"})'
    )
    assert queries["prefix_cache_hit_rate"] == (
        'avg by (model_id) (gpustack:prefix_cache_hit_rate{model_id=~"1|2"})'
    )
    assert (
        "rate(gpustack:time_to_first_token_seconds_sum{model_id=~\"1|2\"}"
        f"[{RATE_WINDOW}])" in queries["ttft_sum_rate"]
    )
    assert (
        "rate(gpustack:time_to_first_token_seconds_count{model_id=~\"1|2\"}"
        f"[{RATE_WINDOW}])" in queries["ttft_count_rate"]
    )


def _entry(model_id, value):
    return {"metric": {"model_id": str(model_id)}, "value": [1750000000, value]}


def test_parse_keeps_known_fields_and_reads_nan_as_unknown():
    results = {
        "requests_running": [_entry(1, "2"), _entry(2, "0")],
        "prompt_tokens_per_second": [_entry(1, "12.5")],
        "kv_cache_usage_ratio": [_entry(1, str(math.nan))],
        "ttft_sum_rate": [_entry(1, "0.46")],
        "ttft_count_rate": [_entry(1, "2.3")],
    }
    per_model = parse_instant_results(results)
    assert per_model[1] == {
        "requests_running": 2.0,
        "requests_waiting": None,
        "prompt_tokens_per_second": 12.5,
        "generation_tokens_per_second": None,
        # NaN reads as "cannot tell": a card must not render a fake 0%.
        "kv_cache_usage_ratio": None,
        "prefix_cache_hit_rate": None,
        "ttft_sum_rate": 0.46,
        "ttft_count_rate": 2.3,
    }
    assert per_model[2]["requests_running"] == 0.0
    assert per_model[2]["ttft_sum_rate"] is None


def test_parse_skips_series_without_a_model_id_label():
    per_model = parse_instant_results(
        {"requests_running": [{"metric": {}, "value": [0, "1"]}]}
    )
    assert per_model == {}


def test_route_aggregation_combines_targets_and_weights_ttft_by_traffic():
    per_model = {
        1: {
            "requests_running": 2.0,
            "requests_waiting": 1.0,
            "prompt_tokens_per_second": 1000.0,
            "generation_tokens_per_second": 35.0,
            "kv_cache_usage_ratio": 0.9,
            "prefix_cache_hit_rate": 0.2,
            "ttft_sum_rate": 0.46,
            "ttft_count_rate": 2.3,
        },
        2: {
            "requests_running": 3.0,
            "requests_waiting": None,
            "prompt_tokens_per_second": None,
            "generation_tokens_per_second": 5.0,
            "kv_cache_usage_ratio": 0.5,
            "prefix_cache_hit_rate": 0.6,
            "ttft_sum_rate": 0.2,
            "ttft_count_rate": 0.2,
        },
    }
    row = aggregate_route_metrics({7: [1, 2]}, per_model)[7]
    assert row["requests_running"] == 5.0
    assert row["requests_waiting"] == 1.0
    assert row["prompt_tokens_per_second"] == 1000.0
    assert row["generation_tokens_per_second"] == 40.0
    # A card shows the bottleneck, not the mean across instances.
    assert row["kv_cache_usage_ratio"] == 0.9
    assert row["prefix_cache_hit_rate"] == pytest.approx(0.4)
    assert row["avg_time_to_first_token_seconds"] == pytest.approx(0.66 / 2.5)


def test_route_aggregation_is_none_when_every_target_is_dark():
    row = aggregate_route_metrics({7: [1, 2]}, {})[7]
    assert row == {
        "requests_running": None,
        "requests_waiting": None,
        "prompt_tokens_per_second": None,
        "generation_tokens_per_second": None,
        "kv_cache_usage_ratio": None,
        "prefix_cache_hit_rate": None,
        "avg_time_to_first_token_seconds": None,
    }

"""Live utilization metrics for the my-models cards, from Prometheus.

One batch endpoint backs the whole card grid: the UI sends the route ids
it renders, and this module turns them into a handful of instant PromQL
queries. The selector over internal model ids is injected server-side, so
a caller reads only series the server resolved for routes it may see, and
never learns PromQL or the exporter's metric names — the same contract as
the cache-service metrics.

A model may sit behind several instances and a route behind several
models; the per-card aggregation is: load counters and token rates sum,
KV cache takes the worst instance, prefix hit rate averages, and TTFT is
weighted by each target's request rate — an idle target must not drag a
busy one's latency toward the mean.
"""

import asyncio
import logging
import time
from typing import Dict, List, Optional

import aiohttp

from gpustack.server.prometheus_query import (
    instant_value,
    prometheus_url,
    promql_regex_literal,
    query_instant,
)

logger = logging.getLogger(__name__)

# Cards refresh every few seconds; a window that short reads jumpy and a
# longer one hides real load swings, so keep the dashboard's 5m.
RATE_WINDOW = "5m"

# The series each card row is built from. ttft_sum_rate / ttft_count_rate
# are internal inputs that combine into avg_time_to_first_token_seconds
# (the ratio must be computed after the per-model sum, not per model).
_FIELD_KEYS = (
    "requests_running",
    "requests_waiting",
    "prompt_tokens_per_second",
    "generation_tokens_per_second",
    "kv_cache_usage_ratio",
    "prefix_cache_hit_rate",
    # Input only: the counter ratio that fills prefix_cache_hit_rate in
    # when the engine publishes no precomputed gauge. Never a card field.
    "prefix_cache_hit_rate_fallback",
    "ttft_sum_rate",
    "ttft_count_rate",
)

# The fields one card row carries: everything except the raw TTFT parts.
_AGGREGATE_KEYS = (
    "requests_running",
    "requests_waiting",
    "prompt_tokens_per_second",
    "generation_tokens_per_second",
    "kv_cache_usage_ratio",
    "prefix_cache_hit_rate",
    "avg_time_to_first_token_seconds",
)


def model_id_selector(model_ids: List[int]) -> str:
    """The batch's model ids -> the one matcher every query shares."""
    return (
        'model_id=~"'
        + "|".join(promql_regex_literal(str(model_id)) for model_id in model_ids)
        + '"'
    )


def build_utilization_queries(model_ids: List[int]) -> Dict[str, str]:
    """One instant query per series: gauges read as-is, counters and
    histogram parts rate over the shared window."""
    selector = model_id_selector(model_ids)
    return {
        "requests_running": (
            f"sum by (model_id) (gpustack:num_requests_running{{{selector}}})"
        ),
        "requests_waiting": (
            f"sum by (model_id) (gpustack:num_requests_waiting{{{selector}}})"
        ),
        "prompt_tokens_per_second": (
            "sum by (model_id) "
            f"(rate(gpustack:prompt_tokens_total{{{selector}}}[{RATE_WINDOW}]))"
        ),
        "generation_tokens_per_second": (
            "sum by (model_id) "
            f"(rate(gpustack:generation_tokens_total{{{selector}}}[{RATE_WINDOW}]))"
        ),
        "kv_cache_usage_ratio": (
            f"max by (model_id) (gpustack:kv_cache_usage_ratio{{{selector}}})"
        ),
        "prefix_cache_hit_rate": (
            f"avg by (model_id) (gpustack:prefix_cache_hit_rate{{{selector}}})"
        ),
        # The gauge is optional: engines that only publish the hit/query
        # counters (most vLLM releases) still yield the hit rate as the
        # windowed counter ratio — the same figure the model dashboard's
        # Cache Hit Rate panel renders.
        "prefix_cache_hit_rate_fallback": (
            "sum by (model_id) "
            f"(rate(gpustack:prefix_cache_hits_total{{{selector}}}[{RATE_WINDOW}]))"
            " / sum by (model_id) "
            f"(rate(gpustack:prefix_cache_queries_total{{{selector}}}[{RATE_WINDOW}]))"
        ),
        "ttft_sum_rate": (
            "sum by (model_id) "
            "(rate(gpustack:time_to_first_token_seconds_sum{"
            f"{selector}}}[{RATE_WINDOW}]))"
        ),
        "ttft_count_rate": (
            "sum by (model_id) "
            "(rate(gpustack:time_to_first_token_seconds_count{"
            f"{selector}}}[{RATE_WINDOW}]))"
        ),
    }


def parse_instant_results(
    results: Dict[str, List[dict]],
) -> Dict[int, Dict[str, Optional[float]]]:
    """Prometheus results -> per-model values. A series without a model_id
    label is not a card's series and is dropped; NaN reads as None."""
    per_model: Dict[int, Dict[str, Optional[float]]] = {}
    for name, series in (results or {}).items():
        if name not in _FIELD_KEYS:
            continue
        for entry in series or []:
            raw = (entry.get("metric") or {}).get("model_id")
            try:
                model_id = int(raw)
            except (TypeError, ValueError):
                continue
            row = per_model.setdefault(model_id, {key: None for key in _FIELD_KEYS})
            row[name] = instant_value(entry)
    for row in per_model.values():
        # The counter ratio fills the gap where the engine publishes no
        # precomputed gauge; a NaN ratio (no traffic in the window) stays
        # "unknown" and the card hides the field.
        if row["prefix_cache_hit_rate"] is None:
            row["prefix_cache_hit_rate"] = row["prefix_cache_hit_rate_fallback"]
        row.pop("prefix_cache_hit_rate_fallback", None)
    return per_model


def aggregate_route_metrics(
    route_models: Dict[int, List[int]],
    per_model: Dict[int, Dict[str, Optional[float]]],
) -> Dict[int, Dict[str, Optional[float]]]:
    """Per-model values -> one card row per route. Counters and rates sum,
    KV cache takes the bottleneck instance, prefix hit rate averages, and
    TTFT divides the summed rates so the weighting follows traffic."""
    aggregated: Dict[int, Dict[str, Optional[float]]] = {}
    for route_id, model_ids in route_models.items():
        rows = [per_model[m] for m in model_ids or [] if m in per_model]
        row: Dict[str, Optional[float]] = {key: None for key in _AGGREGATE_KEYS}
        for key in (
            "requests_running",
            "requests_waiting",
            "prompt_tokens_per_second",
            "generation_tokens_per_second",
        ):
            values = [entry[key] for entry in rows if entry[key] is not None]
            row[key] = sum(values) if values else None
        kv_values = [
            entry["kv_cache_usage_ratio"]
            for entry in rows
            if entry["kv_cache_usage_ratio"] is not None
        ]
        row["kv_cache_usage_ratio"] = max(kv_values) if kv_values else None
        prefix_values = [
            entry["prefix_cache_hit_rate"]
            for entry in rows
            if entry["prefix_cache_hit_rate"] is not None
        ]
        row["prefix_cache_hit_rate"] = (
            sum(prefix_values) / len(prefix_values) if prefix_values else None
        )
        sum_rates = [
            entry["ttft_sum_rate"]
            for entry in rows
            if entry["ttft_sum_rate"] is not None
        ]
        count_rates = [
            entry["ttft_count_rate"]
            for entry in rows
            if entry["ttft_count_rate"] is not None
        ]
        total_count = sum(count_rates)
        if sum_rates and total_count > 0:
            row["avg_time_to_first_token_seconds"] = sum(sum_rates) / total_count
        aggregated[route_id] = row
    return aggregated


async def collect_models_utilization(
    client: Optional[aiohttp.ClientSession], model_ids: List[int]
) -> Dict[int, Dict[str, Optional[float]]]:
    """Per-model values for one batch of cards.

    A single failing query leaves only its own fields unknown — a flaky
    Prometheus must not blank the card grid. No Prometheus at all is the
    empty answer; the endpoint turns that into all-None rows.
    """
    if not model_ids:
        return {}
    base_url = prometheus_url()
    if not base_url:
        return {}
    queries = build_utilization_queries(model_ids)
    names = list(queries)
    if client is None:
        async with aiohttp.ClientSession() as own_client:
            return await _run_queries(own_client, base_url, names, queries)
    return await _run_queries(client, base_url, names, queries)


async def _run_queries(
    client: aiohttp.ClientSession,
    base_url: str,
    names: List[str],
    queries: Dict[str, str],
) -> Dict[int, Dict[str, Optional[float]]]:
    results = await asyncio.gather(
        *(
            query_instant(client, base_url, queries[name], time.time())
            for name in names
        ),
        return_exceptions=True,
    )
    parsed: Dict[str, List[dict]] = {}
    for name, result in zip(names, results):
        if isinstance(result, BaseException):
            logger.warning("Utilization query '%s' failed: %s", name, result)
            continue
        parsed[name] = result
    return parse_instant_results(parsed)

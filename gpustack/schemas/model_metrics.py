from typing import Optional

from pydantic import BaseModel


class MyModelMetrics(BaseModel):
    """Live utilization of one my-model (a route) for the card strip.

    Every field is ``None`` when it cannot be told: the target has no
    traffic, no series, or Prometheus itself is unreachable. A card must
    render "unknown" rather than a fabricated zero.
    """

    id: int
    requests_running: Optional[float] = None
    requests_waiting: Optional[float] = None
    prompt_tokens_per_second: Optional[float] = None
    generation_tokens_per_second: Optional[float] = None
    kv_cache_usage_ratio: Optional[float] = None
    prefix_cache_hit_rate: Optional[float] = None
    avg_time_to_first_token_seconds: Optional[float] = None

"""Live rate-limit windows as enforced by the gateway plugin.

These models are the payload of ``GET /usage/rate-limits``: the limits the
``gateway_rate_limit`` config, the API keys and the user token budgets
publish, paired with the current window counters read from Redis at request
time — a snapshot of what the ``gpustack-rate-limit`` plugin is enforcing
right now, not a reconstruction from historical usage rows. The endpoint
serves the response with ``response_model_exclude_none=True``, so an
optional field that is ``None`` is omitted from the JSON instead of
arriving as ``null``; clients must treat every optional field as
absent-able.
"""

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field


class RateLimitWindowUsage(BaseModel):
    """One enforced window: which counter it tracks and where it stands.

    Mirrors :class:`gpustack.gateway.rate_limit_usage.RateLimitWindowSpec`
    with the live counters and reset time filled in.
    """

    # Which counter the window tracks: ``query`` (requests, sliding),
    # ``token_rolling`` (tokens, sliding) or ``token_calendar`` (tokens,
    # resetting on a calendar boundary in the deployment timezone).
    kind: str
    # The plugin's period vocabulary: ``60s`` (per-minute), ``604800s``
    # (sliding week), ``each_day``, ``each_month``.
    period: str
    # Sliding windows only — a calendar window has no fixed length.
    window_seconds: Optional[int] = None
    # ``None`` when the counter could not be read: Redis unavailable, or the
    # deployment runs in local mode where the plugin keeps no counters.
    used: Optional[int] = None
    limit: int
    # Seconds until the window resets, as of ``generated_at``.
    reset_seconds: Optional[int] = None


class UserRateLimits(BaseModel):
    """The caller's user-level limits and their live windows.

    ``windows`` is the effective per-period view: a token budget overrides
    the deployment default on the period it covers (its quota is the tighter
    cap), and the default plan fills the periods the user has no budget for.
    Every window buckets on the user, so its counter is that user's own
    traffic. Per-minute caps are not folded in here — the per-key table
    carries them.
    """

    # From the ``users`` table; ``None`` = the user has no budget of that
    # kind, so the deployment default governs that axis.
    weekly_limit: Optional[int] = None
    monthly_limit: Optional[int] = None
    windows: List[RateLimitWindowUsage] = Field(default_factory=list)


class ApiKeyRateLimits(BaseModel):
    """One API key's limits and the windows its requests are governed by."""

    id: int
    name: str
    # Explicit per-key limits; ``None`` = the field inherits the deployment
    # default (and may be unset there too, leaving that axis unlimited).
    rpm_limit: Optional[int] = None
    tpm_limit: Optional[int] = None
    daily_token_limit: Optional[int] = None
    # Which published combinations govern the key's requests, mirroring
    # ``source_for``: ``key`` (the key carries explicit limits and binds its
    # own combo), ``user+default`` (the fallback group plus the owner's
    # budgets) or ``default`` (the default plan alone).
    source: str
    windows: List[RateLimitWindowUsage] = Field(default_factory=list)


class RateLimitsResponse(BaseModel):
    """The whole rate-limit picture for the caller's user."""

    # Counter backend: ``redis`` (counters read live), ``local`` (rate
    # limiting on without Redis, so no counters exist) or ``disabled``
    # (rate limiting off).
    mode: str
    generated_at: datetime
    # Short error string when the Redis read failed; ``None`` = healthy.
    usage_error: Optional[str] = None
    user: UserRateLimits
    # Every key the user owns, so the UI renders the per-key view from one
    # response instead of fanning out per key.
    api_keys: List[ApiKeyRateLimits] = Field(default_factory=list)

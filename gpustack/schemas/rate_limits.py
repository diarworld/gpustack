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
from typing import Dict, List, Optional

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


class RateLimitResetStatus(BaseModel):
    """Where the user stands against the floating reset quota of one window.

    The quota floats with the resets themselves: pressing the button twice
    in one week makes the next press available a week after the first
    press, not on the next calendar boundary. ``next_available_at`` is
    ``None`` while presses remain.
    """

    # Resets allowed per floating window (7 days for the weekly budget,
    # 30 for the monthly one).
    quota: int
    # Presses still available now; 0 means the button is disabled.
    available: int
    # When the oldest counted reset ages out of the floating window and a
    # press frees up. Only set once ``available`` reaches 0.
    next_available_at: Optional[datetime] = None


class ModelConcurrencyUsage(BaseModel):
    """One per-model in-flight cap and where it stands.

    Only models the user's row names explicitly are itemized; every other
    model inherits the deployment ``default_concurrency`` and has no
    itemized counter. ``running`` is the live slot count (one member per
    in-flight request), ``None`` when it could not be read (Redis
    unavailable, or local mode where the plugin keeps no server-readable
    counters).
    """

    model: str
    # 0 = explicit unlimited for this model.
    limit: int
    running: Optional[int] = None


class UserRateLimits(BaseModel):
    """The caller's user-level limits and their live windows.

    ``windows`` is the user limit combo exactly as the reconciler publishes
    it: requests and tokens per minute, the weekly rolling budget and the
    monthly calendar quota -- unset user fields inherited the deployment
    default at publish time. Every window buckets on the user, so its
    counter is that user's own traffic across all their keys. Per-key day
    quotas live on the ``api_keys`` entries.
    """

    # From the ``users`` table; ``None`` = the user has no limit of that
    # kind set, so the deployment default governs that axis (and does so
    # inside ``windows``, which carry the effective limits).
    rpm_limit: Optional[int] = None
    tpm_limit: Optional[int] = None
    weekly_limit: Optional[int] = None
    monthly_limit: Optional[int] = None
    windows: List[RateLimitWindowUsage] = Field(default_factory=list)
    # Per-model in-flight caps (``model_concurrency_limits``) with their
    # live slot counts; empty for admins (exempt) and users without
    # per-model overrides.
    model_concurrency: List[ModelConcurrencyUsage] = Field(default_factory=list)
    # Reset-button state per long token window kind (``weekly`` /
    # ``monthly``), only in redis mode -- in local mode there are no
    # counters a reset could clear.
    resets: Optional[Dict[str, RateLimitResetStatus]] = None


class ApiKeyRateLimits(BaseModel):
    """One API key's limits and the windows its requests are governed by."""

    id: int
    name: str
    # The key's own daily token budget; ``None`` = it inherits the
    # deployment default (and may be unset there too, leaving the axis
    # unlimited). Rate and per-minute token caps are user-level.
    daily_token_limit: Optional[int] = None
    # Which published combinations govern the key's requests, mirroring
    # ``source_for``: ``user+key`` (the user limit combo plus the key's own
    # day quota) or ``user`` (the user limit combo alone).
    source: str
    windows: List[RateLimitWindowUsage] = Field(default_factory=list)


class RateLimitResetCreate(BaseModel):
    """One reset-button press: which window and why it was forgiven."""

    # 'weekly' or 'monthly' — the long token window kind to forgive.
    kind: str
    # Required note on what the budget was spent on; the UI enforces the
    # textarea and the route rejects blanks.
    description: str
    # Whose budget to reset; defaults to the caller. Honored only for a
    # platform-wide admin (the same gate as the GET expansion).
    user_id: Optional[int] = None


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

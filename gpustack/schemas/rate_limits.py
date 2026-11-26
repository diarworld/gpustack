"""Live rate-limit windows as enforced by the gateway plugin.

These models are the payload of ``GET /usage/rate-limits``: the limits the
``gateway_rate_limit`` config and the user token budgets publish, paired
with the current window counters read from Redis at request time — a
snapshot of what the ``gpustack-rate-limit`` plugin is enforcing right
now, not a reconstruction from historical usage rows. The endpoint serves
the response with ``response_model_exclude_none=True``, so an optional
field that is ``None`` is omitted from the JSON instead of arriving as
``null``; clients must treat every optional field as absent-able.
"""

from datetime import datetime
from typing import Dict, List, Optional

from pydantic import BaseModel, Field

from gpustack.schemas.common import Pagination


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

    On the reset endpoint the same body carries the press's verdict:
    ``status`` is ``reset`` when the counters were cleared, ``rejected``
    when the decision model refused the description — then
    ``rejection_reason`` names why and the quota state answers as-it-was
    (a rejected press consumes nothing).
    """

    # Resets allowed per floating window (7 days for the weekly budget,
    # 30 for the monthly one).
    quota: int
    # Presses still available now; 0 means the button is disabled.
    available: int
    # When the oldest counted reset ages out of the floating window and a
    # press frees up. Only set once ``available`` reaches 0.
    next_available_at: Optional[datetime] = None
    # What the press became: the counters cleared, or the decision model
    # refused it. The quota reads omit the field (they carry no verdict).
    status: str = "reset"
    # Why the press was rejected, when it was. None otherwise.
    rejection_reason: Optional[str] = None


class ModelConcurrencyUsage(BaseModel):
    """One per-model in-flight cap and where it stands.

    Itemized over the ready models the user can reach: the limit is their
    named override, or the deployment ``default_concurrency`` for models
    without one (rows drop out entirely when no cap resolves). ``running``
    is the user's own live slot count (one member per in-flight request),
    ``None`` when it could not be read (Redis unavailable, or local mode
    where the plugin keeps no server-readable counters).
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
    counter is that user's own traffic across all their keys.
    """

    # From the ``users`` table; ``None`` = the user has no limit of that
    # kind set, so the deployment default governs that axis (and does so
    # inside ``windows``, which carry the effective limits).
    rpm_limit: Optional[int] = None
    tpm_limit: Optional[int] = None
    weekly_limit: Optional[int] = None
    monthly_limit: Optional[int] = None
    # The deployment-wide in-flight cap per model (``default_concurrency``);
    # ``None`` = no default cap. Models named in ``model_concurrency_limits``
    # carry their own override instead.
    concurrency_default: Optional[int] = None
    windows: List[RateLimitWindowUsage] = Field(default_factory=list)
    # Per-model in-flight caps (``model_concurrency_limits``) with their
    # live slot counts; empty for admins (exempt) and users without
    # per-model overrides.
    model_concurrency: List[ModelConcurrencyUsage] = Field(default_factory=list)
    # Reset-button state per long token window kind (``weekly`` /
    # ``monthly``), only in redis mode -- in local mode there are no
    # counters a reset could clear.
    resets: Optional[Dict[str, RateLimitResetStatus]] = None


class UserRateLimitUsage(BaseModel):
    """One user's row in the admin's cross-user limits table.

    Served by ``GET /usage/rate-limits/users``: the effective limits (the
    same resolution the single-user endpoint and the reconciler apply,
    groups and deployment defaults included) paired with the live window
    counters, plus the identity fields the table's name tooltip renders.
    """

    user_id: int
    username: str
    # The human-readable name (``Principal.display_name``); ``None`` when
    # the account carries none and the login is all there is.
    full_name: Optional[str] = None
    is_admin: bool = False
    # Attribute-derived labels for the tooltip, resolved the same way the
    # users list resolves them; ``None`` = the user holds no group of
    # that kind.
    profession: Optional[str] = None
    department: Optional[str] = None
    business_unit: Optional[str] = None
    # The user limit combo verbatim: requests and tokens per minute, the
    # weekly rolling budget and the monthly calendar quota. An axis the
    # user has no effective limit for simply has no window here.
    windows: List[RateLimitWindowUsage] = Field(default_factory=list)


class UsersRateLimitsResponse(BaseModel):
    """One page of the admin's cross-user limits table."""

    items: List[UserRateLimitUsage] = Field(default_factory=list)
    pagination: Pagination
    # Counter backend, ``generated_at`` and ``usage_error`` carry the same
    # semantics as :class:`RateLimitsResponse`.
    mode: str
    generated_at: datetime
    usage_error: Optional[str] = None


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


class FreeWindowUsage(BaseModel):
    """The deployment's free-window schedule and whether it covers ``now``.

    During a free window the gateway suspends every check and the limit
    counters freeze; ``active`` tells the UI to badge the section so a
    frozen counter is not mistaken for a broken read. The schedule is the
    admin-editable one (``gateway_rate_limit_settings``), evaluated in the
    deployment's rate-limit timezone.
    """

    enabled: bool
    # Whether the moment this response was generated at falls inside the
    # schedule. Always false when ``enabled`` is false.
    active: bool = False
    # UTC wall-clock window ("15:00".."06:00"); may wrap past midnight.
    start: str
    end: str
    # Whole free days, "mon".."sun".
    days: List[str] = Field(default_factory=list)


class RateLimitsResponse(BaseModel):
    """The whole rate-limit picture for the caller's user."""

    # Counter backend: ``redis`` (counters read live), ``local`` (rate
    # limiting on without Redis, so no counters exist) or ``disabled``
    # (rate limiting off).
    mode: str
    generated_at: datetime
    # Short error string when the Redis read failed; ``None`` = healthy.
    usage_error: Optional[str] = None
    # The free-window schedule; present whenever rate limiting is on.
    free: Optional[FreeWindowUsage] = None
    user: UserRateLimits

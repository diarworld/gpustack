"""Gateway rate-limit usage snapshots.

``gateway_rate_limit_usages`` stores the last observed usage total per
rate-limit window, upserted by the gateway rate-limit flusher — one row per
(combo_name, kind, period, window_start, user_id, api_key_id), not a time
series. Live counters stay in Redis ZSETs; long windows (day/week/month) are
persisted here for analytics. ``user_id`` / ``api_key_id`` use 0 instead of
NULL for "not attributable" because both sit in the unique upsert key and
MySQL unique indexes treat NULLs as distinct, which would let duplicate rows
accumulate.
"""

from datetime import datetime
from typing import ClassVar, Optional

from pydantic import ConfigDict
from sqlalchemy import BigInteger, Column, Integer, String, UniqueConstraint
from sqlmodel import Field, SQLModel

from gpustack.mixins import BaseModelMixin
from gpustack.schemas.common import UTCDateTime


class GatewayRateLimitUsage(SQLModel, BaseModelMixin, table=True):
    """Rate-limit usage snapshot for one (combo, limit, window)."""

    __tablename__: ClassVar[str] = "gateway_rate_limit_usages"
    __table_args__ = (
        # Upsert key of the flusher: one row per combo + limit + window.
        # Every column is non-null (see user_id / api_key_id below) — a NULL
        # member would make MySQL treat each row as distinct from the next
        # upsert of the same window.
        UniqueConstraint(
            "combo_name",
            "kind",
            "period",
            "window_start",
            "user_id",
            "api_key_id",
            name="uq_gw_rl_usage_window",
        ),
    )

    id: Optional[int] = Field(default=None, primary_key=True)

    # FK-less on purpose, like model_usages: a snapshot must outlive the user
    # or key it was attributed to.
    # Rows are attributed from the combo's consumer identity when it parses
    # (default-plan -> `gpustack-<user_id>`; key combos -> the key's owner);
    # 0 = that attribution failed — an identity that does not parse, or a
    # key-combo row whose key row is gone. 0 rather than NULL: both columns
    # are part of the unique upsert key, and MySQL unique indexes treat NULLs
    # as distinct, so NULLs would break the upsert.
    user_id: int = Field(
        default=0, sa_column=Column(Integer, nullable=False, default=0)
    )
    api_key_id: int = Field(
        default=0, sa_column=Column(Integer, nullable=False, default=0)
    )
    # Reconciler combo segment from the Redis key: `default-plan`, `user-<id>`
    # or `key-<id>`.
    combo_name: str = Field(sa_column=Column(String(255), nullable=False))
    # query | token_rolling | token_calendar — the plugin's kind vocabulary.
    kind: str = Field(sa_column=Column(String(32), nullable=False))
    # 60s | 604800s | each_day | each_month — rolling windows render as
    # `<seconds>s`, calendar ones as each_day / each_month.
    period: str = Field(sa_column=Column(String(32), nullable=False))
    # Start of the window the snapshot covers. Calendar windows: period start
    # in the plugin timezone. Rolling windows: now - window_seconds at flush
    # time.
    window_start: datetime = Field(sa_column=Column(UTCDateTime(), nullable=False))
    # Rolling windows only; None for calendar windows.
    window_seconds: Optional[int] = Field(
        default=None, sa_column=Column(Integer, nullable=True)
    )
    # Last observed usage total for the window. BigInteger: monthly token
    # counts are routine in the billions and int32 tops out at ~2.1e9.
    usage: int = Field(
        default=0, sa_column=Column(BigInteger, nullable=False, default=0)
    )
    # The enforced limit at snapshot time; None when the window is unlimited
    # or the limit could not be resolved.
    quota: Optional[int] = Field(default=None, sa_column=Column(BigInteger))

    model_config = ConfigDict(protected_namespaces=())

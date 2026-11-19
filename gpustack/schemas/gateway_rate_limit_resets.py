"""Rate-limit resets recorded from the Usage page.

A reset forgives a user's weekly or monthly token budget: the gateway's
Redis counters for that window are deleted and the budget restarts from
zero mid-window. Every press is recorded here so the floating reset
quota (two per window per user) survives restarts and stays auditable --
``description`` is the required note on what the budget was spent on.
"""

from datetime import datetime
from typing import ClassVar, Optional

from sqlmodel import Field, SQLModel

from gpustack.mixins import BaseModelMixin
from gpustack.schemas.common import UTCDateTime

# The long token window kinds the Usage page exposes reset buttons for.
RESET_KINDS = ("weekly", "monthly")


class GatewayRateLimitReset(SQLModel, BaseModelMixin, table=True):
    """One forgiveness of a user's weekly or monthly token budget."""

    __tablename__: ClassVar[str] = "gateway_rate_limit_resets"
    id: Optional[int] = Field(default=None, primary_key=True)
    # The budget owner, not necessarily who pressed the button: an admin
    # resetting on someone's behalf records both sides.
    user_id: int = Field(default=..., index=True)
    kind: str = Field(default=..., index=True)
    # When the forgiveness took effect; usage rows older than this no
    # longer count toward the window (enforced by the counter rebuild).
    reset_at: datetime = Field(default=..., sa_type=UTCDateTime)
    # Who pressed the button, when that is not the budget owner.
    reset_by_user_id: Optional[int] = Field(default=None, index=True)
    description: str = Field(default="")

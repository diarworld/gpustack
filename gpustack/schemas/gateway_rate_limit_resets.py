"""Rate-limit resets recorded from the Usage page.

A reset forgives a user's weekly or monthly token budget: the gateway's
Redis counters for that window are deleted and the budget restarts from
zero mid-window. Every press is recorded here so the floating reset
quota (two per window per user) survives restarts and stays auditable --
``description`` is the required note on what the budget was spent on.

A self-service press is gated by the decision model: ``outcome`` says
whether the reset actually happened (``approved``) or the decision model
rejected the description (``rejected`` — an audit row only, never counted
against the quota and never touching the counters). ``work_type`` carries
the decision model's classification of the task the budget is spent on;
``rejection_reason`` explains a rejection.
"""

from datetime import datetime
from typing import ClassVar, Optional

from sqlmodel import Field, SQLModel

from gpustack.mixins import BaseModelMixin
from gpustack.schemas.common import UTCDateTime

# The long token window kinds the Usage page exposes reset buttons for.
RESET_KINDS = ("weekly", "monthly")

# What became of a recorded press. ``approved`` actually cleared counters;
# ``rejected`` is the decision model's audit-only trail.
RESET_OUTCOME_APPROVED = "approved"
RESET_OUTCOME_REJECTED = "rejected"


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
    # The press's verdict: approved (counters cleared) or rejected by the
    # decision model (audit row only). Rows predate the gate, so the
    # default is the only behavior they ever had.
    outcome: str = Field(default=RESET_OUTCOME_APPROVED)
    # The decision model's classification of the task description, on an
    # approved press. NULL when the gate was off or unreachable (fail-open)
    # or on a rejection the model could not classify.
    work_type: Optional[str] = Field(default=None)
    # Why the decision model rejected the press, on a rejected one.
    rejection_reason: Optional[str] = Field(default=None)

"""Deployment-wide rate-limit defaults and the free-window schedule.

One row (the id=1 singleton) is what admins edit from the Usage page:
the per-user unset fields inherit these values instead of the static
``gateway_rate_limit`` config file, and the free-window columns publish
the schedule during which the gateway plugin suspends every check. The
config file remains only the bootstrap seed for a fresh deployment and
the fallback for fields still unset here.
"""

from datetime import datetime
from typing import ClassVar, List, Optional

from pydantic import BaseModel, field_validator, model_validator
from sqlalchemy import BigInteger, Column
from sqlmodel import Field, SQLModel

from gpustack.mixins import BaseModelMixin
from gpustack.schemas.common import JSON as JSONType

# The day tokens the gateway plugin accepts in free_windows.days, in the
# canonical order the UI renders them.
FREE_DAYS = ("mon", "tue", "wed", "thu", "fri", "sat", "sun")


class GatewayRateLimitSettings(SQLModel, BaseModelMixin, table=True):
    """The singleton carrying the editable deployment defaults."""

    __tablename__: ClassVar[str] = "gateway_rate_limit_settings"
    id: Optional[int] = Field(default=None, primary_key=True)
    # NULL inherits the ``gateway_rate_limit`` config-file default; an
    # explicit value wins. Bounds mirror GatewayRateLimitConfig (ge=1,
    # concurrency ge=0) and are enforced on the Update model.
    default_rpm: Optional[int] = Field(default=None, sa_column=Column(nullable=True))
    default_tpm: Optional[int] = Field(
        default=None, sa_column=Column(BigInteger, nullable=True)
    )
    default_monthly_tokens: Optional[int] = Field(
        default=None, sa_column=Column(BigInteger, nullable=True)
    )
    default_concurrency: Optional[int] = Field(
        default=None, sa_column=Column(nullable=True)
    )
    # Free schedule: whole free days plus a daily window (which may wrap
    # past midnight). All wall-clock values are UTC -- the plugin only
    # loads that timezone.
    free_enabled: bool = Field(default=False)
    free_start: str = Field(default="15:00", max_length=5)
    free_end: str = Field(default="06:00", max_length=5)
    free_days: List[str] = Field(
        default_factory=list, sa_column=Column(JSONType, nullable=False)
    )


class FreeWindowPublic(BaseModel):
    """The free-window schedule as the UI sees it (UTC wall-clock)."""

    enabled: bool
    start: str
    end: str
    days: List[str]


class GatewayRateLimitSettingsPublic(BaseModel):
    """What GET /rate-limit-settings returns."""

    default_rpm: Optional[int] = None
    default_tpm: Optional[int] = None
    default_monthly_tokens: Optional[int] = None
    default_concurrency: Optional[int] = None
    free: FreeWindowPublic


def _check_hhmm(value: str) -> str:
    try:
        datetime.strptime(value, "%H:%M")
    except ValueError:
        raise ValueError(f"{value!r} must be HH:MM (00:00..23:59)")
    return value


def _check_days(days: List[str]) -> List[str]:
    normalized = [d.strip().lower() for d in days]
    invalid = [d for d in normalized if d not in FREE_DAYS]
    if invalid:
        raise ValueError(f"unknown day(s) {invalid}, want mon..sun")
    # Canonical order, duplicates dropped: the value renders straight into
    # the plugin's config JSON.
    seen = dict.fromkeys(normalized)
    return [d for d in FREE_DAYS if d in seen]


class FreeWindowUpdate(BaseModel):
    """The editable free-window schedule."""

    enabled: bool = False
    start: str = Field(default="15:00", max_length=5)
    end: str = Field(default="06:00", max_length=5)
    days: List[str] = Field(default_factory=list)

    @field_validator("start", "end")
    @classmethod
    def check_hhmm(cls, v: str) -> str:
        return _check_hhmm(v)

    @field_validator("days")
    @classmethod
    def check_days(cls, v: List[str]) -> List[str]:
        return _check_days(v)

    @model_validator(mode="after")
    def check_window(self):
        if self.enabled and self.start == self.end:
            raise ValueError("free window start and end must differ")
        return self


class GatewayRateLimitSettingsUpdate(BaseModel):
    """What PUT /rate-limit-settings accepts; a full-replace payload.

    ``None`` on a limit field means "inherit the config-file default" --
    the UI sends the whole form every time, so there is no patch
    semantics to reason about.
    """

    default_rpm: Optional[int] = Field(default=None, ge=1)
    default_tpm: Optional[int] = Field(default=None, ge=1)
    default_monthly_tokens: Optional[int] = Field(default=None, ge=1)
    default_concurrency: Optional[int] = Field(default=None, ge=0)
    free: FreeWindowUpdate = Field(default_factory=FreeWindowUpdate)

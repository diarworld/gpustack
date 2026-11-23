"""Pydantic surface for the User Group API.

Backed by ``principals`` rows where ``kind == GROUP``. Groups are
peer-level principals (no structural parent). A Group's relationship
to an Org, when one exists, is one or more rows in
``principal_memberships`` with ``parent=Org, member=Group``; those
confer the membership's role on every active user in the Group.
There is no dedicated ``user_groups`` table — UserGroup is a
Pydantic-only DTO over ``Principal``. User membership in a Group
lives in ``principal_memberships`` too (``parent=Group, member=User``,
``role=NULL``).
"""

from datetime import datetime
from typing import ClassVar, Dict, List, Optional

from pydantic import field_validator
from sqlmodel import Field, SQLModel

from gpustack.schemas.common import ListParams, PaginatedList
from gpustack.schemas.principals import (
    PRINCIPAL_MAX_CONCURRENCY_MODEL_NAME,
    PRINCIPAL_MAX_CONCURRENCY_MODELS,
    Principal,
)
from gpustack.schemas.users import AuthProviderEnum


class UserGroupUpdate(SQLModel):
    name: str = Field(nullable=False)
    description: Optional[str] = Field(default=None, nullable=True)


class UserGroupLimitsUpdate(SQLModel):
    """The group-level gateway quotas, full-replace semantics.

    The server resolves these onto every member's ``user-<id>`` combo
    (see ``gpustack.gateway.effective_limits``): a member's explicit user
    limit wins, otherwise the smallest value across their groups applies,
    otherwise the deployment default. ``model_concurrency_limits`` maps a
    model name to the group's in-flight cap for that model; a stored 0 is
    NOT "unlimited" the way a user-level 0 is — on a group it means the
    group does not constrain that model, so it never enters the min.
    Validated against the same bounds the user-level map accepts, so a
    combo rendered from it is never refused at gateway config parse.
    """

    rpm_limit: Optional[int] = None
    tpm_limit: Optional[int] = None
    weekly_token_limit: Optional[int] = None
    monthly_token_limit: Optional[int] = None
    model_concurrency_limits: Optional[Dict[str, int]] = None

    @field_validator("model_concurrency_limits")
    @classmethod
    def _validate_model_concurrency_limits(
        cls, value: Optional[Dict[str, int]]
    ) -> Optional[Dict[str, int]]:
        if value is None:
            return None
        if len(value) > PRINCIPAL_MAX_CONCURRENCY_MODELS:
            raise ValueError(
                f"model_concurrency_limits supports at most "
                f"{PRINCIPAL_MAX_CONCURRENCY_MODELS} models, got {len(value)}"
            )
        for name, quota in value.items():
            if not name or len(name) > PRINCIPAL_MAX_CONCURRENCY_MODEL_NAME:
                raise ValueError(
                    f"model_concurrency_limits keys must be 1.."
                    f"{PRINCIPAL_MAX_CONCURRENCY_MODEL_NAME} characters"
                )
            if quota < 0:
                raise ValueError(
                    f"model_concurrency_limits[{name!r}] must be >= 0 "
                    "(0 means unconstrained)"
                )
        return value


# Stable type tokens for the UI's Type column. Derived, never stored: the
# three attribute kinds are recognized by their reserved name prefixes
# (only the attribute sync creates those), admin-created groups are Local
# by source, and every other IdP-sourced group falls back to the generic
# ``idp`` token.
GROUP_TYPE_PROFESSION = "profession"
GROUP_TYPE_DEPARTMENT = "department"
GROUP_TYPE_BUSINESS_UNIT = "business-unit"
GROUP_TYPE_UNKNOWN = "unknown"
GROUP_TYPE_LOCAL = "local"
GROUP_TYPE_IDP = "idp"


def resolve_group_type(p: Principal) -> str:
    """Map a group principal to its UI type token.

    Source is checked before name: an admin-created group keeps the
    ``local`` token even if someone names it ``title/…`` — the type must
    not change under a rename, and only the attribute sync owns the
    prefixed names in practice.
    """
    if p.source == AuthProviderEnum.Local.value:
        return GROUP_TYPE_LOCAL
    name = p.name or ""
    if name.startswith("title/"):
        return GROUP_TYPE_PROFESSION
    if name.startswith("dept/"):
        return GROUP_TYPE_DEPARTMENT
    if name.startswith("store/"):
        return GROUP_TYPE_BUSINESS_UNIT
    if name.startswith("unknown/"):
        return GROUP_TYPE_UNKNOWN
    return GROUP_TYPE_IDP


class UserGroupCreate(UserGroupUpdate):
    # Immutable after create (absent from ``UserGroupUpdate``). Used
    # by ``sync_user_group_memberships`` to refuse cross-source name
    # adoption.
    source: str = AuthProviderEnum.Local.value


class UserGroupListParams(ListParams):
    sortable_fields: ClassVar[List[str]] = [
        "name",
        "member_count",
        "created_at",
        "updated_at",
        "rpm_limit",
        "tpm_limit",
        "weekly_token_limit",
        "monthly_token_limit",
    ]


class UserGroupPublic(SQLModel):
    id: int
    name: str
    description: Optional[str] = None
    # Human-readable label. Admin-created groups leave it None and the
    # UI falls back to ``name``; attribute-derived sync fills it with
    # the raw attribute value ("Директор магазина", the department's
    # display name), so listings can show a readable label while the
    # prefixed ``name`` stays the stable identifier.
    display_name: Optional[str] = None
    # Derived UI type token (profession / department / business-unit /
    # unknown / local / idp) — see ``resolve_group_type``.
    group_type: str = GROUP_TYPE_LOCAL
    # Number of distinct USER principals reachable from this group
    # through active memberships — direct members plus everyone inside
    # nested subgroups. Deliberately NOT the row count (subgroup edges
    # are not people) and never summed across groups: the same person
    # counts in every group they belong to. Always populated by the
    # list/get routes (zero for empty groups).
    member_count: int = 0
    # Where the Group originated. ``Local`` = admin created via UI;
    # ``OIDC`` / ``SAML`` / other IdP names = auto-created by IdP sync.
    # UI badges IdP-managed rows distinctly.
    source: str = AuthProviderEnum.Local.value
    # Group-level gateway quotas (admin-settable on the Groups page).
    # Server-owned state, unlike the IdP-owned row fields — editable even
    # on IdP-synced groups. NULL inherits: a member's explicit user limit
    # wins, otherwise the smallest value across their groups applies, see
    # ``gpustack.gateway.effective_limits``.
    rpm_limit: Optional[int] = None
    tpm_limit: Optional[int] = None
    weekly_token_limit: Optional[int] = None
    monthly_token_limit: Optional[int] = None
    model_concurrency_limits: Optional[Dict[str, int]] = None
    created_at: datetime
    updated_at: datetime

    @classmethod
    def from_principal(
        cls, p: Principal, *, member_count: int = 0
    ) -> "UserGroupPublic":
        return cls(
            id=p.id,
            name=p.name,
            description=p.description,
            display_name=p.display_name,
            group_type=resolve_group_type(p),
            member_count=member_count,
            source=p.source,
            rpm_limit=p.rpm_limit,
            tpm_limit=p.tpm_limit,
            weekly_token_limit=p.weekly_token_limit,
            monthly_token_limit=p.monthly_token_limit,
            model_concurrency_limits=p.model_concurrency_limits,
            created_at=p.created_at,
            updated_at=p.updated_at,
        )


UserGroupsPublic = PaginatedList[UserGroupPublic]


class UserGroupMembershipPublic(SQLModel):
    # The member principal's id. For ``user`` members this equals
    # ``user_id`` (principal id == user id since identity
    # consolidation); for ``group`` members it is the subgroup's
    # principal id and ``user_id`` is None.
    member_principal_id: int = 0
    # ``user`` for direct user rows, ``group`` for a nested subgroup.
    member_kind: str = "user"
    user_id: Optional[int] = None
    group_id: int
    created_at: datetime
    # Users: login name and display label. Subgroups surface their
    # ``name`` / ``display_name`` through the same fields so one list
    # renders both kinds.
    username: Optional[str] = None
    full_name: Optional[str] = None
    # Only for ``member_kind == "group"``: the subgroup's derived type
    # token and its transitive unique-user count, so the members
    # drawer's Subgroups tab renders type and size without an N+1
    # fan-out. ``None`` for user rows.
    group_type: Optional[str] = None
    group_member_count: Optional[int] = None
    # Only for user rows: display labels from the user's
    # attribute-derived groups (raw title / department / store value).
    # None when the user holds no group of that kind.
    profession: Optional[str] = None
    department: Optional[str] = None
    business_unit: Optional[str] = None

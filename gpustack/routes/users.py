from datetime import datetime, timezone
from typing import Dict, List

from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy import func, or_
from sqlalchemy.orm import aliased
from sqlmodel import select

from gpustack.api.exceptions import (
    AlreadyExistsException,
    ForbiddenException,
    InternalServerErrorException,
    InvalidException,
    NotFoundException,
    ConflictException,
)
from gpustack.schemas.organizations import OrganizationPublic
from gpustack.schemas.principals import (
    OrgRole,
    Principal,
    PrincipalMembership,
    PrincipalType,
    get_platform_principal_id,
)
from gpustack.server.db import async_session
from gpustack.server.deps import CurrentUserDep, SessionDep, TenantContextDep
from gpustack.schemas.users import (
    AuthProviderEnum,
    User,
    UserActivationUpdate,
    UserCreate,
    UserListParams,
    UserUpdate,
    UserPublic,
    UsersPublic,
    UserSelfUpdate,
)
from gpustack.schemas.user_groups import UserGroupPublic
from gpustack.server.passwords import (
    clear_password,
    is_password_change_required,
    password_change_required_map,
    set_password,
)
from gpustack.server.services import UserService, bulk_user_attribute_labels

router = APIRouter()


async def _to_user_public(session, user: User) -> UserPublic:
    # ``require_password_change`` lives on the ``user_passwords`` row,
    # not on the Principal — hydrate it here so the wire response
    # reflects the actual force-change state.
    result = UserPublic.model_validate(user)
    result.require_password_change = await is_password_change_required(session, user.id)
    labels = await bulk_user_attribute_labels(session, [user.id])
    _apply_attribute_labels(result, labels.get(user.id, {}))
    return result


def _apply_attribute_labels(item: UserPublic, labels: Dict[str, str]) -> None:
    """Copy the resolved attribute labels onto the wire DTO.

    ``bulk_user_attribute_labels`` omits kinds the user doesn't hold;
    the fields default to None, so a plain get keeps them absent.
    """
    item.profession = labels.get("profession")
    item.department = labels.get("department")
    item.business_unit = labels.get("business_unit")


async def _to_users_public(session, page) -> UsersPublic:
    flag_map = await password_change_required_map(session, [u.id for u in page.items])
    labels = await bulk_user_attribute_labels(session, [u.id for u in page.items])
    items = []
    for u in page.items:
        item = UserPublic.model_validate(u)
        item.require_password_change = flag_map.get(u.id, False)
        _apply_attribute_labels(item, labels.get(u.id, {}))
        items.append(item)
    return UsersPublic(items=items, pagination=page.pagination)


class UserMembership(BaseModel):
    organization: OrganizationPublic
    role: OrgRole

    model_config = {"from_attributes": True}


# The users-page filters keyed by the wire param, each mapped to the
# group-name prefix of the attribute kind it resolves from.
_ATTRIBUTE_LABEL_FILTERS = {
    "business_unit": "store/",
    "department": "dept/",
    "profession": "title/",
}


def _attribute_label_member_ids_subquery(prefix: str, label: str):
    """USER ids holding an attribute-derived group labelled ``label``.

    One kind, exact label match against the group's display_name (the
    value the IdP reported) or its prefixed name. Scoped to active
    ``oidc-attrs`` rows: admin-curated groups must not answer these
    filters, and soft-deleted memberships must not resurrect anyone.
    """
    return (
        select(PrincipalMembership.member_principal_id)
        .join(
            Principal,
            Principal.id == PrincipalMembership.parent_principal_id,
        )
        .where(
            PrincipalMembership.source == AuthProviderEnum.OIDC_ATTRIBUTES.value,
            PrincipalMembership.deleted_at.is_(None),
            Principal.kind == PrincipalType.GROUP,
            Principal.deleted_at.is_(None),
            Principal.name.like(f"{prefix}%"),
            or_(
                Principal.display_name == label,
                Principal.name == label,
            ),
        )
    )


@router.get("", response_model=UsersPublic)
async def get_users(
    params: UserListParams = Depends(),
    search: str = None,
    business_unit: str = None,
    department: str = None,
    profession: str = None,
):
    fuzzy_fields = {}
    if search:
        fuzzy_fields = {"name": search, "display_name": search}

    # Attribute-label filters: each narrows to users holding the
    # attribute-derived group with that exact label.
    extra_conditions = []
    label_values = {
        "business_unit": business_unit,
        "department": department,
        "profession": profession,
    }
    for param, value in label_values.items():
        if value:
            prefix = _ATTRIBUTE_LABEL_FILTERS[param]
            extra_conditions.append(
                User.id.in_(_attribute_label_member_ids_subquery(prefix, value))
            )

    if params.watch:
        return StreamingResponse(
            User.streaming(fuzzy_fields=fuzzy_fields),
            media_type="text/event-stream",
        )

    async with async_session() as session:
        page = await User.paginated_by_query(
            session=session,
            fuzzy_fields=fuzzy_fields,
            page=params.page,
            per_page=params.perPage,
            fields={
                "kind": PrincipalType.USER,
                "deleted_at": None,
            },
            order_by=params.order_by,
            extra_conditions=extra_conditions or None,
        )
        return await _to_users_public(session, page)


class UserAttributeValues(BaseModel):
    """Distinct attribute-derived labels per kind — the option lists
    for the users-page filters. Only labels at least one live user
    actually holds (the join drops emptied groups)."""

    business_unit: List[str] = []
    department: List[str] = []
    profession: List[str] = []


@router.get("/attribute-values", response_model=UserAttributeValues)
async def get_user_attribute_values(session: SessionDep):
    # Declared before ``/{id}`` on purpose: "attribute-values" would
    # otherwise be captured as an int path param and 422.
    values = UserAttributeValues()
    for param, prefix in _ATTRIBUTE_LABEL_FILTERS.items():
        stmt = (
            select(func.coalesce(Principal.display_name, Principal.name))
            .join(
                PrincipalMembership,
                PrincipalMembership.parent_principal_id == Principal.id,
            )
            .where(
                Principal.kind == PrincipalType.GROUP,
                Principal.source == AuthProviderEnum.OIDC_ATTRIBUTES.value,
                Principal.deleted_at.is_(None),
                PrincipalMembership.deleted_at.is_(None),
                Principal.name.like(f"{prefix}%"),
            )
            .distinct()
        )
        rows = (await session.exec(stmt)).all()
        setattr(values, param, sorted(row for row in rows if row))
    return values


@router.get("/{id}/groups", response_model=List[UserGroupPublic])
async def get_user_groups(session: SessionDep, id: int):
    """Groups the user actively belongs to via admin-curated
    (``source=Local``) memberships — the surface the user-edit modal
    manages. IdP-synced memberships (attribute-derived, claim-based)
    are excluded: they change only in the IdP, and rendering them here
    would invite edits the sync would undo or refuse."""
    user = await User.one_by_id(session, id)
    if not user or user.deleted_at is not None or user.kind != PrincipalType.USER:
        raise NotFoundException(message="User not found")
    stmt = (
        select(Principal)
        .join(
            PrincipalMembership,
            PrincipalMembership.parent_principal_id == Principal.id,
        )
        .where(
            PrincipalMembership.member_principal_id == id,
            PrincipalMembership.source == AuthProviderEnum.Local.value,
            PrincipalMembership.deleted_at.is_(None),
            Principal.kind == PrincipalType.GROUP,
            Principal.deleted_at.is_(None),
        )
    )
    groups = (await session.exec(stmt)).all()
    return [UserGroupPublic.from_principal(g) for g in groups]


async def _resolve_manageable_groups(
    session: SessionDep, group_ids: List[int]
) -> List[Principal]:
    """Validate the ids as live admin-curated (``source=Local``) groups.

    Raises ``InvalidException`` naming the offenders — attribute-derived
    and other IdP-synced groups are not admin-manageable, the same rule
    the members drawer enforces server-side via ``_load_manageable_group``.
    """
    desired = list(dict.fromkeys(group_ids))
    if not desired:
        return []
    candidates = (
        await session.exec(
            select(Principal).where(
                Principal.id.in_(desired),
                Principal.kind == PrincipalType.GROUP,
                Principal.deleted_at.is_(None),
                Principal.source == AuthProviderEnum.Local.value,
            )
        )
    ).all()
    found = {group.id for group in candidates}
    missing = sorted(gid for gid in desired if gid not in found)
    if missing:
        raise InvalidException(
            message=("Group(s) not found or not admin-managed (Local): " f"{missing}")
        )
    return candidates


async def _apply_local_group_membership_diff(
    session: SessionDep, user_id: int, desired_groups: List[Principal]
) -> None:
    """Reconcile the user's Local-source group membership rows.

    Added ids revive an existing soft-deleted row (a single timeline
    per pair, like ``_upsert_group_memberships``) or insert a fresh
    one; ids absent from the desired set are soft-deleted. Only rows
    whose parent is a GROUP principal are in scope — Local-source
    user-in-Org rows share the source marker and must survive an
    admin's group edit. IdP-synced memberships carry their own source
    markers and are neither seen nor touched, so the next login's
    sync can never be clobbered by this diff.

    Writes only; the caller owns the surrounding transaction.
    """
    desired_ids = {group.id for group in desired_groups}
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = (
        await session.exec(
            select(PrincipalMembership)
            .join(
                Principal,
                Principal.id == PrincipalMembership.parent_principal_id,
            )
            .where(
                PrincipalMembership.member_principal_id == user_id,
                PrincipalMembership.source == AuthProviderEnum.Local.value,
                Principal.kind == PrincipalType.GROUP,
            )
        )
    ).all()
    existing_by_parent = {row.parent_principal_id: row for row in rows}

    for parent_id, row in existing_by_parent.items():
        if parent_id not in desired_ids and row.deleted_at is None:
            row.deleted_at = now
            row.updated_at = now
            session.add(row)

    for group in desired_groups:
        row = existing_by_parent.get(group.id)
        if row is None:
            session.add(
                PrincipalMembership(
                    parent_principal_id=group.id,
                    member_principal_id=user_id,
                    role=None,
                    created_at=now,
                    updated_at=now,
                )
            )
        elif row.deleted_at is not None:
            row.deleted_at = None
            row.updated_at = now
            session.add(row)


@router.get("/{id}", response_model=UserPublic)
async def get_user(session: SessionDep, id: int):
    user = await User.one_by_id(session, id)
    if not user:
        raise NotFoundException(message="User not found")
    return await _to_user_public(session, user)


@router.get("/{id}/memberships", response_model=List[UserMembership])
async def list_user_memberships(session: SessionDep, id: int):
    """Admin-only: list the Orgs a user belongs to.

    Only ORG-kind principals are returned — the user's own
    USER-principal is intrinsic to them, not a "membership" anyone
    grants or revokes.
    """
    user = await User.one_by_id(session, id)
    if not user:
        raise NotFoundException(message="User not found")

    stmt = (
        select(PrincipalMembership, Principal)
        .join(
            Principal,
            Principal.id == PrincipalMembership.parent_principal_id,
        )
        .where(
            PrincipalMembership.member_principal_id == user.id,
            PrincipalMembership.deleted_at.is_(None),
            Principal.kind == PrincipalType.ORG,
            Principal.deleted_at.is_(None),
        )
    )
    rows = (await session.exec(stmt)).all()
    return [
        UserMembership(
            organization=OrganizationPublic.from_principal(org),
            role=membership.role or OrgRole.MEMBER,
        )
        for membership, org in rows
    ]


@router.post("", response_model=UserPublic)
async def create_user(session: SessionDep, user_in: UserCreate):
    # USER has its own name namespace (see partitioning rationale on
    # ``Principal.name``); a same-named ORG / SYSTEM / GROUP lives in a
    # separate partition and does not block USER creation.
    existing = (
        await session.exec(
            select(Principal).where(
                Principal.name == user_in.username,
                Principal.kind == PrincipalType.USER,
                Principal.deleted_at.is_(None),
            )
        )
    ).first()
    if existing:
        raise AlreadyExistsException(
            message=f"User with name '{user_in.username}' already exists."
        )

    # A local password on a non-Local user is a /login bypass around
    # the IdP — ``authenticate_user`` only checks the password hash.
    source = user_in.source if user_in.source is not None else AuthProviderEnum.Local
    if source == AuthProviderEnum.Local and not user_in.password:
        raise InvalidException(message="Password is required for Local users.")
    if source != AuthProviderEnum.Local and user_in.password:
        raise InvalidException(message=f"Password must not be set for {source} users.")
    if source != AuthProviderEnum.Local and user_in.require_password_change:
        raise InvalidException(
            message=f"require_password_change is not applicable to {source} users."
        )

    try:
        to_create = User(
            name=user_in.username,
            display_name=user_in.full_name,
            is_admin=user_in.is_admin,
            is_active=user_in.is_active,
            source=source,
        )
        # Admin additionally joins the platform Org as OWNER; regular
        # users do NOT auto-join — admin can add them later if shared
        # workspace access is needed.
        user = await User.create(session, to_create, auto_commit=False)
        if user_in.password:
            await set_password(
                session,
                user.id,
                user_in.password,
                require_password_change=user_in.require_password_change,
                auto_commit=False,
            )
        if user.is_admin:
            now = datetime.now(timezone.utc).replace(tzinfo=None)
            platform_id = await get_platform_principal_id(session)
            session.add(
                PrincipalMembership(
                    parent_principal_id=platform_id,
                    member_principal_id=user.id,
                    role=OrgRole.OWNER,
                    created_at=now,
                    updated_at=now,
                )
            )

        await session.commit()
        await session.refresh(user)
    except Exception as e:
        raise InternalServerErrorException(message=f"Failed to create user: {e}")

    return await _to_user_public(session, user)


@router.put("/{id}", response_model=UserPublic)
async def update_user(session: SessionDep, id: int, user_in: UserUpdate):
    user = await User.one_by_id(session, id)
    if not user:
        raise NotFoundException(message="User not found")

    if (
        user.is_active
        and user_in.is_active is False
        and await is_only_admin_user(session, user)
    ):
        raise ConflictException(message="Cannot deactivate the only admin user")

    # Authentication-source switch (Local ↔ OIDC / SAML / CAS / …).
    # An omitted ``source`` means "leave as-is" — see the override on
    # :class:`UserUpdate.source`. When the caller does change it, the
    # switch is a sensitive linking operation: it rewires how the
    # user logs in, so the credential side (password row) must move
    # in lockstep with the source column.
    new_source = user_in.source or user.source
    is_switching = new_source != user.source

    # Same /login-bypass guard as in :func:`create_user`, but keyed
    # off the *resulting* source so a Local → SSO switch in the same
    # PUT can't smuggle a leftover password through.
    if new_source != AuthProviderEnum.Local and user_in.password:
        raise InvalidException(
            message=f"Password must not be set for {new_source} users."
        )
    if new_source != AuthProviderEnum.Local and user_in.require_password_change:
        raise InvalidException(
            message=f"require_password_change is not applicable to {new_source} users."
        )
    # SSO → Local needs a fresh credential in the same request:
    # there's no password row to fall back on, and leaving the account
    # password-less would lock the user out of /login.
    if is_switching and new_source == AuthProviderEnum.Local and not user_in.password:
        raise InvalidException(
            message=("Password is required when switching a user's source to Local.")
        )

    # Group memberships are resolved before the write transaction so a
    # bad id surfaces as a 400 rather than the generic 500 handler.
    desired_groups: List[Principal] = []
    if user_in.group_ids is not None:
        desired_groups = await _resolve_manageable_groups(session, user_in.group_ids)

    try:
        # ``exclude_unset=True`` so only the fields the client
        # actually sent get written — a partial PUT that omits e.g.
        # ``is_admin`` must not silently reset it to the schema
        # default (``False``, i.e. demote the user). Wire-level
        # ``username`` / ``full_name`` are mapped back onto the
        # Principal columns ``name`` / ``display_name`` for the
        # storage write — see :class:`UserBase`. Password +
        # require-change flag live on the ``user_passwords`` row,
        # handled separately.
        update_data = user_in.model_dump(exclude_unset=True)
        if "username" in update_data:
            update_data["name"] = update_data.pop("username")
        if "full_name" in update_data:
            update_data["display_name"] = update_data.pop("full_name")
        update_data.pop("password", None)
        require_change = update_data.pop("require_password_change", False)
        # ``source`` is written through the switch-validated path
        # below rather than as-is from ``update_data``: sending
        # ``{"source": null}`` explicitly would otherwise write NULL
        # to a NOT NULL column, and using the client's raw value
        # bypasses the enum check that lives at the schema layer.
        update_data.pop("source", None)
        # Memberships are handled by the diff below, not as a column
        # write on the user row.
        update_data.pop("group_ids", None)
        if is_switching:
            update_data["source"] = new_source

        # Single transaction across all three writes (source column,
        # password row create, password row clear). Otherwise a
        # ``clear_password`` failure on a Local → SSO switch would
        # leave the row committed with ``source=SSO`` while the local
        # password hash is still verifiable — exactly the /login
        # bypass state the switch is meant to eliminate. Matches
        # :func:`create_user`'s ``auto_commit=False`` composition.
        service = UserService(session)
        old_name = user.name
        await service.update(user, update_data, auto_commit=False)
        if user_in.password:
            await set_password(
                session,
                user.id,
                user_in.password,
                require_password_change=require_change,
                auto_commit=False,
            )
        if is_switching and new_source != AuthProviderEnum.Local:
            # Local → SSO: drop the local password so /login can't be
            # used as a parallel ingress that bypasses the IdP.
            await clear_password(session, user.id, auto_commit=False)
        if user_in.group_ids is not None:
            await _apply_local_group_membership_diff(session, user.id, desired_groups)
        await session.commit()
        # Cache invalidation runs *after* the commit so a concurrent
        # read during the transaction's window can't refill the entry
        # from the pre-commit row and outlive our invalidation.
        await service.invalidate_cache(user, old_name=old_name)
    except Exception as e:
        raise InternalServerErrorException(message=f"Failed to update user: {e}")

    return await _to_user_public(session, user)


@router.patch("/{id}/activation", response_model=UserPublic)
async def update_user_activation(
    session: SessionDep, id: int, activation_data: UserActivationUpdate
):
    """
    Activate or deactivate a user account.
    Only administrators can perform this action.
    """
    user = await User.one_by_id(session, id)
    if not user:
        raise NotFoundException(message="User not found")

    changed = user.is_active != activation_data.is_active
    if not changed:
        return await _to_user_public(session, user)

    if (
        user.is_active
        and activation_data.is_active is False
        and await is_only_admin_user(session, user)
    ):
        raise ConflictException(message="Cannot deactivate the only admin user")

    try:
        await UserService(session).update(
            user, {"is_active": activation_data.is_active}
        )
    except Exception as e:
        raise InternalServerErrorException(
            message=f"Failed to update user activation: {e}"
        )

    return await _to_user_public(session, user)


@router.delete("/{id}")
async def delete_user(session: SessionDep, id: int):
    user_service = UserService(session)
    user = await user_service.get_by_id(id)
    if not user:
        raise NotFoundException(message="User not found")

    if await is_only_admin_user(session, user):
        raise ConflictException(message="Cannot delete the only admin user")

    # After identity consolidation the user IS its own principal, so
    # deleting the user row also tears down every resource owned by
    # ``owner_principal_id == user.id`` via FK CASCADE. No separate
    # principal row to clean up.
    try:
        await user_service.delete(user)
    except Exception as e:
        raise InternalServerErrorException(message=f"Failed to delete user: {e}")


async def is_only_admin_user(session: SessionDep, user: User) -> bool:
    if not user.is_admin:
        return False
    admin_count = await User.count_by_fields(
        session, {"is_admin": True, "is_active": True}
    )
    return admin_count == 1


me_router = APIRouter()


@me_router.get("/me", response_model=UserPublic)
async def get_user_me(session: SessionDep, user: CurrentUserDep):
    return await _to_user_public(session, user)


@me_router.put("/me", response_model=UserPublic)
async def update_user_me(
    session: SessionDep, user: CurrentUserDep, user_in: UserSelfUpdate
):
    # See the guard in :func:`create_user` — same /login bypass.
    if user.source != AuthProviderEnum.Local and user_in.password:
        raise InvalidException(
            message=f"Password must not be set for {user.source} users."
        )

    try:
        update_data = user_in.model_dump(exclude_none=True)
        if "full_name" in update_data:
            update_data["display_name"] = update_data.pop("full_name")
        plain = update_data.pop("password", None)
        await UserService(session).update(user, update_data)
        if plain:
            await set_password(session, user.id, plain)
    except Exception as e:
        raise InternalServerErrorException(message=f"Failed to update user: {e}")

    return await _to_user_public(session, user)


# User-search endpoint accessible to org owners (any) and platform
# admins, so the Add Member picker works without the admin-gated full
# /users endpoint. Returns the standard UsersPublic page.
directory_router = APIRouter()


@directory_router.get("/user-directory", response_model=UsersPublic)
async def list_user_directory(
    ctx: TenantContextDep,
    page: int = 1,
    perPage: int = 30,
    search: str = None,
    # ``scope=current_org`` narrows results to USER principals that are
    # members of the request's current Org (directly or via a Group
    # joined to that Org). Used by pickers whose surrounding list is
    # already Org-scoped (e.g. the API keys "Filter by creator" select)
    # so the picker options match the rows the user can actually see.
    # Silently ignored when the request has no Org context — platform
    # admins in "All orgs" mode keep getting the full directory.
    scope: str = None,
):
    if not ctx.is_platform_admin and ctx.org_role != OrgRole.OWNER:
        raise ForbiddenException(message="Insufficient permission")
    fuzzy_fields = {}
    if search:
        fuzzy_fields = {"name": search, "display_name": search}
    extra_conditions = []
    if scope == "current_org" and ctx.has_org_context:
        extra_conditions.append(
            User.id.in_(_org_member_user_ids_subquery(ctx.current_principal_id))
        )
    async with async_session() as session:
        result = await User.paginated_by_query(
            session=session,
            fuzzy_fields=fuzzy_fields,
            extra_conditions=extra_conditions or None,
            page=page,
            per_page=perPage,
            fields={
                "kind": PrincipalType.USER,
                "deleted_at": None,
            },
        )
        return await _to_users_public(session, result)


def _org_member_user_ids_subquery(org_principal_id: int):
    """USER principal ids reachable as members of an Org.

    Mirrors the two membership paths in
    :func:`gpustack.api.tenant._resolve_effective_org_role`: a direct
    ``(parent=org, member=user)`` row, or via a Group that is itself
    a member of the Org. Soft-deleted rows on either hop are excluded.
    """
    direct = (
        select(PrincipalMembership.member_principal_id)
        .join(
            Principal,
            Principal.id == PrincipalMembership.member_principal_id,
        )
        .where(
            PrincipalMembership.parent_principal_id == org_principal_id,
            PrincipalMembership.deleted_at.is_(None),
            Principal.kind == PrincipalType.USER,
            Principal.deleted_at.is_(None),
        )
    )
    org_pm = aliased(PrincipalMembership)
    group_pm = aliased(PrincipalMembership)
    via_group = (
        select(group_pm.member_principal_id)
        .join(org_pm, org_pm.member_principal_id == group_pm.parent_principal_id)
        .join(Principal, Principal.id == group_pm.parent_principal_id)
        .where(
            org_pm.parent_principal_id == org_principal_id,
            org_pm.deleted_at.is_(None),
            group_pm.deleted_at.is_(None),
            Principal.kind == PrincipalType.GROUP,
            Principal.deleted_at.is_(None),
        )
    )
    return direct.union_all(via_group)

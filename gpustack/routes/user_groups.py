"""UserGroup management — platform admin only.

Groups are GROUP-kind ``Principal`` rows. They are peer-level
principals: no structural parent, no Org affiliation baked into the
row. A Group may be a member of zero or more Orgs via rows in
``principal_memberships`` with ``parent=Org, member=Group`` — managed
through the ``organization_members`` routes, not here.

User membership in a Group lives in ``principal_memberships`` too
(``parent=Group, member=User``, ``role=NULL``).
"""

from datetime import datetime, timezone
import math
from typing import Dict, List, Optional

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy import func, literal, or_
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from gpustack.api.exceptions import (
    AlreadyExistsException,
    InvalidException,
    NotFoundException,
)
from gpustack.api.tenant import require_platform_admin
from gpustack.mixins.active_record import _fuzzy_conditions
from gpustack.schemas.common import Pagination
from gpustack.schemas.principals import (
    Principal,
    PrincipalMembership,
    PrincipalType,
    RESERVED_PRINCIPAL_NAME_PREFIX,
    is_reserved_principal_name,
)
from gpustack.schemas.user_groups import (
    UserGroupCreate,
    UserGroupListParams,
    UserGroupMembershipPublic,
    UserGroupPublic,
    UserGroupUpdate,
    UserGroupsPublic,
    resolve_group_type,
)
from gpustack.schemas.users import User
from gpustack.server.deps import SessionDep, TenantContextDep

router = APIRouter()


class GroupMembershipCreate(BaseModel):
    user_ids: List[int] = []
    group_ids: List[int] = []


async def _load_group(session, group_id: int) -> Principal:
    """Load a user-curated group by id.

    Built-in reserved groups (``system/`` prefix) are deliberately
    hidden from the Group-CRUD surface — admins shouldn't be able to
    rename / delete them, and the Groups list page has no useful
    rendering for them (Members is always 0 since membership is
    implicit). They're still consumed by cluster_access / ACL views,
    which render principal_kind=GROUP grants directly.
    """
    group = await Principal.one_by_id(session, group_id)
    if (
        not group
        or group.deleted_at is not None
        or group.kind != PrincipalType.GROUP
        or is_reserved_principal_name(group.name)
    ):
        raise NotFoundException(message="Group not found")
    return group


async def _bulk_transitive_member_counts(
    session: AsyncSession, group_ids: List[int]
) -> Dict[int, int]:
    """Distinct USER count reachable per group, in one recursive query.

    A group's size is the number of unique user principals reachable
    through active membership rows — direct users plus everyone inside
    nested subgroups. Subgroup edges are expanded, not counted; the
    same person reached through two branches counts once for that
    group. Counts of *different* groups stay independent (a person in
    several groups counts in each) — they are never summed anywhere.

    One recursive CTE for the whole batch keeps the list route at a
    single query regardless of page size. The depth cap mirrors the
    rights resolution in ``api/tenant.py``: add-time cycle checks keep
    the graph acyclic, the cap bounds the walk if data were corrupted
    by hand.
    """
    if not group_ids:
        return {}
    membership = PrincipalMembership.__table__
    principal = Principal.__table__

    # Seed: every active edge leaving the requested groups (users and
    # subgroup edges alike — the recursion only expands the latter).
    reach = (
        select(
            membership.c.parent_principal_id.label("root_id"),
            membership.c.member_principal_id.label("member_id"),
            literal(0).label("depth"),
        ).where(
            membership.c.parent_principal_id.in_(group_ids),
            membership.c.deleted_at.is_(None),
        )
    ).cte("group_member_reach", recursive=True)
    reach = reach.union_all(
        select(
            reach.c.root_id.label("root_id"),
            membership.c.member_principal_id.label("member_id"),
            (reach.c.depth + 1).label("depth"),
        )
        .join(membership, membership.c.parent_principal_id == reach.c.member_id)
        .where(
            membership.c.deleted_at.is_(None),
            reach.c.depth < 31,
        )
    )
    stmt = (
        select(reach.c.root_id, func.count(func.distinct(reach.c.member_id)))
        .join(principal, principal.c.id == reach.c.member_id)
        .where(
            principal.c.kind == PrincipalType.USER,
            principal.c.deleted_at.is_(None),
        )
        .group_by(reach.c.root_id)
    )
    return {row[0]: row[1] for row in (await session.exec(stmt)).all()}


# ---- groups (platform-admin CRUD) ------------------------------------------


def _member_count_sort_requested(
    order_by: Optional[List[tuple[str, str]]],
) -> bool:
    """True when the list must be ordered by transitive member count.

    That covers the explicit ``member_count`` sort token AND the
    no-sort default: the UI's primary ordering is "biggest groups
    first", so the absence of a sort param means member_count desc,
    not the mixin's created_at fallback.
    """
    if not order_by:
        return True
    return any(field == "member_count" for field, _ in order_by)


async def _list_groups_by_member_count(
    session: AsyncSession,
    params: UserGroupListParams,
    fuzzy_fields: Dict[str, str],
    extra_conditions: List,
    order_by: Optional[List[tuple[str, str]]] = None,
) -> UserGroupsPublic:
    """Member-count-ordered listing, paginated in Python.

    The count is computed (not stored), so SQL-side OFFSET/LIMIT cannot
    order by it. All matching groups are fetched once, counted by
    ``_bulk_transitive_member_counts`` in a single recursive query, and
    sliced here — the group table is small (hundreds of rows), and the
    count query dominates either way. ``member_count`` governs the
    primary key in its requested direction; name is the stable
    tiebreaker so pages don't reshuffle between identical counts.
    """
    stmt = select(Principal).where(
        Principal.kind == PrincipalType.GROUP,
        Principal.deleted_at.is_(None),
        *extra_conditions,
    )
    fuzzy = _fuzzy_conditions(Principal, fuzzy_fields)
    if fuzzy:
        stmt = stmt.where(or_(*fuzzy))
    groups = list((await session.exec(stmt)).all())

    counts = await _bulk_transitive_member_counts(session, [g.id for g in groups])
    ascending = any(
        field == "member_count" and direction == "asc"
        for field, direction in (order_by or [])
    )

    def _sort_key(g: Principal) -> tuple:
        label = (g.display_name or g.name or "").lower()
        count = counts.get(g.id, 0)
        return (count, label) if ascending else (-count, label)

    groups.sort(key=_sort_key)

    total = len(groups)
    per_page = params.perPage or 100
    total_page = math.ceil(total / per_page)
    start = (max(params.page, 1) - 1) * per_page
    page_items = groups[start : start + per_page]
    return UserGroupsPublic(
        items=[
            UserGroupPublic.from_principal(g, member_count=counts.get(g.id, 0))
            for g in page_items
        ],
        pagination=Pagination(
            page=params.page,
            perPage=per_page,
            total=total,
            totalPage=total_page,
        ),
    )


@router.get("/groups", response_model=UserGroupsPublic)
async def list_groups(
    session: SessionDep,
    ctx: TenantContextDep,
    params: UserGroupListParams = Depends(),
    search: Optional[str] = None,
    include_reserved: bool = False,
):
    """List all groups. Visible to any authenticated user — Group names
    are global identifiers in the system; rendering an Org's
    group-memberships needs to be able to enumerate candidate groups.

    By default reserved built-in groups (``system/…``) are excluded:
    the Groups CRUD page would render them as 0-member editable rows
    that the server then rejects on write. ACL pickers (cluster_access
    grant, model-route ACL, …) pass ``include_reserved=true`` so
    ``system/authenticated`` shows up and can be granted to.

    Default (and explicit) ordering by ``member_count`` paginates in
    Python — the count is derived, so it cannot be ordered by SQL
    OFFSET/LIMIT. Every other sort stays on the mixin's SQL pagination.
    """
    fuzzy_fields = {"name": search} if search else {}
    extra_conditions = []
    if not include_reserved:
        extra_conditions.append(
            Principal.name.not_like(f"{RESERVED_PRINCIPAL_NAME_PREFIX}%"),
        )
    if _member_count_sort_requested(params.order_by):
        return await _list_groups_by_member_count(
            session, params, fuzzy_fields, extra_conditions, params.order_by
        )

    page = await Principal.paginated_by_query(
        session=session,
        fields={
            "kind": PrincipalType.GROUP,
            "deleted_at": None,
        },
        fuzzy_fields=fuzzy_fields,
        page=params.page,
        per_page=params.perPage,
        order_by=params.order_by,
        extra_conditions=extra_conditions,
    )
    counts = await _bulk_transitive_member_counts(session, [g.id for g in page.items])
    page.items = [
        UserGroupPublic.from_principal(g, member_count=counts.get(g.id, 0))
        for g in page.items
    ]
    return page


@router.get("/groups/{group_id}", response_model=UserGroupPublic)
async def get_group(session: SessionDep, ctx: TenantContextDep, group_id: int):
    group = await _load_group(session, group_id)
    counts = await _bulk_transitive_member_counts(session, [group.id])
    return UserGroupPublic.from_principal(group, member_count=counts.get(group.id, 0))


@router.post(
    "/groups",
    response_model=UserGroupPublic,
    dependencies=[Depends(require_platform_admin)],
)
async def create_group(session: SessionDep, body: UserGroupCreate):
    if is_reserved_principal_name(body.name):
        # ``system/`` is reserved for built-in principals
        # (``system/authenticated``, ``system/cluster-…``, …) that the
        # server seeds itself.
        raise InvalidException(
            message=(
                f"Group name must not start with "
                f"'{RESERVED_PRINCIPAL_NAME_PREFIX}' (reserved prefix)"
            )
        )
    existing = await Principal.one_by_fields(
        session,
        {
            "kind": PrincipalType.GROUP,
            "name": body.name,
            "deleted_at": None,
        },
    )
    if existing:
        raise AlreadyExistsException(
            message=f"Group with name '{body.name}' already exists."
        )

    try:
        group = Principal(
            kind=PrincipalType.GROUP,
            name=body.name,
            description=body.description,
            source=body.source,
        )
        created = await Principal.create(session, group)
    except Exception as e:
        raise InvalidException(message=f"Failed to create group: {e}")
    # New group has no members yet; skip the COUNT round trip.
    return UserGroupPublic.from_principal(created, member_count=0)


@router.put(
    "/groups/{group_id}",
    response_model=UserGroupPublic,
    dependencies=[Depends(require_platform_admin)],
)
async def update_group(session: SessionDep, group_id: int, body: UserGroupUpdate):
    group = await _load_group(session, group_id)

    if body.name != group.name:
        if is_reserved_principal_name(body.name):
            raise InvalidException(
                message=(
                    f"Group name must not start with "
                    f"'{RESERVED_PRINCIPAL_NAME_PREFIX}' (reserved prefix)"
                )
            )
        clash = await Principal.one_by_fields(
            session,
            {
                "kind": PrincipalType.GROUP,
                "name": body.name,
                "deleted_at": None,
            },
        )
        if clash and clash.id != group.id:
            raise AlreadyExistsException(
                message=f"Group with name '{body.name}' already exists."
            )

    try:
        await group.update(session, body.model_dump(exclude_unset=True))
    except Exception as e:
        raise InvalidException(message=f"Failed to update group: {e}")
    counts = await _bulk_transitive_member_counts(session, [group.id])
    return UserGroupPublic.from_principal(group, member_count=counts.get(group.id, 0))


@router.delete(
    "/groups/{group_id}",
    dependencies=[Depends(require_platform_admin)],
)
async def delete_group(session: SessionDep, group_id: int):
    group = await _load_group(session, group_id)
    try:
        await group.delete(session)
    except Exception as e:
        raise InvalidException(message=f"Failed to delete group: {e}")


# ---- group members ---------------------------------------------------------


async def _resolve_user(session, user_id: int) -> Optional[User]:
    user = await User.one_by_id(session, user_id)
    if not user or user.kind != PrincipalType.USER or user.deleted_at is not None:
        return None
    return user


async def _resolve_group_member(
    session, group_principal_id: int
) -> Optional[Principal]:
    """Resolve a GROUP principal eligible for nesting.

    Reserved system groups (``system/…``) are membership-implicit and
    hidden from the Group-CRUD surface — nesting one under a curated
    group would leak a non-editable row into the member lists, so they
    are refused here like everywhere else on this surface.
    """
    group = await Principal.one_by_id(session, group_principal_id)
    if (
        not group
        or group.deleted_at is not None
        or group.kind != PrincipalType.GROUP
        or is_reserved_principal_name(group.name)
    ):
        return None
    return group


async def _would_create_cycle(
    session, parent_principal_id: int, child_principal_id: int
) -> bool:
    """True if making ``child`` a member of ``parent`` closes a cycle.

    The new edge puts ``child`` inside ``parent``; it loops only if
    ``child`` is already an ancestor of ``parent`` — walking up from
    ``parent`` through active memberships reaches ``child``. The walk
    bound mirrors the recursive-resolution cap in ``api/tenant.py`` —
    add-time checks are the primary defense, the cap is belt and
    suspenders, so a pathologically deep (or corrupt) graph is
    rejected rather than walked forever.
    """
    frontier = {parent_principal_id}
    visited: set = set()
    for _ in range(64):
        if not frontier:
            return False
        parents = set(
            (
                await session.exec(
                    select(PrincipalMembership.parent_principal_id)
                    .join(
                        Principal,
                        Principal.id == PrincipalMembership.parent_principal_id,
                    )
                    .where(
                        PrincipalMembership.member_principal_id.in_(frontier),
                        PrincipalMembership.deleted_at.is_(None),
                        Principal.kind == PrincipalType.GROUP,
                        Principal.deleted_at.is_(None),
                    )
                )
            ).all()
        )
        if child_principal_id in parents:
            return True
        visited |= frontier
        frontier = parents - visited
    return True


@router.get(
    "/groups/{group_id}/members",
    response_model=List[UserGroupMembershipPublic],
)
async def list_group_members(session: SessionDep, ctx: TenantContextDep, group_id: int):
    await _load_group(session, group_id)

    stmt = select(PrincipalMembership).where(
        PrincipalMembership.parent_principal_id == group_id,
        PrincipalMembership.deleted_at.is_(None),
    )
    rows = list((await session.exec(stmt)).all())
    member_ids = {r.member_principal_id for r in rows}
    user_by_principal: dict[int, User] = {}
    group_by_principal: dict[int, Principal] = {}
    if member_ids:
        # ``User`` is an alias of ``Principal`` — one table. Without the
        # kind filter a nested subgroup would come back in this result
        # and be rendered as a user.
        result = await session.exec(
            select(User).where(User.id.in_(member_ids), User.kind == PrincipalType.USER)
        )
        user_by_principal = {u.id: u for u in result.all()}
        # Nested subgroups ride the same membership rows; resolve them
        # so the list can render both member kinds.
        result = await session.exec(
            select(Principal).where(
                Principal.id.in_(member_ids),
                Principal.kind == PrincipalType.GROUP,
                Principal.deleted_at.is_(None),
            )
        )
        group_by_principal = {g.id: g for g in result.all()}
    # The Subgroups tab renders each subgroup's type and its own
    # transitive size — computed in bulk so a deep nest costs one
    # query, not one per row.
    subgroup_counts = await _bulk_transitive_member_counts(
        session, list(group_by_principal)
    )
    out: List[UserGroupMembershipPublic] = []
    for r in rows:
        u = user_by_principal.get(r.member_principal_id)
        g = group_by_principal.get(r.member_principal_id)
        if u is not None:
            out.append(
                UserGroupMembershipPublic(
                    member_principal_id=u.id,
                    member_kind="user",
                    user_id=u.id,
                    group_id=group_id,
                    created_at=r.created_at,
                    username=u.name,
                    full_name=u.display_name,
                )
            )
        elif g is not None:
            out.append(
                UserGroupMembershipPublic(
                    member_principal_id=g.id,
                    member_kind="group",
                    user_id=None,
                    group_id=group_id,
                    created_at=r.created_at,
                    username=g.name,
                    full_name=g.display_name,
                    group_type=resolve_group_type(g),
                    group_member_count=subgroup_counts.get(g.id, 0),
                )
            )
        else:
            # Dangling member (principal hard-deleted out from under
            # the membership row): keep the row visible so the admin
            # can remove it, with nothing to render as identity.
            out.append(
                UserGroupMembershipPublic(
                    member_principal_id=r.member_principal_id,
                    member_kind="user",
                    user_id=None,
                    group_id=group_id,
                    created_at=r.created_at,
                    username=None,
                    full_name=None,
                )
            )
    return out


async def _validate_subgroup_candidates(
    session,
    parent_group_id: int,
    candidates: dict[int, Principal],
) -> None:
    """Reject subgroup candidates that cannot nest under the parent.

    Reserved and non-GROUP principals are refused the same way the
    CRUD surface refuses them; self-nesting would corrupt the DAG the
    recursive rights resolution walks. Cycle closure is checked
    separately, after the duplicate pass — an existing edge is a
    409, not a cycle.
    """
    for gid in sorted(candidates):
        group = candidates[gid]
        if (
            group.kind != PrincipalType.GROUP
            or group.deleted_at is not None
            or is_reserved_principal_name(group.name)
        ):
            raise NotFoundException(message=f"Group(s) not found: {gid}")
    if parent_group_id in candidates:
        raise InvalidException(message="A group cannot be nested inside itself")


async def _upsert_group_memberships(
    session,
    parent_group_id: int,
    candidates: List[tuple[Principal, str]],
    existing_by_principal: dict[int, PrincipalMembership],
) -> List[tuple[str, Principal, PrincipalMembership]]:
    """Insert or revive one membership row per candidate.

    A soft-deleted row is resurrected so a member removed from the
    group and re-added keeps a single timeline row (matches the
    add_org_members handling); a fresh candidate gets a new row.
    Commits on success; rolls back and raises on any write failure so
    the request stays all-or-nothing.
    """
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    stored: List[tuple[str, Principal, PrincipalMembership]] = []
    try:
        for principal, member_kind in candidates:
            existing = existing_by_principal.get(principal.id)
            if existing is not None:
                existing.deleted_at = None
                existing.updated_at = now
                session.add(existing)
                link = existing
            else:
                link = PrincipalMembership(
                    parent_principal_id=parent_group_id,
                    member_principal_id=principal.id,
                    role=None,
                    created_at=now,
                    updated_at=now,
                )
                session.add(link)
            stored.append((member_kind, principal, link))
        await session.commit()
    except Exception as e:
        await session.rollback()
        raise InvalidException(message=f"Failed to add group members: {e}")
    return stored


@router.post(
    "/groups/{group_id}/members",
    response_model=List[UserGroupMembershipPublic],
    dependencies=[Depends(require_platform_admin)],
)
async def add_group_members(
    session: SessionDep,
    group_id: int,
    body: GroupMembershipCreate,
):
    """Add users and/or subgroups to a group in a single request.

    All-or-nothing: validate every candidate up front — users must be
    live USER principals, subgroups live non-reserved GROUP principals
    whose inclusion would not close a cycle; any failure raises and
    writes nothing. Otherwise all rows insert (or revive) in a single
    transaction.
    """
    await _load_group(session, group_id)

    user_ids = list(dict.fromkeys(body.user_ids))
    group_ids = list(dict.fromkeys(body.group_ids))
    if not user_ids and not group_ids:
        raise InvalidException(message="user_ids/group_ids must not both be empty")
    overlap = set(user_ids) & set(group_ids)
    if overlap:
        # The id namespaces are shared, so one of the two lookups
        # below would fail anyway — reject the contradiction directly.
        raise InvalidException(
            message=f"Principal(s) listed as both user and group: {sorted(overlap)}"
        )

    users_by_id: dict[int, User] = {}
    if user_ids:
        rows = (await session.exec(select(User).where(User.id.in_(user_ids)))).all()
        users_by_id = {
            u.id: u
            for u in rows
            if u.kind == PrincipalType.USER and u.deleted_at is None
        }
        missing = [uid for uid in user_ids if uid not in users_by_id]
        if missing:
            raise NotFoundException(message=f"User(s) not found: {missing}")

    groups_by_id: dict[int, Principal] = {}
    if group_ids:
        rows = (
            await session.exec(select(Principal).where(Principal.id.in_(group_ids)))
        ).all()
        groups_by_id = {g.id: g for g in rows}
        await _validate_subgroup_candidates(session, group_id, groups_by_id)

    # After identity consolidation, a user's principal id IS user.id.
    principal_ids = list(users_by_id.keys()) + list(groups_by_id.keys())

    # Bulk-load existing memberships (including soft-deleted, so we can
    # resurrect them instead of producing duplicate rows). Matches the
    # add_org_members soft-delete handling so a member removed from a
    # group and re-added keeps a single timeline row.
    existing_rows = (
        await session.exec(
            select(PrincipalMembership).where(
                PrincipalMembership.parent_principal_id == group_id,
                PrincipalMembership.member_principal_id.in_(principal_ids),
            )
        )
    ).all()
    existing_by_principal: dict[int, PrincipalMembership] = {
        m.member_principal_id: m for m in existing_rows
    }
    duplicates = [
        pid
        for pid in principal_ids
        if (m := existing_by_principal.get(pid)) is not None and m.deleted_at is None
    ]
    if duplicates:
        raise AlreadyExistsException(
            message=f"Member(s) already in group {group_id}: {duplicates}"
        )

    # Only now, with duplicates ruled out, can a cycle check be
    # trusted: the edge being re-added is a 409 above, not a loop.
    for gid in sorted(groups_by_id):
        if await _would_create_cycle(session, group_id, gid):
            raise InvalidException(
                message=f"Adding group {gid} would create a membership cycle"
            )

    candidates = [
        *[(users_by_id[uid], "user") for uid in user_ids],
        *[(groups_by_id[gid], "group") for gid in group_ids],
    ]
    stored = await _upsert_group_memberships(
        session, group_id, candidates, existing_by_principal
    )

    return [
        UserGroupMembershipPublic(
            member_principal_id=principal.id,
            member_kind=member_kind,
            user_id=principal.id if member_kind == "user" else None,
            group_id=group_id,
            created_at=link.created_at,
            username=principal.name,
            full_name=principal.display_name,
        )
        for member_kind, principal, link in stored
    ]


@router.delete(
    "/groups/{group_id}/members/{member_id}",
    dependencies=[Depends(require_platform_admin)],
)
async def remove_group_member(
    session: SessionDep,
    group_id: int,
    member_id: int,
):
    """Remove a member (user or nested subgroup) from a group.

    The path id is the member principal's id — a user's id (principal
    id == user id) or a subgroup's principal id. Deleting the
    membership row for a subgroup immediately cuts its users'
    transitive membership; the recursive resolution walks only active
    rows.
    """
    await _load_group(session, group_id)

    member = await _resolve_user(session, member_id)
    if member is None:
        member = await _resolve_group_member(session, member_id)
    if member is None:
        raise NotFoundException(message="Group membership not found")

    stmt = select(PrincipalMembership).where(
        PrincipalMembership.parent_principal_id == group_id,
        PrincipalMembership.member_principal_id == member.id,
        PrincipalMembership.deleted_at.is_(None),
    )
    link = (await session.exec(stmt)).first()
    if not link:
        raise NotFoundException(message="Group membership not found")

    try:
        await link.delete(session, soft=True)
    except Exception as e:
        await session.rollback()
        raise InvalidException(message=f"Failed to remove group member: {e}")

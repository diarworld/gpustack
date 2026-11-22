"""Tests for nested groups (group-in-group membership).

The nesting endpoints validate kind, reservation, self-nesting and
cycle closure before writing; rights resolution (``_user_group_principal_ids``)
must see a user's groups transitively. Both run against a real
in-memory SQLite so the soft-delete and recursion semantics hold
against actual rows.
"""

from datetime import datetime, timezone

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession

from gpustack.api.exceptions import (
    AlreadyExistsException,
    InvalidException,
    NotFoundException,
)
from gpustack.api.tenant import _user_group_principal_ids
from gpustack.routes.user_groups import (
    GroupMembershipCreate,
    _bulk_transitive_member_counts,
    add_group_members,
    delete_group,
    get_group,
    list_group_members,
    list_groups,
    remove_group_member,
    update_group,
)
from gpustack.routes.users import (
    _apply_local_group_membership_diff,
    _resolve_manageable_groups,
    get_user_groups,
)
from gpustack.server.services import bulk_user_attribute_labels
from gpustack.schemas.principals import Principal, PrincipalMembership, PrincipalType
from gpustack.schemas.user_groups import (
    UserGroupListParams,
    UserGroupUpdate,
    resolve_group_type,
)
from gpustack.schemas.users import AuthProviderEnum


@pytest_asyncio.fixture
async def session():
    engine = create_async_engine("sqlite+aiosqlite://")
    async with engine.begin() as conn:
        await conn.run_sync(Principal.__table__.create)
        await conn.run_sync(PrincipalMembership.__table__.create)
    async with AsyncSession(engine, expire_on_commit=False) as s:
        yield s
    await engine.dispose()


async def _seed_group(session, principal_id: int, name: str) -> Principal:
    group = Principal(
        id=principal_id,
        kind=PrincipalType.GROUP,
        name=name,
        source=AuthProviderEnum.Local.value,
    )
    session.add(group)
    await session.commit()
    return group


async def _seed_user(session, principal_id: int, name: str) -> Principal:
    user = Principal(
        id=principal_id,
        kind=PrincipalType.USER,
        name=name,
        source=AuthProviderEnum.OIDC.value,
    )
    session.add(user)
    await session.commit()
    return user


async def _active_member_ids(session, group_id: int) -> set:
    rows = (
        await session.exec(
            PrincipalMembership.__table__.select().where(
                PrincipalMembership.__table__.c.parent_principal_id == group_id,
                PrincipalMembership.__table__.c.deleted_at.is_(None),
            )
        )
    ).all()
    return {r.member_principal_id for r in rows}


async def _seed_membership(
    session,
    parent_id: int,
    member_id: int,
    *,
    deleted_at=None,
    source: str = AuthProviderEnum.Local.value,
) -> PrincipalMembership:
    row = PrincipalMembership(
        parent_principal_id=parent_id,
        member_principal_id=member_id,
        source=source,
        deleted_at=deleted_at,
    )
    session.add(row)
    await session.commit()
    return row


async def _active_parent_ids_of_user(session, user_id: int) -> set:
    rows = (
        await session.exec(
            PrincipalMembership.__table__.select().where(
                PrincipalMembership.__table__.c.member_principal_id == user_id,
                PrincipalMembership.__table__.c.deleted_at.is_(None),
            )
        )
    ).all()
    return {r.parent_principal_id for r in rows}


# ---- add subgroup -----------------------------------------------------------


@pytest.mark.asyncio
async def test_add_subgroup_creates_membership(session):
    await _seed_group(session, 10, "SDLC users")
    await _seed_group(session, 20, "title/разработчик")

    result = await add_group_members(session, 10, GroupMembershipCreate(group_ids=[20]))
    await session.commit()

    assert len(result) == 1
    assert result[0].member_kind == "group"
    assert result[0].member_principal_id == 20
    assert result[0].user_id is None
    assert result[0].username == "title/разработчик"
    assert await _active_member_ids(session, 10) == {20}


@pytest.mark.asyncio
async def test_add_duplicate_subgroup_conflicts(session):
    await _seed_group(session, 10, "SDLC users")
    await _seed_group(session, 20, "title/разработчик")
    await add_group_members(session, 10, GroupMembershipCreate(group_ids=[20]))

    with pytest.raises(AlreadyExistsException):
        await add_group_members(session, 10, GroupMembershipCreate(group_ids=[20]))


@pytest.mark.asyncio
async def test_add_self_nesting_rejected(session):
    await _seed_group(session, 10, "SDLC users")

    with pytest.raises(InvalidException):
        await add_group_members(session, 10, GroupMembershipCreate(group_ids=[10]))


@pytest.mark.asyncio
async def test_add_cycle_rejected(session):
    await _seed_group(session, 10, "A")
    await _seed_group(session, 20, "B")
    # B into A is fine; A into B would close the loop.
    await add_group_members(session, 10, GroupMembershipCreate(group_ids=[20]))

    with pytest.raises(InvalidException):
        await add_group_members(session, 20, GroupMembershipCreate(group_ids=[10]))


@pytest.mark.asyncio
async def test_deep_chain_allowed(session):
    await _seed_group(session, 10, "top")
    await _seed_group(session, 20, "mid")
    await _seed_group(session, 30, "leaf")
    await add_group_members(session, 20, GroupMembershipCreate(group_ids=[30]))
    await add_group_members(session, 10, GroupMembershipCreate(group_ids=[20]))

    assert await _active_member_ids(session, 10) == {20}
    assert await _active_member_ids(session, 20) == {30}


@pytest.mark.asyncio
async def test_reserved_group_rejected(session):
    await _seed_group(session, 10, "SDLC users")
    reserved = Principal(
        id=40,
        kind=PrincipalType.GROUP,
        name="system/authenticated",
        source=AuthProviderEnum.Local.value,
    )
    session.add(reserved)
    await session.commit()

    with pytest.raises(NotFoundException):
        await add_group_members(session, 10, GroupMembershipCreate(group_ids=[40]))


@pytest.mark.asyncio
async def test_empty_body_rejected(session):
    await _seed_group(session, 10, "SDLC users")

    with pytest.raises(InvalidException):
        await add_group_members(session, 10, GroupMembershipCreate())


# ---- list / remove members --------------------------------------------------


@pytest.mark.asyncio
async def test_list_members_renders_both_kinds(session):
    await _seed_group(session, 10, "SDLC users")
    await _seed_user(session, 5, "dev@example.com")
    await _seed_group(session, 20, "title/разработчик")
    await add_group_members(
        session, 10, GroupMembershipCreate(user_ids=[5], group_ids=[20])
    )

    members = await list_group_members(session, None, 10)
    kinds = {m.member_principal_id: m.member_kind for m in members}
    assert kinds == {5: "user", 20: "group"}
    by_id = {m.member_principal_id: m for m in members}
    assert by_id[5].username == "dev@example.com"
    assert by_id[5].user_id == 5
    assert by_id[20].username == "title/разработчик"
    assert by_id[20].user_id is None


@pytest.mark.asyncio
async def test_remove_subgroup_cuts_membership(session):
    await _seed_group(session, 10, "SDLC users")
    await _seed_group(session, 20, "title/разработчик")
    await add_group_members(session, 10, GroupMembershipCreate(group_ids=[20]))

    await remove_group_member(session, 10, 20)

    assert await _active_member_ids(session, 10) == set()
    # Soft delete keeps the audit row.
    rows = (await session.exec(PrincipalMembership.__table__.select())).all()
    assert len(rows) == 1
    assert rows[0].deleted_at is not None


@pytest.mark.asyncio
async def test_remove_unknown_member_not_found(session):
    await _seed_group(session, 10, "SDLC users")

    with pytest.raises(NotFoundException):
        await remove_group_member(session, 10, 999)


# ---- transitive rights resolution ------------------------------------------


@pytest.mark.asyncio
async def test_resolution_is_transitive(session):
    await _seed_group(session, 10, "SDLC users")
    await _seed_group(session, 20, "title/разработчик")
    await _seed_user(session, 5, "dev@example.com")
    # user 5 → title/разработчик (20) → SDLC users (10)
    session.add(PrincipalMembership(parent_principal_id=20, member_principal_id=5))
    session.add(PrincipalMembership(parent_principal_id=10, member_principal_id=20))
    await session.commit()

    ids = await _user_group_principal_ids(session, 5)
    assert set(ids) == {20, 10}


@pytest.mark.asyncio
async def test_soft_deleted_edge_cuts_transitive_membership(session):
    await _seed_group(session, 10, "SDLC users")
    await _seed_group(session, 20, "title/разработчик")
    await _seed_user(session, 5, "dev@example.com")
    edge = PrincipalMembership(parent_principal_id=10, member_principal_id=20)
    session.add(edge)
    session.add(PrincipalMembership(parent_principal_id=20, member_principal_id=5))
    await session.commit()

    ids = await _user_group_principal_ids(session, 5)
    assert 10 in ids

    edge.deleted_at = edge.updated_at
    session.add(edge)
    await session.commit()

    ids = await _user_group_principal_ids(session, 5)
    assert set(ids) == {20}


@pytest.mark.asyncio
async def test_direct_only_membership_unchanged(session):
    await _seed_group(session, 30, "solo")
    await _seed_user(session, 5, "solo@example.com")
    session.add(PrincipalMembership(parent_principal_id=30, member_principal_id=5))
    await session.commit()

    ids = await _user_group_principal_ids(session, 5)
    assert set(ids) == {30}


# ---- transitive member counts ------------------------------------------------


async def _edge(session, parent: int, member: int) -> None:
    session.add(
        PrincipalMembership(parent_principal_id=parent, member_principal_id=member)
    )
    await session.commit()


@pytest.mark.asyncio
async def test_transitive_count_counts_users_through_nesting(session):
    await _seed_group(session, 10, "top")
    await _seed_group(session, 20, "mid")
    await _seed_user(session, 5, "dev@example.com")
    # user 5 -> mid (20) -> top (10): top's size is 1, not 0 (its direct
    # rows hold only a subgroup edge).
    await _edge(session, 20, 5)
    await _edge(session, 10, 20)

    counts = await _bulk_transitive_member_counts(session, [10, 20])
    assert counts == {10: 1, 20: 1}


@pytest.mark.asyncio
async def test_transitive_count_deduplicates_diamond(session):
    await _seed_group(session, 10, "root")
    await _seed_group(session, 20, "left")
    await _seed_group(session, 30, "right")
    await _seed_user(session, 5, "dev@example.com")
    await _seed_user(session, 6, "qa@example.com")
    # dev is reachable through BOTH branches; the root counts him once.
    await _edge(session, 20, 5)
    await _edge(session, 30, 5)
    await _edge(session, 30, 6)
    await _edge(session, 10, 20)
    await _edge(session, 10, 30)

    counts = await _bulk_transitive_member_counts(session, [10, 20, 30])
    assert counts[10] == 2
    assert counts[20] == 1
    assert counts[30] == 2


@pytest.mark.asyncio
async def test_transitive_count_ignores_soft_deleted_edges(session):
    await _seed_group(session, 10, "top")
    await _seed_group(session, 20, "mid")
    await _seed_user(session, 5, "dev@example.com")
    await _edge(session, 20, 5)
    await _edge(session, 10, 20)

    edge = (
        await session.exec(
            select(PrincipalMembership).where(
                PrincipalMembership.parent_principal_id == 10
            )
        )
    ).first()
    edge.deleted_at = edge.updated_at
    session.add(edge)
    await session.commit()

    counts = await _bulk_transitive_member_counts(session, [10])
    assert counts == {}


@pytest.mark.asyncio
async def test_transitive_count_group_members_are_not_people(session):
    await _seed_group(session, 10, "top")
    await _seed_group(session, 20, "mid")
    # An empty subgroup edge: without the user-kind filter this edge
    # would inflate the count to 2.
    await _edge(session, 10, 20)

    counts = await _bulk_transitive_member_counts(session, [10])
    assert counts == {}


# ---- group type --------------------------------------------------------------


@pytest.mark.asyncio
async def test_group_type_tokens(session):
    title_group = Principal(
        id=60,
        kind=PrincipalType.GROUP,
        name="title/разработчик",
        source="oidc-attrs",
    )
    dept_group = Principal(
        id=61, kind=PrincipalType.GROUP, name="dept/713", source="oidc-attrs"
    )
    store_group = Principal(
        id=62,
        kind=PrincipalType.GROUP,
        name="store/Магазин Уфа 2",
        source="oidc-attrs",
    )
    local_group = Principal(
        id=63, kind=PrincipalType.GROUP, name="SDLC users", source="Local"
    )
    # A manually created group that borrowed the attribute naming must
    # stay Local: the type may not flip when the admin renames it.
    local_prefixed = Principal(
        id=64, kind=PrincipalType.GROUP, name="title/ impostor", source="Local"
    )
    claim_group = Principal(
        id=65, kind=PrincipalType.GROUP, name="platform-team", source="OIDC"
    )
    unknown_group = Principal(
        id=66,
        kind=PrincipalType.GROUP,
        name="unknown/users",
        source="oidc-attrs",
    )
    for group in (
        title_group,
        dept_group,
        store_group,
        local_group,
        local_prefixed,
        claim_group,
        unknown_group,
    ):
        assert (
            resolve_group_type(group)
            == {
                60: "profession",
                61: "department",
                62: "business-unit",
                63: "local",
                64: "local",
                65: "idp",
                66: "unknown",
            }[group.id]
        )


# ---- list ordering -----------------------------------------------------------


@pytest.mark.asyncio
async def test_list_default_sorts_by_member_count_desc(session):
    await _seed_group(session, 10, "small")
    await _seed_group(session, 20, "big")
    await _seed_group(session, 30, "mid")
    for uid in (5, 6, 7):
        await _seed_user(session, uid, f"u{uid}@example.com")
    await _edge(session, 30, 5)
    await _edge(session, 20, 6)
    await _edge(session, 20, 7)
    # small stays empty.

    page = await list_groups(session, None, UserGroupListParams())
    assert [g.name for g in page.items] == ["big", "mid", "small"]
    assert [g.member_count for g in page.items] == [2, 1, 0]
    assert page.pagination.total == 3


@pytest.mark.asyncio
async def test_list_explicit_member_count_sort(session):
    big = await _seed_group(session, 20, "big")
    await _seed_group(session, 10, "small")
    await _seed_user(session, 5, "u5@example.com")
    await _seed_user(session, 6, "u6@example.com")
    await _edge(session, 20, 5)
    await _edge(session, 20, 6)

    page = await list_groups(
        session, None, UserGroupListParams(sort_by="-member_count")
    )
    assert [g.name for g in page.items] == ["big", "small"]
    # Ascending flips the order.
    page = await list_groups(session, None, UserGroupListParams(sort_by="member_count"))
    assert [g.name for g in page.items] == ["small", big.name]
    assert [g.member_count for g in page.items] == [0, 2]


@pytest.mark.asyncio
async def test_list_name_sort_stays_on_sql_path(session):
    await _seed_group(session, 10, "beta")
    await _seed_group(session, 20, "alpha")
    await _seed_user(session, 5, "u5@example.com")
    await _edge(session, 10, 5)

    page = await list_groups(session, None, UserGroupListParams(sort_by="name"))
    assert [g.name for g in page.items] == ["alpha", "beta"]
    assert page.items[0].member_count == 0
    assert page.items[1].member_count == 1


@pytest.mark.asyncio
async def test_list_page_non_positive_returns_all_groups(session):
    # ``page=-1`` is the codebase's fetch-everything convention: the
    # members-drawer subgroup picker loads the full candidate list with
    # it, so the Python-paginated member-count path must honor it too
    # and not clamp to the first page.
    await _seed_group(session, 10, "beta")
    await _seed_group(session, 20, "alpha")
    await _seed_group(session, 30, "gamma")

    page = await list_groups(session, None, UserGroupListParams(page=-1, perPage=2))
    # All counts are zero here, so the name tiebreaker orders the page.
    assert [g.name for g in page.items] == ["alpha", "beta", "gamma"]
    assert page.pagination.total == 3

    page = await list_groups(session, None, UserGroupListParams(page=1, perPage=2))
    assert len(page.items) == 2
    assert page.pagination.total == 3


@pytest.mark.asyncio
async def test_get_group_reports_transitive_count(session):
    await _seed_group(session, 10, "top")
    await _seed_group(session, 20, "mid")
    await _seed_user(session, 5, "dev@example.com")
    await _edge(session, 20, 5)
    await _edge(session, 10, 20)

    group = await get_group(session, None, 10)
    assert group.member_count == 1
    assert group.group_type == "local"


# ---- group_type filter --------------------------------------------------------


async def _seed_attr_group(
    session, principal_id: int, name: str, display_name: str | None = None
) -> Principal:
    group = Principal(
        id=principal_id,
        kind=PrincipalType.GROUP,
        name=name,
        display_name=display_name,
        source="oidc-attrs",
    )
    session.add(group)
    await session.commit()
    return group


@pytest.mark.asyncio
async def test_list_filters_by_group_type(session):
    await _seed_group(session, 10, "SDLC users")  # local
    await _seed_attr_group(session, 20, "title/разработчик")
    await _seed_attr_group(session, 30, "dept/713")
    await _seed_attr_group(session, 40, "store/Магазин Уфа 2")
    await _seed_attr_group(session, 50, "unknown/users")
    session.add(
        Principal(
            id=60,
            kind=PrincipalType.GROUP,
            name="platform-team",
            source=AuthProviderEnum.OIDC.value,
        )
    )
    await session.commit()
    # A Local group borrowing the attribute naming must stay out of
    # the profession filter — the type is derived source-first.
    await _seed_group(session, 70, "title/ impostor")

    page = await list_groups(
        session, None, UserGroupListParams(), group_type="profession"
    )
    assert [g.name for g in page.items] == ["title/разработчик"]
    page = await list_groups(session, None, UserGroupListParams(), group_type="local")
    assert {g.name for g in page.items} == {"SDLC users", "title/ impostor"}
    page = await list_groups(
        session, None, UserGroupListParams(), group_type="department"
    )
    assert [g.name for g in page.items] == ["dept/713"]
    page = await list_groups(
        session, None, UserGroupListParams(), group_type="business-unit"
    )
    assert [g.name for g in page.items] == ["store/Магазин Уфа 2"]
    page = await list_groups(session, None, UserGroupListParams(), group_type="unknown")
    assert [g.name for g in page.items] == ["unknown/users"]
    page = await list_groups(session, None, UserGroupListParams(), group_type="idp")
    assert [g.name for g in page.items] == ["platform-team"]


@pytest.mark.asyncio
async def test_list_rejects_unknown_group_type(session):
    await _seed_group(session, 10, "solo")
    with pytest.raises(InvalidException):
        await list_groups(session, None, UserGroupListParams(), group_type="bogus")


# ---- IdP-synced groups are read-only on the admin surface ---------------------


@pytest.mark.asyncio
async def test_member_writes_rejected_for_idp_synced_group(session):
    await _seed_user(session, 5, "dev@example.com")
    await _seed_attr_group(session, 20, "title/разработчик")

    with pytest.raises(InvalidException):
        await add_group_members(session, 20, GroupMembershipCreate(user_ids=[5]))
    with pytest.raises(InvalidException):
        await remove_group_member(session, 20, 5)


@pytest.mark.asyncio
async def test_group_row_writes_rejected_for_idp_synced_group(session):
    await _seed_attr_group(session, 20, "title/разработчик")

    with pytest.raises(InvalidException):
        await update_group(session, 20, UserGroupUpdate(name="renamed"))
    with pytest.raises(InvalidException):
        await delete_group(session, 20)


@pytest.mark.asyncio
async def test_resolve_manageable_groups_rejects_non_local_ids(session):
    local = await _seed_group(session, 10, "Manual")
    await _seed_attr_group(session, 20, "title/разработчик")

    # Duplicates collapse; only live Local GROUP principals survive.
    resolved = await _resolve_manageable_groups(session, [10, 10])
    assert [g.id for g in resolved] == [local.id]

    # An IdP-synced group is not admin-manageable, same rule the
    # members drawer enforces.
    with pytest.raises(InvalidException):
        await _resolve_manageable_groups(session, [20])
    with pytest.raises(InvalidException):
        await _resolve_manageable_groups(session, [99])


@pytest.mark.asyncio
async def test_local_group_membership_diff_adds_and_soft_deletes(session):
    await _seed_user(session, 100, "jdoe@example.com")
    keep = await _seed_group(session, 10, "keep")
    drop = await _seed_group(session, 20, "drop")
    revive = await _seed_group(session, 30, "revive")
    # A Local-source user-in-Org row shares the source marker but is
    # out of scope: the diff only touches rows whose parent is a GROUP.
    org = Principal(
        id=40,
        kind=PrincipalType.ORG,
        name="retail-org",
        source=AuthProviderEnum.Local.value,
    )
    session.add(org)
    await session.commit()

    await _seed_membership(session, keep.id, 100)
    await _seed_membership(session, drop.id, 100)
    await _seed_membership(
        session,
        revive.id,
        100,
        deleted_at=datetime.now(timezone.utc).replace(tzinfo=None),
    )
    await _seed_membership(session, org.id, 100)

    desired = await _resolve_manageable_groups(session, [10, 30])
    await _apply_local_group_membership_diff(session, 100, desired)
    await session.commit()

    active = await _active_parent_ids_of_user(session, 100)
    assert active == {keep.id, revive.id, org.id}
    # The removal is a soft delete: the timeline row survives.
    rows = (
        await session.exec(
            PrincipalMembership.__table__.select().where(
                PrincipalMembership.__table__.c.member_principal_id == 100,
                PrincipalMembership.__table__.c.parent_principal_id == drop.id,
            )
        )
    ).all()
    assert len(rows) == 1
    assert rows[0].deleted_at is not None


# ---- GET /users/{id}/groups ---------------------------------------------------


@pytest.mark.asyncio
async def test_get_user_groups_returns_all_kinds(session, monkeypatch):
    # The user-edit modal renders Local groups as editable checkboxes
    # and IdP-synced ones read-only, so the endpoint must surface both
    # kinds (each row carries group_type) — not only admin-curated
    # ones. Rows are ordered by group name and scoped to the one user.
    user = await _seed_user(session, 100, "jdoe@example.com")
    local = await _seed_group(session, 10, "a-manual")
    attrs = await _seed_attr_group(session, 20, "title/z-dev")
    await _seed_membership(session, local.id, 100)
    await _seed_membership(session, attrs.id, 100, source="oidc-attrs")
    # A second user's membership must not leak into the response.
    await _seed_user(session, 200, "other@example.com")
    await _seed_membership(session, local.id, 200)

    from unittest.mock import AsyncMock

    from gpustack.routes import users as users_route

    monkeypatch.setattr(users_route.User, "one_by_id", AsyncMock(return_value=user))
    groups = await get_user_groups(session, 100)
    assert [g.id for g in groups] == [10, 20]
    by_id = {g.id: g for g in groups}
    assert by_id[10].group_type == "local"
    assert by_id[20].group_type == "profession"


# ---- user attribute labels -----------------------------------------------------


@pytest.mark.asyncio
async def test_bulk_user_attribute_labels(session):
    await _seed_user(session, 5, "dev@example.com")
    await _seed_user(session, 6, "bare@example.com")
    await _seed_attr_group(session, 20, "title/разработчик", "Разработчик")
    await _seed_attr_group(session, 30, "dept/713", "ПК Платформа разработки")
    # No display_name: the prefixed name is the fallback label.
    await _seed_attr_group(session, 40, "store/Магазин Уфа 2")
    # An admin-curated membership in a Local group that borrowed the
    # attribute naming must not leak into the labels.
    await _seed_group(session, 50, "title/ impostor")
    for parent, source in (
        (20, "oidc-attrs"),
        (30, "oidc-attrs"),
        (40, "oidc-attrs"),
        (50, "Local"),
    ):
        session.add(
            PrincipalMembership(
                parent_principal_id=parent, member_principal_id=5, source=source
            )
        )
    await session.commit()

    labels = await bulk_user_attribute_labels(session, [5, 6])
    assert labels[5] == {
        "profession": "Разработчик",
        "department": "ПК Платформа разработки",
        # No display_name on the store group: the prefixed name is the
        # fallback label, prefix included.
        "business_unit": "store/Магазин Уфа 2",
    }
    # The bare user holds no attribute groups at all.
    assert 6 not in labels


@pytest.mark.asyncio
async def test_member_rows_carry_attribute_labels(session):
    await _seed_group(session, 10, "SDLC users")
    await _seed_user(session, 5, "dev@example.com")
    await _seed_attr_group(session, 20, "title/разработчик", "Разработчик")
    await _seed_attr_group(session, 30, "dept/713", "ПК Платформа разработки")
    await _seed_attr_group(session, 40, "store/Магазин Уфа 2", "Магазин Уфа 2")
    await _edge(session, 10, 5)
    for parent in (20, 30, 40):
        session.add(
            PrincipalMembership(
                parent_principal_id=parent,
                member_principal_id=5,
                source="oidc-attrs",
            )
        )
    await session.commit()

    rows = await list_group_members(session, None, 10)
    assert len(rows) == 1
    assert rows[0].profession == "Разработчик"
    assert rows[0].department == "ПК Платформа разработки"
    assert rows[0].business_unit == "Магазин Уфа 2"

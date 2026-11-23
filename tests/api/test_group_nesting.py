"""Tests for nested groups (group-in-group membership).

The nesting endpoints validate kind, reservation, self-nesting and
cycle closure before writing; rights resolution (``_user_group_principal_ids``)
must see a user's groups transitively. Both run against a real
in-memory SQLite so the soft-delete and recursion semantics hold
against actual rows.
"""

from types import SimpleNamespace
from datetime import datetime, timezone

import pytest
import pytest_asyncio
from sqlalchemy import event, text
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
    update_group_limits,
)
from gpustack.routes import users as users_route
from gpustack.routes.users import (
    _apply_local_group_membership_diff,
    _resolve_manageable_groups,
    create_user,
    get_user_groups,
)
from gpustack.server.services import (
    UNKNOWN_USERS_GROUP_NAME,
    bulk_user_attribute_labels,
)
from gpustack.schemas.principals import Principal, PrincipalMembership, PrincipalType
from gpustack.schemas.user_groups import (
    UserGroupLimitsUpdate,
    UserGroupListParams,
    UserGroupUpdate,
    resolve_group_type,
)
from gpustack.schemas.user_passwords import UserPassword
from gpustack.schemas.users import AuthProviderEnum, UserCreate

from pydantic import ValidationError


@pytest_asyncio.fixture
async def session():
    engine = create_async_engine("sqlite+aiosqlite://")

    # SQLite's built-in lower() folds ASCII only, while the production
    # dialects fold the full Unicode range (PostgreSQL per locale, MySQL
    # per collation); override it so search tests exercise the same
    # case-insensitive matching users see.
    @event.listens_for(engine.sync_engine, "connect")
    def _unicode_lower(dbapi_conn, _record):
        dbapi_conn.create_function(
            "lower", 1, lambda s: s.lower() if isinstance(s, str) else s
        )

    async with engine.begin() as conn:
        await conn.run_sync(Principal.__table__.create)
        await conn.run_sync(PrincipalMembership.__table__.create)
        # create_user's response path consults the password row
        # (require_password_change), so the table must exist here too.
        await conn.run_sync(UserPassword.__table__.create)
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
async def test_list_search_matches_display_name_and_name(session):
    # The table renders display_name, whose values share nothing with
    # the prefixed names — a department is dept/713 internally and a
    # readable label on screen. The search box must match either field,
    # exactly like the users list already does; both listing paths
    # (member_count Python pagination and the SQL sort) agree.
    await _seed_group(session, 10, "SDLC users")
    await _seed_attr_group(
        session,
        30,
        "dept/713",
        "ПК Отдел разработки платформенных сервисов",
    )

    page = await list_groups(session, None, UserGroupListParams(), search="ПК Отдел")
    assert [g.name for g in page.items] == ["dept/713"]
    page = await list_groups(
        session, None, UserGroupListParams(), search="платформенных"
    )
    assert [g.name for g in page.items] == ["dept/713"]
    # The prefixed name still answers an id-style needle.
    page = await list_groups(session, None, UserGroupListParams(), search="713")
    assert [g.name for g in page.items] == ["dept/713"]
    page = await list_groups(session, None, UserGroupListParams(), search="sdlc")
    assert [g.name for g in page.items] == ["SDLC users"]
    page = await list_groups(session, None, UserGroupListParams(), search="нет такой")
    assert page.items == []

    # The SQL-sorted path (any non-member_count order) matches too.
    page = await list_groups(
        session,
        None,
        UserGroupListParams(sort_by="name"),
        search="ПК Отдел",
    )
    assert [g.name for g in page.items] == ["dept/713"]


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


# ---- create_user external_attributes ------------------------------------------


def _patch_attr_groups_flag(monkeypatch, enabled: bool) -> None:
    # The route reads the flag off the global config at call time;
    # patching the module binding keeps the per-test toggle independent
    # of the settings object the root conftest installs.
    monkeypatch.setattr(
        users_route,
        "get_global_config",
        lambda: SimpleNamespace(external_auth_attribute_groups=enabled),
    )


@pytest.mark.asyncio
async def test_create_user_with_external_attributes_syncs_groups(session, monkeypatch):
    # Login parity: an OIDC account provisioned with external_attributes
    # starts in the same attribute-derived groups (created when new)
    # the SSO callback would derive at first sign-in.
    _patch_attr_groups_flag(monkeypatch, True)
    result = await create_user(
        session,
        UserCreate(
            username="provisioned",
            source=AuthProviderEnum.OIDC.value,
            external_attributes={
                "title": "Разработчик",
                "department_id": "713",
                "department_name": "Отдел ИТ",
                "store_name": "Магазин 7",
            },
        ),
    )
    assert result.external_attributes["title"] == "Разработчик"
    assert result.profession == "Разработчик"
    assert result.department == "Отдел ИТ"
    assert result.business_unit == "Магазин 7"

    groups = (
        await session.exec(
            select(Principal).where(
                Principal.kind == PrincipalType.GROUP,
                Principal.source == "oidc-attrs",
            )
        )
    ).all()
    by_name = {g.name: g for g in groups}
    assert by_name["title/разработчик"].display_name == "Разработчик"
    assert by_name["dept/713"].display_name == "Отдел ИТ"
    assert by_name["store/Магазин 7"].display_name == "Магазин 7"

    memberships = (
        await session.exec(
            select(PrincipalMembership).where(
                PrincipalMembership.member_principal_id == result.id,
                PrincipalMembership.source == "oidc-attrs",
                PrincipalMembership.deleted_at.is_(None),
            )
        )
    ).all()
    assert {m.parent_principal_id for m in memberships} == {g.id for g in groups}


@pytest.mark.asyncio
async def test_create_user_joins_existing_attr_group(session, monkeypatch):
    # The "exists → add" half of the login-parity contract: the same
    # IdP title (in either casing) resolves to the one existing group,
    # not a duplicate.
    _patch_attr_groups_flag(monkeypatch, True)
    existing = await _seed_attr_group(session, 500, "title/аналитик", "Аналитик")
    first = await create_user(
        session,
        UserCreate(
            username="u1",
            source=AuthProviderEnum.OIDC.value,
            external_attributes={"title": "Аналитик"},
        ),
    )
    second = await create_user(
        session,
        UserCreate(
            username="u2",
            source=AuthProviderEnum.OIDC.value,
            external_attributes={"title": "аналитик"},
        ),
    )
    rows = (
        await session.exec(
            select(Principal).where(
                Principal.name == "title/аналитик",
                Principal.kind == PrincipalType.GROUP,
            )
        )
    ).all()
    assert len(rows) == 1
    assert rows[0].id == existing.id
    assert await _active_member_ids(session, existing.id) == {first.id, second.id}


@pytest.mark.asyncio
async def test_create_oidc_user_without_attributes_parks_in_unknown(
    session, monkeypatch
):
    # Same parking rule as the callback: an OIDC account whose claims
    # are unknown starts in unknown/users until real attributes land.
    _patch_attr_groups_flag(monkeypatch, True)
    result = await create_user(
        session,
        UserCreate(username="noattrs", source=AuthProviderEnum.OIDC.value),
    )
    groups = (
        await session.exec(
            select(Principal).where(Principal.kind == PrincipalType.GROUP)
        )
    ).all()
    assert [g.name for g in groups] == [UNKNOWN_USERS_GROUP_NAME]
    assert await _active_member_ids(session, groups[0].id) == {result.id}


@pytest.mark.asyncio
async def test_create_user_flag_off_stores_attributes_without_groups(
    session, monkeypatch
):
    # The flag gates the group derivation, not the attribute storage —
    # the same split as the login callback.
    _patch_attr_groups_flag(monkeypatch, False)
    result = await create_user(
        session,
        UserCreate(
            username="flagoff",
            source=AuthProviderEnum.OIDC.value,
            external_attributes={"title": "Разработчик"},
        ),
    )
    assert result.external_attributes == {"title": "Разработчик"}
    groups = (
        await session.exec(
            select(Principal).where(Principal.kind == PrincipalType.GROUP)
        )
    ).all()
    assert groups == []


@pytest.mark.asyncio
async def test_create_local_user_stores_attributes_without_sync(session, monkeypatch):
    # The oidc-attrs rows belong to the OIDC flow: a Local account
    # keeps its stored attributes but derives no groups from them.
    _patch_attr_groups_flag(monkeypatch, True)
    result = await create_user(
        session,
        UserCreate(
            username="localop",
            password="S3curePass!",
            external_attributes={"title": "Оператор"},
        ),
    )
    assert result.external_attributes == {"title": "Оператор"}
    memberships = (await session.exec(select(PrincipalMembership))).all()
    assert memberships == []


@pytest.mark.asyncio
async def test_create_outstaff_user_joins_outstaff_group(session, monkeypatch):
    # _ext accounts carry no claims; the marker maps onto the shared
    # outstaff store group instead of the unknown-users parking — the
    # same derivation the SSO callback runs at their first login.
    _patch_attr_groups_flag(monkeypatch, True)
    result = await create_user(
        session,
        UserCreate(
            username="i.ivanov_ext@lemanapro.ru",
            source=AuthProviderEnum.OIDC.value,
        ),
    )
    groups = (
        await session.exec(
            select(Principal).where(
                Principal.kind == PrincipalType.GROUP,
                Principal.source == "oidc-attrs",
            )
        )
    ).all()
    assert [g.name for g in groups] == ["store/outstaff"]
    assert groups[0].display_name == "outstaff"
    assert await _active_member_ids(session, groups[0].id) == {result.id}


@pytest.mark.asyncio
async def test_create_user_without_optional_maps_stores_sql_null(session):
    # An absent map must bind as SQL NULL, not the JSON 'null' scalar:
    # IS NULL predicates rely on the storage shape, and the raw column
    # text must not be the four-character string 'null'.
    result = await create_user(
        session,
        UserCreate(username="bare", source=AuthProviderEnum.OIDC.value),
    )
    rows = (
        await session.exec(
            text(
                'select external_attributes, model_concurrency_limits '
                f'from principals where id = {result.id}'
            )
        )
    ).all()
    assert tuple(rows[0]) == (None, None)


# ---- group-level gateway quotas ---------------------------------------------


@pytest.mark.asyncio
async def test_group_limits_set_and_returned(session):
    group = await _seed_group(session, 20, "team")
    page = await update_group_limits(
        session,
        20,
        UserGroupLimitsUpdate(
            rpm_limit=10,
            tpm_limit=1000,
            weekly_token_limit=50_000,
            monthly_token_limit=1_000_000,
            model_concurrency_limits={"glm-5.3-flash": 2},
        ),
    )
    assert page.rpm_limit == 10
    assert page.tpm_limit == 1000
    assert page.weekly_token_limit == 50_000
    assert page.monthly_token_limit == 1_000_000
    assert page.model_concurrency_limits == {"glm-5.3-flash": 2}
    # The listing carries the same values: the table columns read them.
    listed = await list_groups(session, None, UserGroupListParams())
    row = next(g for g in listed.items if g.id == 20)
    assert row.rpm_limit == 10 and row.tpm_limit == 1000
    assert group.name == "team"  # the name column stays untouched


@pytest.mark.asyncio
async def test_group_limits_full_replace_clears_with_null(session):
    # The modal submits the complete set: an explicit null lands on the
    # row and shows as a dash again.
    await _seed_group(session, 20, "team")
    await update_group_limits(
        session, 20, UserGroupLimitsUpdate(rpm_limit=10, tpm_limit=1000)
    )
    page = await update_group_limits(
        session,
        20,
        UserGroupLimitsUpdate(rpm_limit=None, tpm_limit=2000),
    )
    assert page.rpm_limit is None
    assert page.tpm_limit == 2000


@pytest.mark.asyncio
async def test_group_limits_settable_on_idp_synced_group(session):
    # Quotas are server-owned state, nothing the IdP sync owns: the row
    # writes above stay refused while the limits write goes through.
    await _seed_attr_group(session, 20, "dept/713")
    with pytest.raises(InvalidException):
        await update_group(session, 20, UserGroupUpdate(name="renamed"))
    page = await update_group_limits(session, 20, UserGroupLimitsUpdate(rpm_limit=3))
    assert page.rpm_limit == 3
    assert page.name == "dept/713"


@pytest.mark.asyncio
async def test_group_limits_validation_bounds(session):
    await _seed_group(session, 20, "team")
    # The DTO refuses out-of-bounds maps at validation time — the API
    # surface answers 422 before the route runs.
    with pytest.raises(ValidationError):
        UserGroupLimitsUpdate(model_concurrency_limits={"m": -1})
    with pytest.raises(ValidationError):
        UserGroupLimitsUpdate(model_concurrency_limits={f"m{i}": 1 for i in range(101)})


@pytest.mark.asyncio
async def test_list_sorts_by_limit_columns(session):
    # Sorting by a quota column rides the SQL path; where the unset rows
    # land relative to the set ones is the dialect's NULL ordering, so the
    # assertion pins only the order of the set values.
    await _seed_group(session, 10, "no-limit")
    await _seed_group(session, 20, "tight")
    await _seed_group(session, 30, "wide")
    await update_group_limits(session, 20, UserGroupLimitsUpdate(rpm_limit=5))
    await update_group_limits(session, 30, UserGroupLimitsUpdate(rpm_limit=100))

    page = await list_groups(session, None, UserGroupListParams(sort_by="rpm_limit"))
    names = [g.name for g in page.items]
    assert names.index("tight") < names.index("wide")
    page = await list_groups(session, None, UserGroupListParams(sort_by="-rpm_limit"))
    names = [g.name for g in page.items]
    assert names.index("wide") < names.index("tight")

"""Tests for nested groups (group-in-group membership).

The nesting endpoints validate kind, reservation, self-nesting and
cycle closure before writing; rights resolution (``_user_group_principal_ids``)
must see a user's groups transitively. Both run against a real
in-memory SQLite so the soft-delete and recursion semantics hold
against actual rows.
"""

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel.ext.asyncio.session import AsyncSession

from gpustack.api.exceptions import (
    AlreadyExistsException,
    InvalidException,
    NotFoundException,
)
from gpustack.api.tenant import _user_group_principal_ids
from gpustack.routes.user_groups import (
    GroupMembershipCreate,
    add_group_members,
    list_group_members,
    remove_group_member,
)
from gpustack.schemas.principals import Principal, PrincipalMembership, PrincipalType
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

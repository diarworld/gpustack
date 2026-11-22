"""Tests for the attribute-derived group sync.

``_attribute_group_names`` builds the prefixed group set from the
claims persisted in ``external_attributes``; the sync wrapper hooks it
into the OIDC callback behind ``external_auth_attribute_groups``. The
service-level behaviour (diff, revive, cross-source isolation) runs
against a real in-memory SQLite so the source-scoping guarantees hold
against actual rows, not mocks.
"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import create_async_engine
from sqlmodel.ext.asyncio.session import AsyncSession

from gpustack.routes.auth import (
    UNKNOWN_USERS_GROUP_DISPLAY_NAME,
    UNKNOWN_USERS_GROUP_NAME,
    _attribute_group_names,
    _attribute_group_names_for_user,
    _sync_attribute_groups_if_enabled,
)
from gpustack.schemas.principals import Principal, PrincipalMembership, PrincipalType
from gpustack.schemas.users import AuthProviderEnum
from gpustack.server.services import sync_user_group_memberships

# ---- _attribute_group_names ------------------------------------------------


def test_names_full_attribute_set():
    names, display = _attribute_group_names(
        {
            "title": "Директор магазина",
            "department_id": "050",
            "department_name": "дирекция",
            "store_name": "Магазин Уфа 2",
        }
    )
    assert names == [
        "title/директор магазина",
        "dept/050",
        "store/Магазин Уфа 2",
    ]
    assert display == {
        "title/директор магазина": "Директор магазина",
        "dept/050": "дирекция",
        "store/Магазин Уфа 2": "Магазин Уфа 2",
    }


def test_names_title_case_variants_share_one_group():
    # The group name is lower-cased so IdP case variants resolve to a
    # single group; the display label keeps the value as received.
    n1, d1 = _attribute_group_names({"title": "Разработчик"})
    n2, d2 = _attribute_group_names({"title": "разработчик"})
    assert n1 == n2 == ["title/разработчик"]
    assert d1[n1[0]] == "Разработчик"
    assert d2[n2[0]] == "разработчик"


def test_names_missing_values_skipped():
    names, display = _attribute_group_names({"title": "аналитик"})
    assert names == ["title/аналитик"]
    assert display == {"title/аналитик": "аналитик"}


def test_names_empty_attributes_yield_nothing():
    # The unknown/users parking is the user-aware wrapper's decision
    # (see _attribute_group_names_for_user), not the claims parser's.
    assert _attribute_group_names({}) == ([], {})


def test_names_whitespace_only_value_is_skipped():
    assert _attribute_group_names({"title": "   ", "store_name": ""}) == ([], {})


# ---- user-aware wrapper: the unknown/users parking ---------------------------


def _user(source: str, external_attributes=None):
    return SimpleNamespace(id=1, source=source, external_attributes=external_attributes)


def test_unknown_parking_applies_to_oidc_accounts_only():
    # An OIDC account without attributes parks in unknown/users.
    names, display = _attribute_group_names_for_user(_user(AuthProviderEnum.OIDC.value))
    assert names == [UNKNOWN_USERS_GROUP_NAME]
    assert display == {UNKNOWN_USERS_GROUP_NAME: UNKNOWN_USERS_GROUP_DISPLAY_NAME}

    # Local and SAML accounts never park: a Local account is refused
    # at the SSO callback (source mismatch), so its attributes can
    # never arrive and 'unknown' would be a lie that never heals.
    for source in (AuthProviderEnum.Local.value, AuthProviderEnum.SAML.value):
        assert _attribute_group_names_for_user(_user(source)) == ([], {})


def test_unknown_parking_skipped_when_attributes_derive_groups():
    names, display = _attribute_group_names_for_user(
        _user(AuthProviderEnum.OIDC.value, {"title": "Аналитик"})
    )
    assert names == ["title/аналитик"]
    assert display == {"title/аналитик": "Аналитик"}
    assert UNKNOWN_USERS_GROUP_NAME not in names


def test_names_dept_without_name_falls_back_to_id():
    names, display = _attribute_group_names({"department_id": "713"})
    assert names == ["dept/713"]
    assert display == {"dept/713": "713"}


# ---- sync wrapper ----------------------------------------------------------


def _queued_session(exec_results):
    """Mock AsyncSession whose successive ``exec`` calls return queued
    row lists (same shape as the helper in test_group_sync.py): the
    wrapper's sync path runs two selects — the group-name lookup and
    the owned-membership diff. ``begin_nested`` backs the SAVEPOINT
    wrapper around group inserts."""
    session = MagicMock()
    session.exec = AsyncMock(side_effect=[_result(rows) for rows in exec_results])
    session.add = MagicMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    session.flush = AsyncMock()
    nested = MagicMock()
    nested.__aenter__ = AsyncMock(return_value=None)
    nested.__aexit__ = AsyncMock(return_value=False)
    session.begin_nested = MagicMock(return_value=nested)
    return session


def _result(rows):
    result = MagicMock()
    result.all.return_value = rows
    return result


def _config(**overrides):
    defaults = SimpleNamespace(external_auth_attribute_groups=True)
    defaults.__dict__.update(overrides)
    return defaults


@pytest.mark.asyncio
async def test_wrapper_disabled_is_noop():
    session = AsyncMock()
    user = SimpleNamespace(id=7, external_attributes={"title": "аналитик"})
    await _sync_attribute_groups_if_enabled(
        session, user, {}, _config(external_auth_attribute_groups=False)
    )
    session.exec.assert_not_called()
    session.commit.assert_not_called()


@pytest.mark.asyncio
async def test_wrapper_syncs_from_persisted_attributes():
    session = _queued_session([[], []])
    user = SimpleNamespace(
        id=7,
        external_attributes={"title": "Аналитик", "department_id": "713"},
    )
    await _sync_attribute_groups_if_enabled(session, user, {}, _config())
    session.commit.assert_awaited_once()


# ---- service-level sync with attribute source (real SQLite) -----------------


@pytest_asyncio.fixture
async def engine():
    e = create_async_engine("sqlite+aiosqlite://")
    async with e.begin() as conn:
        await conn.run_sync(Principal.__table__.create)
        await conn.run_sync(PrincipalMembership.__table__.create)
    yield e
    await e.dispose()


@pytest_asyncio.fixture
async def session(engine):
    async with AsyncSession(engine, expire_on_commit=False) as s:
        yield s


async def _seed_user(session, principal_id: int, name: str) -> None:
    session.add(
        Principal(
            id=principal_id,
            kind=PrincipalType.USER,
            name=name,
            source=AuthProviderEnum.OIDC.value,
        )
    )
    await session.commit()


@pytest.mark.asyncio
async def test_sync_creates_groups_with_display_names(session):
    await _seed_user(session, 1, "u1@example.com")
    await sync_user_group_memberships(
        session,
        1,
        AuthProviderEnum.OIDC_ATTRIBUTES,
        ["title/разработчик", "dept/823"],
        {"title/разработчик": "Разработчик", "dept/823": "ПК Платформа разработки"},
    )
    await session.commit()

    rows = (await session.exec(Principal.__table__.select())).all()
    groups = {r.name: r for r in rows if r.kind == PrincipalType.GROUP}
    assert set(groups) == {"title/разработчик", "dept/823"}
    assert all(g.source == "oidc-attrs" for g in groups.values())
    assert groups["title/разработчик"].display_name == "Разработчик"
    assert groups["dept/823"].display_name == "ПК Платформа разработки"
    memberships = (await session.exec(PrincipalMembership.__table__.select())).all()
    assert {m.parent_principal_id for m in memberships} == {
        g.id for g in groups.values()
    }


@pytest.mark.asyncio
async def test_sync_recomputes_authoritatively(session):
    await _seed_user(session, 1, "u1@example.com")
    await sync_user_group_memberships(
        session, 1, AuthProviderEnum.OIDC_ATTRIBUTES, ["title/аналитик"], {}
    )
    await session.commit()
    # Attribute set changes: analyst → developer. The stale membership
    # must be soft-deleted, the new one appear; the group row itself
    # survives (groups are never deleted by the sync — a department
    # may re-hire), so exactly two principals remain.
    await sync_user_group_memberships(
        session, 1, AuthProviderEnum.OIDC_ATTRIBUTES, ["title/разработчик"], {}
    )
    await session.commit()

    memberships = (await session.exec(PrincipalMembership.__table__.select())).all()
    active = [m for m in memberships if m.deleted_at is None]
    assert len(active) == 1
    group = (
        await session.exec(
            Principal.__table__.select().where(
                Principal.__table__.c.id == active[0].parent_principal_id
            )
        )
    ).first()
    assert group.name == "title/разработчик"
    groups = [
        r
        for r in (await session.exec(Principal.__table__.select())).all()
        if r.kind == PrincipalType.GROUP
    ]
    assert {g.name for g in groups} == {"title/аналитик", "title/разработчик"}


@pytest.mark.asyncio
async def test_sync_leaves_other_sources_alone(session):
    await _seed_user(session, 1, "u1@example.com")
    # An admin-curated membership and a claim-synced one (source OIDC).
    manual_group = Principal(
        id=90,
        kind=PrincipalType.GROUP,
        name="SDLC users",
        source=AuthProviderEnum.Local.value,
    )
    claim_group = Principal(
        id=91,
        kind=PrincipalType.GROUP,
        name="platform-team",
        source=AuthProviderEnum.OIDC.value,
    )
    session.add_all([manual_group, claim_group])
    await session.commit()
    session.add_all(
        [
            PrincipalMembership(
                parent_principal_id=90, member_principal_id=1, source="Local"
            ),
            PrincipalMembership(
                parent_principal_id=91,
                member_principal_id=1,
                source=AuthProviderEnum.OIDC.value,
            ),
        ]
    )
    await session.commit()

    # Attribute sync runs with an empty desired set — it must clear
    # only its own source, never the Local or OIDC rows.
    await sync_user_group_memberships(
        session, 1, AuthProviderEnum.OIDC_ATTRIBUTES, [], None
    )
    await session.commit()
    rows = (await session.exec(PrincipalMembership.__table__.select())).all()
    active_sources = {m.source for m in rows if m.deleted_at is None}
    assert active_sources == {"Local", AuthProviderEnum.OIDC.value}


@pytest.mark.asyncio
async def test_sync_empty_attributes_clears_own_memberships(session):
    await _seed_user(session, 1, "u1@example.com")
    await sync_user_group_memberships(
        session, 1, AuthProviderEnum.OIDC_ATTRIBUTES, ["title/аналитик"], {}
    )
    await session.commit()
    await sync_user_group_memberships(
        session, 1, AuthProviderEnum.OIDC_ATTRIBUTES, [], None
    )
    await session.commit()
    rows = (await session.exec(PrincipalMembership.__table__.select())).all()
    assert all(m.deleted_at is not None for m in rows)


@pytest.mark.asyncio
async def test_unknown_users_group_moves_out_when_attributes_arrive(session):
    """A user parked in unknown/users by the backfill (attributes never
    arrived) migrates to the real attribute groups on the first login
    that carries them — the oidc-attrs diff owns both sides of the
    move, so no stale membership survives."""
    await _seed_user(session, 1, "u1@example.com")

    # Backfill pass: no attributes → unknown/users.
    names, display = _attribute_group_names_for_user(
        _user(AuthProviderEnum.OIDC.value, external_attributes=None)
    )
    await sync_user_group_memberships(session, 1, "oidc-attrs", names, display)
    await session.commit()
    rows = (await session.exec(PrincipalMembership.__table__.select())).all()
    assert [m.deleted_at for m in rows] == [None]
    unknown_group = (
        await session.exec(
            Principal.__table__.select().where(
                Principal.__table__.c.name == UNKNOWN_USERS_GROUP_NAME
            )
        )
    ).first()
    assert unknown_group is not None
    assert unknown_group.display_name == UNKNOWN_USERS_GROUP_DISPLAY_NAME

    # Login pass: attributes arrive → moved into the real groups.
    names, display = _attribute_group_names_for_user(
        _user(
            AuthProviderEnum.OIDC.value,
            {"title": "Разработчик", "department_id": "713"},
        )
    )
    await sync_user_group_memberships(session, 1, "oidc-attrs", names, display)
    await session.commit()
    rows = (await session.exec(PrincipalMembership.__table__.select())).all()
    active = [m for m in rows if m.deleted_at is None]
    assert len(active) == 2
    moved = [m for m in rows if m.deleted_at is not None]
    assert len(moved) == 1
    assert moved[0].parent_principal_id == unknown_group.id


@pytest.mark.asyncio
async def test_attributes_vanishing_returns_user_to_unknown_group(session):
    await _seed_user(session, 1, "u1@example.com")
    names, display = _attribute_group_names_for_user(
        _user(AuthProviderEnum.OIDC.value, {"title": "аналитик"})
    )
    await sync_user_group_memberships(session, 1, "oidc-attrs", names, display)
    await session.commit()

    # The IdP stops sending attributes: the fallback group is the
    # desired state, exactly like any other attribute change.
    names, display = _attribute_group_names_for_user(_user(AuthProviderEnum.OIDC.value))
    await sync_user_group_memberships(session, 1, "oidc-attrs", names, display)
    await session.commit()
    rows = (await session.exec(PrincipalMembership.__table__.select())).all()
    active = [m for m in rows if m.deleted_at is None]
    assert len(active) == 1
    group = (
        await session.exec(
            Principal.__table__.select().where(
                Principal.__table__.c.id == active[0].parent_principal_id
            )
        )
    ).first()
    assert group.name == UNKNOWN_USERS_GROUP_NAME

"""``GET /usage/user-attribute-options`` — the overview tables' attribute
picker options, and its platform-wide admin gate."""

from types import SimpleNamespace

import pytest

from gpustack.api.exceptions import ForbiddenException
from gpustack.routes import usage


def _user(is_admin: bool):
    return SimpleNamespace(is_admin=is_admin)


def _ctx(current_principal_id=None):
    return SimpleNamespace(current_principal_id=current_principal_id)


@pytest.mark.asyncio
async def test_options_are_the_sorted_label_lists(monkeypatch):
    async def _options(session):
        return {
            "business_unit": ["Магазин 2", "Магазин 1"],
            "department": [],
            "profession": ["Инженер"],
        }

    monkeypatch.setattr(usage, "user_attribute_label_options", _options)

    # The endpoint passes the lists through — sorting is the service's
    # contract, asserted in tests/server/test_user_attribute_labels.py.
    response = await usage.get_user_attribute_options(
        session=None, user=_user(is_admin=True), ctx=_ctx(None)
    )
    assert response.business_units == ["Магазин 2", "Магазин 1"]
    assert response.departments == []
    assert response.professions == ["Инженер"]


@pytest.mark.asyncio
async def test_options_require_platform_wide_admin():
    """The label lists enumerate the whole Org structure, so a plain user
    is refused outright."""
    with pytest.raises(ForbiddenException):
        await usage.get_user_attribute_options(
            session=None, user=_user(is_admin=False), ctx=_ctx(None)
        )


@pytest.mark.asyncio
async def test_options_refuse_an_org_pinned_admin():
    """An Org-pinned admin is not the cross-tenant view the pickers sit
    on — same gate the cross-Org tables apply."""
    with pytest.raises(ForbiddenException):
        await usage.get_user_attribute_options(
            session=None, user=_user(is_admin=True), ctx=_ctx(10)
        )

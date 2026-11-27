"""The user-attribute label helpers the overview tables' filters run:
the pickers' options, and the label → user-id resolution — the
filter-side mirror of the tooltip hydration in
``bulk_user_attribute_labels``."""

import re
from types import SimpleNamespace

import pytest

from gpustack.server import services

# (member id, group name, display name) — the membership rows the
# attribute queries read. ``curated/group`` carries no known kind prefix
# and must stay out of every kind; ``dept/other`` has no display name and
# labels by its raw name.
ROWS = [
    (1, "store/101", "Магазин 101"),
    (1, "dept/platform", "Платформа"),
    (1, "title/engineer", "Инженер"),
    (2, "store/101", "Магазин 101"),
    (2, "dept/other", None),
    (3, "store/202", "Магазин 202"),
    (4, "curated/group", "Admin-curated"),
]


class MembershipSession:
    """Answers the attribute-membership queries with the fixed row set;
    the kind's LIKE prefix is honored the way the real database would."""

    def __init__(self, rows):
        self._rows = rows

    async def exec(self, statement):
        text = str(statement.compile(compile_kwargs={"literal_binds": True}))
        rows = self._rows
        match = re.search(r"principals\.name LIKE '([^']*)'", text)
        if match:
            prefix = match.group(1).rstrip("%")
            rows = [row for row in rows if row[1].startswith(prefix)]
        # The resolver selects (member id, name, display name); the
        # options query selects (name, display name) only.
        if "member_principal_id" not in text:
            rows = [(row[1], row[2]) for row in rows]
        return SimpleNamespace(all=lambda: rows)


@pytest.mark.asyncio
async def test_user_ids_with_attribute_label_matches_the_display_label():
    session = MembershipSession(ROWS)
    assert await services.user_ids_with_attribute_label(
        session, "business_unit", "Магазин 101"
    ) == {1, 2}


@pytest.mark.asyncio
async def test_user_ids_with_attribute_label_falls_back_to_the_name():
    session = MembershipSession(ROWS)
    assert await services.user_ids_with_attribute_label(
        session, "department", "dept/other"
    ) == {2}


@pytest.mark.asyncio
async def test_user_ids_with_attribute_label_unknown_kind_matches_nobody():
    assert (
        await services.user_ids_with_attribute_label(MembershipSession(ROWS), "x", "x")
        == set()
    )


@pytest.mark.asyncio
async def test_resolve_user_attribute_filter_intersects_the_labels():
    """Several provided labels AND together: the user must hold them all."""
    ids = await services.resolve_user_attribute_filter(
        MembershipSession(ROWS), business_unit="Магазин 101", profession="Инженер"
    )
    assert ids == {1}


@pytest.mark.asyncio
async def test_resolve_user_attribute_filter_without_labels_is_none():
    """No attribute param means no filter — ``None``, not an empty set."""
    assert await services.resolve_user_attribute_filter(MembershipSession(ROWS)) is None


@pytest.mark.asyncio
async def test_resolve_user_attribute_filter_no_match_is_empty():
    ids = await services.resolve_user_attribute_filter(
        MembershipSession(ROWS), profession="Нет такого"
    )
    assert ids == set()


@pytest.mark.asyncio
async def test_user_attribute_label_options_lists_each_kind_sorted():
    options = await services.user_attribute_label_options(MembershipSession(ROWS))
    assert set(options) == {"business_unit", "department", "profession"}
    assert options["business_unit"] == sorted(["Магазин 101", "Магазин 202"])
    assert options["department"] == sorted(["Платформа", "dept/other"])
    assert options["profession"] == ["Инженер"]

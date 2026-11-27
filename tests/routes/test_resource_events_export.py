"""The ``/resource-events/export`` endpoints: one file for the events list.

Offline by construction: the session fake answers the COUNT and the
chunked page selects by parsing the compiled statements, the way
``test_resource_usage_export.py`` fakes the breakdown export's cursor —
so the tests exercise the real predicate building, hydration calls and
delivery verdicts, not a mocked statement.
"""

import csv
import io
import re
import zipfile
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from gpustack import envs
from gpustack.api.exceptions import InvalidException
from gpustack.routes.resource_usage import (
    ResourceEventsExportRequest,
    estimate_resource_events_export,
    export_resource_events,
    resource_events_export_column_keys,
)
from gpustack.schemas.users import User

FIXED_NOW = datetime(2026, 11, 19, 12, 0, 0, tzinfo=timezone.utc)


def _ctx_for(user):
    ctx = MagicMock()
    ctx.user = user
    ctx.is_platform_admin = bool(getattr(user, "is_admin", False))
    ctx.current_principal_id = None if ctx.is_platform_admin else 1
    ctx.org_role = None
    ctx.current_is_personal_scope = False
    return ctx


def _event(
    id,
    at,
    *,
    creator_id=1,
    creator_name="olga",
    event_type="limit_rate_tokens",
    resource_type="limit",
    resource_name=None,
    event_message="rpm tripped",
):
    """One ``resource_events`` row with the fields the export reads."""
    return SimpleNamespace(
        id=id,
        occurred_at=at,
        creator_id=creator_id,
        creator_name=creator_name,
        resource_type=resource_type,
        resource_id=None,
        resource_name=resource_name,
        event_type=event_type,
        event_message=event_message,
        phase=None,
        spec_snapshot=None,
    )


class _EventsSession:
    """Answers the export's two statement families: the COUNT over the
    predicate and the chunked page selects. Filters are read off the
    compiled SQL — the same reading the real database would do — so a
    scope downgrade or a filter change is visible to the test."""

    def __init__(self, rows):
        self._rows = sorted(rows, key=lambda r: (r.occurred_at, r.id), reverse=True)

    async def exec(self, statement):
        try:
            text = str(statement.compile(compile_kwargs={"literal_binds": True}))
        except Exception:
            text = str(statement)
        if "count(" in text:
            return SimpleNamespace(first=lambda: len(self._matching(text)))
        offset = int(re.search(r"OFFSET (\d+)", text).group(1))
        limit = int(re.search(r"LIMIT (\d+)", text).group(1))
        page = self._matching(text)[offset : offset + limit]
        return SimpleNamespace(all=lambda: page)

    def _matching(self, text):
        matched = self._rows
        # The self-scope downgrade narrows to the caller's own events.
        owner = re.search(r"creator_id = (\d+)", text)
        if owner:
            matched = [r for r in matched if r.creator_id == int(owner.group(1))]
        kinds = re.search(r"event_type IN \(([^)]*)\)", text)
        if kinds:
            wanted = set(re.findall(r"'([^']*)'", kinds.group(1)))
            matched = [r for r in matched if r.event_type in wanted]
        # The attribute filters arrive as the creator-id IN the resolver
        # produced; an empty IN renders as a false predicate.
        if "1 != 1" in text:
            return []
        creators = re.search(r"creator_id IN \(([\d, ]+)\)", text)
        if creators:
            wanted = {int(v) for v in creators.group(1).split(",") if v.strip()}
            matched = [r for r in matched if r.creator_id in wanted]
        name = re.search(r"LIKE '(%[^']*%)'", text)
        if name:
            needle = name.group(1).strip("%").lower()
            matched = [
                r
                for r in matched
                if r.resource_name and needle in r.resource_name.lower()
            ]
        return matched


async def _collect(response) -> bytes:
    chunks = []
    async for chunk in response.body_iterator:
        chunks.append(chunk if isinstance(chunk, bytes) else chunk.encode("utf-8"))
    return b"".join(chunks)


def _csv_rows(text: str):
    """(header, data rows, trailer) of one streamed CSV export — the
    trailer comment line is part of the file, not one of its rows."""
    lines = text.strip().splitlines()
    header = next(csv.reader([lines[0]]))
    return header, list(csv.reader(lines[1:-1])), lines[-1]


@pytest.fixture(autouse=True)
def _hydration(monkeypatch):
    """The creators' resolved tooltip fields, the way the directory would
    answer a bulk read."""
    labels = {1: {"business_unit": "BU 1", "department": "Dep 1"}}

    async def _labels(session, ids):
        return {uid: labels[uid] for uid in ids if uid in labels}

    async def _names(session, ids):
        return {uid: f"User {uid}" for uid in ids}

    monkeypatch.setattr(
        "gpustack.routes.resource_usage.bulk_user_attribute_labels", _labels
    )
    monkeypatch.setattr(
        "gpustack.routes.resource_usage._principal_display_names", _names
    )


def _rows():
    return [
        _event(
            1,
            FIXED_NOW - timedelta(days=2),
            creator_id=2,
            creator_name="pavel",
            event_message="quota hit",
        ),
        _event(
            2,
            FIXED_NOW - timedelta(days=1),
            resource_name="volume-77",
            event_type="created",
            resource_type="persistent_volume",
        ),
        _event(3, FIXED_NOW, creator_id=2, creator_name="pavel"),
    ]


@pytest.mark.asyncio
async def test_events_export_is_the_list_plus_the_tooltip():
    """The file serves the list's rows newest-first and spells the name
    column's tooltip out as columns — creator login, full name and the
    attribute-derived org labels."""
    response = await export_resource_events(
        session=_EventsSession(_rows()),
        user=User(id=1, name="admin", is_admin=True),
        ctx=_ctx_for(User(id=1, name="admin", is_admin=True)),
        request=ResourceEventsExportRequest(format="csv"),
    )
    header, data, trailer = _csv_rows((await _collect(response)).decode("utf-8-sig"))

    assert response.media_type.startswith("text/csv")
    assert header == [
        "Time",
        "Resource Type",
        "Resource",
        "Event",
        "Message",
        "Phase",
        "Phase Message",
        "User ID",
        "User",
        "Full Name",
        "Business Unit",
        "Department",
        "Profession",
    ]
    # Newest first: the User ID column reads pavel, olga, pavel.
    assert [row[7] for row in data] == ["2", "1", "2"]
    created = data[1]
    assert created[1] == "persistent_volume"
    assert created[2] == "volume-77"
    assert created[3] == "created"
    assert created[7] == "1"
    assert created[8] == "olga"
    assert created[9] == "User 1"
    # A creator the labels read knows nothing about leaves the cells empty.
    tripped = data[0]
    assert tripped[8] == "pavel"
    assert tripped[9] == "User 2"
    assert tripped[10] == ""
    assert trailer.startswith("# rows=3 scope=all organization=*")


@pytest.mark.asyncio
async def test_events_export_self_scope_downgrades_the_rows():
    """A member's ``all`` is answered with their own events — and the
    trailer says so, so the reader of the file can tell."""
    caller = User(id=2, name="pavel")
    response = await export_resource_events(
        session=_EventsSession(_rows()),
        user=caller,
        ctx=_ctx_for(caller),
        request=ResourceEventsExportRequest(format="csv"),
    )
    _header, data, trailer = _csv_rows((await _collect(response)).decode("utf-8-sig"))
    # Pavel's two rows, newest first; olga's never leave the predicate.
    assert [row[4] for row in data] == ["rpm tripped", "quota hit"]
    assert all(row[7] == "2" for row in data)
    assert trailer.startswith("# rows=2 scope=self organization=1")


@pytest.mark.asyncio
async def test_events_export_honors_the_type_and_name_filters():
    """The file runs the list's predicate: the event-type set and the
    fuzzy resource-name match narrow it exactly as they narrow the page."""
    response = await export_resource_events(
        session=_EventsSession(_rows()),
        user=User(id=1, name="admin", is_admin=True),
        ctx=_ctx_for(User(id=1, name="admin", is_admin=True)),
        request=ResourceEventsExportRequest(
            format="csv",
            event_types=["created"],
            resource_name="volume-77",
        ),
    )
    _header, data, trailer = _csv_rows((await _collect(response)).decode("utf-8-sig"))
    assert len(data) == 1
    assert data[0][3] == "created"
    assert trailer.startswith("# rows=1 ")


@pytest.mark.asyncio
async def test_events_export_applies_attribute_filters(monkeypatch):
    """The Business Unit / Department / Profession labels narrow the file
    to the creators they resolve to, ANDed with the picker's creator
    filter; a label nobody holds streams an empty file."""
    rows = _rows()

    async def _resolve(session, business_unit=None, department=None, profession=None):
        assert business_unit == "BU 1"
        return [2]

    monkeypatch.setattr(
        "gpustack.routes.resource_usage.resolve_user_attribute_filter", _resolve
    )
    response = await export_resource_events(
        session=_EventsSession(rows),
        user=User(id=1, name="admin", is_admin=True),
        ctx=_ctx_for(User(id=1, name="admin", is_admin=True)),
        request=ResourceEventsExportRequest(format="csv", business_unit="BU 1"),
    )
    _header, data, trailer = _csv_rows((await _collect(response)).decode("utf-8-sig"))
    # Only creator 2's events survive the attribute narrow.
    assert [row[7] for row in data] == ["2", "2"]
    assert trailer.startswith("# rows=2 ")

    async def _nobody(session, business_unit=None, department=None, profession=None):
        return []

    monkeypatch.setattr(
        "gpustack.routes.resource_usage.resolve_user_attribute_filter", _nobody
    )
    empty = await export_resource_events(
        session=_EventsSession(rows),
        user=User(id=1, name="admin", is_admin=True),
        ctx=_ctx_for(User(id=1, name="admin", is_admin=True)),
        request=ResourceEventsExportRequest(format="csv", business_unit="Нет такого"),
    )
    _header, empty_rows, empty_trailer = _csv_rows(
        (await _collect(empty)).decode("utf-8-sig")
    )
    assert empty_rows == []
    assert empty_trailer.startswith("# rows=0 ")


@pytest.mark.asyncio
async def test_events_export_defaults_to_a_workbook():
    """xlsx is the default format — the same default every export keeps."""
    response = await export_resource_events(
        session=_EventsSession(_rows()),
        user=User(id=1, name="admin", is_admin=True),
        ctx=_ctx_for(User(id=1, name="admin", is_admin=True)),
        request=ResourceEventsExportRequest(),
    )
    assert response.media_type == (
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
    assert response.headers["content-disposition"].endswith('.xlsx"') or (
        "filename*=UTF-8''" in response.headers["content-disposition"]
        and ".xlsx" in response.headers["content-disposition"]
    )


@pytest.mark.asyncio
async def test_events_export_over_limit_refuses_with_details(monkeypatch):
    """An over-limit result is refused before a byte streams, and the
    details carry the split remedy — the only one a table without a date
    range can follow."""
    monkeypatch.setattr(envs, "USAGE_EXPORT_MAX_ROWS", 2)
    with pytest.raises(InvalidException) as excinfo:
        await export_resource_events(
            session=_EventsSession(_rows()),
            user=User(id=1, name="admin", is_admin=True),
            ctx=_ctx_for(User(id=1, name="admin", is_admin=True)),
            request=ResourceEventsExportRequest(),
        )
    details = excinfo.value.details
    assert details["kind"] == "export_too_large"
    assert details["total"] == 3
    assert details["limit"] == 2
    # No date range to shorten, so the split is the whole remedy list.
    assert details["suggestions"] == [{"action": "split_export", "parts": 2}]


@pytest.mark.asyncio
async def test_events_export_split_produces_row_sliced_parts(monkeypatch):
    """``split='auto'`` answers an over-limit query with one archive of
    row slices — the same packaging the breakdown exports use."""
    monkeypatch.setattr(envs, "USAGE_EXPORT_MAX_ROWS", 2)
    response = await export_resource_events(
        session=_EventsSession(_rows()),
        user=User(id=1, name="admin", is_admin=True),
        ctx=_ctx_for(User(id=1, name="admin", is_admin=True)),
        request=ResourceEventsExportRequest(split="auto"),
    )
    archive = zipfile.ZipFile(io.BytesIO(await _collect(response)))
    members = sorted(archive.namelist())
    assert members == [
        "resource-events_by_events_part-1-of-2.csv",
        "resource-events_by_events_part-2-of-2.csv",
    ]
    first = archive.read(members[0]).decode("utf-8-sig").strip().splitlines()
    second = archive.read(members[1]).decode("utf-8-sig").strip().splitlines()
    # 3 rows over 2 parts: the newest two, then the remainder — slices of
    # the newest-first row stream.
    assert len(first) - 2 == 2  # header + trailer excluded
    assert len(second) - 2 == 1


@pytest.mark.asyncio
async def test_events_export_estimate_sizes_the_predicate():
    """The preflight sizes the same predicate the file streams, and its
    columns are the file's schema."""
    result = await estimate_resource_events_export(
        session=_EventsSession(_rows()),
        user=User(id=1, name="admin", is_admin=True),
        ctx=_ctx_for(User(id=1, name="admin", is_admin=True)),
        request=ResourceEventsExportRequest(),
    )
    assert result.total == 3
    assert result.sheets[0].total == 3
    assert result.exceeds_hard_limit is False
    assert [column.key for column in result.sheets[0].columns] == (
        resource_events_export_column_keys()
    )

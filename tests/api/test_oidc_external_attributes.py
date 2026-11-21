"""OIDC login: configurable authorization scopes and IdP claim capture.

``_oidc_scope_param`` decides which scopes the login redirect asks the
IdP for; ``_save_oidc_external_attributes`` is the hook that persists
the claims named by ``external_auth_claims`` onto the user row so
automation (e.g. quota assignment by job title / org division) can read
them without touching the IdP again.
"""

import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gpustack.routes.auth import (
    _oidc_scope_param,
    _save_oidc_external_attributes,
    oidc_callback,
    oidc_login,
)
from gpustack.schemas.users import AuthProviderEnum

STATE_COOKIE = "gpustack_oidc_state"


def _config(**overrides):
    defaults = dict(
        oidc_client_id="client-id",
        oidc_client_secret="client-secret",
        oidc_redirect_uri="https://gpustack.example.com/auth/oidc/callback",
        openid_configuration={
            "authorization_endpoint": "https://issuer.example.com/auth",
            "token_endpoint": "https://issuer.example.com/token",
        },
        external_auth_name="email",
        external_auth_full_name=None,
        external_auth_avatar_url=None,
        external_auth_claims=None,
        external_auth_default_inactive=False,
        external_auth_insecure_skip_tls_verify=False,
        external_auth_group_sync=False,
        external_auth_groups=None,
        oidc_scopes=None,
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _request(config, cookies=None, query=None):
    return SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(server_config=config)),
        cookies=cookies or {},
        query_params=query or {},
        url=SimpleNamespace(scheme="https"),
    )


class FakeSession:
    def __init__(self):
        self.commits = 0

    def add(self, obj):
        pass

    async def commit(self):
        self.commits += 1


class StubUser:
    def __init__(self, external_attributes=None):
        self.name = "user@example.com"
        self.is_active = True
        self.source = AuthProviderEnum.OIDC
        self.external_attributes = external_attributes


class FakeAsyncClient:
    def __init__(self, **kwargs):
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    async def request(self, method, url, data=None):
        return type(
            "Resp",
            (),
            {"status_code": 200, "text": '{"access_token":"token","id_token":"id"}'},
        )()


# ---- _oidc_scope_param ------------------------------------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        (None, "openid profile email"),
        ("", "openid profile email"),
        ("   ", "openid"),
        ("openid profile email", "openid profile email"),
        ("openid,profile,email,title", "openid profile email title"),
        ("  openid   profile  ", "openid profile"),
        # ``openid`` is forced in: the code flow is undefined without it.
        ("profile email", "openid profile email"),
    ],
)
def test_oidc_scope_param(raw, expected):
    assert _oidc_scope_param(_config(oidc_scopes=raw)) == expected


@pytest.mark.asyncio
async def test_oidc_login_requests_configured_scopes(monkeypatch):
    config = _config(oidc_scopes="openid profile email org_id")
    request = _request(config, query={})

    response = await oidc_login(request)

    assert "scope=openid+profile+email+org_id" in response.headers["location"]


# ---- claim capture ----------------------------------------------------------


def _stub_callback(monkeypatch, user, user_data):
    monkeypatch.setattr("gpustack.routes.auth.httpx.AsyncClient", FakeAsyncClient)
    monkeypatch.setattr("gpustack.routes.auth.use_proxy_env_for_url", lambda url: False)
    monkeypatch.setattr(
        "gpustack.routes.auth.get_oidc_user_data", AsyncMock(return_value=user_data)
    )
    monkeypatch.setattr(
        "gpustack.routes.auth.User.first_by_fields", AsyncMock(return_value=user)
    )
    request = _request(
        _config(),
        cookies={STATE_COOKIE: "test-state"},
        query={"code": "auth-code", "state": "test-state"},
    )
    request.app.state.jwt_manager = SimpleNamespace(
        create_jwt_token=lambda username: "jwt-token"
    )
    return request


async def _run_callback(monkeypatch, user, user_data, **config_overrides):
    session = FakeSession()
    request = _stub_callback(monkeypatch, user, user_data)
    request.app.state.server_config = _config(**config_overrides)

    response = await oidc_callback(request=request, session=session)
    assert response.status_code == 307, "callback failed and hit the error redirect"
    return session


USER_DATA = {
    "email": "user@example.com",
    "title": "Senior Engineer",
    "org_id": "12345",
    "division": ["Retail", "IT"],
    "cost_center": "CC-42",
}


@pytest.mark.asyncio
async def test_callback_persists_configured_claims(monkeypatch):
    user = StubUser()
    session = await _run_callback(
        monkeypatch,
        user,
        USER_DATA,
        external_auth_claims="title,org_id,division,cost_center",
    )

    assert user.external_attributes == {
        "title": "Senior Engineer",
        "org_id": "12345",
        "division": ["Retail", "IT"],
        "cost_center": "CC-42",
    }
    assert session.commits == 1


@pytest.mark.asyncio
async def test_callback_updates_changed_attributes(monkeypatch):
    user = StubUser(external_attributes={"title": "Engineer", "org_id": "1"})
    session = await _run_callback(
        monkeypatch,
        user,
        USER_DATA,
        external_auth_claims="title,org_id",
    )

    # The IdP is authoritative: a promotion or transfer must land on the
    # row at the next login, not stay stale.
    assert user.external_attributes == {"title": "Senior Engineer", "org_id": "12345"}
    assert session.commits == 1


@pytest.mark.asyncio
async def test_callback_skips_write_when_unchanged(monkeypatch):
    user = StubUser(external_attributes={"title": "Senior Engineer", "org_id": "12345"})
    session = await _run_callback(
        monkeypatch,
        user,
        USER_DATA,
        external_auth_claims="title,org_id",
    )

    assert user.external_attributes == {"title": "Senior Engineer", "org_id": "12345"}
    assert session.commits == 0


@pytest.mark.asyncio
async def test_callback_clears_attributes_when_claims_missing(monkeypatch, caplog):
    user = StubUser(external_attributes={"title": "Senior Engineer"})
    session = await _run_callback(
        monkeypatch,
        user,
        {"email": "user@example.com"},
        external_auth_claims="title,org_id",
    )

    assert user.external_attributes == {}
    assert session.commits == 1
    assert any(
        "external_auth_claims" in record.getMessage()
        and record.levelno == logging.WARNING
        for record in caplog.records
    )


@pytest.mark.asyncio
async def test_callback_ignores_attributes_when_not_configured(monkeypatch):
    user = StubUser()
    session = await _run_callback(monkeypatch, user, USER_DATA)

    assert user.external_attributes is None
    assert session.commits == 0


@pytest.mark.asyncio
async def test_callback_logs_available_claim_names(monkeypatch, caplog):
    # Without IdP console access, the debug line listing the claim names
    # the IdP actually sent is how a missing claim gets diagnosed.
    user = StubUser()
    with caplog.at_level(logging.DEBUG, logger="gpustack.routes.auth"):
        await _run_callback(
            monkeypatch,
            user,
            USER_DATA,
            external_auth_claims="title,store_name",
        )

    debug_lines = [
        record.getMessage()
        for record in caplog.records
        if record.levelno == logging.DEBUG
        and "OIDC user data claims" in record.getMessage()
    ]
    assert any(
        "'title'" in line and "'org_id'" in line and "'store_name'" not in line
        for line in debug_lines
    )


# ---- _save_oidc_external_attributes (unit) ----------------------------------


@pytest.mark.asyncio
async def test_save_attributes_tolerates_whitespace_in_config():
    user = StubUser()
    session = FakeSession()

    await _save_oidc_external_attributes(
        session,
        user,
        {"title": "Engineer", "org_id": "1"},
        _config(external_auth_claims=" title , org_id ,"),
    )

    assert user.external_attributes == {"title": "Engineer", "org_id": "1"}

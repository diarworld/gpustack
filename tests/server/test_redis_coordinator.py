"""RedisCoordinator: leader election, fail-stop, and cross-instance delivery.

All tests run against a shared in-memory fakeredis server, so no network or
real Redis is involved. The behaviors pinned here are the ones the election
loop in ``Server`` depends on: exclusive acquisition, owner-checked renewal
(CAS), fail-stop when Redis is unreachable beyond the lease TTL, and bus
delivery that reaches every subscriber including the publisher.
"""

import asyncio
import time
from types import SimpleNamespace

import pytest
from fakeredis import FakeServer
from fakeredis.aioredis import FakeRedis
from fastapi import FastAPI

from gpustack.extension import Plugin, PluginContext
from gpustack.server import bus as bus_module
from gpustack.server import cache as cache_module
from gpustack.server.bus import Event, EventType
from gpustack.server.coordinator.local import LocalCoordinator
from gpustack.server.coordinator.redis import RedisCoordinator
from gpustack.server.server import Server


def _config(server_id):
    return SimpleNamespace(
        server_id=server_id,
        namespace="tests",
        coordinator_redis_url="redis://tests.invalid/0",
    )


def _coordinator(server_id, redis_server):
    client = FakeRedis(server=redis_server, decode_responses=True)
    return RedisCoordinator(_config(server_id), client=client)


@pytest.mark.asyncio
async def test_leadership_is_exclusive():
    redis_server = FakeServer()
    first = _coordinator("first", redis_server)
    second = _coordinator("second", redis_server)

    assert await first.acquire_leadership(30) is True
    assert await second.acquire_leadership(30) is False
    assert first.is_leader() and not second.is_leader()


@pytest.mark.asyncio
async def test_renewal_only_by_owner():
    redis_server = FakeServer()
    first = _coordinator("first", redis_server)
    second = _coordinator("second", redis_server)

    assert await first.acquire_leadership(30) is True
    assert await first.renew_leadership(30) is True
    # A standby never held the lease; renewal must not start it.
    assert await second.renew_leadership(30) is False

    # Lease taken by another instance: the previous holder must fail. The
    # stale leader still believes it leads — the compare-and-set renewal
    # must refuse and clear the flag.
    await first.release_leadership()
    assert await second.acquire_leadership(30) is True
    first._set_leader(True)
    assert await first.renew_leadership(30) is False
    assert not first.is_leader()


@pytest.mark.asyncio
async def test_reacquire_adopts_own_lease():
    redis_server = FakeServer()
    first = _coordinator("first", redis_server)

    assert await first.acquire_leadership(30) is True
    # A renew that raced a reconnect can find the key still ours: the
    # acquisition path must adopt and refresh it, not report a loss.
    assert await first.acquire_leadership(30) is True
    assert first.is_leader()


@pytest.mark.asyncio
async def test_release_lets_standby_take_over():
    redis_server = FakeServer()
    first = _coordinator("first", redis_server)
    second = _coordinator("second", redis_server)

    assert await first.acquire_leadership(30) is True
    await first.release_leadership()
    assert not first.is_leader()
    assert await second.acquire_leadership(30) is True


@pytest.mark.asyncio
async def test_expired_lease_is_not_renewed():
    redis_server = FakeServer()
    first = _coordinator("first", redis_server)

    assert await first.acquire_leadership(30) is True
    # The lease is gone (expired and swept, or Redis restarted empty): a
    # stale leader must report the loss rather than renew into a claim it
    # can no longer verify.
    client = FakeRedis(server=redis_server, decode_responses=True)
    await client.delete("gpustack:coordinator:tests:leader")
    assert await first.renew_leadership(30) is False
    assert not first.is_leader()


class _FailingEvalClient:
    """Delegates everything except eval, which simulates Redis being down."""

    def __init__(self, inner):
        self._inner = inner

    def __getattr__(self, name):
        return getattr(self._inner, name)

    async def eval(self, *args, **kwargs):
        raise ConnectionError("redis unreachable")


@pytest.mark.asyncio
async def test_unreachable_redis_beyond_ttl_deposes_leader():
    redis_server = FakeServer()
    first = _coordinator("first", redis_server)

    assert await first.acquire_leadership(30) is True
    first._client = _FailingEvalClient(first._client)
    # Outage outlasts the lease: the key may have expired and been taken
    # over, so renewal must report the loss (the election loop then exits
    # the process) instead of claiming a renewal it cannot verify.
    first._last_contact = time.monotonic() - 31
    assert await first.renew_leadership(30) is False
    assert not first.is_leader()


@pytest.mark.asyncio
async def test_unreachable_redis_within_ttl_keeps_leader():
    redis_server = FakeServer()
    first = _coordinator("first", redis_server)

    assert await first.acquire_leadership(30) is True
    first._client = _FailingEvalClient(first._client)
    # Short outage: the lease key still exists and nobody can take it, so
    # the leader holds until the TTL actually runs out.
    first._last_contact = time.monotonic() - 5
    assert await first.renew_leadership(30) is True
    assert first.is_leader()


@pytest.mark.asyncio
async def test_events_reach_every_subscriber_including_publisher():
    redis_server = FakeServer()
    first = _coordinator("first", redis_server)
    second = _coordinator("second", redis_server)
    await first.start()
    await second.start()

    received_first, received_second = [], []
    gate_first, gate_second = asyncio.Event(), asyncio.Event()

    def on_first(event):
        received_first.append(event)
        gate_first.set()

    def on_second(event):
        received_second.append(event)
        gate_second.set()

    first.subscribe("model", on_first)
    second.subscribe("model", on_second)

    await first.publish("model", Event(type=EventType.UPDATED, data={"id": 7}))

    await asyncio.wait_for(gate_first.wait(), timeout=5)
    await asyncio.wait_for(gate_second.wait(), timeout=5)

    # Loopback: the bus routes exclusively through the coordinator, so the
    # publisher must see its own event again.
    assert received_first[0].id == 7
    # Cross-instance: the id-only wire shape, enriched by the receiving bus.
    assert received_second[0].id == 7
    assert received_second[0].data == {"id": 7}

    await first.stop()
    await second.stop()


@pytest.mark.asyncio
async def test_cache_channel_payloads_survive_the_wire():
    """Cache invalidation carries a key, not an id; the wire must keep it."""
    redis_server = FakeServer()
    first = _coordinator("first", redis_server)
    second = _coordinator("second", redis_server)
    await first.start()
    await second.start()

    keys, gate = [], asyncio.Event()

    def on_cache(event):
        keys.append(event.data.get("key"))
        gate.set()

    second.subscribe("cache", on_cache)

    await first.publish(
        "cache", Event(type=EventType.DELETED, data={"key": "auth:abc"})
    )
    await asyncio.wait_for(gate.wait(), timeout=5)
    assert keys == ["auth:abc"]

    await first.stop()
    await second.stop()


class _UnreachableClient:
    async def ping(self):
        raise ConnectionError("redis unreachable")


@pytest.mark.asyncio
async def test_start_refuses_unreachable_redis():
    config = _config("first")
    coordinator = RedisCoordinator(config, client=_UnreachableClient())
    with pytest.raises(ConnectionError):
        await coordinator.start()
    assert not coordinator.is_leader()


@pytest.mark.asyncio
async def test_start_refuses_missing_redis_url():
    config = SimpleNamespace(server_id="first", namespace="tests")
    coordinator = RedisCoordinator(config)
    with pytest.raises(RuntimeError, match="coordinator_redis_url"):
        await coordinator.start()


def _server_stub(config):
    server = object.__new__(Server)
    server._config = config
    return server


async def _teardown_coordinators():
    bus_module.event_bus.set_coordinator(None)
    cache_module.set_coordinator(None)


@pytest.mark.asyncio
async def test_init_coordinator_defaults_to_local(monkeypatch):
    config = SimpleNamespace(
        coordinator_mode="local",
        coordinator_redis_url=None,
        namespace="tests",
        server_id="first",
        _jwt_secret_key_user_provided=True,
        jwt_secret_key="secret",
    )
    app = FastAPI()
    app.state.plugin_contexts = {}
    stub = _server_stub(config)
    try:
        await Server._init_coordinator(stub, app)
        assert isinstance(stub._coordinator, LocalCoordinator)
    finally:
        await _teardown_coordinators()


@pytest.mark.asyncio
async def test_init_coordinator_selects_redis_mode(monkeypatch):
    fake_client = FakeRedis(decode_responses=True)
    monkeypatch.setattr(
        "gpustack.server.coordinator.redis.redis_asyncio.Redis",
        SimpleNamespace(from_url=lambda *args, **kwargs: fake_client),
    )
    config = SimpleNamespace(
        coordinator_mode="redis",
        coordinator_redis_url="redis://tests.invalid/0",
        namespace="tests",
        server_id="first",
        _jwt_secret_key_user_provided=True,
        jwt_secret_key="secret",
    )
    app = FastAPI()
    app.state.plugin_contexts = {}
    stub = _server_stub(config)
    try:
        await Server._init_coordinator(stub, app)
        assert isinstance(stub._coordinator, RedisCoordinator)
    finally:
        await _teardown_coordinators()


@pytest.mark.asyncio
async def test_plugin_coordinator_wins_over_builtin():
    class _Plugin(Plugin):
        @classmethod
        def build_coordinator(cls, ctx):
            return LocalCoordinator(ctx.cfg)

    config = SimpleNamespace(
        coordinator_mode="redis",
        coordinator_redis_url="redis://tests.invalid/0",
        namespace="tests",
        server_id="first",
        _jwt_secret_key_user_provided=True,
        jwt_secret_key="secret",
    )
    app = FastAPI()
    app.state.plugin_contexts = {
        "fake": PluginContext(name="fake", app=app, cfg=config, plugin_cls=_Plugin)
    }
    stub = _server_stub(config)
    try:
        await Server._init_coordinator(stub, app)
        assert isinstance(stub._coordinator, LocalCoordinator)
    finally:
        await _teardown_coordinators()

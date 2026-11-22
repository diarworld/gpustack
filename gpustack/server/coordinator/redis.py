"""
Redis Coordinator - distributed implementation.

Leader election and cross-instance event delivery backed by a shared Redis.
Selected when ``coordinator_mode`` is ``redis`` (``GPUSTACK_COORDINATOR_MODE``)
and ``coordinator_redis_url`` is set; see ``Server._init_coordinator``.

Mode of operation:

* **Leader election** -- one lease key per namespace
  (``gpustack:coordinator:<namespace>:leader``) whose value is the leader's
  ``server_id``. Acquisition is ``SET ... NX PX``; renewal and release are
  compare-and-set Lua scripts, so an expired lease taken by another instance
  can never be renewed or deleted by the previous holder. A leader whose lease
  expires is deposed by the election loop (the process exits), which bounds
  the control-plane failover time to the lease TTL.
* **Fail-stop on Redis loss** -- while the leader cannot reach Redis, its
  lease still holds until the TTL runs out, so a short outage changes nothing:
  nobody can take the key while it exists. Once the outage outlasts the TTL
  the lease may have been taken over, so renewal reports failure and the
  leader exits rather than keep scheduling.
* **Event delivery** -- one Redis pub/sub channel per namespace. Events travel
  in the bus's id-only wire shape (``Event.to_dict`` / ``Event.from_dict``).
  Delivery includes the publisher (loopback): the bus routes events
  exclusively through the coordinator when one is set, so the local fan-out
  comes back through here. Pub/sub is fire-and-forget -- events published
  while a subscriber's connection is down are not replayed. That is by
  design: the controllers' resync loops are the durability net, and server
  cache invalidations are bounded by the cache TTL.
* **One bounded blocking pool** -- every command shares a single
  ``BlockingConnectionPool`` capped at
  ``GPUSTACK_COORDINATOR_REDIS_MAX_CONNECTIONS`` connections; a command
  issued when the cap is hit waits for a free connection instead of failing.
  The pub/sub listener holds one of those connections.
"""

import asyncio
import json
import logging
import time
from typing import Any, Optional

import redis.asyncio as redis_asyncio
from redis.asyncio import BlockingConnectionPool

from gpustack import envs
from gpustack.server.coordinator.base import Coordinator, Event

logger = logging.getLogger(__name__)

# Compare-and-expire: refresh the lease only if we still own it.
_RENEW_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('pexpire', KEYS[1], ARGV[2])
else
    return 0
end
"""

# Compare-and-delete: release the lease only if we still own it.
_RELEASE_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
else
    return 0
end
"""

# How long a pub/sub reconnect waits before dialing again. The election loop
# runs on its own cadence, so this only delays event delivery, not failover.
_RECONNECT_DELAY_SECONDS = 1

# How long start() waits for the listener's first successful SUBSCRIBE. A
# replica that cannot join the bus must refuse startup, not serve with a
# silently dead event path.
_LISTENER_READY_TIMEOUT_SECONDS = 5

# How long a command waits for a free pool connection before failing. A
# PUBLISH round-trip is sub-millisecond on a healthy Redis, so a burst
# drains far faster than this; the wait only binds when Redis is slow.
_POOL_ACQUIRE_TIMEOUT_SECONDS = 10


def _build_client(url: str) -> "redis_asyncio.Redis":
    """Build the coordinator's Redis client.

    A blocking pool, not redis-py's default one: ``ConnectionPool`` caps
    itself at 100 connections and raises ``MaxConnectionsError("Too many
    connections")`` the moment they are all in use. The startup reconcile
    burst issues more than 100 concurrent publishes, so the default pool
    turned every overflow into a dropped cross-instance event plus an ERROR
    line in the bus. A blocking pool waits out the burst instead, and the
    cap then bounds the Redis-side connection footprint rather than
    throughput.
    """
    pool = BlockingConnectionPool.from_url(
        url,
        max_connections=envs.COORDINATOR_REDIS_MAX_CONNECTIONS,
        timeout=_POOL_ACQUIRE_TIMEOUT_SECONDS,
        decode_responses=True,
        socket_connect_timeout=5,
        socket_timeout=5,
    )
    return redis_asyncio.Redis.from_pool(pool)


class RedisCoordinator(Coordinator):
    """Distributed coordinator over a shared Redis. See module docstring."""

    def __init__(self, config: Any, client: Optional["redis_asyncio.Redis"] = None):
        super().__init__(
            config,
            leader_election_ttl=envs.COORDINATOR_LEADER_TTL_SECONDS,
            leader_election_renew_interval=envs.COORDINATOR_RENEW_INTERVAL_SECONDS,
        )
        # Injectable for tests; built from coordinator_redis_url otherwise.
        self._client = client
        self._owns_client = client is None
        self._namespace = getattr(config, "namespace", None) or "default"
        self._server_id = getattr(config, "server_id", None) or "unknown"
        self._leader_key = f"gpustack:coordinator:{self._namespace}:leader"
        self._bus_channel = f"gpustack:coordinator:{self._namespace}:events"
        # Last confirmed contact with Redis (monotonic). Drives the fail-stop:
        # an unreachable leader holds its lease until the TTL runs out, then
        # must assume it was taken over and stop.
        self._last_contact = time.monotonic()
        self._listener_task: Optional[asyncio.Task] = None
        # Set by the listener once the bus SUBSCRIBE is in place; start()
        # waits for it so no event published right after startup is missed
        # by the loopback path.
        self._listener_ready = asyncio.Event()

    @property
    def is_distributed(self) -> bool:
        """Events cross instances, so subscribers must handle id-only payloads."""
        return True

    async def start(self):
        if self._client is None:
            url = getattr(self._config, "coordinator_redis_url", None)
            if not url:
                raise RuntimeError(
                    "coordinator_mode is 'redis' but coordinator_redis_url is "
                    "not set. Point GPUSTACK_COORDINATOR_REDIS_URL at the "
                    "shared Redis instance."
                )
            self._client = _build_client(url)
        # Refuse to come up against an unreachable Redis: a replica that
        # cannot coordinate must neither serve nor silently fall back to
        # running the full control plane.
        await self._client.ping()
        self._last_contact = time.monotonic()
        self._listener_task = asyncio.create_task(self._listen_loop())
        try:
            await asyncio.wait_for(
                self._listener_ready.wait(), timeout=_LISTENER_READY_TIMEOUT_SECONDS
            )
        except asyncio.TimeoutError:
            self._listener_task.cancel()
            raise RuntimeError(
                "Coordinator pub/sub listener failed to subscribe within "
                f"{_LISTENER_READY_TIMEOUT_SECONDS}s."
            )
        logger.info(
            "Redis coordinator started (leader key %s, bus channel %s).",
            self._leader_key,
            self._bus_channel,
        )

    async def stop(self):
        if self._listener_task is not None:
            self._listener_task.cancel()
            self._listener_task = None
        await self.release_leadership()
        if self._owns_client and self._client is not None:
            await self._client.aclose()
            self._client = None

    # Leader Election

    async def acquire_leadership(self, ttl: int) -> bool:
        """Take the lease, or adopt it if this instance already owns it."""
        acquired = await self._client.set(
            self._leader_key,
            self._server_id,
            nx=True,
            px=ttl * 1000,
        )
        self._last_contact = time.monotonic()
        if acquired:
            self._set_leader(True)
            return True
        # SET NX failed: the key exists. If it holds our id -- e.g. our lease
        # outlived a reconnect -- refresh it and keep leadership; otherwise
        # someone else holds it.
        current = await self._client.get(self._leader_key)
        if current == self._server_id:
            await self._client.pexpire(self._leader_key, ttl * 1000)
            self._set_leader(True)
            return True
        return False

    async def renew_leadership(self, ttl: int) -> bool:
        """Refresh the lease TTL; False means leadership is gone.

        A Redis error is not immediately a loss: while the lease key exists,
        nobody can take it, so a short outage changes nothing. Only an outage
        that outlasts the TTL forces the conclusion that the lease may have
        expired and been taken over -- then report the loss so the election
        loop exits the process instead of scheduling against a new leader.
        """
        if not self._is_leader:
            return False
        try:
            renewed = await self._client.eval(
                _RENEW_SCRIPT, 1, self._leader_key, self._server_id, ttl * 1000
            )
        except Exception as e:
            unreachable_for = time.monotonic() - self._last_contact
            if unreachable_for >= ttl:
                logger.error(
                    "Leader cannot reach Redis for %.1fs (lease TTL %ds); "
                    "assuming leadership lost.",
                    unreachable_for,
                    ttl,
                )
                self._set_leader(False)
                return False
            logger.warning(
                "Leader renewal failed (Redis unreachable for %.1fs, "
                "lease still holds): %s",
                unreachable_for,
                e,
            )
            return True
        self._last_contact = time.monotonic()
        if renewed:
            return True
        logger.error(
            "Leadership lease %s is gone or held by another instance.",
            self._leader_key,
        )
        self._set_leader(False)
        return False

    async def release_leadership(self):
        """Release the lease if we still hold it (compare-and-delete)."""
        self._set_leader(False)
        if self._client is None:
            return
        try:
            await self._client.eval(
                _RELEASE_SCRIPT, 1, self._leader_key, self._server_id
            )
        except Exception as e:
            # Best-effort on shutdown: the lease expires on its own and the
            # standby takes over after the TTL either way.
            logger.warning("Failed to release leadership: %s", e)

    # Pub/Sub

    async def publish(self, channel: str, event: Event):
        """Publish an event to the namespace's bus channel.

        Delivered to every instance subscribed to ``channel``, including this
        one. Errors propagate: the bus falls back to local delivery when the
        coordinator cannot publish (see ``EventBus.publish``).
        """
        envelope = json.dumps({"topic": channel, "event": event.to_dict()})
        await self._client.publish(self._bus_channel, envelope)
        self._last_contact = time.monotonic()

    async def _listen_loop(self):
        """Deliver bus messages to local subscribers, reconnecting on failure.

        Runs on this process's event loop, so subscriber callbacks fire on the
        main loop without a thread bridge -- the contract in
        ``Coordinator.subscribe``.
        """
        while True:
            pubsub = None
            try:
                pubsub = self._client.pubsub()
                await pubsub.subscribe(self._bus_channel)
                self._listener_ready.set()
                self._last_contact = time.monotonic()
                async for message in pubsub.listen():
                    if message.get("type") != "message":
                        continue
                    self._last_contact = time.monotonic()
                    self._dispatch(message["data"])
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.warning("Coordinator pub/sub disconnected: %s", e)
                await asyncio.sleep(_RECONNECT_DELAY_SECONDS)
            finally:
                if pubsub is not None:
                    try:
                        await pubsub.aclose()
                    except Exception:
                        pass

    def _dispatch(self, raw: Any):
        """Parse one bus message and notify local subscribers of its topic."""
        try:
            payload = json.loads(raw)
            event = Event.from_dict(payload["event"])
            topic = payload["topic"]
        except Exception as e:
            logger.error("Dropping malformed coordinator bus message: %s", e)
            return
        self._notify_local_subscribers(topic, event)

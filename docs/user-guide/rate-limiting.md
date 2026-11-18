# Rate Limiting

GPUStack can enforce request-rate and token-consumption limits per API key and
per user, at the API gateway, before traffic reaches a model instance. Limits
are enforced by the `gpustack-rate-limit` plugin running on the Higress
gateway, with counters kept in Redis so they hold across every gateway
replica.

## How It Works

- **Request rate (RPM)** — a sliding per-minute window counted in requests.
- **Token rate (TPM)** — a sliding per-minute window counted in the tokens
  each request consumes.
- **Daily / monthly token budgets** — calendar-aligned allowances that reset
  at midnight on the deployment timezone (daily) and on the 1st of the month
  (monthly).
- **Weekly token budgets** — a rolling 7-day window.

A request that exceeds a limit is rejected at the gateway with
`429 Too Many Requests` in the OpenAI error format, before any model instance
is contacted. The rejected response carries an `x-ratelimit-limited-key`
header naming the Redis key that triggered the rejection, which maps a 429
back to the user and window that produced it.

Limits are bucketed **per user**: a user's API-key traffic and their UI
traffic draw on the same allowance. A request is subject to rate limiting only
on inference paths (the standard OpenAI/Anthropic endpoints and
`/model/proxy`).

## Deployment-Wide Defaults

Defaults apply to every consumer and are set in the server config file:

```yaml
gateway_rate_limit:
  enabled: true
  # Redis is strongly recommended in production: without it the plugin keeps
  # counters per gateway replica, and a multi-replica gateway enforces roughly
  # limit x replicas.
  redis:
    service_name: redis.gpustack-system.svc.cluster.local
    service_port: 6379
  timezone: UTC
  default_rpm: 60
  default_tpm: 100000
  default_daily_tokens: 500000
  default_monthly_tokens: 5000000
```

All the `default_*` fields are optional; an unset field means no limit of that
kind. The plugin stays a passthrough until at least one limit is configured.

Redis must be reachable from the gateway pods. Declare it in the McpBridge
that serves the gateway (or point `service_name` at a Kubernetes Service
after setting `global.onlyPushRouteCluster: false` in the Higress
configuration).

## Per-Key and Per-User Limits

Explicit limits override the deployment defaults:

- **Per API key** — `rpm_limit`, `tpm_limit` and `daily_token_limit`, set on
  the key. Unset fields on a limited key inherit the deployment defaults:
  capping only a key's TPM does not remove its default RPM guard.
- **Per user** — `weekly_token_limit` and `monthly_token_limit`, set on the
  user account. These are shared across all of the user's API keys and their
  UI sessions.

There is no deployment-wide default for the weekly and monthly budgets: a
default that silently caps every account is not a safe thing to imply. Users
without explicit budgets and keys without explicit limits are governed by the
deployment defaults alone.

Changes take effect within one reconcile interval (30 seconds by default, see
`GPUSTACK_GATEWAY_RATE_LIMIT_RECONCILE_INTERVAL_SECONDS`).

!!! note

    Per-key rate limits are enforced at the gateway, so they apply to traffic
    that arrives through it. Inference calls made directly to the server's
    API port are not rate limited.

## Failure Behaviour

The rate limiter fails open: if Redis is unreachable, requests are allowed
through and a warning is logged, rather than blocking traffic because of the
limiter's own infrastructure. Monitor the `gpustack_rate_limit_rejected_total`
and `gpustack_rate_limit_request_total` counters (exported by the gateway's
Prometheus endpoint) for evidence that limits are being enforced.

## Metrics

The gateway emits three counters per rate-limit decision:

- `gpustack_rate_limit_request_total` — requests that ran limit checks,
  labelled `passed` / `limited`.
- `gpustack_rate_limit_rejected_total` — rejected requests, labelled with the
  triggering combination and window.
- `gpustack_rate_limit_value_total` — the counts written into each window
  bucket (one write per declared window per request).

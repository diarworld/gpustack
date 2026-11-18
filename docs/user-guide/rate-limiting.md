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
limiter's own infrastructure. Monitor the rejection counters in the Metrics
section below for evidence that limits are being enforced.

## Metrics

The plugin declares three counters per rate-limit decision:

- `gpustack_rate_limit_request_total` — requests that ran limit checks, split
  into `result="passed"` and `result="limited"`.
- `gpustack_rate_limit_rejected_total` — rejected requests, attributed to the
  combination and window that triggered the rejection.
- `gpustack_rate_limit_value_total` — the spend written into each window
  bucket (1 per request for query windows, the token count for token windows).

The Higress AI statistics plugin fuses these into the route-level Envoy
metrics, so on the gateway's Prometheus endpoint they are exposed under the
`route_upstream_model_consumer_metric_` prefix, with the combination and
window flattened into the name and the AI dimensions attached as labels:

```promql
route_upstream_model_consumer_metric_gpustack_rate_limit_request_total_rule_gpustack_rate_limit_result_passed{ai_route="ai-route-route-1.internal",ai_cluster="outbound_80__model-1-46.static",ai_model="glm-5.3-flash",ai_consumer="a3d73e9f0c402120.gpustack-6"}
```

- `ai_consumer` — the authenticated consumer, e.g. `a3d73e9f0c402120.gpustack-6`.
- `ai_route`, `ai_cluster`, `ai_model` — the AI route and the model instance
  the request was headed for.
- The combination and window become name segments (non-alphanumerics
  flattened to `_`):
  `..._rejected_total_rule_gpustack_rate_limit_combo_<combination>_kind_<query|token_rolling|token_calendar>_period_<60s|604800s|each_day|each_month>_bucket_consumer_<consumer>`.

The counters are exported by the gateway pods on
`:15020/stats/prometheus`; the pods carry the standard
`prometheus.io/scrape: "true"`, `prometheus.io/port: "15020"` and
`prometheus.io/path: "/stats/prometheus"` annotations, so a Prometheus that
scrapes pod annotations picks them up.

### Shortening the Names with Relabeling

Envoy stats have no label dimensions — the plugin can only encode them in
the stat name, and the AI skeleton prefix is what makes Higress's built-in
bootstrap extractors produce the `ai_*` labels. The cluster-side extractor
config is fixed on this Higress version (an EnvoyFilter BOOTSTRAP patch is
not honored), so the way to get short names with the ids as labels is
`metric_relabel_configs` on the Prometheus side. Applied to the gateway
scrape job, this turns the names above into
`gpustack_rate_limit_{request,rejected,value}_total` with
`rl_combo` / `rl_kind` / `rl_period` / `rl_bucket` / `rl_result` labels:

```yaml
metric_relabel_configs:
  # ids out of the flattened name, into labels — these rules read the
  # ORIGINAL name, so they must come before the rename below.
  - source_labels: [__name__]
    regex: gpustack_rate_limit_request_total_rule_.*_result_(.+)
    target_label: rl_result
  - source_labels: [__name__]
    regex: .*_combo_(.+?)_kind_.*
    target_label: rl_combo
  - source_labels: [__name__]
    regex: .*_kind_(.+?)_period_.*
    target_label: rl_kind
  - source_labels: [__name__]
    regex: .*_period_(.+?)_bucket_consumer_.*
    target_label: rl_period
  - source_labels: [__name__]
    regex: .*_bucket_consumer_(.+)
    target_label: rl_bucket
  # shorten the name last
  - source_labels: [__name__]
    regex: route_upstream_model_consumer_metric_(gpustack_rate_limit_(?:request|rejected|value)_total).*
    target_label: __name__
    replacement: ${1}
```

Example queries in terms of the relabeled names:

```promql
# rejections per consumer
sum by (ai_consumer) (rate(gpustack_rate_limit_rejected_total[5m]))

# which combination and window produce them
sum by (rl_combo, rl_kind, rl_period) (rate(gpustack_rate_limit_rejected_total[5m]))

# enforcement sanity: rejected share of checked requests
sum (rate(gpustack_rate_limit_request_total{rl_result="limited"}[5m]))
/
sum (rate(gpustack_rate_limit_request_total[5m]))
```

!!! note

    On Higress 2.1.x with the AI statistics plugin enabled the bare
    `gpustack_rate_limit_*` names do not exist in the raw exposition — they
    appear only after the relabeling above. Without it, always query through
    the `route_upstream_model_consumer_metric_` prefix. For what is currently
    counted inside each window, the Redis keys named by
    `x-ratelimit-limited-key` are the source of truth.

## Reconciler Events

The server runs a reconciler that watches the API-key and user tables and
republishes the rate-limit WasmPlugin CR within one reconcile interval (30
seconds by default, `GPUSTACK_GATEWAY_RATE_LIMIT_RECONCILE_INTERVAL_SECONDS`).
Its events and log lines carry the `gateway_rate_limit_reconciler` source
tag (for example `gateway_rate_limit_reconciler.api_key`). That tag marks
an event source, not a Prometheus metric — the reconciler itself exports no
metrics. Seeing it in the event log means a limit change was picked up and
pushed to the gateway.

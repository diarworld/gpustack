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
- **Concurrent requests per model** — a cap on how many of a user's requests
  may run against one model at the same time; a slot is taken while a request
  runs and freed when its stream ends.

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

Defaults apply to every consumer and are set in the server config file (the
YAML file the server loads through `--config-file` or the
`GPUSTACK_CONFIG_FILE` environment variable):

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
  default_rpm: 3
  default_tpm: 768000
  default_daily_tokens: 10000000
  default_monthly_tokens: 100000000
  default_concurrency: 1
```

All the `default_*` fields except `default_concurrency` are optional; an unset
field means no limit of that kind. `default_concurrency` defaults to **1** —
one in-flight request per model per user — because a runaway batch client
holding dozens of parallel streams is the failure mode the cap exists for; set
it to `0` to turn the concurrency cap off deployment-wide. Every other field
stays a passthrough until configured.

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

The weekly budget has no deployment-wide default: only accounts with an
explicit weekly budget are capped on it. Users without explicit budgets and
keys without explicit limits are governed by the deployment defaults alone.

### Per-Model Concurrency

The `default_concurrency` deployment default (1) caps how many requests one
user may run against **one model** at the same time: a second parallel
request to the same model is rejected with 429 while the first is still
streaming, while requests to different models proceed independently. The
count is of in-flight requests, not a rate — a slot is acquired when the
request passes the gateway and released when its stream ends, so a finished
request frees its slot immediately.

Admins are exempt from the concurrency cap. Per-model overrides are set on
the user account via `model_concurrency_limits` — a map of model name to
that model's cap for the account, where `0` means explicit unlimited:

```json
{
  "model_concurrency_limits": {
    "glm-5.3-flash": 4,
    "qwen3.8-27b": 2,
    "text-embedding-model": 0
  }
}
```

Models absent from the map inherit `default_concurrency`. An admin can edit
the map per user in the user settings drawer; changes take effect within one
reconcile interval (30 seconds by default).

!!! note

    A slot whose release is lost — a gateway crash mid-stream — is reclaimed
    automatically after an hour (`concurrency_stale_seconds` on the plugin,
    tunable through `gateway_rate_limit.plugin_config`), so a crashed stream
    cannot lock a user's slot forever.

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

## Current Limits and Usage

The server exposes what the gateway is enforcing right now at
`GET /v2/usage/rate-limits`. Any authenticated user gets their own picture by
default; a platform-wide admin (an admin acting without an Org pinned — the
same gate the usage routes apply to cross-tenant views) may pass `user_id` to
view any user, while everyone else silently gets their own view.

The response carries:

- `mode` — `redis` (live counters, read from Redis at request time), `local`
  (rate limiting is on without Redis, so limits come back without counters)
  or `disabled` (the feature is off; the Usage page hides the section).
- `user` — the user's weekly and monthly token budgets (whichever are set),
  each with its live window, plus `model_concurrency` — the per-model
  in-flight caps set on the account (`model_concurrency_limits`), each with
  its live `running` slot count. Only explicitly named models are itemized;
  admins (exempt) and users without overrides get an empty list.
- `api_keys` — one entry per live API key: its explicit `rpm_limit`,
  `tpm_limit` and `daily_token_limit` (unset fields are omitted — they
  inherit the deployment defaults), which limit `source` governs it (`key`,
  `user+default` or `default`), and every window its requests draw on — RPM,
  TPM, day, week and month — each with `used`, `limit` and `reset_seconds`.
- `generated_at` — the instant every window was read at.
- `usage_error` — set only when the Redis read failed, and carrying just the
  exception class name, never its message (which can contain hostnames).

Fields with no value are omitted rather than sent as `null`: in `local` mode,
or whenever Redis is unreachable, a window simply has no `used`. The read
fails open — the limits are always returned. The Usage page's Summary tab
renders this response as its **Rate limits** section.

For a deployment with defaults of 3 RPM, 768000 TPM, 10000000 daily and
100000000 monthly tokens, and a user with a 100000 weekly budget:

```json
{
  "mode": "redis",
  "generated_at": "2026-11-19T10:30:00Z",
  "user": {
    "weekly_limit": 100000,
    "windows": [
      {"kind": "token_rolling", "period": "604800s", "window_seconds": 604800, "limit": 100000, "used": 37215, "reset_seconds": 172800}
    ]
  },
  "api_keys": [
    {
      "id": 6,
      "name": "ci-runner",
      "source": "user+default",
      "windows": [
        {"kind": "query", "period": "60s", "window_seconds": 60, "limit": 3, "used": 2, "reset_seconds": 41},
        {"kind": "token_rolling", "period": "60s", "window_seconds": 60, "limit": 768000, "used": 4820, "reset_seconds": 27},
        {"kind": "token_calendar", "period": "each_day", "limit": 10000000, "used": 18450, "reset_seconds": 48600},
        {"kind": "token_calendar", "period": "each_month", "limit": 100000000, "used": 210000, "reset_seconds": 999000},
        {"kind": "token_rolling", "period": "604800s", "window_seconds": 604800, "limit": 100000, "used": 37215, "reset_seconds": 172800}
      ]
    }
  ]
}
```

The weekly window appears under both the user and the key: API-key traffic and
UI traffic draw on the same allowance, so both read the same counter. Calendar
windows have no `window_seconds` — their length follows the calendar, and
their `reset_seconds` counts down to the boundary in the deployment timezone.

### Counter Recovery

The plugin's Redis counters are the only enforcement state: when the
rate-limit Redis loses them — a fresh instance, a failover, a flush without
persistence — calendar quotas restart from zero and users who already spent
their day / week / month budget get it back for the rest of the window. To
close that gap, the server replays the consumption it already recorded in
`model_usage_details` into the very counters the plugin reads, and only
into counters that do not exist yet — a live counter is never touched. The
pass runs on a loop (every 5 minutes): each cycle probes the counters with
one pipelined EXISTS, and the details scan only runs when something is
actually missing, so a Redis flush heals itself within one interval.

Not exact by design:

- Minute windows are not rebuilt — they age out in 60 seconds, long before a
  rebuild can matter.
- Rebuilt members are per-hour aggregates, so the sliding week decays in
  hour steps rather than per request.
- The plugin counts at the gateway while the server records at the proxy, so
  aborted or untracked requests can leave a rebuilt total slightly off the
  lost one.

Enabling persistence (AOF / RDB) on the rate-limit Redis avoids the rebuild
entirely.

!!! note

    The endpoint and the rebuilder talk to the same Redis the gateway writes
    its counters to — `gateway_rate_limit.redis` — not the server's
    top-level `redis_url`.

### Resetting a Budget

The weekly and monthly budget cards on the Usage page carry a reset
button: pressing it deletes the user's counters of that window kind, so
the budget restarts from zero mid-window without waiting for the natural
boundary. Every press is recorded in `gateway_rate_limit_resets` — the
reason note is required and lands in the row, together with the user, the
press date and, when an admin pressed on someone's behalf, who did.

Two presses per window are free; the quota floats with the presses
themselves — two resets inside one week make the next one available a
week after the first press, not on the next calendar boundary. Once both
are spent the button disables until the oldest press ages out of the
window. A platform-wide admin is not quota-limited: their presses (their
own budget or another user's) are recorded but never consume the user's
own allowance.

The counter rebuild cooperates with resets: usage recorded before the
latest reset of a window kind is forgiven, so a rebuild after a Redis
loss resumes from the reset instead of resurrecting the forgiven spend.

## Metrics

The plugin declares three counters per rate-limit decision:

- `gpustack_rate_limit_request_total` — requests that ran limit checks, split
  into `result="passed"` and `result="limited"`.
- `gpustack_rate_limit_rejected_total` — rejected requests, attributed to the
  combination and window that triggered the rejection.
- `gpustack_rate_limit_value_total` — the spend written into each window
  bucket (1 per request for query windows and concurrency acquires, the token
  count for token windows).

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
    regex: .*gpustack_rate_limit_request_total_rule_.*_result_(.+)
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

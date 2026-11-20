# Quotas

The **Limits** section on the Usage → Overview page shows the quotas the gateway
enforces against your traffic right now: how much of each allowance is spent,
when every window resets and — for the weekly and monthly budgets — a reset
button. The section is always self-scoped: you see your own limits, an
administrator sees their own.

![Usage quotas](../assets/usage/usage-quotas.png)

## The Limit Cards

Each card carries a spent / limit readout, a progress bar and a reset countdown.
The `?` icon on a card explains its window.

| Card      | Window                                                                                  | Reset                                            |
| --------- | --------------------------------------------------------------------------------------- | ------------------------------------------------ |
| **RPM**   | Requests per minute — a sliding 60-second window.                                       | Continuously, as the oldest requests age out.    |
| **TPM**   | Tokens per minute — the same sliding window, counted in tokens.                         | Continuously.                                    |
| **Week**  | Tokens over a rolling 7-day window, shared across all of your API keys and UI sessions. | Continuously, as week-old usage ages out.        |
| **Month** | Tokens in the calendar month.                                                           | At the month boundary, in the deployment timezone. |

When a limit is hit, further requests are rejected at the gateway with
`429 Too Many Requests` — before any model instance is contacted — until the
window frees up again.

RPM and TPM guard against short bursts; the weekly and monthly budgets cap
sustained consumption. A request counts against every window it can touch: your
API-key traffic and your UI sessions draw on the same user-level allowance.

## Per-Key Daily Usage

The table under the cards lists each of your API keys against its daily token
quota — the key's own daily limit or the deployment default — with a spent /
limit readout for the current day. The daily window is calendar-aligned and
resets at midnight in the deployment timezone.

## Model Concurrency

Beyond the rate and budget windows, the gateway caps how many of your requests
— across all of your API keys and sessions — may run against a single model at
the same time. The deployment default is **1**: while one request to a model is
still running, a second parallel request to the same model is rejected with
`429 Too Many Requests` until the first one finishes. The slot frees up the
moment a request completes — there is no window to wait out.

Each model is counted separately: a running request against one model does not
block requests against other models. Administrators are not subject to this
limit.

The default and the per-model overrides are set by the administrator on your
account. A model without an explicit override follows the deployment default,
and an override of `0` lifts the cap for that model entirely.

## Where the Numbers Come From

- **Deployment defaults** — set by the administrator for everyone: RPM, TPM,
  daily and monthly tokens. An unset default means that window is not limited.
- **Per-user budgets** — weekly and monthly token budgets an administrator sets
  on your account, shared across all of your keys and sessions.
- **Per-key limits** — RPM, TPM and a daily token limit on an individual key.
  Unset fields inherit the deployment defaults.

This deployment ships the following defaults:

| Window | Default                                 |
| ------ | --------------------------------------- |
| RPM    | 3 requests / minute                     |
| TPM    | 768,000 tokens / minute                 |
| Day    | 10,000,000 tokens per API key           |
| Month  | 100,000,000 tokens per user             |
| Week   | no default — only accounts with an explicit weekly budget are capped |

The internals — gateway configuration, defaults, failure behaviour and metrics
— are described in [Rate Limiting](rate-limiting.md).

## Resetting a Budget

The **Week** and **Month** cards carry a **Reset** button. Pressing it drops the
window's counter to zero immediately: the budget restarts without waiting for
the natural boundary. Usage already recorded stays in the usage reports — only
the quota counter is affected.

A press requires a reason. The note is stored together with the press — who,
when and why — so the audit trail shows how each budget came to restart.

!!! note

    Resets apply to the weekly and monthly budgets only. RPM, TPM and the
    per-key daily quotas are short windows that free up on their own.

### Reset Quota

Two resets per window are free. The quota floats with the presses themselves:
two resets inside one week make the next one available a week after the first
press, not on the next calendar boundary. When both are spent, the button
disables until the oldest press ages out of the window — hovering the disabled
button shows the date the next reset becomes available.

Administrators are not quota-limited: an administrator can reset a budget at
any time — their own or another user's — and an administrator's presses never
consume the user's own two free resets.

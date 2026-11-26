"""Live rate-limit windows for the Usage page (``GET /usage/rate-limits``).

Pairs the limits the ``gateway_rate_limit`` config and the target user's
token budgets publish (via
:func:`gpustack.gateway.rate_limit_usage.expected_windows`) with the live
counters the gateway's ``gpustack-rate-limit`` plugin keeps in Redis, read
through :func:`gpustack.gateway.rate_limit_usage.read_windows`.

A self-service reset press is additionally gated by the decision model
(``D3_DECISION_URL``): the required description is classified before the
counters clear, and a confident "no real task" verdict rejects the press
as an audit-only row.
"""

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from math import ceil
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import APIRouter, Request
from sqlalchemy import or_
from sqlmodel import select

from gpustack import envs
from gpustack.api.exceptions import (
    BadRequestException,
    ForbiddenException,
    InvalidException,
    NotFoundException,
)
from gpustack.api.tenant import TenantContext, is_platform_wide
from gpustack.config.config import Config, GatewayRateLimitConfig
from gpustack.gateway.effective_limits import (
    effective_limits_view,
    load_group_limit_snapshots,
)
from gpustack.gateway.rate_limit_usage import (
    RateLimitWindowState,
    RateLimitWindowSpec,
    default_plan_windows,
    model_concurrency_specs,
    read_concurrency,
    read_windows,
    reader_from_config,
    user_limit_windows,
)
from gpustack.gateway.rate_limit_settings import effective_rate_limit
from gpustack.schemas.common import Pagination
from gpustack.schemas.gateway_rate_limit_resets import (
    RESET_OUTCOME_APPROVED,
    RESET_OUTCOME_REJECTED,
    GatewayRateLimitReset,
)
from gpustack.schemas.model_routes import MyModel
from gpustack.schemas.principals import Principal, PrincipalType
from gpustack.schemas.rate_limits import (
    FreeWindowUsage,
    ModelConcurrencyUsage,
    RateLimitResetCreate,
    RateLimitResetStatus,
    RateLimitResetUsage,
    RateLimitResetsResponse,
    RateLimitWindowUsage,
    RateLimitsResponse,
    UserRateLimitUsage,
    UserRateLimits,
    UsersRateLimitsResponse,
)
from gpustack.schemas.users import User
from gpustack.server.deps import CurrentUserDep, SessionDep, TenantContextDep
from gpustack.server.services import bulk_user_attribute_labels

router = APIRouter()

logger = logging.getLogger(__name__)

# Module-level indirection so tests can substitute the reader construction;
# production callers get the config-wired factory.
_reader_factory = reader_from_config

# Resets allowed per floating window, per reset kind. The window floats
# with the presses themselves: two presses in one week make the next press
# available a week after the first one, not on the next calendar boundary.
RESET_QUOTA = 2
RESET_WINDOW_SECONDS = {"weekly": 604800, "monthly": 30 * 86400}
# The long token window periods each reset kind forgives, matched against
# the published windows of the target user.
_RESET_PERIODS = {"weekly": "604800s", "monthly": "each_month"}

# The confidence the decision model must reach on "no real task" for the
# press to be rejected; a less confident verdict lets the reset through
# (the gate decides on its strong answers only).
D3_REJECT_CONFIDENCE = 0.5

# The three questions the decision model answers for a reset description,
# verbatim the production prompts: is this a concrete work task, what kind
# of work it is, and why a rejected press was rejected. The wording is the
# contract with the d3 service — changing it here changes what the model
# judges.
_D3_QUESTIONS = {
    "answered": {
        "type": "noul",
        "instructions": (
            "Это ответ на вопрос \"Напишите какой задачей вы занимаетесь?\" "
            "для запроса на сброс лимита LLM токенов. Проверь, содержит ли "
            "ответ КОНКРЕТНУЮ рабочую задачу. Верни false, если ответ состоит "
            "из общих отписок, бессмысленных фраз или абстрактных действий "
            "без привязки к реальному проекту (например: 'для работы', "
            "'тестирую модель')"
        ),
    },
    "WorkType": {
        "type": "choice",
        "instructions": (
            "Классифицируй род занятий из ответа пользователя. Если ответ не "
            "содержит реальной рабочей задачи, является отпиской или попыткой "
            "обойти проверку (например: 'ищу баг в вашей системе', 'тестирую "
            "платформу', 'просто проверяю'), строго выбирай Unknown."
        ),
        "criteria": {
            "SoftwareDevelopment": (
                "Разработка программного обеспечения. Создание веб-сайтов, "
                "мобильных приложений, десктопных программ, игр и встроенного "
                "ПО (фронтенд, бэкенд, фулстек) с указанием функционала или "
                "стека."
            ),
            "QA": (
                "Тестирование и обеспечение качества. Написание автотестов, "
                "проверка конкретного функционала продукта, нагрузочное "
                "тестирование. НЕ включать абстрактные фразы вроде 'ищу баги' "
                "или 'тестирую систему' без описания проверяемого продукта."
            ),
            "Data": (
                "Аналитика и работа с данными. Бизнес- и системный анализ, "
                "сбор требований, работа с базами данных, SQL, ML и ИИ (Data "
                "Science, Data Engineering)."
            ),
            "Run": (
                "Инфраструктура и эксплуатация. Настройка и поддержка "
                "серверов, сетей, облачных хранилищ и БД, автоматизация "
                "процессов доставки кода (DevOps, сисадминство)."
            ),
            "InfoSec": (
                "Информационная безопасность. Защита систем и данных от "
                "взломов и утечек, аудит уязвимостей конкретных сервисов. НЕ "
                "включать непрошеный 'поиск багов в вашей системе'."
            ),
            "UIUX": (
                "Дизайн и проектирование интерфейсов. Создание визуального "
                "стиля, прототипов и пользовательских сценариев для цифровых "
                "продуктов."
            ),
            "Management": (
                "Управление продуктами и проектами. Планирование сроков, "
                "координация команд (Project Management) и стратегия развития "
                "продукта (Product Management)."
            ),
            "SupportServiceDesk": (
                "Техническая поддержка. Консультация пользователей, "
                "диагностика и устранение сбоев в работе внедренных систем."
            ),
            "Unknown": (
                "Прочая деятельность, отписки, абстрактные формулировки без "
                "указания проекта/продукта (например, 'ищу баг в системе', "
                "'тестирую ИИ'), либо работа, для которой не требуется доступ "
                "к LLM."
            ),
        },
    },
    "rejection_reason": {
        "type": "choice",
        "instructions": (
            "Укажи код причины отклонения заявки. Если answered = true и "
            "WorkType != Unknown, строго выбирай None."
        ),
        "criteria": {
            "None": "Заявка корректна, отклонение не требуется.",
            "TooVague": (
                "Слишком общее описание: указано направление, но не хватает "
                "конкретной задачи или объекта/проекта (например: 'пишу код "
                "для бэкенда', 'делаю дизайн')."
            ),
            "BypassAttempt": (
                "Отписка, манипуляция или попытка обойти проверку без "
                "реальной задачи (например: 'ищу баг в вашей системе', "
                "'тестирую лимиты', 'для работы', 'очень надо')."
            ),
            "NonIT": (
                "Задача не относится к ИТ-работам (например: написание "
                "реферата, генерация текстов, личные вопросы)."
            ),
            "Gibberish": (
                "Бессмысленный набор символов, пустой ответ или несвязный " "текст."
            ),
        },
    },
}


@dataclass
class _D3Verdict:
    """One classification round: the answers the decision model gave.

    ``confidence`` is the confidence of whichever yes/no class won the
    concrete-task question (the service's own ``confidence`` field carries
    P(yes) regardless of the verdict, so the pair is recomputed from the
    probability pair). ``rejection_reason`` is ``None`` when the model
    named no cause (or answered the literal string "None", its own way of
    saying none); the reject decision itself rides on ``answered`` +
    ``confidence``.
    """

    answered: bool
    confidence: float
    work_type: Optional[str]
    rejection_reason: Optional[str]


def _mode(rlc: Optional[GatewayRateLimitConfig]) -> str:
    """Which counter backend backs the windows: redis, local or disabled."""
    if rlc is None or not rlc.enabled:
        return "disabled"
    return "redis" if rlc.redis is not None else "local"


async def _resolve_target(
    session, user: User, ctx: TenantContext, user_id: Optional[int]
) -> User:
    """The user whose limits to show.

    Self-scoped by construction: a foreign ``user_id`` is honored only for
    a platform-wide admin — admin WITHOUT a pinned Org, the same
    ``is_platform_wide`` gate the usage routes apply to their cross-tenant
    views. An Org-pinned admin is scoped to their Org like anyone else, so
    their foreign ``user_id`` quietly degrades to the caller's own data
    (deny-by-default: the alternative would leak another Org's budgets and
    API keys).
    """
    if user_id in (None, user.id) or not is_platform_wide(user, ctx):
        return user
    target = await User.one_by_id(session, user_id)
    if target is None:
        raise NotFoundException(message="User not found")
    return target


def _window_usage(
    spec: RateLimitWindowSpec, state: Optional[RateLimitWindowState]
) -> RateLimitWindowUsage:
    """Map one spec and its read onto the wire model.

    A ``None`` state (local mode, or a discarded read) omits ``used`` /
    ``reset_seconds``; the response is served with
    ``response_model_exclude_none=True``, so they drop off the wire and the
    UI renders "—".
    """
    return RateLimitWindowUsage(
        kind=spec.kind,
        period=spec.period,
        window_seconds=spec.window_seconds,
        limit=spec.limit,
        used=state.used if state is not None else None,
        reset_seconds=state.reset_seconds if state is not None else None,
    )


def _reader(request: Request, cfg: Config):
    """The config-wired Redis reader, cached on the app for the process."""
    reader = getattr(request.app.state, "rate_limit_usage_reader", None)
    if reader is None:
        reader = _reader_factory(cfg)
        # "redis" mode implies a reader by construction; cache it for the
        # process (see reader_from_config: one pool, reused connections).
        request.app.state.rate_limit_usage_reader = reader
    return reader


def _d3_answered(answer: Dict[str, Any]) -> Optional[Any]:
    """The yes/no verdict on the concrete-task question, with the
    confidence of the class that was picked.

    The service answers a noul question without a ``choice`` field: the
    verdict is the ``yes``/``no`` probability pair, and the ``confidence``
    field carries P(yes) — not the confidence of whichever class won. A
    "no" answered with P(yes)=0.2 is a *strong* no (P(no)=0.8), so the
    confidence of the verdict is the larger of the two. A boolean
    ``choice`` (or its string spellings) is still accepted as a fallback;
    anything else is not a verdict.
    """
    probabilities = answer.get("probabilities")
    if isinstance(probabilities, dict):
        p_yes = _as_probability(probabilities.get("yes"))
        p_no = _as_probability(probabilities.get("no"))
        if p_yes is not None and p_no is not None:
            return p_yes >= p_no, max(p_yes, p_no)
    choice = answer.get("choice")
    if isinstance(choice, bool):
        return choice, _d3_confidence(answer)
    if isinstance(choice, str):
        lowered = choice.strip().lower()
        if lowered in ("true", "yes"):
            return True, _d3_confidence(answer)
        if lowered in ("false", "no"):
            return False, _d3_confidence(answer)
    return None


def _as_probability(value: Any) -> Optional[float]:
    """A probability in 0..1, or None when the value is not one."""
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if 0.0 <= number <= 1.0 else None


def _d3_confidence(answer: Dict[str, Any]) -> float:
    """The model's stated confidence, clamped into 0..1.

    Used for the boolean-choice fallback; a missing or malformed
    confidence reads as certain — an answer without a stated confidence
    was not a hedge the model chose to publish. The 0..1 clamp also
    accepts a percentage that slipped through the service.
    """
    try:
        confidence = float(answer.get("confidence", 1.0))
    except (TypeError, ValueError):
        return 1.0
    if confidence > 1.0:
        confidence /= 100.0
    return min(max(confidence, 0.0), 1.0)


def _d3_choice_str(answer: Dict[str, Any]) -> Optional[str]:
    """A choice answer as a plain string, or None when not a real pick."""
    choice = answer.get("choice")
    if isinstance(choice, str):
        stripped = choice.strip()
        # "None" is the model spelling its own no-cause option, not a cause.
        return None if stripped.lower() in ("", "none") else stripped
    return None


async def _classify_with_d3(description: str) -> Optional[_D3Verdict]:
    """Ask the decision model to judge a reset description.

    Sends the three production questions (a concrete-task check, a work-type
    classification, a rejection cause) over the jevcompat wire contract the
    model-selection path already uses. Returns ``None`` — never raises —
    when the gate is disabled or the verdict is unusable: an unreachable
    judge must not hold a user's budget hostage, so every failure mode
    funnels into the fail-open approve path. The URL, key and Host header
    come from the deployment env; the key is never logged.
    """
    url = envs.D3_DECISION_URL
    if not url:
        return None
    data = {
        "state": {"messages": [{"role": "user", "content": description}]},
        "questions": _D3_QUESTIONS,
        "model": "d3",
    }
    headers = {"Authorization": f"Bearer {envs.D3_DECISION_API_KEY}"}
    if envs.D3_DECISION_HOST:
        headers["Host"] = envs.D3_DECISION_HOST
    try:
        async with httpx.AsyncClient(trust_env=True) as client:
            response = await client.post(
                url, json=data, headers=headers, timeout=envs.D3_DECISION_TIMEOUT
            )
            response.raise_for_status()
            verdict = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        logger.warning(
            "reset decision model unavailable, approving reset: %s: %s",
            type(exc).__name__,
            exc,
        )
        return None
    answers = verdict.get("answers") if isinstance(verdict, dict) else None
    answered_answer = answers.get("answered") if isinstance(answers, dict) else None
    answered = (
        _d3_answered(answered_answer) if isinstance(answered_answer, dict) else None
    )
    if answered is None:
        logger.warning(
            "reset decision model returned no usable answered verdict, "
            "approving reset: %s",
            str(verdict)[:200],
        )
        return None
    answered_flag, answered_confidence = answered
    work_type_answer = answers.get("WorkType") if isinstance(answers, dict) else None
    reason_answer = (
        answers.get("rejection_reason") if isinstance(answers, dict) else None
    )
    return _D3Verdict(
        answered=answered_flag,
        confidence=answered_confidence,
        work_type=(
            _d3_choice_str(work_type_answer)
            if isinstance(work_type_answer, dict)
            else None
        ),
        rejection_reason=(
            _d3_choice_str(reason_answer) if isinstance(reason_answer, dict) else None
        ),
    )


async def _ready_model_names(session, target: User) -> List[str]:
    """The ready routes the target can reach, by name.

    A non-admin's reachable set is the ``non_admin_user_models`` view: its
    ``user_id`` rows encode the visibility grants, and ``ready_targets > 0``
    mirrors the READY filter the models page uses. These are the model
    names the gateway's routing header carries for this user's traffic --
    the fragments the plugin's concurrency counters key on. Admins are
    exempt from the concurrency cap (their combo omits the block, no
    counter exists), so there is nothing to itemize for them and no query
    is spent.
    """
    if target.is_admin:
        return []
    stmt = select(MyModel.name).where(
        MyModel.deleted_at.is_(None),
        MyModel.ready_targets > 0,
        MyModel.user_id == target.id,
    )
    rows = (await session.exec(stmt)).all()
    return sorted(set(rows))


async def _reset_status(
    session, target_id: int, kind: str, now: datetime
) -> RateLimitResetStatus:
    """Presses still available for one window kind under the floating quota.

    Only self-service presses count against the quota: an admin resetting
    on the user's behalf (``reset_by_user_id`` pointing at someone else)
    is a support action and never consumes the user's own allowance.
    Rejected presses are audit rows the decision model produced — they
    cleared nothing, so they never consume either.
    """
    window = timedelta(seconds=RESET_WINDOW_SECONDS[kind])
    counted = (
        await session.exec(
            select(GatewayRateLimitReset)
            .where(
                GatewayRateLimitReset.user_id == target_id,
                GatewayRateLimitReset.kind == kind,
                GatewayRateLimitReset.reset_at > now - window,
                GatewayRateLimitReset.outcome == RESET_OUTCOME_APPROVED,
                or_(
                    GatewayRateLimitReset.reset_by_user_id.is_(None),
                    GatewayRateLimitReset.reset_by_user_id == target_id,
                ),
            )
            .order_by(GatewayRateLimitReset.reset_at)
        )
    ).all()
    available = RESET_QUOTA - len(counted)
    # The oldest counted press ages out of the floating window first; when
    # it does, its allowance frees up for the next press.
    next_available_at = counted[0].reset_at + window if available <= 0 else None
    return RateLimitResetStatus(
        quota=RESET_QUOTA,
        available=max(available, 0),
        next_available_at=next_available_at,
    )


@router.get(
    "/rate-limits",
    response_model=RateLimitsResponse,
    response_model_exclude_none=True,
)
async def get_rate_limits(
    session: SessionDep,
    user: CurrentUserDep,
    ctx: TenantContextDep,
    request: Request,
    user_id: Optional[int] = None,
):
    """The target user's rate-limit picture with live window counters.

    Scoping: self by default; a foreign ``user_id`` is honored only for a
    platform-wide admin (``is_platform_wide`` — admin without a pinned
    Org, the usage routes' cross-tenant gate); anything else — non-admins
    and Org-pinned admins alike — silently degrades to the caller's own
    data.

    Args:
        session: async DB session.
        user: the authenticated caller.
        ctx: the request's tenant context; gates the admin expansion.
        request: the FastAPI request; the server config lives on app.state.
        user_id: whose limits to show; defaults to the caller.

    Returns:
        RateLimitsResponse: limits with live counters in redis mode, windows
        without counters in local mode, and empty structures when rate
        limiting is disabled.
    """
    cfg: Config = request.app.state.server_config
    # Feature on/off and the counter backend stay config-file state (they
    # decide whether the reconciler and the plugin run at all); the limit
    # values are the effective ones -- the settings row admins edit,
    # layered over the config file.
    mode = _mode(cfg.gateway_rate_limit)
    # One aware instant shared by every read in this request: the counters
    # and their resets must describe the same moment.
    now = datetime.now(timezone.utc)

    if mode == "disabled":
        # The UI hides the section entirely: no Redis dial, no budget read
        # — nothing here is meaningful when the plugin enforces nothing.
        return RateLimitsResponse(mode=mode, generated_at=now, user=UserRateLimits())

    target = await _resolve_target(session, user, ctx, user_id)
    rlc, free = await effective_rate_limit(session, cfg)

    # The user limit combo is non-fallback and governs every request of the
    # user, so these windows are the effective user-level picture: requests
    # and tokens per minute, the weekly budget and the monthly quota --
    # resolved through the user's explicit values, their groups' quotas and
    # the deployment defaults (see gpustack.gateway.effective_limits), the
    # same resolution the reconciler publishes onto the combo. All bucket
    # on ``gpustack-<id>`` -- the user's own traffic across all their keys.
    view = effective_limits_view(
        target,
        (await load_group_limit_snapshots(session, [target.id])).get(target.id, []),
        rlc,
    )
    user_specs = user_limit_windows(rlc, target.id, view)
    all_specs = list(user_specs)

    states: List[Optional[RateLimitWindowState]]
    usage_error = None
    if mode == "redis":
        reader = _reader(request, cfg)
        states, usage_error = await read_windows(
            reader, all_specs, now=now, timezone_name=rlc.timezone
        )
    else:
        # Local mode keeps no counters: windows render with limits only.
        states = [None] * len(all_specs)

    # Per-model in-flight caps with their live slot counts, over the ready
    # models the target can reach. Each row is personal: ``running`` is the
    # user's own slot count from the plugin's per-user counter, and the
    # limit is their override or the deployment default. Local mode has no
    # server-readable counters, so ``running`` stays unset there; admins
    # are exempt and itemize nothing.
    ready_models = await _ready_model_names(session, target)
    concurrency_specs = model_concurrency_specs(rlc, target.id, view, ready_models)
    concurrency_running: List[Optional[int]] = [None] * len(concurrency_specs)
    concurrency_error = None
    if concurrency_specs and mode == "redis":
        reader = _reader(request, cfg)
        counts, concurrency_error = await read_concurrency(
            reader, [spec.redis_key for spec in concurrency_specs]
        )
        if concurrency_error is not None:
            usage_error = usage_error or concurrency_error
        else:
            concurrency_running = list(counts)

    # Display order the Usage page renders: busiest model first (a row the
    # read could not fill in sorts last), name as the tiebreaker.
    model_usage = [
        ModelConcurrencyUsage(model=spec.model, limit=spec.limit, running=running)
        for spec, running in zip(concurrency_specs, concurrency_running)
    ]
    model_usage.sort(
        key=lambda row: (row.running is None, -(row.running or 0), row.model)
    )

    # Reset-button state only exists where there are counters to clear;
    # in local mode the plugin keeps nothing a reset could forgive.
    resets = None
    if mode == "redis":
        resets = {
            kind: await _reset_status(session, target.id, kind, now)
            for kind in RESET_WINDOW_SECONDS
        }

    # Keyed by id(): the specs are plain dataclasses, so two windows of the
    # same shape compare equal and a value-keyed dict would collapse
    # distinct windows onto one read; identity keys cannot collide.
    state_by_spec = {id(spec): state for spec, state in zip(all_specs, states)}
    return RateLimitsResponse(
        mode=mode,
        generated_at=now,
        usage_error=usage_error,
        free=FreeWindowUsage(
            enabled=free.enabled,
            active=free.is_active(now, rlc.timezone),
            start=free.start,
            end=free.end,
            days=free.days,
        ),
        user=UserRateLimits(
            rpm_limit=view.rpm_limit,
            tpm_limit=view.tpm_limit,
            weekly_limit=view.weekly_token_limit,
            monthly_limit=view.monthly_token_limit,
            concurrency_default=rlc.default_concurrency,
            windows=[
                _window_usage(spec, state_by_spec[id(spec)]) for spec in user_specs
            ],
            model_concurrency=model_usage,
            resets=resets,
        ),
    )


# The cross-user table's usage columns, mapped to the window each one
# reads off a row (kind, period) — the same identities the single-user
# endpoint emits. A user with no effective limit for an axis has no
# window there, and the column sorts them as zero.
_USER_TABLE_WINDOWS = {
    "rpm": ("query", "60s"),
    "tpm": ("token_rolling", "60s"),
    "weekly": ("token_rolling", "604800s"),
    "monthly": ("token_calendar", "each_month"),
}
# The table's landing order: the biggest live monthly burn first (a user
# burning their quota is the row an admin is looking for), idle rows
# under it by quota size, ties and the name column alphabetically.
_USER_TABLE_DEFAULT_SORT = "-monthly"
_USER_TABLE_SORTABLE = ("username",) + tuple(_USER_TABLE_WINDOWS)


def _user_table_sort_token(sort_by: Optional[str]) -> Tuple[str, bool]:
    """The validated (field, descending) pair the table sorts on.

    One click is one column, so a multi-key string is refused rather than
    silently truncated; an empty value falls back to the default order.
    """
    token = (sort_by or "").strip() or _USER_TABLE_DEFAULT_SORT
    descending = token.startswith("-")
    field = token[1:] if descending else token
    if field not in _USER_TABLE_SORTABLE:
        raise InvalidException(
            message=f"sort_by must be one of {sorted(_USER_TABLE_SORTABLE)}"
        )
    return field, descending


def _user_table_value(
    field: str, windows: List[RateLimitWindowUsage]
) -> Tuple[int, int]:
    """One row's sort key for a usage column: (current value, limit).

    The live counter is the primary key — a user actually burning the
    budget outranks every idle account, however generous their cap. The
    limit only breaks ties between equal counters (zero-usage rows order
    by quota size, busiest cap first under a descending sort). A
    ``None`` count — local mode, or a failed Redis read — reads as zero:
    without a number the row cannot claim a rank. An axis with no window
    at all sorts as (0, 0).
    """
    kind, period = _USER_TABLE_WINDOWS[field]
    for window in windows:
        if window.kind == kind and window.period == period:
            return (window.used or 0, window.limit)
    return (0, 0)


async def _active_user_principals(session, user_id: Optional[int]) -> List[Principal]:
    """The active USER principals behind the table's rows.

    Module-level indirection like ``_reader_factory``: tests call the
    handler directly against session fakes, and standing this query in
    keeps those fakes free of SQLModel row-selection logic.
    """
    stmt = select(Principal).where(
        Principal.kind == PrincipalType.USER,
        Principal.is_active.is_(True),
    )
    if user_id is not None:
        stmt = stmt.where(Principal.id == user_id)
    return list((await session.exec(stmt)).all())


@router.get(
    "/rate-limits/users",
    response_model=UsersRateLimitsResponse,
    response_model_exclude_none=True,
)
async def list_user_rate_limits(
    session: SessionDep,
    user: CurrentUserDep,
    ctx: TenantContextDep,
    request: Request,
    page: int = 1,
    perPage: int = 10,
    sort_by: Optional[str] = None,
    user_id: Optional[int] = None,
):
    """The admin's cross-user limits table, one row per active user.

    Platform-wide admins only (``is_platform_wide`` — the gate the
    single-user expansion applies): the rows carry every account's
    effective limits and live counters, cross-Org data by construction.

    Answers ``page`` / ``perPage`` / ``sort_by`` like the other list
    endpoints, plus the UI picker's ``user_id`` filter. Usage columns
    sort on the live counter first; the limit breaks ties between equal
    counters, so idle rows order by quota size. ``username`` sorts
    alphabetically. Sorting must see every
    row's counter, so counters are read for the whole filtered set in
    one batched Redis pipeline and the page is sliced in memory; the
    attribute hydration runs for the returned rows only.

    Args:
        session: async DB session.
        user: the authenticated caller.
        ctx: the request's tenant context; gates the endpoint entirely.
        request: the FastAPI request; the server config lives on app.state.
        page: 1-based page number (clamped to >= 1).
        perPage: rows per page, clamped to 1..200.
        sort_by: one column token, ``-`` prefix for descending; empty =
            the default (busiest monthly budget first).
        user_id: restrict the table to one user (the picker filter).

    Returns:
        UsersRateLimitsResponse: the page of rows with limits and live
        counters in redis mode, limits without counters in local mode,
        and an empty page when rate limiting is disabled.
    """
    if not is_platform_wide(user, ctx):
        raise ForbiddenException(
            message="Only platform-wide admins may list user rate limits"
        )
    # Plain scalar params arrive unvalidated from hand-rolled callers;
    # clamp here so a bad page just answers an empty page.
    page = max(1, page)
    perPage = max(1, min(perPage, 200))
    cfg: Config = request.app.state.server_config
    mode = _mode(cfg.gateway_rate_limit)
    now = datetime.now(timezone.utc)

    if mode == "disabled":
        return UsersRateLimitsResponse(
            items=[],
            pagination=Pagination(page=page, perPage=perPage, total=0, totalPage=0),
            mode=mode,
            generated_at=now,
        )

    field, descending = _user_table_sort_token(sort_by)
    principals = await _active_user_principals(session, user_id)
    rlc, _ = await effective_rate_limit(session, cfg)
    group_snapshots = await load_group_limit_snapshots(
        session, [p.id for p in principals]
    )

    per_user_specs: List[List[RateLimitWindowSpec]] = []
    all_specs: List[RateLimitWindowSpec] = []
    for principal in principals:
        view = effective_limits_view(
            principal,
            group_snapshots.get(principal.id, []),
            rlc,
        )
        specs = user_limit_windows(rlc, principal.id, view)
        per_user_specs.append(specs)
        all_specs.extend(specs)

    states: List[Optional[RateLimitWindowState]]
    usage_error = None
    if mode == "redis" and all_specs:
        reader = _reader(request, cfg)
        states, usage_error = await read_windows(
            reader, all_specs, now=now, timezone_name=rlc.timezone
        )
    else:
        # Local mode keeps no counters, and an empty selection has
        # nothing to read: windows render with limits only.
        states = [None] * len(all_specs)

    # Regroup the flat read onto its owner. Keyed by id(): the specs are
    # plain dataclasses, so two windows of the same shape compare equal
    # and a value-keyed dict would collapse distinct windows onto one
    # read; identity keys cannot collide (the single-user endpoint's
    # trick, reused so the two cannot drift).
    state_by_spec = {id(spec): state for spec, state in zip(all_specs, states)}
    rows = [
        (
            principal,
            [_window_usage(spec, state_by_spec[id(spec)]) for spec in user_specs],
        )
        for principal, user_specs in zip(principals, per_user_specs)
    ]

    # Stable two-pass sort: username ascending as the tiebreak, then the
    # requested column — reversing only the second pass keeps the
    # tiebreak ascending under a descending column.
    rows.sort(key=lambda row: (row[0].name or "").lower())
    if field == "username":
        if descending:
            rows.reverse()
    else:
        rows.sort(
            key=lambda row: _user_table_value(field, row[1]),
            reverse=descending,
        )

    total = len(rows)
    start = (page - 1) * perPage
    page_rows = rows[start : start + perPage]

    # Tooltip labels are page-scoped: they only render for rows the
    # admin sees on this page.
    labels = await bulk_user_attribute_labels(session, [p.id for p, _ in page_rows])
    items = [
        UserRateLimitUsage(
            user_id=principal.id,
            username=principal.name,
            full_name=principal.display_name,
            is_admin=bool(principal.is_admin),
            profession=labels.get(principal.id, {}).get("profession"),
            department=labels.get(principal.id, {}).get("department"),
            business_unit=labels.get(principal.id, {}).get("business_unit"),
            windows=windows,
        )
        for principal, windows in page_rows
    ]
    return UsersRateLimitsResponse(
        items=items,
        pagination=Pagination(
            page=page,
            perPage=perPage,
            total=total,
            totalPage=ceil(total / perPage) if total else 0,
        ),
        mode=mode,
        generated_at=now,
        usage_error=usage_error,
    )


async def _reset_user_names(
    session, user_ids: List[int]
) -> Dict[int, Tuple[Optional[str], Optional[str]]]:
    """``user_id -> (login, display name)`` for the resets table's name
    column.

    Module-level indirection like ``_active_user_principals``: tests stub
    it instead of standing up row selection. Deactivated accounts resolve
    too — an audit row outlives the user's active flag — and a deleted
    account simply drops out of the mapping.
    """
    ids = list(dict.fromkeys(user_ids))
    if not ids:
        return {}
    stmt = select(Principal).where(Principal.id.in_(ids))
    principals = (await session.exec(stmt)).all()
    return {p.id: (p.name, p.display_name) for p in principals}


@router.get(
    "/rate-limits/resets",
    response_model=RateLimitResetsResponse,
    response_model_exclude_none=True,
)
async def list_rate_limit_resets(
    session: SessionDep,
    user: CurrentUserDep,
    ctx: TenantContextDep,
    page: int = 1,
    perPage: int = 10,
    user_id: Optional[int] = None,
    outcome: Optional[str] = None,
):
    """The admin's rate-limit resets table, one row per recorded press.

    Platform-wide admins only (``is_platform_wide`` — the gate the user
    limits table applies): the rows are cross-Org audit data by
    construction.

    Answers ``page`` / ``perPage`` like the other list endpoints, plus the
    UI's ``user_id`` and ``outcome`` filters. ``outcome`` narrows the table
    to one verdict (``approved`` or ``rejected``); absent, every press is
    returned — the approved-only landing view is the UI's default state,
    not a server-side one. Rows order newest press first, ``id`` breaking
    ties so a same-second pair keeps its insertion order. Names hydrate
    for the returned rows only.

    Args:
        session: async DB session.
        user: the authenticated caller.
        ctx: the request's tenant context; gates the endpoint entirely.
        page: 1-based page number (clamped to >= 1).
        perPage: rows per page, clamped to 1..200.
        user_id: restrict the table to one budget owner (the picker).
        outcome: ``approved`` or ``rejected``; None = every press.

    Returns:
        RateLimitResetsResponse: the page of recorded presses with the
        owner's display names resolved.
    """
    if not is_platform_wide(user, ctx):
        raise ForbiddenException(
            message="Only platform-wide admins may list rate-limit resets"
        )
    # Plain scalar params arrive unvalidated from hand-rolled callers;
    # clamp here so a bad page just answers an empty page.
    page = max(1, page)
    perPage = max(1, min(perPage, 200))
    if outcome is not None and outcome not in (
        RESET_OUTCOME_APPROVED,
        RESET_OUTCOME_REJECTED,
    ):
        raise InvalidException(
            message="outcome must be one of "
            f"{sorted((RESET_OUTCOME_APPROVED, RESET_OUTCOME_REJECTED))}"
        )

    stmt = select(GatewayRateLimitReset)
    if user_id is not None:
        stmt = stmt.where(GatewayRateLimitReset.user_id == user_id)
    if outcome is not None:
        stmt = stmt.where(GatewayRateLimitReset.outcome == outcome)
    rows = list((await session.exec(stmt)).all())
    # Newest press first; the id breaks ties so a same-second pair keeps
    # its insertion order under the reverse sort.
    rows.sort(key=lambda row: (row.reset_at, row.id or 0), reverse=True)

    total = len(rows)
    start = (page - 1) * perPage
    page_rows = rows[start : start + perPage]

    names = await _reset_user_names(session, [row.user_id for row in page_rows])
    items = [
        RateLimitResetUsage(
            id=row.id,
            user_id=row.user_id,
            username=names.get(row.user_id, (None, None))[0],
            full_name=names.get(row.user_id, (None, None))[1],
            kind=row.kind,
            reset_at=row.reset_at,
            outcome=row.outcome,
            work_type=row.work_type,
            rejection_reason=row.rejection_reason,
            description=row.description,
        )
        for row in page_rows
    ]
    return RateLimitResetsResponse(
        items=items,
        pagination=Pagination(
            page=page,
            perPage=perPage,
            total=total,
            totalPage=ceil(total / perPage) if total else 0,
        ),
    )


@router.post("/rate-limits/reset", response_model=RateLimitResetStatus)
async def reset_rate_limit(
    body: RateLimitResetCreate,
    session: SessionDep,
    user: CurrentUserDep,
    ctx: TenantContextDep,
    request: Request,
):
    """Forgive the target user's weekly or monthly token budget mid-window.

    Deletes the gateway's Redis counters of that window kind (the budget
    restarts from zero on the next request), records the press in
    ``gateway_rate_limit_resets`` and returns the fresh quota state.

    A self-service press is first judged by the decision model: a
    description the model confidently rejects ("no concrete work task")
    leaves the counters alone and is recorded as a rejected audit row —
    the response carries ``status=rejected`` with the model's rejection
    reason. An unreachable decision model approves the press (fail-open);
    an admin pressing on a user's behalf is not gated.

    Scoping mirrors ``get_rate_limits``: self by default, a foreign
    ``user_id`` only for a platform-wide admin. An admin bypasses the
    floating quota — their press is a support action, recorded with
    ``reset_by_user_id`` and never counted against the user's own
    allowance.

    Args:
        body: the window kind, the required reason note, and optionally
            the user to reset (admin expansion).
        session: async DB session.
        user: the authenticated caller.
        ctx: the request's tenant context; gates the admin expansion.
        request: the FastAPI request; the server config lives on app.state.

    Returns:
        RateLimitResetStatus: the quota state after the press.
    """
    cfg: Config = request.app.state.server_config
    rlc = cfg.gateway_rate_limit
    if _mode(rlc) != "redis":
        raise InvalidException(
            message="Rate-limit resets require the Redis counter backend"
        )
    if body.kind not in _RESET_PERIODS:
        raise InvalidException(message=f"kind must be one of {sorted(_RESET_PERIODS)}")
    if not body.description.strip():
        raise InvalidException(message="description is required")

    target = await _resolve_target(session, user, ctx, body.user_id)
    now = datetime.now(timezone.utc)

    # Everything the reset forgives: the user-level long token windows of
    # this kind, the user's own limits and the default-plan fallback alike.
    # A user with no such window has nothing to forgive. The key layout
    # does not depend on the resolved quotas, but the specs go through the
    # same view as the read endpoint so the two never disagree.
    view = effective_limits_view(
        target,
        (await load_group_limit_snapshots(session, [target.id])).get(target.id, []),
        rlc,
    )
    windows = user_limit_windows(rlc, target.id, view) + default_plan_windows(
        rlc, target.id
    )
    counter_keys = [
        w.redis_key for w in windows if w.period == _RESET_PERIODS[body.kind]
    ]
    if not counter_keys:
        raise InvalidException(
            message=f"No {body.kind} budget is configured for this user"
        )

    status = await _reset_status(session, target.id, body.kind, now)
    if not is_platform_wide(user, ctx) and status.available <= 0:
        raise BadRequestException(message="Reset quota exhausted for this period")

    # The decision-model gate only judges the person whose budget it is: the
    # description question ("what task are you working on?") is addressed to
    # the requester, and an admin pressing on someone's behalf is a trusted
    # support action already exempt from the quota. A None verdict (gate
    # disabled, unreachable, or a non-self press) approves unconditionally —
    # the gate may never hold a budget hostage to its own availability.
    verdict = None
    if user.id == target.id:
        verdict = await _classify_with_d3(body.description.strip())

    if verdict is not None and (
        not verdict.answered and verdict.confidence > D3_REJECT_CONFIDENCE
    ):
        # A confident "no real task": record the refusal, touch nothing. The
        # counters stay and the quota stays — only the audit trail is kept,
        # and the caller gets the verdict to phrase for the user.
        await GatewayRateLimitReset.create(
            session,
            GatewayRateLimitReset(
                user_id=target.id,
                kind=body.kind,
                reset_at=now,
                reset_by_user_id=None,
                description=body.description.strip(),
                outcome=RESET_OUTCOME_REJECTED,
                work_type=verdict.work_type,
                rejection_reason=verdict.rejection_reason,
            ),
        )
        fresh = await _reset_status(session, target.id, body.kind, now)
        fresh.status = RESET_OUTCOME_REJECTED
        fresh.rejection_reason = verdict.rejection_reason
        return fresh

    # The row lands before the counters are deleted: the counter rebuild
    # keys its cutoff off this table, so a rebuild that races the delete
    # must never see deleted counters without their cutoff.
    await GatewayRateLimitReset.create(
        session,
        GatewayRateLimitReset(
            user_id=target.id,
            kind=body.kind,
            reset_at=now,
            reset_by_user_id=user.id if user.id != target.id else None,
            description=body.description.strip(),
            outcome=RESET_OUTCOME_APPROVED,
            work_type=verdict.work_type if verdict is not None else None,
        ),
    )

    reader = _reader(request, cfg)
    await reader.client.delete(*counter_keys)

    return await _reset_status(session, target.id, body.kind, now)

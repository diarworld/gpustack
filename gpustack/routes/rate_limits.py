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
from typing import Any, Dict, List, Optional

import httpx
from fastapi import APIRouter, Request
from sqlalchemy import or_
from sqlmodel import select

from gpustack import envs
from gpustack.api.exceptions import (
    BadRequestException,
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
from gpustack.schemas.gateway_rate_limit_resets import (
    RESET_OUTCOME_APPROVED,
    RESET_OUTCOME_REJECTED,
    GatewayRateLimitReset,
)
from gpustack.schemas.model_routes import MyModel
from gpustack.schemas.rate_limits import (
    FreeWindowUsage,
    ModelConcurrencyUsage,
    RateLimitResetCreate,
    RateLimitResetStatus,
    RateLimitWindowUsage,
    RateLimitsResponse,
    UserRateLimits,
)
from gpustack.schemas.users import User
from gpustack.server.deps import CurrentUserDep, SessionDep, TenantContextDep

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

    ``rejection_reason`` is ``None`` when the model named no cause (or
    answered the literal string "None", its own way of saying none); the
    reject decision itself rides on ``answered`` + ``confidence``.
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


def _d3_answer_bool(answer: Dict[str, Any]) -> Optional[bool]:
    """The yes/no the model returned, in whatever clothing it arrives.

    The wire contract says ``choice`` is boolean for a noul question, but a
    lenient read costs nothing: boolean JSON, or the usual string spellings
    ("true"/"false"/"yes"/"no"). Anything else is not a verdict.
    """
    choice = answer.get("choice")
    if isinstance(choice, bool):
        return choice
    if isinstance(choice, str):
        lowered = choice.strip().lower()
        if lowered in ("true", "yes"):
            return True
        if lowered in ("false", "no"):
            return False
    return None


def _d3_confidence(answer: Dict[str, Any]) -> float:
    """The model's certainty in its answer, clamped into 0..1.

    A missing or malformed confidence reads as certain: the reject decision
    keys on a *strong* no, and an answer without a stated confidence was
    not a hedge the model chose to publish — treating it as weak would let
    every malformed verdict through. The 0..1 clamp also accepts a
    percentage that slipped through the service.
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
        _d3_answer_bool(answered_answer) if isinstance(answered_answer, dict) else None
    )
    if answered is None:
        logger.warning(
            "reset decision model returned no usable answered verdict, "
            "approving reset: %s",
            str(verdict)[:200],
        )
        return None
    work_type_answer = answers.get("WorkType") if isinstance(answers, dict) else None
    reason_answer = (
        answers.get("rejection_reason") if isinstance(answers, dict) else None
    )
    return _D3Verdict(
        answered=answered,
        confidence=(
            _d3_confidence(answered_answer)
            if isinstance(answered_answer, dict)
            else 1.0
        ),
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

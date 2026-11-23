"""Resolves a user's effective gateway limits out of their groups.

The ``gpustack-rate-limit`` plugin enforces one shared allowance per user
(the non-fallback ``user-<id>`` combo), so the group-level quotas admins
set on the Groups page must be resolved onto that combo server-side: the
plugin never sees groups, it only ever receives the current effective
numbers (see :mod:`gpustack.server.gateway_rate_limit_reconciler`).

The priority, per field and independently for RPM, TPM, the weekly and
the monthly token budget:

1. the user's explicit value — it wins in both directions;
2. otherwise the smallest value across the user's groups (transitively,
   through nested subgroups). A group limit both tightens and relaxes the
   deployment default — priority over the default is the point of the
   feature ("this team gets more RPM than everyone else");
3. otherwise the deployment default (``default_rpm`` / ``default_tpm`` /
   ``default_monthly_tokens``). The weekly budget has no deployment
   default: a default budget silently capping every account is not a safe
   thing to imply.

Per-model in-flight caps resolve the same way, one model at a time: the
user's map entry wins (a stored 0 is an explicit unlimited), otherwise
the smallest non-zero group value — a group 0 means "this group does not
constrain the model" and never lifts a cap by itself — otherwise the
deployment ``default_concurrency``, otherwise the model carries no cap
and stays out of the map (the combo's block default covers it).

Groups are loaded with :func:`load_group_limit_snapshots`; the resolved
values ride to the enforcement and read-side builders inside
:class:`EffectiveLimitsView`, which duck-types the Principal those
builders already accept (they read ``id``, ``is_admin`` and the five
limit fields and nothing else).
"""

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence

from sqlmodel import select

from gpustack.config.config import GatewayRateLimitConfig
from gpustack.schemas.principals import Principal, PrincipalMembership, PrincipalType

# The nesting walk's depth cap — same bound the membership endpoints
# enforce at add time; here it only bounds the climb if hand-edited rows
# ever formed a cycle.
GROUP_RESOLUTION_DEPTH = 32


@dataclass
class EffectiveLimits:
    """One user's resolved quotas: what the gateway will actually enforce."""

    rpm_limit: Optional[int] = None
    tpm_limit: Optional[int] = None
    weekly_token_limit: Optional[int] = None
    monthly_token_limit: Optional[int] = None
    # Per-model in-flight caps after resolution: only the models with an
    # explicit user entry or a group cap are named; everything else rides
    # the deployment concurrency default.
    model_concurrency_limits: Optional[Dict[str, int]] = None


@dataclass
class EffectiveLimitsView:
    """The Principal shape the limit builders read, carrying resolved values.

    ``id`` and ``is_admin`` pass through from the real user; the five
    limit fields hold the group-resolved numbers. Fed to
    ``user_limits_combination`` (enforcement) and
    ``user_limit_windows`` / ``model_concurrency_specs`` (the usage
    endpoint) so the two sides can never disagree about what applies.
    """

    id: int
    is_admin: bool = False
    rpm_limit: Optional[int] = None
    tpm_limit: Optional[int] = None
    weekly_token_limit: Optional[int] = None
    monthly_token_limit: Optional[int] = None
    model_concurrency_limits: Optional[Dict[str, int]] = None


def _resolve_scalar(
    user_value: Optional[int],
    group_values: Iterable[Optional[int]],
    default: Optional[int],
) -> Optional[int]:
    """One scalar field through the three-level priority."""
    if user_value is not None:
        return user_value
    candidates = [value for value in group_values if value is not None]
    if candidates:
        return min(candidates)
    return default


def _resolve_concurrency(
    user_map: Optional[Dict[str, int]],
    group_maps: Sequence[Optional[Dict[str, int]]],
) -> Optional[Dict[str, int]]:
    """The per-model map after resolution, or None when nothing constrains.

    Only models the user named or a group caps land in the map; a group
    0 never contributes (it declares "no constraint", not "unlimited for
    everyone"), so it can neither tighten nor lift the default. Models
    nobody names stay out entirely — the combo's block default (the
    deployment ``default_concurrency``) covers them.
    """
    models = set(user_map or {})
    for group_map in group_maps:
        models.update(group_map or {})
    resolved: Dict[str, int] = {}
    for model in models:
        if user_map and model in user_map:
            resolved[model] = user_map[model]
            continue
        caps = [
            group_map[model]
            for group_map in group_maps
            if group_map and group_map.get(model)
        ]
        if caps:
            resolved[model] = min(caps)
    return resolved or None


def resolve_effective_limits(
    user: Any,
    groups: Sequence[Any],
    rlc: GatewayRateLimitConfig,
) -> EffectiveLimits:
    """Resolve one user's quotas from their groups and the deployment
    defaults, following the priority in the module docstring.

    ``user`` and the ``groups`` entries are duck-typed Principal rows:
    only the five limit fields are read.
    """
    group_rpm = [getattr(g, "rpm_limit", None) for g in groups]
    group_tpm = [getattr(g, "tpm_limit", None) for g in groups]
    group_weekly = [getattr(g, "weekly_token_limit", None) for g in groups]
    group_monthly = [getattr(g, "monthly_token_limit", None) for g in groups]
    group_concurrency = [getattr(g, "model_concurrency_limits", None) for g in groups]

    return EffectiveLimits(
        rpm_limit=_resolve_scalar(
            getattr(user, "rpm_limit", None), group_rpm, rlc.default_rpm
        ),
        tpm_limit=_resolve_scalar(
            getattr(user, "tpm_limit", None), group_tpm, rlc.default_tpm
        ),
        weekly_token_limit=_resolve_scalar(
            getattr(user, "weekly_token_limit", None), group_weekly, None
        ),
        monthly_token_limit=_resolve_scalar(
            getattr(user, "monthly_token_limit", None),
            group_monthly,
            rlc.default_monthly_tokens,
        ),
        model_concurrency_limits=_resolve_concurrency(
            getattr(user, "model_concurrency_limits", None),
            group_concurrency,
        ),
    )


def effective_limits_view(
    user: Any,
    groups: Sequence[Any],
    rlc: GatewayRateLimitConfig,
) -> EffectiveLimitsView:
    """The resolved quotas wrapped in the Principal shape the limit
    builders already accept — the single adapter every caller goes
    through, so enforcement and the usage endpoint resolve identically."""
    resolved = resolve_effective_limits(user, groups, rlc)
    return EffectiveLimitsView(
        id=user.id,
        is_admin=bool(getattr(user, "is_admin", False)),
        rpm_limit=resolved.rpm_limit,
        tpm_limit=resolved.tpm_limit,
        weekly_token_limit=resolved.weekly_token_limit,
        monthly_token_limit=resolved.monthly_token_limit,
        model_concurrency_limits=resolved.model_concurrency_limits,
    )


async def load_group_limit_snapshots(
    session, user_ids: List[int]
) -> Dict[int, List[Principal]]:
    """The live GROUP rows each user's quotas draw on, transitively.

    Returns ``{user_id: [Principal, ...]}``; users with no groups map to
    an empty list. One pass over the membership edges and the principal
    kinds (both tables are small) plus an in-memory climb, so the whole
    deployment resolves in two queries instead of one per user. Org
    memberships never contribute — only GROUP-kind parents do.
    """
    user_ids = [uid for uid in set(user_ids) if uid is not None]
    if not user_ids:
        return {}

    edges = (
        await session.exec(
            select(
                PrincipalMembership.member_principal_id,
                PrincipalMembership.parent_principal_id,
            ).where(PrincipalMembership.deleted_at.is_(None))
        )
    ).all()
    kinds = dict(
        (
            await session.exec(
                select(Principal.id, Principal.kind).where(
                    Principal.deleted_at.is_(None)
                )
            )
        ).all()
    )

    by_member: Dict[int, List[int]] = {}
    for member_id, parent_id in edges:
        by_member.setdefault(member_id, []).append(parent_id)

    def group_parents(principal_id: int) -> List[int]:
        return [
            parent_id
            for parent_id in by_member.get(principal_id, [])
            if kinds.get(parent_id) == PrincipalType.GROUP
        ]

    closure: Dict[int, set] = {}
    needed: set = set()
    for user_id in user_ids:
        reached: set = set()
        frontier = group_parents(user_id)
        depth = 0
        while frontier and depth < GROUP_RESOLUTION_DEPTH:
            reached.update(frontier)
            next_frontier = {
                parent_id
                for group_id in frontier
                for parent_id in group_parents(group_id)
                if parent_id not in reached
            }
            frontier = sorted(next_frontier)
            depth += 1
        closure[user_id] = reached
        needed.update(reached)

    if not needed:
        return {user_id: [] for user_id in user_ids}

    groups = (
        await session.exec(
            select(Principal).where(
                Principal.id.in_(needed),
                Principal.kind == PrincipalType.GROUP,
                Principal.deleted_at.is_(None),
            )
        )
    ).all()
    by_id = {group.id: group for group in groups}

    return {
        user_id: [by_id[gid] for gid in closure[user_id] if gid in by_id]
        for user_id in user_ids
    }

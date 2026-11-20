"""Who may read the my-models utilization metrics, and in what order the
routes match.

The card grid on /models/user-models is a user-facing page, so the batch
endpoint rides ``my_models_router`` (mounted for any authenticated user)
and must be declared before the router's ``GET /{id}`` — Starlette matches
in declaration order and an int path parameter would otherwise turn a
request for ``/metrics`` into a 422 instead of falling through.
"""

from gpustack.api.auth import get_admin_user
from gpustack.routes.routes import api_router


def _routes(path: str):
    return [
        (index, route)
        for index, route in enumerate(api_router.routes)
        if route.path == path
    ]


def test_metrics_endpoint_is_mounted_for_any_authenticated_user():
    matches = _routes("/v2/my-models/metrics")
    assert len(matches) == 1, "the batch metrics endpoint must exist exactly once"
    route = matches[0][1]
    assert "GET" in route.methods
    assert get_admin_user not in [d.dependency for d in route.dependencies]


def test_my_models_list_tier_unchanged():
    matches = _routes("/v2/my-models")
    assert matches, "the my-models list route must stay mounted"
    route = matches[0][1]
    assert "GET" in route.methods


def test_metrics_matches_before_the_int_id_route():
    metrics = [index for index, _ in _routes("/v2/my-models/metrics")]
    by_id = [
        index
        for index, route in enumerate(api_router.routes)
        if route.path == "/v2/my-models/{id}"
    ]
    assert metrics and by_id, "both routes must be registered"
    assert min(metrics) < min(by_id), (
        "/metrics must be declared before /{id} or the int parameter "
        "answers with 422"
    )

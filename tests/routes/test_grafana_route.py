"""Who reaches the embedded Grafana through the ``/grafana`` proxy.

The embedded Grafana serves its dashboards to anonymous viewers, so the
proxy mount is the only auth boundary in front of them: any authenticated
user may open the monitoring links, while the sibling ``/prometheus``
proxy stays admin-only — the browser never calls it (Grafana's datasource
proxies server-side) and it exposes raw metric querying.
"""

from gpustack.api.auth import get_admin_user, get_current_user
from gpustack.routes.routes import api_router


def _proxy_routes(prefix: str):
    return [
        route for route in api_router.routes if route.path == f"{prefix}/{{path:path}}"
    ]


def test_the_grafana_proxy_is_open_to_any_authenticated_user():
    routes = _proxy_routes("/grafana")
    assert routes, "grafana proxy not mounted"
    for route in routes:
        dependencies = [dependency.dependency for dependency in route.dependencies]
        assert get_current_user in dependencies
        assert get_admin_user not in dependencies


def test_the_prometheus_proxy_stays_admin_only():
    routes = _proxy_routes("/prometheus")
    assert routes, "prometheus proxy not mounted"
    for route in routes:
        dependencies = [dependency.dependency for dependency in route.dependencies]
        assert get_admin_user in dependencies
        assert get_current_user not in dependencies

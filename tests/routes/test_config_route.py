"""Who may read and write the runtime config.

The GET is display-facing: the UI reads it at startup for the monitoring
links (``grafana_url``, the observability flag, the dashboard uids), so
every authenticated user may call it -- an admin gate here silently
blanked ``systemConfig`` for ordinary users and hid the monitoring
buttons. The PUT mutates runtime behavior and stays admin-only. Both
enforcements live at the mount sites (the worker mounts the same routers
behind the worker token), which is why this test reads the assembled
tree instead of the handlers.
"""

from gpustack.api.auth import get_admin_user, get_current_user
from gpustack.routes.routes import api_router


def _config_routes(method: str):
    return [
        route
        for route in api_router.routes
        if route.path == "/v2/config" and method in route.methods
    ]


def test_reading_the_config_is_open_to_any_authenticated_user():
    routes = _config_routes("GET")
    assert len(routes) == 1, "GET /config must be mounted exactly once"
    dependencies = [dependency.dependency for dependency in routes[0].dependencies]
    assert get_current_user in dependencies
    assert get_admin_user not in dependencies


def test_writing_the_config_stays_admin_only():
    routes = _config_routes("PUT")
    assert len(routes) == 1, "PUT /config must be mounted exactly once"
    dependencies = [dependency.dependency for dependency in routes[0].dependencies]
    assert get_admin_user in dependencies
    assert get_current_user not in dependencies

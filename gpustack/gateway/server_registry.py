"""The McpBridge registry entry that carries the GPUStack server service.

Every gateway plugin that calls back into the server (token-usage metrics
reports, rate-limit rejection reports) addresses it through this registry:
the gateway publishes no Kubernetes services of its own, so the endpoint
must be named as a McpBridge registry, not a raw Service FQDN. Lives in a
leaf module so the config builders in :mod:`gpustack.gateway.rate_limit`
can share it without an import cycle through the package ``__init__``.
"""

from gpustack.config.config import Config, GatewayModeEnum
from gpustack.gateway.client import McpBridgeRegistry

mcp_registry_port = 80


def get_gpustack_higress_registry(cfg: Config) -> McpBridgeRegistry:
    registry_type = "dns"
    domain = f"{cfg.service_discovery_name}.{cfg.get_namespace()}.svc"
    mcp_port = cfg.get_api_port()
    if cfg.gateway_mode != GatewayModeEnum.incluster:
        registry_type = "static"
        mcp_port = mcp_registry_port
        port = cfg.get_api_port()
        if cfg.gateway_mode == GatewayModeEnum.external:
            address = cfg.get_advertise_address()
        elif cfg.gateway_mode == GatewayModeEnum.embedded:
            address = "127.0.0.1"
        domain = f"{address}:{port}"

    mcp_registry_name = (
        "gpustack"
        if cfg.server_role() != Config.ServerRole.WORKER
        else "gpustack-worker"
    )
    registry = McpBridgeRegistry(
        type=registry_type,
        name=mcp_registry_name,
        port=mcp_port,
        protocol="http",
        domain=domain,
    )
    return registry

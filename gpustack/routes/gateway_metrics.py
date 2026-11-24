import logging

from fastapi import APIRouter, Depends

from gpustack.api.auth import gateway_token_auth
from gpustack.server.limit_event_collector import (
    GatewayLimitEvent,
    accumulate_limit_events,
)
from gpustack.server.metrics_collector import (
    ModelUsageMetrics,
    accumulate_gateway_metrics,
)
from pydantic import BaseModel
from typing import List

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/gateway-metrics", include_in_schema=False)
async def report_gateway_metrics(
    item: ModelUsageMetrics,
    _: None = Depends(gateway_token_auth),
):
    await accumulate_gateway_metrics([item])


class GatewayLimitEventReport(BaseModel):
    """Envelope for the rate-limit plugin's rejection reports. The plugin
    posts one event per rejected request; the list shape leaves room for
    batching without a wire-format change."""

    events: List[GatewayLimitEvent]


@router.post("/gateway-limit-events", include_in_schema=False)
async def report_gateway_limit_events(
    report: GatewayLimitEventReport,
    _: None = Depends(gateway_token_auth),
):
    await accumulate_limit_events(report.events)

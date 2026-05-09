"""Biblioteca de telemetria usada pelas DAGs da plataforma.

Uso típico dentro de uma task:

    from platform_telemetry import TelemetryContext

    with TelemetryContext.from_airflow(context, tenant_id) as tele:
        rows = extract_and_load(...)
        tele.add_rows_inserted(rows)

Na saída do `with`: publica pipeline_task (status, duração, linhas) via callback na API Core.
Em caso de exceção: publica status=failed com a mensagem e re-levanta.
"""
from platform_telemetry.client import PlatformClient
from platform_telemetry.context import TelemetryContext
from platform_telemetry.dbt import dbt_model_callback
from platform_telemetry.helpers import dw_connection, resolve_tenant_by_slug
from platform_telemetry.pipeline_runs import (
    pipeline_run_finalize,
    pipeline_run_start,
)
from platform_telemetry.watermarks import get_watermark, set_watermark

__all__ = [
    "PlatformClient",
    "TelemetryContext",
    "dbt_model_callback",
    "dw_connection",
    "resolve_tenant_by_slug",
    "get_watermark",
    "set_watermark",
    "pipeline_run_start",
    "pipeline_run_finalize",
]

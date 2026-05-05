"""Biblioteca de telemetria usada pelas DAGs da plataforma.

Uso típico dentro de uma task:

    from platform_telemetry import TelemetryContext

    with TelemetryContext.from_airflow(context, tenant_id, service_instance_id) as tele:
        rows = extract_and_load(...)
        tele.add_rows_inserted(rows)
        tele.emit("bytes_processed", bytes_count, "bytes")

Na saída do `with`: publica job_run (status, duração, linhas) via callback na API Core.
Em caso de exceção: publica status=failed com a mensagem e re-levanta.
"""
from platform_telemetry.client import PlatformClient
from platform_telemetry.context import TelemetryContext
from platform_telemetry.dbt import (
    DbtRunSummary,
    dbt_model_callback,
    emit_dbt_metrics,
    parse_run_results,
    run_dbt,
)
from platform_telemetry.helpers import dw_connection, resolve_tenant_by_slug
from platform_telemetry.sync import sync_run_finalize, sync_run_start
from platform_telemetry.watermarks import get_watermark, set_watermark

__all__ = [
    "PlatformClient",
    "TelemetryContext",
    "DbtRunSummary",
    "run_dbt",
    "parse_run_results",
    "emit_dbt_metrics",
    "dbt_model_callback",
    "dw_connection",
    "resolve_tenant_by_slug",
    "get_watermark",
    "set_watermark",
    "sync_run_start",
    "sync_run_finalize",
]

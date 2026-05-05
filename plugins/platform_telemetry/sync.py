"""Callbacks de DAG-level pra publicar SyncRun (rollup de DAG run).

Uso na DAG:

    from platform_telemetry.sync import sync_run_start, sync_run_finalize

    with DAG(
        "luminea__conta_azul_etl",
        on_success_callback=lambda ctx: sync_run_finalize(
            ctx, tenant_slug="luminea", status="success"),
        on_failure_callback=lambda ctx: sync_run_finalize(
            ctx, tenant_slug="luminea", status="failed"),
        ...
    ) as dag:
        @task
        def _start_sync(**ctx):
            sync_run_start(ctx, tenant_slug="luminea")

Por que `_start_sync` em vez de `on_dag_run_start`: Airflow 3.x ainda nao
oferece um hook estavel pra "comecou de verdade". Hook `on_execute` e'
por-task. Solucao pratica: primeira task da DAG dispara o start callback.
Idempotente — se a DAG re-roda, finalize recomputa do zero.
"""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from platform_telemetry.client import PlatformClient
from platform_telemetry.context import _map_airflow_run_type
from platform_telemetry.helpers import resolve_tenant_by_slug

logger = logging.getLogger(__name__)


def _resolve_triggered_by(dag_run: Any) -> str | None:
    run_type = getattr(dag_run, "run_type", None)
    if run_type == "manual":
        return getattr(dag_run, "triggered_by", None) or "airflow-ui"
    if run_type == "scheduled":
        return "airflow-scheduler"
    return None


def _started_at(dag_run: Any) -> str:
    started = (
        getattr(dag_run, "start_date", None)
        or getattr(dag_run, "logical_date", None)
        or getattr(dag_run, "execution_date", None)
        or datetime.now(timezone.utc)
    )
    return started.isoformat() if hasattr(started, "isoformat") else str(started)


def sync_run_start(
    context: dict,
    *,
    tenant_slug: str,
    service_instance_id: str | None = None,
) -> None:
    """Cria a SyncRun com status=running. Idempotente.

    Chamada como primeira task da DAG. Se start callback ja rodou (retry),
    o backend faz UPDATE no lugar de INSERT.
    """
    dag_run = context["dag_run"]
    payload = {
        "dag_id": dag_run.dag_id,
        "dag_run_id": dag_run.run_id,
        "run_type": _map_airflow_run_type(getattr(dag_run, "run_type", None)),
        "triggered_by": _resolve_triggered_by(dag_run),
        "started_at": _started_at(dag_run),
        "service_instance_id": service_instance_id,
    }
    try:
        tenant = resolve_tenant_by_slug(tenant_slug)
        client = PlatformClient()
        try:
            client.start_sync_run(tenant["id"], payload)
        finally:
            client.close()
    except Exception:
        # Telemetria NUNCA pode quebrar a DAG. Sem SyncRun, o tenant view
        # mostra orphan job_runs (depois o backfill pode reconciliar).
        logger.exception("sync_run_start failed (tenant=%s)", tenant_slug)


def sync_run_finalize(
    context: dict,
    *,
    tenant_slug: str,
    status: str,
) -> None:
    """Atualiza a SyncRun com rollup das job_runs filhas. Idempotente.

    Chamada via on_success_callback / on_failure_callback da DAG.
    `status` reflete o que o Airflow viu — backend pode sobrescrever pra
    'partial_failed' se rollup das filhas indicar.
    """
    dag_run = context["dag_run"]
    finished_at = (
        getattr(dag_run, "end_date", None) or datetime.now(timezone.utc)
    )
    payload = {
        "dag_id": dag_run.dag_id,
        "dag_run_id": dag_run.run_id,
        "finished_at": (
            finished_at.isoformat() if hasattr(finished_at, "isoformat") else str(finished_at)
        ),
        "request_status": status,
    }
    try:
        tenant = resolve_tenant_by_slug(tenant_slug)
        client = PlatformClient()
        try:
            client.finalize_sync_run(tenant["id"], payload)
        finally:
            client.close()
    except Exception:
        logger.exception("sync_run_finalize failed (tenant=%s)", tenant_slug)

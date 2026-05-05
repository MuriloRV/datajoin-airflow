"""Watermarks de ETL — controle de progresso incremental por tenant/source/entity.

Resolve "de onde paro/continuo?" pra cargas incrementais. Cada conector
decide o shape do watermark — pra plataforma e' opaco (jsonb).

Onde mora: tabela `etl_watermarks` na METADATA DB da datajoin
(database `platform_metadata` no servidor `postgres`), nao no DW.
Razao detalhada na migration 0028:
- Watermark e' estado operacional da plataforma, nao dado de cliente.
- Migracao de tenant entre DWs nao perde historico.
- Sem variacao de DDL por engine de DW (jsonb/JSON/VARIANT).

Conector NAO acessa a metadata DB direto — vai via REST API
(`PlatformClient`). Mantem Airflow desacoplado do schema da metadata DB.

Convencao recomendada do shape:
    {"type": "timestamp", "value": "2026-05-01T12:00:00+00:00"}
    {"type": "id",        "value": 12345}
    {"type": "cursor",    "page_token": "abc..."}
    {"type": "compound",  "high_water_ts": "...", "page_token": "..."}

Uso tipico no loader:

    from platform_telemetry import get_watermark, set_watermark

    wm = get_watermark("luminea", "conta_azul", "customers")
    since = wm["value"] if wm else None
    rows = client.list_customers(since=since)
    new_high_water = max(c.updated_at for c in rows)
    set_watermark(
        "luminea", "conta_azul", "customers",
        {"type": "timestamp", "value": new_high_water.isoformat()},
        rows_loaded=len(rows),
        run_id=context["run_id"],
    )

Idempotencia: PUT sobrescreve. Re-rodar com mesmo (tenant, source,
entity) zera `updated_at` pra now().
"""
from __future__ import annotations

import logging
from typing import Any

from platform_telemetry.client import PlatformClient

log = logging.getLogger(__name__)


def get_watermark(
    tenant_slug: str,
    source: str,
    entity: str,
    *,
    client: PlatformClient | None = None,
) -> dict[str, Any] | None:
    """Le o watermark atual de (tenant, source, entity). None na primeira run.

    Args:
        tenant_slug: slug do tenant (ex: "luminea").
        source: nome do conector (ex: "conta_azul").
        entity: entidade extraida (ex: "customers").
        client: opcional. Se None, cria um novo PlatformClient (le envs
            PLATFORM_API_URL + PLATFORM_API_TOKEN). Util passar um existente
            pra reaproveitar conexao httpx em hot loop.

    Returns:
        Dict com o shape gravado por set_watermark, ou None se nao existe.
    """
    owns_client = client is None
    client = client or PlatformClient()
    try:
        return client.get_watermark(tenant_slug, source, entity)
    finally:
        if owns_client:
            client.close()


def set_watermark(
    tenant_slug: str,
    source: str,
    entity: str,
    watermark: dict[str, Any],
    *,
    rows_loaded: int | None = None,
    run_id: str | None = None,
    client: PlatformClient | None = None,
) -> None:
    """Upsert do watermark via API.

    Args:
        tenant_slug: slug do tenant.
        source: nome do conector.
        entity: entidade.
        watermark: dict com o novo cursor. Sera serializado como jsonb.
        rows_loaded: opcional. Quantas linhas a run gravou (informativo).
        run_id: opcional. Identificador da run (Airflow run_id, etc).
        client: opcional. Reaproveita PlatformClient existente.
    """
    owns_client = client is None
    client = client or PlatformClient()
    try:
        client.set_watermark(
            tenant_slug,
            source,
            entity,
            watermark,
            rows_loaded=rows_loaded,
            run_id=run_id,
        )
        log.info(
            "watermark gravado: %s/%s/%s -> %s (rows=%s)",
            tenant_slug,
            source,
            entity,
            watermark,
            rows_loaded,
        )
    finally:
        if owns_client:
            client.close()

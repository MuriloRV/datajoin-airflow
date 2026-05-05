"""Loader generico Conta Azul -> camada raw do DW.

Resolve a entidade pelo registry de extractors e delega DDL/UPSERT/fetch.
DAG so chama `load_entity_to_raw(client, conn, schema_raw, entity_name,
tenant_slug, run_id)` — sem if-else por entidade, sem SQL inline.

Convencao de naming:
- Tabela raw: `<schema_raw>.conta_azul__<entity_name>`
  (ex: "pessoas" -> `luminea_raw.conta_azul__pessoas`).
- Schema criado dinamicamente (CREATE SCHEMA IF NOT EXISTS) caso o
  seed nao tenha provisionado.

Watermarks (mig 0028):
- Se o extractor declarar INCREMENTAL_FIELD, loader le watermark via API
  da plataforma, passa como filtro pro fetch, e ao final grava o novo
  high-water = max(item.<INCREMENTAL_FIELD> visto).
- Se INCREMENTAL_FIELD=None: full refresh sempre, watermark nao envolvido.

Idempotencia: UPSERT por `id` (UUID estavel da API). Re-rodar e' safe —
atualiza loaded_at + sobrescreve campos mutaveis.
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from connectors.conta_azul.extractors import EXTRACTORS

if TYPE_CHECKING:
    import psycopg
    from connectors.conta_azul.client import ContaAzulClient


log = logging.getLogger(__name__)


def load_entity_to_raw(
    client: "ContaAzulClient",
    conn: "psycopg.Connection",
    schema_raw: str,
    entity_name: str,
    *,
    tenant_slug: str | None = None,
    run_id: str | None = None,
) -> int:
    """Extrai entidade do Conta Azul e faz upsert na camada raw.

    Args:
        client: ContaAzulClient ja autenticado (ou mock).
        conn: conexao Postgres aberta (autocommit recomendado).
        schema_raw: schema destino (ex: "luminea_raw").
        entity_name: chave do registry EXTRACTORS (ex: "pessoas").
        tenant_slug: slug do tenant — usado pra ler/gravar watermark.
            Se None, watermark nao e' lido nem gravado (extractor faz
            full refresh independentemente de INCREMENTAL_FIELD).
        run_id: identificador da run — gravado no watermark pra auditoria.

    Returns:
        Total de rows processadas (inseridas + atualizadas).

    Raises:
        KeyError: entity_name nao registrado em EXTRACTORS.
    """
    extractor_cls = EXTRACTORS.get(entity_name)
    if extractor_cls is None:
        raise KeyError(
            f"Entidade '{entity_name}' nao registrada em EXTRACTORS. "
            f"Disponiveis: {sorted(EXTRACTORS.keys())}"
        )
    extractor = extractor_cls()
    log.info("[%s] extractor=%s starting", entity_name, extractor_cls.__name__)

    # Le watermark se incremental + tenant fornecido. Import lazy pra
    # nao acoplar loader a platform_telemetry em uso fora do Airflow.
    watermark = None
    if extractor.INCREMENTAL_FIELD and tenant_slug:
        from platform_telemetry import get_watermark
        watermark = get_watermark(tenant_slug, "conta_azul", entity_name)
        log.info(
            "[%s] watermark lido: %s (incremental=%s)",
            entity_name, watermark, extractor.INCREMENTAL_FIELD,
        )

    with conn.cursor() as cur:
        log.info("[%s] CREATE SCHEMA + DDL", entity_name)
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_raw}"')
        # Advisory lock serializa DDL do mesmo entity entre task instances
        # concorrentes. Postgres tem race em pg_type catalog mesmo com
        # CREATE TABLE IF NOT EXISTS — duas transacoes podem passar pela
        # verificacao simultaneamente e tentar criar o type. Lock por hash
        # do (schema, entity) garante exclusao mutua sem afetar entities
        # diferentes. pg_advisory_lock e' session-scope, liberado no
        # commit/close.
        cur.execute(
            "SELECT pg_advisory_lock(hashtextextended(%s, 0))",
            (f"ddl:{schema_raw}:{entity_name}",),
        )
        try:
            cur.execute(extractor.DDL.format(schema=schema_raw))
        finally:
            cur.execute(
                "SELECT pg_advisory_unlock(hashtextextended(%s, 0))",
                (f"ddl:{schema_raw}:{entity_name}",),
            )

        total = 0
        max_seen: object | None = None
        upsert_sql = extractor.UPSERT_SQL.format(schema=schema_raw)
        log.info("[%s] starting fetch loop", entity_name)
        # Passa todo contexto util como kwargs — extractor pega o que precisa.
        fetch_kwargs = {
            "watermark": watermark,
            "dw_conn": conn,
            "schema_raw": schema_raw,
            "tenant_slug": tenant_slug,
            "run_id": run_id,
        }
        for item in extractor.fetch(client, **fetch_kwargs):
            cur.execute(upsert_sql, extractor.serialize_for_upsert(item))
            total += 1
            # Track high-water pra gravar no fim
            if extractor.INCREMENTAL_FIELD:
                v = extractor.watermark_value(item)
                if v is not None and (max_seen is None or v > max_seen):
                    max_seen = v
            if total % 100 == 0:
                log.info("[%s] processed %d rows so far", entity_name, total)
        log.info("[%s] fetch loop done — total=%d", entity_name, total)

    # Grava watermark fora do cursor pra usar a sessao httpx do PlatformClient.
    if extractor.INCREMENTAL_FIELD and tenant_slug and max_seen is not None:
        from platform_telemetry import set_watermark
        # max_seen pode ser datetime — serializa pra ISO 8601.
        wm_value = max_seen.isoformat() if hasattr(max_seen, "isoformat") else str(max_seen)
        set_watermark(
            tenant_slug,
            "conta_azul",
            entity_name,
            {"type": "timestamp", "value": wm_value},
            rows_loaded=total,
            run_id=run_id,
        )
        log.info(
            "[%s] watermark gravado: max(%s)=%s",
            entity_name, extractor.INCREMENTAL_FIELD, wm_value,
        )

    return total

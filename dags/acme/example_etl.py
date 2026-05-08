"""DAG de exemplo: ingestão fake -> Postgres DW do tenant ACME.

Demonstra o fluxo ponta a ponta:
1. Resolve tenant + warehouse config via API Core (lookup por slug).
2. Abre TelemetryContext — emite pipeline_task 'running' imediatamente.
3. "Extrai" dados de uma fonte fake (lista inline).
4. Grava no schema do tenant no Postgres DW (cria schema+tabela se não existirem).
5. Reporta rows_inserted via TelemetryContext.
6. Ao fechar o contexto, publica pipeline_task 'success' com duração total.

run_type é detectado automaticamente: 'scheduled' quando o Airflow dispara pelo
cron; 'manual' quando alguém dispara pela UI ou pelo botão do portal.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

import httpx
import psycopg
from airflow.sdk import dag, task
from platform_telemetry import PlatformClient, TelemetryContext

TENANT_SLUG = "acme"
PLATFORM_API_URL = os.environ["PLATFORM_API_URL"]
PLATFORM_API_TOKEN = os.environ["PLATFORM_API_TOKEN"]


def _resolve_tenant() -> dict:
    """Consulta a API Core para descobrir tenant_id + schema do DW."""
    r = httpx.get(
        f"{PLATFORM_API_URL}/tenants/by-slug/{TENANT_SLUG}",
        headers={"Authorization": f"Bearer {PLATFORM_API_TOKEN}"},
        timeout=10.0,
    )
    r.raise_for_status()
    return r.json()


def _dw_connection() -> psycopg.Connection:
    return psycopg.connect(
        host=os.environ["DW_DEFAULT_HOST"],
        port=int(os.environ["DW_DEFAULT_PORT"]),
        user=os.environ["DW_DEFAULT_USER"],
        password=os.environ["DW_DEFAULT_PASSWORD"],
        dbname=os.environ["DW_DEFAULT_DATABASE"],
        autocommit=True,
    )


@dag(
    dag_id="acme__example_etl",
    schedule="@hourly",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    tags=["tenant:acme", "kind:etl"],
    doc_md=__doc__,
)
def acme__example_etl():
    @task
    def ingest_customers(**context) -> None:
        tenant = _resolve_tenant()
        tenant_id = tenant["id"]
        schema = tenant["default_warehouse"]["connection_config"].get("schema_name", "tenant_acme")

        with TelemetryContext.from_airflow(context, tenant_id=tenant_id) as tele:
            # ─── Fonte fake: imagine que isto é uma API externa ───
            fake_rows = [
                {"id": i, "name": f"Customer {i}", "ingested_at": datetime.now(timezone.utc).isoformat()}
                for i in range(1, 26)
            ]

            # ─── Load no DW ───
            with _dw_connection() as conn, conn.cursor() as cur:
                cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
                cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS "{schema}"."customers_raw" (
                        id BIGINT PRIMARY KEY,
                        name TEXT NOT NULL,
                        ingested_at TIMESTAMPTZ NOT NULL,
                        loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                for row in fake_rows:
                    cur.execute(
                        f"""
                        INSERT INTO "{schema}"."customers_raw" (id, name, ingested_at)
                        VALUES (%(id)s, %(name)s, %(ingested_at)s)
                        ON CONFLICT (id) DO UPDATE SET
                            name = EXCLUDED.name,
                            ingested_at = EXCLUDED.ingested_at,
                            loaded_at = NOW()
                        """,
                        row,
                    )

            # ─── Telemetria ───
            tele.add_rows_inserted(len(fake_rows))

    ingest_customers()


acme__example_etl()

"""Helpers que toda DAG da plataforma datajoin precisa.

Cross-cutting concerns (nao pertencem a um conector especifico):
- resolve_tenant_by_slug: busca metadata do tenant na API Core
- dw_connection: conexao com DW via env vars padrao do compose

Mover esses helpers pra ca evita que cada DAG copie 30 linhas de
boilerplate. Mexer aqui afeta TODAS as DAGs — diferente da regra de
isolamento entre conectores externos, aqui isolar seria absurdo
(cada DAG ia ter sua versao de "como bater na API datajoin").
"""
from __future__ import annotations

import os
from typing import Any

import httpx
import psycopg


def resolve_tenant_by_slug(slug: str) -> dict[str, Any]:
    """Busca tenant_id, schemas e config do warehouse na API Core.

    Faz cache trivial via @lru_cache nao seria seguro (DAG pode rodar
    por dias e tenant pode ter sido movido entre DWs no meio). Cada
    chamada e' 1 GET — barato.
    """
    base_url = os.environ["PLATFORM_API_URL"]
    token = os.environ["PLATFORM_API_TOKEN"]
    r = httpx.get(
        f"{base_url}/tenants/by-slug/{slug}",
        headers={"Authorization": f"Bearer {token}"},
        timeout=10.0,
    )
    r.raise_for_status()
    return r.json()


def dw_connection() -> psycopg.Connection:
    """Conexao com o DW default do compose (Postgres).

    Em prod multi-DW, a DAG resolve qual DW usar via
    resolve_tenant_by_slug(...)['default_warehouse'] e passa host/port
    explicito. No MVP local, todos tenants apontam pro mesmo DW (db
    `warehouse` no servidor `postgres`) — env vars DW_DEFAULT_*
    injetadas pelo compose.
    """
    return psycopg.connect(
        host=os.environ["DW_DEFAULT_HOST"],
        port=int(os.environ["DW_DEFAULT_PORT"]),
        user=os.environ["DW_DEFAULT_USER"],
        password=os.environ["DW_DEFAULT_PASSWORD"],
        dbname=os.environ["DW_DEFAULT_DATABASE"],
        autocommit=True,
    )

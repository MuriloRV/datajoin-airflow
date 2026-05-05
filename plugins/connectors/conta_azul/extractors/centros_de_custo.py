"""Extractor de centros de custo.

Endpoint API:  GET /v1/centro-de-custo
Tabela raw:    <schema>.conta_azul__centros_de_custo

Volume baixo (dimensao estatica). Sem nested.
"""
from __future__ import annotations

from typing import Any, Iterator

from connectors.conta_azul.extractors.base import Extractor
from connectors.conta_azul.schemas import CentroDeCusto


class CentrosDeCustoExtractor(Extractor):
    NAME = "centros_de_custo"
    SCHEMA = CentroDeCusto

    DDL = """
    CREATE TABLE IF NOT EXISTS "{schema}"."conta_azul__centros_de_custo" (
        id        UUID PRIMARY KEY,
        nome      TEXT NOT NULL,
        descricao TEXT,
        ativo     BOOLEAN,
        loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """

    UPSERT_SQL = """
    INSERT INTO "{schema}"."conta_azul__centros_de_custo"
        (id, nome, descricao, ativo)
    VALUES
        (%(id)s, %(nome)s, %(descricao)s, %(ativo)s)
    ON CONFLICT (id) DO UPDATE SET
        nome      = EXCLUDED.nome,
        descricao = EXCLUDED.descricao,
        ativo     = EXCLUDED.ativo,
        loaded_at = NOW()
    """

    def fetch(self, client: Any, **kwargs: Any) -> Iterator[CentroDeCusto]:
        yield from client.paginate_all("/centro-de-custo", CentroDeCusto)

    def serialize_for_upsert(self, item: CentroDeCusto) -> dict[str, Any]:
        return {
            "id": str(item.id),
            "nome": item.nome,
            "descricao": item.descricao,
            "ativo": item.ativo,
        }

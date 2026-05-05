"""Extractor de unidades de medida (UN, KG, M, ...).

Endpoint API:  GET /v1/produtos/unidades-medida
Tabela raw:    <schema>.conta_azul__produto_unidades_medida

Master data — full refresh.
"""
from __future__ import annotations

import json
from typing import Any, Iterator

from connectors.conta_azul.extractors.base import Extractor
from connectors.conta_azul.schemas import ProdutoUnidadeMedida


class ProdutoUnidadesMedidaExtractor(Extractor):
    NAME = "produto_unidades_medida"
    SCHEMA = ProdutoUnidadeMedida

    # id BIGINT — API retorna ID legacy numerico.
    DDL = """
    CREATE TABLE IF NOT EXISTS "{schema}"."conta_azul__produto_unidades_medida" (
        id            BIGINT PRIMARY KEY,
        sigla         TEXT,
        descricao     TEXT,
        fracionavel   BOOLEAN,
        raw           JSONB,
        loaded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """

    UPSERT_SQL = """
    INSERT INTO "{schema}"."conta_azul__produto_unidades_medida"
        (id, sigla, descricao, fracionavel, raw)
    VALUES
        (%(id)s, %(sigla)s, %(descricao)s, %(fracionavel)s, %(raw)s::jsonb)
    ON CONFLICT (id) DO UPDATE SET
        sigla       = EXCLUDED.sigla,
        descricao   = EXCLUDED.descricao,
        fracionavel = EXCLUDED.fracionavel,
        raw         = EXCLUDED.raw,
        loaded_at   = NOW()
    """

    def fetch(self, client: Any, **kwargs: Any) -> Iterator[ProdutoUnidadeMedida]:
        yield from client.paginate_all(
            "/produtos/unidades-medida", ProdutoUnidadeMedida,
        )

    def serialize_for_upsert(self, item: ProdutoUnidadeMedida) -> dict[str, Any]:
        return {
            "id": item.id,
            "sigla": item.sigla,
            "descricao": item.descricao,
            "fracionavel": item.fracionavel,
            "raw": json.dumps(item.model_dump(mode="json"), default=str),
        }

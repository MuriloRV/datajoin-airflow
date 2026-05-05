"""Extractor de codigos CEST (Codigo Especificador da Substituicao Tributaria).

Endpoint API:  GET /v1/produtos/cest
Tabela raw:    <schema>.conta_azul__produto_cest

PK natural = codigo. Master data fiscal.
"""
from __future__ import annotations

import json
from typing import Any, Iterator

from connectors.conta_azul.extractors.base import Extractor
from connectors.conta_azul.schemas import ProdutoCest


class ProdutoCestExtractor(Extractor):
    NAME = "produto_cest"
    SCHEMA = ProdutoCest

    DDL = """
    CREATE TABLE IF NOT EXISTS "{schema}"."conta_azul__produto_cest" (
        codigo            TEXT PRIMARY KEY,
        descricao         TEXT,
        ncm_relacionado   TEXT,
        raw               JSONB,
        loaded_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """

    UPSERT_SQL = """
    INSERT INTO "{schema}"."conta_azul__produto_cest"
        (codigo, descricao, ncm_relacionado, raw)
    VALUES
        (%(codigo)s, %(descricao)s, %(ncm_relacionado)s, %(raw)s::jsonb)
    ON CONFLICT (codigo) DO UPDATE SET
        descricao       = EXCLUDED.descricao,
        ncm_relacionado = EXCLUDED.ncm_relacionado,
        raw             = EXCLUDED.raw,
        loaded_at       = NOW()
    """

    def fetch(self, client: Any, **kwargs: Any) -> Iterator[ProdutoCest]:
        yield from client.paginate_all(
            "/produtos/cest", ProdutoCest,
        )

    def serialize_for_upsert(self, item: ProdutoCest) -> dict[str, Any]:
        return {
            "codigo": item.codigo,
            "descricao": item.descricao,
            "ncm_relacionado": item.ncm_relacionado,
            "raw": json.dumps(item.model_dump(mode="json"), default=str),
        }

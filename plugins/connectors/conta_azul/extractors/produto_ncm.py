"""Extractor de codigos NCM (Nomenclatura Comum do Mercosul).

Endpoint API:  GET /v1/produtos/ncm
Tabela raw:    <schema>.conta_azul__produto_ncm

PK natural = codigo (8 digitos numericos, padrao fiscal). Master data
publica do governo, mas a API retorna a lista que o tenant tem ativa.
Volume tipicamente baixo. Full refresh.
"""
from __future__ import annotations

import json
from typing import Any, Iterator

from connectors.conta_azul.extractors.base import Extractor
from connectors.conta_azul.schemas import ProdutoNcm


class ProdutoNcmExtractor(Extractor):
    NAME = "produto_ncm"
    SCHEMA = ProdutoNcm

    DDL = """
    CREATE TABLE IF NOT EXISTS "{schema}"."conta_azul__produto_ncm" (
        codigo     TEXT PRIMARY KEY,
        descricao  TEXT,
        raw        JSONB,
        loaded_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """

    UPSERT_SQL = """
    INSERT INTO "{schema}"."conta_azul__produto_ncm"
        (codigo, descricao, raw)
    VALUES
        (%(codigo)s, %(descricao)s, %(raw)s::jsonb)
    ON CONFLICT (codigo) DO UPDATE SET
        descricao = EXCLUDED.descricao,
        raw       = EXCLUDED.raw,
        loaded_at = NOW()
    """

    def fetch(self, client: Any, **kwargs: Any) -> Iterator[ProdutoNcm]:
        yield from client.paginate_all(
            "/produtos/ncm", ProdutoNcm,
        )

    def serialize_for_upsert(self, item: ProdutoNcm) -> dict[str, Any]:
        return {
            "codigo": item.codigo,
            "descricao": item.descricao,
            "raw": json.dumps(item.model_dump(mode="json"), default=str),
        }

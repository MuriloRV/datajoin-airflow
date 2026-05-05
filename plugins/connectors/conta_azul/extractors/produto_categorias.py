"""Extractor de categorias de produto.

Endpoint API:  GET /v1/produtos/categorias
Tabela raw:    <schema>.conta_azul__produto_categorias

Master data simples — full refresh.
"""
from __future__ import annotations

import json
from typing import Any, Iterator

from connectors.conta_azul.extractors.base import Extractor
from connectors.conta_azul.schemas import ProdutoCategoria


class ProdutoCategoriasExtractor(Extractor):
    NAME = "produto_categorias"
    SCHEMA = ProdutoCategoria

    # id BIGINT — API retorna ID legacy numerico (ex: 62920252).
    DDL = """
    CREATE TABLE IF NOT EXISTS "{schema}"."conta_azul__produto_categorias" (
        id         BIGINT PRIMARY KEY,
        nome       TEXT,
        descricao  TEXT,
        ativo      BOOLEAN,
        raw        JSONB,
        loaded_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """

    UPSERT_SQL = """
    INSERT INTO "{schema}"."conta_azul__produto_categorias"
        (id, nome, descricao, ativo, raw)
    VALUES
        (%(id)s, %(nome)s, %(descricao)s, %(ativo)s, %(raw)s::jsonb)
    ON CONFLICT (id) DO UPDATE SET
        nome      = EXCLUDED.nome,
        descricao = EXCLUDED.descricao,
        ativo     = EXCLUDED.ativo,
        raw       = EXCLUDED.raw,
        loaded_at = NOW()
    """

    def fetch(self, client: Any, **kwargs: Any) -> Iterator[ProdutoCategoria]:
        yield from client.paginate_all(
            "/produtos/categorias", ProdutoCategoria,
        )

    def serialize_for_upsert(self, item: ProdutoCategoria) -> dict[str, Any]:
        return {
            "id": item.id,
            "nome": item.nome,
            "descricao": item.descricao,
            "ativo": item.ativo,
            "raw": json.dumps(item.model_dump(mode="json"), default=str),
        }

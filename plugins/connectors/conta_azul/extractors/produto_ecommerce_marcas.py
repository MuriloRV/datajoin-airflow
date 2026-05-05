"""Extractor de marcas e-commerce.

Endpoint API:  GET /v1/produtos/ecommerce-marcas
Tabela raw:    <schema>.conta_azul__produto_ecommerce_marcas

Volume tipicamente 0 em tenants sem integracao e-commerce ativa.
"""
from __future__ import annotations

import json
from typing import Any, Iterator

from connectors.conta_azul.extractors.base import Extractor
from connectors.conta_azul.schemas import ProdutoEcommerceMarca


class ProdutoEcommerceMarcasExtractor(Extractor):
    NAME = "produto_ecommerce_marcas"
    SCHEMA = ProdutoEcommerceMarca

    DDL = """
    CREATE TABLE IF NOT EXISTS "{schema}"."conta_azul__produto_ecommerce_marcas" (
        id         UUID PRIMARY KEY,
        nome       TEXT NOT NULL,
        raw        JSONB,
        loaded_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """

    UPSERT_SQL = """
    INSERT INTO "{schema}"."conta_azul__produto_ecommerce_marcas"
        (id, nome, raw)
    VALUES
        (%(id)s, %(nome)s, %(raw)s::jsonb)
    ON CONFLICT (id) DO UPDATE SET
        nome      = EXCLUDED.nome,
        raw       = EXCLUDED.raw,
        loaded_at = NOW()
    """

    def fetch(self, client: Any, **kwargs: Any) -> Iterator[ProdutoEcommerceMarca]:
        yield from client.paginate_all(
            "/produtos/ecommerce-marcas", ProdutoEcommerceMarca,
        )

    def serialize_for_upsert(self, item: ProdutoEcommerceMarca) -> dict[str, Any]:
        return {
            "id": str(item.id),
            "nome": item.nome,
            "raw": json.dumps(item.model_dump(mode="json"), default=str),
        }

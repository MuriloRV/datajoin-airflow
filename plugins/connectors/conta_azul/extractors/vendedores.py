"""Extractor de vendedores.

Endpoint API:  GET /v1/venda/vendedores  (retorna LISTA direta, sem envelope)
Tabela raw:    <schema>.conta_azul__vendedores

Volume baixo (lookup de quem fez a venda). Sem nested. Sem incremental.
"""
from __future__ import annotations

from typing import Any, Iterator

from connectors.conta_azul.extractors.base import Extractor
from connectors.conta_azul.schemas import Vendedor


class VendedoresExtractor(Extractor):
    NAME = "vendedores"
    SCHEMA = Vendedor

    DDL = """
    CREATE TABLE IF NOT EXISTS "{schema}"."conta_azul__vendedores" (
        id        UUID PRIMARY KEY,
        nome      TEXT NOT NULL,
        loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """

    UPSERT_SQL = """
    INSERT INTO "{schema}"."conta_azul__vendedores"
        (id, nome)
    VALUES
        (%(id)s, %(nome)s)
    ON CONFLICT (id) DO UPDATE SET
        nome      = EXCLUDED.nome,
        loaded_at = NOW()
    """

    def fetch(self, client: Any, **kwargs: Any) -> Iterator[Vendedor]:
        # Endpoint sem paginacao — retorna lista direta.
        yield from client.list_simple("/venda/vendedores", Vendedor)

    def serialize_for_upsert(self, item: Vendedor) -> dict[str, Any]:
        return {"id": str(item.id), "nome": item.nome}

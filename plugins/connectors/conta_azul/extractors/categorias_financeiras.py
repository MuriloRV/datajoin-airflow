"""Extractor de categorias financeiras (arvore DRE).

Endpoint API:  GET /v1/categorias
Tabela raw:    <schema>.conta_azul__categorias_financeiras

Sem nested. Arvore via FK auto-referencial `categoria_pai` -> `id`.
Volume baixo (dezenas a centenas de categorias).
"""
from __future__ import annotations

from typing import Any, Iterator

from connectors.conta_azul.extractors.base import Extractor
from connectors.conta_azul.schemas import CategoriaFinanceira


class CategoriasFinanceirasExtractor(Extractor):
    NAME = "categorias_financeiras"
    SCHEMA = CategoriaFinanceira

    DDL = """
    CREATE TABLE IF NOT EXISTS "{schema}"."conta_azul__categorias_financeiras" (
        id                    UUID PRIMARY KEY,
        nome                  TEXT NOT NULL,
        tipo                  TEXT,
        categoria_pai         UUID,
        entrada_dre           TEXT,
        considera_custo_dre   BOOLEAN,
        versao                INTEGER,
        loaded_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """

    UPSERT_SQL = """
    INSERT INTO "{schema}"."conta_azul__categorias_financeiras"
        (id, nome, tipo, categoria_pai, entrada_dre, considera_custo_dre, versao)
    VALUES
        (%(id)s, %(nome)s, %(tipo)s, %(categoria_pai)s, %(entrada_dre)s,
         %(considera_custo_dre)s, %(versao)s)
    ON CONFLICT (id) DO UPDATE SET
        nome                = EXCLUDED.nome,
        tipo                = EXCLUDED.tipo,
        categoria_pai       = EXCLUDED.categoria_pai,
        entrada_dre         = EXCLUDED.entrada_dre,
        considera_custo_dre = EXCLUDED.considera_custo_dre,
        versao              = EXCLUDED.versao,
        loaded_at           = NOW()
    """

    def fetch(self, client: Any, **kwargs: Any) -> Iterator[CategoriaFinanceira]:
        # Lookup: full refresh sempre (categorias mudam raramente).
        yield from client.paginate_all("/categorias", CategoriaFinanceira)

    def serialize_for_upsert(self, item: CategoriaFinanceira) -> dict[str, Any]:
        return {
            "id": str(item.id),
            "nome": item.nome,
            "tipo": item.tipo,
            "categoria_pai": str(item.categoria_pai) if item.categoria_pai else None,
            "entrada_dre": item.entrada_dre,
            "considera_custo_dre": item.considera_custo_dre,
            "versao": item.versao,
        }

"""Extractor de categorias do DRE.

Endpoint API:  GET /v1/financeiro/categorias-dre
Tabela raw:    <schema>.conta_azul__categorias_dre

Distinto de categorias_financeiras (/v1/categorias) — esta e' a estrutura
contabil do DRE (Demonstrativo de Resultado), com hierarquia fixa.
Master data — full refresh.
"""
from __future__ import annotations

import json
from typing import Any, Iterator

from connectors.conta_azul.extractors.base import Extractor
from connectors.conta_azul.schemas import CategoriaDre


class CategoriasDreExtractor(Extractor):
    NAME = "categorias_dre"
    SCHEMA = CategoriaDre

    # DDL alinhada com shape real (probe mai/2026):
    # - descricao = nome da categoria (campo nome da API vem null)
    # - subitens = arvore aninhada de filhos (jsonb)
    # - categorias_financeiras = categorias financeiras vinculadas (jsonb)
    DDL = """
    CREATE TABLE IF NOT EXISTS "{schema}"."conta_azul__categorias_dre" (
        id                            UUID PRIMARY KEY,
        descricao                     TEXT,
        codigo                        TEXT,
        posicao                       INTEGER,
        indica_totalizador            BOOLEAN,
        representa_soma_custo_medio   BOOLEAN,
        subitens                      JSONB,
        categorias_financeiras        JSONB,
        raw                           JSONB,
        loaded_at                     TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """

    UPSERT_SQL = """
    INSERT INTO "{schema}"."conta_azul__categorias_dre"
        (id, descricao, codigo, posicao, indica_totalizador,
         representa_soma_custo_medio, subitens, categorias_financeiras, raw)
    VALUES
        (%(id)s, %(descricao)s, %(codigo)s, %(posicao)s, %(indica_totalizador)s,
         %(representa_soma_custo_medio)s,
         %(subitens)s::jsonb, %(categorias_financeiras)s::jsonb,
         %(raw)s::jsonb)
    ON CONFLICT (id) DO UPDATE SET
        descricao                   = EXCLUDED.descricao,
        codigo                      = EXCLUDED.codigo,
        posicao                     = EXCLUDED.posicao,
        indica_totalizador          = EXCLUDED.indica_totalizador,
        representa_soma_custo_medio = EXCLUDED.representa_soma_custo_medio,
        subitens                    = EXCLUDED.subitens,
        categorias_financeiras      = EXCLUDED.categorias_financeiras,
        raw                         = EXCLUDED.raw,
        loaded_at                   = NOW()
    """

    def fetch(self, client: Any, **kwargs: Any) -> Iterator[CategoriaDre]:
        # Endpoint nao paginado (doc nao expoe pagina/tamanho_pagina), mas
        # paginate_all com terminator detecta lista pequena e retorna logo.
        # Se vier sem envelope (lista direta), client.list_simple cobre.
        # Default seguro: tentar paginate_all e fallback pra list_simple
        # caso envelope nao seja detectado. Por ora, paginate_all primeiro.
        yield from client.paginate_all(
            "/financeiro/categorias-dre", CategoriaDre,
        )

    def serialize_for_upsert(self, item: CategoriaDre) -> dict[str, Any]:
        return {
            "id": str(item.id),
            "descricao": item.descricao,
            "codigo": item.codigo,
            "posicao": item.posicao,
            "indica_totalizador": item.indica_totalizador,
            "representa_soma_custo_medio": item.representa_soma_custo_medio,
            "subitens": json.dumps(item.subitens) if item.subitens else None,
            "categorias_financeiras": (
                json.dumps(item.categorias_financeiras)
                if item.categorias_financeiras else None
            ),
            "raw": json.dumps(item.model_dump(mode="json"), default=str),
        }

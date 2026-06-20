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
        #
        # apenas_filhos=False traz TAMBEM as categorias pai (niveis
        # intermediarios da arvore, ex: "4.03 Despesas com Salarios e
        # Encargos"), nao so as folhas. Sem esse param a API /v1/categorias
        # retorna apenas as folhas -> os UUIDs em `categoria_pai` ficam
        # orfaos (apontam pra pais que nunca foram ingeridos), impossibilitando
        # montar a hierarquia completa de categorias no DRE/relatorios.
        #
        # Testado contra a API real (GET /v1/categorias):
        #   apenas_filhos=false        -> 141 itens (124 folhas + 17 pais) OK
        #   permite_apenas_filhos=false -> 124 itens (so folhas) -- NAO traz pais
        # ou seja: o parametro correto e' `apenas_filhos`, nao `permite_apenas_filhos`.
        yield from client.paginate_all(
            "/categorias",
            CategoriaFinanceira,
            extra_params={"apenas_filhos": False},
        )

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

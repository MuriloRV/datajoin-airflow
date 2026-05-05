"""Extractor de produtos.

Endpoint API:  GET /v1/produtos (PLURAL — singular retorna 405)
Envelope:      `items` (EN — diferente das outras entidades em pt-BR!)
Tabela raw:    <schema>.conta_azul__produtos

Volume medio. Sem nested.

Incremental por `data_alteracao_de` (mesmo padrao de pessoas/vendas):
le watermark da plataforma, filtra a API e grava max(data_alteracao)
no fim. Primeira run = full; subsequentes pegam so deltas.
"""
from __future__ import annotations

from typing import Any, Iterator

from connectors.conta_azul.extractors.base import Extractor
from connectors.conta_azul.schemas import Produto


class ProdutosExtractor(Extractor):
    NAME = "produtos"
    SCHEMA = Produto
    INCREMENTAL_FIELD = "data_alteracao"
    INCREMENTAL_PARAM = "data_alteracao_de"

    DDL = """
    CREATE TABLE IF NOT EXISTS "{schema}"."conta_azul__produtos" (
        id              UUID PRIMARY KEY,
        nome            TEXT,
        codigo          TEXT,
        descricao       TEXT,
        preco           NUMERIC(18, 4),
        custo           NUMERIC(18, 4),
        estoque         NUMERIC(18, 4),
        estoque_minimo  NUMERIC(18, 4),
        unidade         TEXT,
        status          TEXT,
        data_criacao    TIMESTAMPTZ,
        data_alteracao  TIMESTAMPTZ,
        loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """

    UPSERT_SQL = """
    INSERT INTO "{schema}"."conta_azul__produtos"
        (id, nome, codigo, descricao, preco, custo, estoque, estoque_minimo,
         unidade, status, data_criacao, data_alteracao)
    VALUES
        (%(id)s, %(nome)s, %(codigo)s, %(descricao)s, %(preco)s, %(custo)s,
         %(estoque)s, %(estoque_minimo)s, %(unidade)s, %(status)s,
         %(data_criacao)s, %(data_alteracao)s)
    ON CONFLICT (id) DO UPDATE SET
        nome           = EXCLUDED.nome,
        codigo         = EXCLUDED.codigo,
        descricao      = EXCLUDED.descricao,
        preco          = EXCLUDED.preco,
        custo          = EXCLUDED.custo,
        estoque        = EXCLUDED.estoque,
        estoque_minimo = EXCLUDED.estoque_minimo,
        unidade        = EXCLUDED.unidade,
        status         = EXCLUDED.status,
        data_alteracao = EXCLUDED.data_alteracao,
        loaded_at      = NOW()
    """

    def fetch(self, client: Any, **kwargs: Any) -> Iterator[Produto]:
        # /produtos restringe page_size a 10|20|50|100|200|500|1000.
        # API exige ambos data_alteracao_de + ate. Formato ISO sem ms/TZ.
        from datetime import datetime
        watermark = kwargs.get("watermark")
        extra_params = None
        if watermark and watermark.get("value"):
            wm = watermark["value"].split(".")[0].split("+")[0].rstrip("Z")
            extra_params = {
                "data_alteracao_de": wm,
                "data_alteracao_ate": datetime.utcnow().replace(microsecond=0).isoformat(),
            }
        yield from client.paginate_all(
            "/produtos", Produto,
            page_size=100,
            extra_params=extra_params,
        )

    def serialize_for_upsert(self, item: Produto) -> dict[str, Any]:
        return {
            "id": str(item.id),
            "nome": item.nome,
            "codigo": item.codigo,
            "descricao": item.descricao,
            "preco": item.preco,
            "custo": item.custo,
            "estoque": item.estoque,
            "estoque_minimo": item.estoque_minimo,
            "unidade": item.unidade,
            "status": item.status,
            "data_criacao": item.data_criacao,
            "data_alteracao": item.data_alteracao,
        }

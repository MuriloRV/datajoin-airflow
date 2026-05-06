"""Extractor de detalhe de produtos (N+1 a partir de raw.produtos).

Endpoint API: GET /v1/produtos/{id}
Tabela raw:   <schema>.conta_azul__produtos_detalhe

Adiciona tributacao (ICMS/IPI/PIS/COFINS), dimensoes (peso/altura/largura/
comprimento), fotos, NCM/CEST detalhados — campos AUSENTES do agregado
de /v1/produtos.
"""
from __future__ import annotations

import json
import logging
from typing import Any, ClassVar, Iterator

from connectors.conta_azul.extractors.base import Extractor
from connectors.conta_azul.schemas import ProdutoDetalhe


log = logging.getLogger(__name__)


class ProdutosDetalheExtractor(Extractor):
    NAME = "produtos_detalhe"
    SCHEMA = ProdutoDetalhe
    INCREMENTAL_FIELD = "data_alteracao"
    INCREMENTAL_PARAM = None

    REQUIRES_DW_READ: ClassVar[bool] = True

    DDL = """
    CREATE TABLE IF NOT EXISTS "{schema}"."conta_azul__produtos_detalhe" (
        id              UUID PRIMARY KEY,
        nome            TEXT,
        codigo          TEXT,
        descricao       TEXT,
        preco           NUMERIC(18, 4),
        custo           NUMERIC(18, 4),
        estoque         NUMERIC(18, 4),
        estoque_minimo  NUMERIC(18, 4),
        ncm             TEXT,
        cest            TEXT,
        unidade         TEXT,
        peso            NUMERIC(18, 4),
        altura          NUMERIC(18, 4),
        largura         NUMERIC(18, 4),
        comprimento     NUMERIC(18, 4),
        tributacao      JSONB,
        fotos           JSONB,
        status          TEXT,
        data_criacao    TIMESTAMPTZ,
        data_alteracao  TIMESTAMPTZ,
        raw             JSONB,
        loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """

    UPSERT_SQL = """
    INSERT INTO "{schema}"."conta_azul__produtos_detalhe"
        (id, nome, codigo, descricao, preco, custo, estoque, estoque_minimo,
         ncm, cest, unidade, peso, altura, largura, comprimento, tributacao,
         fotos, status, data_criacao, data_alteracao, raw)
    VALUES
        (%(id)s, %(nome)s, %(codigo)s, %(descricao)s, %(preco)s, %(custo)s,
         %(estoque)s, %(estoque_minimo)s, %(ncm)s, %(cest)s, %(unidade)s,
         %(peso)s, %(altura)s, %(largura)s, %(comprimento)s,
         %(tributacao)s::jsonb, %(fotos)s::jsonb, %(status)s,
         %(data_criacao)s, %(data_alteracao)s, %(raw)s::jsonb)
    ON CONFLICT (id) DO UPDATE SET
        nome           = EXCLUDED.nome,
        codigo         = EXCLUDED.codigo,
        descricao      = EXCLUDED.descricao,
        preco          = EXCLUDED.preco,
        custo          = EXCLUDED.custo,
        estoque        = EXCLUDED.estoque,
        estoque_minimo = EXCLUDED.estoque_minimo,
        ncm            = EXCLUDED.ncm,
        cest           = EXCLUDED.cest,
        unidade        = EXCLUDED.unidade,
        peso           = EXCLUDED.peso,
        altura         = EXCLUDED.altura,
        largura        = EXCLUDED.largura,
        comprimento    = EXCLUDED.comprimento,
        tributacao     = EXCLUDED.tributacao,
        fotos          = EXCLUDED.fotos,
        status         = EXCLUDED.status,
        data_alteracao = EXCLUDED.data_alteracao,
        raw            = EXCLUDED.raw,
        loaded_at      = NOW()
    """

    def fetch(self, client: Any, **kwargs: Any) -> Iterator[ProdutoDetalhe]:
        watermark = kwargs.get("watermark")
        dw_conn = kwargs.get("dw_conn")
        schema_raw = kwargs.get("schema_raw")

        self._latest_data_alt = None

        if dw_conn is None or schema_raw is None:
            log.error("ProdutosDetalheExtractor: faltando dw_conn ou schema_raw")
            return

        wm_value = watermark.get("value") if watermark else None
        with dw_conn.cursor() as cur:
            if wm_value:
                cur.execute(
                    f'SELECT id::text, data_alteracao FROM "{schema_raw}".conta_azul__produtos '
                    f'WHERE data_alteracao > %s ORDER BY data_alteracao',
                    (wm_value,),
                )
            else:
                cur.execute(
                    f'SELECT id::text, data_alteracao FROM "{schema_raw}".conta_azul__produtos '
                    f'ORDER BY data_alteracao NULLS LAST'
                )
            rows = cur.fetchall()

        log.info(
            "ProdutosDetalhe: %d produtos pra buscar detalhe (watermark=%s)",
            len(rows), wm_value,
        )
        for pid, data_alt in rows:
            detalhe_raw = client.get_produto_detalhe(pid)
            if detalhe_raw is None:
                continue
            detalhe_raw.setdefault("id", pid)
            try:
                detalhe = ProdutoDetalhe.model_validate(detalhe_raw)
            except Exception:
                log.exception(
                    "ProdutosDetalhe: falha parseando produto %s — pulando", pid
                )
                continue
            wm_candidate = detalhe.data_alteracao or data_alt
            if wm_candidate is not None and (
                self._latest_data_alt is None
                or wm_candidate > self._latest_data_alt
            ):
                self._latest_data_alt = wm_candidate
            yield detalhe

    def watermark_value(self, item: ProdutoDetalhe) -> Any:
        return getattr(self, "_latest_data_alt", None)

    def serialize_for_upsert(self, item: ProdutoDetalhe) -> dict[str, Any]:
        raw_dict = item.model_dump(mode="json")
        return {
            "id": str(item.id),
            "nome": item.nome,
            "codigo": item.codigo,
            "descricao": item.descricao,
            "preco": item.preco,
            "custo": item.custo,
            "estoque": item.estoque,
            "estoque_minimo": item.estoque_minimo,
            "ncm": item.ncm,
            "cest": item.cest,
            "unidade": item.unidade,
            "peso": item.peso,
            "altura": item.altura,
            "largura": item.largura,
            "comprimento": item.comprimento,
            "tributacao": json.dumps(item.tributacao) if item.tributacao else None,
            "fotos": json.dumps(item.fotos) if item.fotos else None,
            "status": item.status,
            "data_criacao": item.data_criacao,
            "data_alteracao": item.data_alteracao,
            "raw": json.dumps(raw_dict, default=str),
        }

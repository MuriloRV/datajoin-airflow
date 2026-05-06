"""Extractor de detalhe de vendas (N+1 a partir de raw.vendas).

Endpoint API: GET /v1/vendas/{id}  (fallback singular /v1/venda/{id})
Tabela raw:   <schema>.conta_azul__vendas_detalhe

Complementa o agregado de /venda/busca (extractor `vendas`) com:
condicao_pagamento, parcelas, endereco_entrega, observacoes, impostos
totais, frete, desconto consolidado, natureza_operacao.

Padrao N+1 com watermark — le venda_ids de raw.vendas alteradas desde
o ultimo run.
"""
from __future__ import annotations

import json
import logging
from typing import Any, ClassVar, Iterator

from connectors.conta_azul.extractors.base import Extractor
from connectors.conta_azul.schemas import VendaDetalhe


log = logging.getLogger(__name__)


class VendasDetalheExtractor(Extractor):
    NAME = "vendas_detalhe"
    SCHEMA = VendaDetalhe
    INCREMENTAL_FIELD = "data_alteracao"
    INCREMENTAL_PARAM = None

    REQUIRES_DW_READ: ClassVar[bool] = True

    DDL = """
    CREATE TABLE IF NOT EXISTS "{schema}"."conta_azul__vendas_detalhe" (
        id                  UUID PRIMARY KEY,
        numero              BIGINT,
        situacao            TEXT,
        data_emissao        TIMESTAMPTZ,
        data_alteracao      TIMESTAMPTZ,
        valor_bruto         NUMERIC(18, 4),
        valor_desconto      NUMERIC(18, 4),
        valor_frete         NUMERIC(18, 4),
        valor_impostos      NUMERIC(18, 4),
        valor_liquido       NUMERIC(18, 4),
        observacoes         TEXT,
        cliente             JSONB,
        vendedor            JSONB,
        natureza_operacao   JSONB,
        condicao_pagamento  JSONB,
        parcelas            JSONB,
        endereco_entrega    JSONB,
        raw                 JSONB,
        loaded_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """

    UPSERT_SQL = """
    INSERT INTO "{schema}"."conta_azul__vendas_detalhe"
        (id, numero, situacao, data_emissao, data_alteracao, valor_bruto,
         valor_desconto, valor_frete, valor_impostos, valor_liquido,
         observacoes, cliente, vendedor, natureza_operacao,
         condicao_pagamento, parcelas, endereco_entrega, raw)
    VALUES
        (%(id)s, %(numero)s, %(situacao)s, %(data_emissao)s,
         %(data_alteracao)s, %(valor_bruto)s, %(valor_desconto)s,
         %(valor_frete)s, %(valor_impostos)s, %(valor_liquido)s,
         %(observacoes)s, %(cliente)s::jsonb, %(vendedor)s::jsonb,
         %(natureza_operacao)s::jsonb, %(condicao_pagamento)s::jsonb,
         %(parcelas)s::jsonb, %(endereco_entrega)s::jsonb, %(raw)s::jsonb)
    ON CONFLICT (id) DO UPDATE SET
        numero             = EXCLUDED.numero,
        situacao           = EXCLUDED.situacao,
        data_emissao       = EXCLUDED.data_emissao,
        data_alteracao     = EXCLUDED.data_alteracao,
        valor_bruto        = EXCLUDED.valor_bruto,
        valor_desconto     = EXCLUDED.valor_desconto,
        valor_frete        = EXCLUDED.valor_frete,
        valor_impostos     = EXCLUDED.valor_impostos,
        valor_liquido      = EXCLUDED.valor_liquido,
        observacoes        = EXCLUDED.observacoes,
        cliente            = EXCLUDED.cliente,
        vendedor           = EXCLUDED.vendedor,
        natureza_operacao  = EXCLUDED.natureza_operacao,
        condicao_pagamento = EXCLUDED.condicao_pagamento,
        parcelas           = EXCLUDED.parcelas,
        endereco_entrega   = EXCLUDED.endereco_entrega,
        raw                = EXCLUDED.raw,
        loaded_at          = NOW()
    """

    def fetch(self, client: Any, **kwargs: Any) -> Iterator[VendaDetalhe]:
        watermark = kwargs.get("watermark")
        dw_conn = kwargs.get("dw_conn")
        schema_raw = kwargs.get("schema_raw")

        self._latest_data_alt = None

        if dw_conn is None or schema_raw is None:
            log.error("VendasDetalheExtractor: faltando dw_conn ou schema_raw")
            return

        wm_value = watermark.get("value") if watermark else None
        with dw_conn.cursor() as cur:
            if wm_value:
                cur.execute(
                    f'SELECT id::text, data_alteracao FROM "{schema_raw}".conta_azul__vendas '
                    f'WHERE data_alteracao > %s ORDER BY data_alteracao',
                    (wm_value,),
                )
            else:
                cur.execute(
                    f'SELECT id::text, data_alteracao FROM "{schema_raw}".conta_azul__vendas '
                    f'ORDER BY data_alteracao'
                )
            rows = cur.fetchall()

        log.info(
            "VendasDetalhe: %d vendas pra buscar detalhe (watermark=%s)",
            len(rows), wm_value,
        )
        for vid, data_alt in rows:
            detalhe_raw = client.get_venda_detalhe(vid)
            if detalhe_raw is None:
                continue
            detalhe_raw.setdefault("id", vid)
            try:
                detalhe = VendaDetalhe.model_validate(detalhe_raw)
            except Exception:
                log.exception(
                    "VendasDetalhe: falha parseando venda %s — pulando", vid
                )
                continue
            wm_candidate = detalhe.data_alteracao or data_alt
            if wm_candidate is not None and (
                self._latest_data_alt is None
                or wm_candidate > self._latest_data_alt
            ):
                self._latest_data_alt = wm_candidate
            yield detalhe

    def watermark_value(self, item: VendaDetalhe) -> Any:
        return getattr(self, "_latest_data_alt", None)

    def serialize_for_upsert(self, item: VendaDetalhe) -> dict[str, Any]:
        raw_dict = item.model_dump(mode="json")
        return {
            "id": str(item.id),
            "numero": item.numero,
            "situacao": item.situacao,
            "data_emissao": item.data_emissao,
            "data_alteracao": item.data_alteracao,
            "valor_bruto": item.valor_bruto,
            "valor_desconto": item.valor_desconto,
            "valor_frete": item.valor_frete,
            "valor_impostos": item.valor_impostos,
            "valor_liquido": item.valor_liquido,
            "observacoes": item.observacoes,
            "cliente": json.dumps(item.cliente) if item.cliente else None,
            "vendedor": json.dumps(item.vendedor) if item.vendedor else None,
            "natureza_operacao": (
                json.dumps(item.natureza_operacao) if item.natureza_operacao else None
            ),
            "condicao_pagamento": (
                json.dumps(item.condicao_pagamento) if item.condicao_pagamento else None
            ),
            "parcelas": json.dumps(item.parcelas) if item.parcelas else None,
            "endereco_entrega": (
                json.dumps(item.endereco_entrega) if item.endereco_entrega else None
            ),
            "raw": json.dumps(raw_dict, default=str),
        }

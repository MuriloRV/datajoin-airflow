"""Extractor de detalhe de contratos (N+1 a partir de raw.contratos).

Endpoint API: GET /v1/contratos/{id}
Tabela raw:   <schema>.conta_azul__contratos_detalhe

Complementa o agregado de /v1/contratos com composicao de valores,
condicao de pagamento, configuracao de recorrencia, termos e vendedor.
"""
from __future__ import annotations

import json
import logging
from typing import Any, ClassVar, Iterator

from connectors.conta_azul.extractors.base import Extractor
from connectors.conta_azul.schemas import ContratoDetalhe


log = logging.getLogger(__name__)


class ContratosDetalheExtractor(Extractor):
    NAME = "contratos_detalhe"
    SCHEMA = ContratoDetalhe
    INCREMENTAL_FIELD = "data_alteracao"
    INCREMENTAL_PARAM = None

    REQUIRES_DW_READ: ClassVar[bool] = True

    DDL = """
    CREATE TABLE IF NOT EXISTS "{schema}"."conta_azul__contratos_detalhe" (
        id                       UUID PRIMARY KEY,
        numero                   TEXT,
        descricao                TEXT,
        cliente                  JSONB,
        vendedor                 JSONB,
        composicao_de_valor      JSONB,
        condicao_pagamento       JSONB,
        configuracao_recorrencia JSONB,
        termos                   JSONB,
        data_emissao             TIMESTAMPTZ,
        data_inicio              TIMESTAMPTZ,
        data_fim                 TIMESTAMPTZ,
        status                   TEXT,
        valor_bruto              NUMERIC(18, 4),
        valor_liquido            NUMERIC(18, 4),
        data_criacao             TIMESTAMPTZ,
        data_alteracao           TIMESTAMPTZ,
        raw                      JSONB,
        loaded_at                TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """

    UPSERT_SQL = """
    INSERT INTO "{schema}"."conta_azul__contratos_detalhe"
        (id, numero, descricao, cliente, vendedor, composicao_de_valor,
         condicao_pagamento, configuracao_recorrencia, termos,
         data_emissao, data_inicio, data_fim, status, valor_bruto,
         valor_liquido, data_criacao, data_alteracao, raw)
    VALUES
        (%(id)s, %(numero)s, %(descricao)s, %(cliente)s::jsonb,
         %(vendedor)s::jsonb, %(composicao_de_valor)s::jsonb,
         %(condicao_pagamento)s::jsonb, %(configuracao_recorrencia)s::jsonb,
         %(termos)s::jsonb, %(data_emissao)s, %(data_inicio)s,
         %(data_fim)s, %(status)s, %(valor_bruto)s, %(valor_liquido)s,
         %(data_criacao)s, %(data_alteracao)s, %(raw)s::jsonb)
    ON CONFLICT (id) DO UPDATE SET
        numero                   = EXCLUDED.numero,
        descricao                = EXCLUDED.descricao,
        cliente                  = EXCLUDED.cliente,
        vendedor                 = EXCLUDED.vendedor,
        composicao_de_valor      = EXCLUDED.composicao_de_valor,
        condicao_pagamento       = EXCLUDED.condicao_pagamento,
        configuracao_recorrencia = EXCLUDED.configuracao_recorrencia,
        termos                   = EXCLUDED.termos,
        data_emissao             = EXCLUDED.data_emissao,
        data_inicio              = EXCLUDED.data_inicio,
        data_fim                 = EXCLUDED.data_fim,
        status                   = EXCLUDED.status,
        valor_bruto              = EXCLUDED.valor_bruto,
        valor_liquido            = EXCLUDED.valor_liquido,
        data_alteracao           = EXCLUDED.data_alteracao,
        raw                      = EXCLUDED.raw,
        loaded_at                = NOW()
    """

    def fetch(self, client: Any, **kwargs: Any) -> Iterator[ContratoDetalhe]:
        watermark = kwargs.get("watermark")
        dw_conn = kwargs.get("dw_conn")
        schema_raw = kwargs.get("schema_raw")

        self._latest_data_alt = None

        if dw_conn is None or schema_raw is None:
            log.error("ContratosDetalheExtractor: faltando dw_conn ou schema_raw")
            return

        wm_value = watermark.get("value") if watermark else None
        with dw_conn.cursor() as cur:
            if wm_value:
                cur.execute(
                    f'SELECT id::text, data_alteracao FROM "{schema_raw}".conta_azul__contratos '
                    f'WHERE data_alteracao > %s ORDER BY data_alteracao',
                    (wm_value,),
                )
            else:
                cur.execute(
                    f'SELECT id::text, data_alteracao FROM "{schema_raw}".conta_azul__contratos '
                    f'ORDER BY data_alteracao'
                )
            rows = cur.fetchall()

        log.info(
            "ContratosDetalhe: %d contratos pra buscar detalhe (watermark=%s)",
            len(rows), wm_value,
        )
        for cid, data_alt in rows:
            detalhe_raw = client.get_contrato_detalhe(cid)
            if detalhe_raw is None:
                continue
            detalhe_raw.setdefault("id", cid)
            try:
                detalhe = ContratoDetalhe.model_validate(detalhe_raw)
            except Exception:
                log.exception(
                    "ContratosDetalhe: falha parseando contrato %s — pulando", cid
                )
                continue
            wm_candidate = detalhe.data_alteracao or data_alt
            if wm_candidate is not None and (
                self._latest_data_alt is None
                or wm_candidate > self._latest_data_alt
            ):
                self._latest_data_alt = wm_candidate
            yield detalhe

    def watermark_value(self, item: ContratoDetalhe) -> Any:
        return getattr(self, "_latest_data_alt", None)

    def serialize_for_upsert(self, item: ContratoDetalhe) -> dict[str, Any]:
        raw_dict = item.model_dump(mode="json")
        return {
            "id": str(item.id),
            "numero": item.numero,
            "descricao": item.descricao,
            "cliente": json.dumps(item.cliente) if item.cliente else None,
            "vendedor": json.dumps(item.vendedor) if item.vendedor else None,
            "composicao_de_valor": (
                json.dumps(item.composicao_de_valor) if item.composicao_de_valor else None
            ),
            "condicao_pagamento": (
                json.dumps(item.condicao_pagamento) if item.condicao_pagamento else None
            ),
            "configuracao_recorrencia": (
                json.dumps(item.configuracao_recorrencia)
                if item.configuracao_recorrencia else None
            ),
            "termos": json.dumps(item.termos) if item.termos else None,
            "data_emissao": item.data_emissao,
            "data_inicio": item.data_inicio,
            "data_fim": item.data_fim,
            "status": item.status,
            "valor_bruto": item.valor_bruto,
            "valor_liquido": item.valor_liquido,
            "data_criacao": item.data_criacao,
            "data_alteracao": item.data_alteracao,
            "raw": json.dumps(raw_dict, default=str),
        }

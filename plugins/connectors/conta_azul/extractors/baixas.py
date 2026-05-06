"""Extractor de baixas (pagamentos efetivos) por parcela.

Endpoint API: GET /v1/financeiro/eventos-financeiros/parcelas/{parcela_id}/baixas
Tabela raw:   <schema>.conta_azul__baixas

Granularidade: 1 row = 1 baixa (pagamento parcial ou total). 1 parcela
pode ter N baixas, ou nenhuma se nao paga ainda. Aqui mora a verdade
absoluta de "o que foi pago, quando, como, com qual juros/multa/desconto
aplicado naquele momento" — base pra:
  - DRE de regime de caixa (vs competencia em contas_a_*)
  - prazo medio de recebimento (data_pagamento - data_vencimento)
  - recuperacao de multas e juros
  - reconciliacao bancaria (conta_financeira do pagamento)

Padrao N+1: le parcela_ids de raw.parcelas_detalhe (que ja foi populado
em estagio anterior do chain). Roda APOS parcelas_detalhe.

Watermark: max(data_alteracao) das baixas vistas. Filtro local lendo
parcelas_detalhe.data_alteracao > watermark — parcelas alteradas tendem
a ter baixas alteradas/novas. Conservador: pode reprocessar baixas
inalteradas em parcelas mexidas, mas o UPSERT por id absorve.
"""
from __future__ import annotations

import json
import logging
from typing import Any, ClassVar, Iterator

from connectors.conta_azul.extractors.base import Extractor
from connectors.conta_azul.schemas import Baixa


log = logging.getLogger(__name__)


class BaixasExtractor(Extractor):
    NAME = "baixas"
    SCHEMA = Baixa
    INCREMENTAL_FIELD = "data_alteracao"
    INCREMENTAL_PARAM = None

    REQUIRES_DW_READ: ClassVar[bool] = True

    DDL = """
    CREATE TABLE IF NOT EXISTS "{schema}"."conta_azul__baixas" (
        id                    UUID PRIMARY KEY,
        parcela_id            UUID,
        data_pagamento        TIMESTAMPTZ,
        valor                 NUMERIC(18, 4),
        juros                 NUMERIC(18, 4),
        multa                 NUMERIC(18, 4),
        desconto              NUMERIC(18, 4),
        metodo_pagamento      TEXT,
        forma_pagamento       TEXT,
        conta_financeira_id   UUID,
        conta_financeira      JSONB,
        observacao            TEXT,
        data_criacao          TIMESTAMPTZ,
        data_alteracao        TIMESTAMPTZ,
        raw                   JSONB,
        loaded_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """

    UPSERT_SQL = """
    INSERT INTO "{schema}"."conta_azul__baixas"
        (id, parcela_id, data_pagamento, valor, juros, multa, desconto,
         metodo_pagamento, forma_pagamento, conta_financeira_id,
         conta_financeira, observacao, data_criacao, data_alteracao, raw)
    VALUES
        (%(id)s, %(parcela_id)s, %(data_pagamento)s, %(valor)s, %(juros)s,
         %(multa)s, %(desconto)s, %(metodo_pagamento)s, %(forma_pagamento)s,
         %(conta_financeira_id)s, %(conta_financeira)s::jsonb,
         %(observacao)s, %(data_criacao)s, %(data_alteracao)s, %(raw)s::jsonb)
    ON CONFLICT (id) DO UPDATE SET
        parcela_id          = EXCLUDED.parcela_id,
        data_pagamento      = EXCLUDED.data_pagamento,
        valor               = EXCLUDED.valor,
        juros               = EXCLUDED.juros,
        multa               = EXCLUDED.multa,
        desconto            = EXCLUDED.desconto,
        metodo_pagamento    = EXCLUDED.metodo_pagamento,
        forma_pagamento     = EXCLUDED.forma_pagamento,
        conta_financeira_id = EXCLUDED.conta_financeira_id,
        conta_financeira    = EXCLUDED.conta_financeira,
        observacao          = EXCLUDED.observacao,
        data_alteracao      = EXCLUDED.data_alteracao,
        raw                 = EXCLUDED.raw,
        loaded_at           = NOW()
    """

    def fetch(self, client: Any, **kwargs: Any) -> Iterator[Baixa]:
        """Le parcela_ids de raw.parcelas_detalhe e busca baixas N+1."""
        watermark = kwargs.get("watermark")
        dw_conn = kwargs.get("dw_conn")
        schema_raw = kwargs.get("schema_raw")

        self._latest_data_alt = None

        if dw_conn is None or schema_raw is None:
            log.error("BaixasExtractor: faltando dw_conn ou schema_raw")
            return

        wm_value = watermark.get("value") if watermark else None
        with dw_conn.cursor() as cur:
            if wm_value:
                cur.execute(
                    f'SELECT id::text, data_alteracao FROM "{schema_raw}".conta_azul__parcelas_detalhe '
                    f'WHERE data_alteracao > %s ORDER BY data_alteracao',
                    (wm_value,),
                )
            else:
                cur.execute(
                    f'SELECT id::text, data_alteracao FROM "{schema_raw}".conta_azul__parcelas_detalhe '
                    f'ORDER BY data_alteracao'
                )
            parcela_rows = cur.fetchall()

        log.info(
            "Baixas: %d parcelas pra iterar baixas (watermark=%s)",
            len(parcela_rows), wm_value,
        )
        for parcela_id, parcela_data_alt in parcela_rows:
            baixas_raw = client.list_baixas_de_parcela(parcela_id)
            if not baixas_raw:
                continue
            for baixa_raw in baixas_raw:
                if not isinstance(baixa_raw, dict):
                    continue
                baixa_raw.setdefault("parcela_id", parcela_id)
                try:
                    baixa = Baixa.model_validate(baixa_raw)
                except Exception:
                    log.exception(
                        "Baixas: falha parseando baixa %s (parcela %s) — pulando",
                        baixa_raw.get("id"), parcela_id,
                    )
                    continue
                wm_candidate = baixa.data_alteracao or parcela_data_alt
                if wm_candidate is not None and (
                    self._latest_data_alt is None
                    or wm_candidate > self._latest_data_alt
                ):
                    self._latest_data_alt = wm_candidate
                yield baixa

    def watermark_value(self, item: Baixa) -> Any:
        return getattr(self, "_latest_data_alt", None)

    def serialize_for_upsert(self, item: Baixa) -> dict[str, Any]:
        raw_dict = item.model_dump(mode="json")
        return {
            "id": str(item.id),
            "parcela_id": str(item.parcela_id) if item.parcela_id else None,
            "data_pagamento": item.data_pagamento,
            "valor": item.valor,
            "juros": item.juros,
            "multa": item.multa,
            "desconto": item.desconto,
            "metodo_pagamento": item.metodo_pagamento,
            "forma_pagamento": item.forma_pagamento,
            "conta_financeira_id": (
                str(item.conta_financeira_id) if item.conta_financeira_id else None
            ),
            "conta_financeira": (
                json.dumps(item.conta_financeira) if item.conta_financeira else None
            ),
            "observacao": item.observacao,
            "data_criacao": item.data_criacao,
            "data_alteracao": item.data_alteracao,
            "raw": json.dumps(raw_dict, default=str),
        }

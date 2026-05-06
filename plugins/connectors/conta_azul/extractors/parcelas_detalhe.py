"""Extractor de detalhe de parcelas de eventos financeiros.

Endpoints API:
    GET /v1/financeiro/eventos-financeiros/{id_evento}/parcelas    (lista IDs)
    GET /v1/financeiro/eventos-financeiros/parcelas/{id}            (detalhe)
Tabela raw: <schema>.conta_azul__parcelas_detalhe

Padrao N+1 com 2 niveis:
1. Le evento_ids de raw.conta_azul__contas_a_receber + contas_a_pagar.
2. Pra cada evento, GET /eventos/{id}/parcelas -> lista de parcela_ids.
3. Pra cada parcela_id, GET /parcelas/{id} -> detalhe completo.

Por que combinado num so extractor: o endpoint de listagem (passo 2)
nao traz `data_pagamento`, `juros`, `multa`, `valor_liquido` — campos
criticos que motivam o extractor. So o detalhe (passo 3) traz. Fazer 2
extractors separados (parcelas_lista + parcelas_detalhe) duplicaria N+1
sem ganho. O detalhe ja inclui o que a lista traria + mais.

Watermark: max(data_alteracao) das parcelas vistas. Na 1a run, busca
detalhe de todas as parcelas de todos os eventos — pode ser pesado em
tenants com muitos eventos. Runs seguintes filtram via raw.contas_*.
data_alteracao > watermark anterior.

Requer raw.contas_a_receber + raw.contas_a_pagar populados (chain via
ENTITIES_DEPENDENT na DAG).
"""
from __future__ import annotations

import json
import logging
from typing import Any, ClassVar, Iterator

from connectors.conta_azul.extractors.base import Extractor
from connectors.conta_azul.schemas import ParcelaDetalhe


log = logging.getLogger(__name__)


class ParcelasDetalheExtractor(Extractor):
    NAME = "parcelas_detalhe"
    SCHEMA = ParcelaDetalhe
    INCREMENTAL_FIELD = "data_alteracao"
    INCREMENTAL_PARAM = None  # filtro feito local sobre raw

    REQUIRES_DW_READ: ClassVar[bool] = True

    DDL = """
    CREATE TABLE IF NOT EXISTS "{schema}"."conta_azul__parcelas_detalhe" (
        id                      UUID PRIMARY KEY,
        evento_financeiro_id    UUID,
        numero_parcela          INTEGER,
        valor                   NUMERIC(18, 4),
        valor_bruto             NUMERIC(18, 4),
        valor_liquido           NUMERIC(18, 4),
        desconto                NUMERIC(18, 4),
        juros                   NUMERIC(18, 4),
        multa                   NUMERIC(18, 4),
        data_vencimento         TIMESTAMPTZ,
        data_pagamento          TIMESTAMPTZ,
        status                  TEXT,
        forma_pagamento         TEXT,
        conta_financeira_id     UUID,
        conta_financeira        JSONB,
        data_criacao            TIMESTAMPTZ,
        data_alteracao          TIMESTAMPTZ,
        raw                     JSONB,
        loaded_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """

    UPSERT_SQL = """
    INSERT INTO "{schema}"."conta_azul__parcelas_detalhe"
        (id, evento_financeiro_id, numero_parcela, valor, valor_bruto,
         valor_liquido, desconto, juros, multa, data_vencimento,
         data_pagamento, status, forma_pagamento, conta_financeira_id,
         conta_financeira, data_criacao, data_alteracao, raw)
    VALUES
        (%(id)s, %(evento_financeiro_id)s, %(numero_parcela)s, %(valor)s,
         %(valor_bruto)s, %(valor_liquido)s, %(desconto)s, %(juros)s,
         %(multa)s, %(data_vencimento)s, %(data_pagamento)s, %(status)s,
         %(forma_pagamento)s, %(conta_financeira_id)s,
         %(conta_financeira)s::jsonb, %(data_criacao)s, %(data_alteracao)s,
         %(raw)s::jsonb)
    ON CONFLICT (id) DO UPDATE SET
        evento_financeiro_id = EXCLUDED.evento_financeiro_id,
        numero_parcela       = EXCLUDED.numero_parcela,
        valor                = EXCLUDED.valor,
        valor_bruto          = EXCLUDED.valor_bruto,
        valor_liquido        = EXCLUDED.valor_liquido,
        desconto             = EXCLUDED.desconto,
        juros                = EXCLUDED.juros,
        multa                = EXCLUDED.multa,
        data_vencimento      = EXCLUDED.data_vencimento,
        data_pagamento       = EXCLUDED.data_pagamento,
        status               = EXCLUDED.status,
        forma_pagamento      = EXCLUDED.forma_pagamento,
        conta_financeira_id  = EXCLUDED.conta_financeira_id,
        conta_financeira     = EXCLUDED.conta_financeira,
        data_alteracao       = EXCLUDED.data_alteracao,
        raw                  = EXCLUDED.raw,
        loaded_at            = NOW()
    """

    def fetch(self, client: Any, **kwargs: Any) -> Iterator[ParcelaDetalhe]:
        """Itera eventos -> parcelas -> detalhe da parcela.

        Watermark trackeado em self._latest_data_alt (max visto durante o
        fetch); watermark_value() ignora o item e devolve o tracker.
        """
        watermark = kwargs.get("watermark")
        dw_conn = kwargs.get("dw_conn")
        schema_raw = kwargs.get("schema_raw")

        self._latest_data_alt = None

        if dw_conn is None or schema_raw is None:
            log.error("ParcelasDetalheExtractor: faltando dw_conn ou schema_raw")
            return

        wm_value = watermark.get("value") if watermark else None
        # UNION dos eventos a receber + a pagar. Filtra pelos alterados desde
        # o watermark — eventos antigos sem mudanca recente nao precisam ser
        # re-iterados. Evento novo OU alterado puxa todas suas parcelas.
        with dw_conn.cursor() as cur:
            base_sql = (
                f'SELECT id::text, data_alteracao FROM "{schema_raw}".conta_azul__contas_a_receber '
                f'{{filter}} '
                f'UNION ALL '
                f'SELECT id::text, data_alteracao FROM "{schema_raw}".conta_azul__contas_a_pagar '
                f'{{filter}} '
                f'ORDER BY data_alteracao'
            )
            if wm_value:
                cur.execute(
                    base_sql.format(filter='WHERE data_alteracao > %s'),
                    (wm_value, wm_value),
                )
            else:
                cur.execute(base_sql.format(filter=''))
            evento_rows = cur.fetchall()

        log.info(
            "ParcelasDetalhe: %d eventos pra iterar parcelas (watermark=%s)",
            len(evento_rows), wm_value,
        )
        for evento_id, evento_data_alt in evento_rows:
            parcelas_lista = client.list_parcelas_de_evento(evento_id)
            if not parcelas_lista:
                continue
            for parcela_raw in parcelas_lista:
                parcela_id = parcela_raw.get("id") if isinstance(parcela_raw, dict) else None
                if not parcela_id:
                    continue
                detalhe_raw = client.get_parcela_detalhe(parcela_id)
                if detalhe_raw is None:
                    continue
                # Garante FK pro evento (caso o detalhe nao traga).
                detalhe_raw.setdefault("evento_financeiro_id", evento_id)
                try:
                    detalhe = ParcelaDetalhe.model_validate(detalhe_raw)
                except Exception:
                    log.exception(
                        "ParcelasDetalhe: falha parseando parcela %s (evento %s) — pulando",
                        parcela_id, evento_id,
                    )
                    continue
                # Watermark: usa data_alteracao do detalhe quando vier; senao
                # cai no data_alteracao do evento (que ja serviu de filtro).
                wm_candidate = detalhe.data_alteracao or evento_data_alt
                if wm_candidate is not None and (
                    self._latest_data_alt is None
                    or wm_candidate > self._latest_data_alt
                ):
                    self._latest_data_alt = wm_candidate
                yield detalhe

    def watermark_value(self, item: ParcelaDetalhe) -> Any:
        return getattr(self, "_latest_data_alt", None)

    def serialize_for_upsert(self, item: ParcelaDetalhe) -> dict[str, Any]:
        raw_dict = item.model_dump(mode="json")
        return {
            "id": str(item.id),
            "evento_financeiro_id": (
                str(item.evento_financeiro_id) if item.evento_financeiro_id else None
            ),
            "numero_parcela": item.numero_parcela,
            "valor": item.valor,
            "valor_bruto": item.valor_bruto,
            "valor_liquido": item.valor_liquido,
            "desconto": item.desconto,
            "juros": item.juros,
            "multa": item.multa,
            "data_vencimento": item.data_vencimento,
            "data_pagamento": item.data_pagamento,
            "status": item.status,
            "forma_pagamento": item.forma_pagamento,
            "conta_financeira_id": (
                str(item.conta_financeira_id) if item.conta_financeira_id else None
            ),
            "conta_financeira": (
                json.dumps(item.conta_financeira) if item.conta_financeira else None
            ),
            "data_criacao": item.data_criacao,
            "data_alteracao": item.data_alteracao,
            "raw": json.dumps(raw_dict, default=str),
        }

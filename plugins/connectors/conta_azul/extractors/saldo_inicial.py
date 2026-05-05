"""Extractor de saldo inicial por conta financeira.

Endpoint API:  GET /v1/financeiro/eventos-financeiros/saldo-inicial
Tabela raw:    <schema>.conta_azul__saldo_inicial

Doc exige data_inicio + data_fim. Granularidade: 1 row por
(conta_financeira_id, data_referencia). Quando a API nao expoe `id`
explicito, geramos UUID v5 deterministico baseado em (conta+data) —
garante UPSERT idempotente em reruns.

Janela: 1 ano. Window only (sem data_alteracao_de).
"""
from __future__ import annotations

import json
import logging
from datetime import date, timedelta
from typing import Any, Iterator
from uuid import NAMESPACE_OID, uuid5

from connectors.conta_azul.extractors.base import Extractor
from connectors.conta_azul.schemas import SaldoInicial


log = logging.getLogger(__name__)


def _deterministic_id(conta_id: object, data_ref: object) -> str:
    """UUID v5 estavel a partir de (conta_financeira_id, data_referencia).

    Mesmo (conta, data) sempre gera o mesmo UUID — ON CONFLICT do UPSERT
    funciona corretamente em reruns sem duplicar.
    """
    key = f"saldo_inicial|{conta_id or 'null'}|{data_ref or 'null'}"
    return str(uuid5(NAMESPACE_OID, key))


class SaldoInicialExtractor(Extractor):
    NAME = "saldo_inicial"
    SCHEMA = SaldoInicial

    # PK = id quando a API expoe; senao gerado deterministico via hash em
    # serialize_for_upsert. UPSERT por id garante idempotencia.
    DDL = """
    CREATE TABLE IF NOT EXISTS "{schema}"."conta_azul__saldo_inicial" (
        id                    UUID PRIMARY KEY,
        conta_financeira_id   UUID,
        data_referencia       TIMESTAMPTZ,
        saldo                 NUMERIC(18, 4),
        saldo_inicial         NUMERIC(18, 4),
        conta_financeira      JSONB,
        raw                   JSONB,
        loaded_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """

    UPSERT_SQL = """
    INSERT INTO "{schema}"."conta_azul__saldo_inicial"
        (id, conta_financeira_id, data_referencia, saldo, saldo_inicial,
         conta_financeira, raw)
    VALUES
        (%(id)s, %(conta_financeira_id)s, %(data_referencia)s,
         %(saldo)s, %(saldo_inicial)s, %(conta_financeira)s::jsonb,
         %(raw)s::jsonb)
    ON CONFLICT (id) DO UPDATE SET
        conta_financeira_id = EXCLUDED.conta_financeira_id,
        data_referencia     = EXCLUDED.data_referencia,
        saldo               = EXCLUDED.saldo,
        saldo_inicial       = EXCLUDED.saldo_inicial,
        conta_financeira    = EXCLUDED.conta_financeira,
        raw                 = EXCLUDED.raw,
        loaded_at           = NOW()
    """

    def fetch(self, client: Any, **kwargs: Any) -> Iterator[SaldoInicial]:
        # API exige formato ISO 8601 datetime (ex: 2025-10-20T07:59:59), nao
        # date simples. Erro explicito: "Os parametros de data devem estar
        # no formato ISO 8601 date-time".
        today = date.today()
        one_year_ago = today - timedelta(days=365)
        yield from client.paginate_all(
            "/financeiro/eventos-financeiros/saldo-inicial", SaldoInicial,
            extra_params={
                "data_inicio": f"{one_year_ago.isoformat()}T00:00:00",
                "data_fim": f"{today.isoformat()}T23:59:59",
            },
        )

    def serialize_for_upsert(self, item: SaldoInicial) -> dict[str, Any]:
        # ID pode nao existir na resposta — neste caso geramos UUID v5
        # deterministico de (conta_financeira_id, data_referencia). Mesmo
        # input -> mesmo UUID, entao UPSERT por id funciona em reruns
        # sem criar duplicata.
        conta_id = item.conta_financeira_id
        if not conta_id and item.conta_financeira and "id" in item.conta_financeira:
            conta_id = item.conta_financeira["id"]
        item_id = item.id
        if not item_id:
            item_id = _deterministic_id(conta_id, item.data_referencia)
            log.info(
                "saldo_inicial sem id da API — usando UUID v5 deterministico "
                "(conta=%s, data=%s) -> %s",
                conta_id, item.data_referencia, item_id,
            )
        return {
            "id": str(item_id),
            "conta_financeira_id": str(conta_id) if conta_id else None,
            "data_referencia": item.data_referencia,
            "saldo": item.saldo,
            "saldo_inicial": item.saldo_inicial,
            "conta_financeira": (
                json.dumps(item.conta_financeira) if item.conta_financeira else None
            ),
            "raw": json.dumps(item.model_dump(mode="json"), default=str),
        }

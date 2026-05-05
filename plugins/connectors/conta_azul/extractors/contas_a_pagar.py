"""Extractor de contas a pagar (eventos financeiros).

Endpoint API:  GET /v1/financeiro/eventos-financeiros/contas-a-pagar/buscar
Tabela raw:    <schema>.conta_azul__contas_a_pagar

Volume HIGH (450 itens na Luminea). Espelha contas_a_receber, com
`fornecedor` em vez de `cliente`.

**Hibrido window + incremental:** data_vencimento_de/ate (REQUIRED) define
a janela; data_alteracao_de (opcional) entra quando ha watermark — pega
deltas server-side incluindo eventos antigos cujo status mudou.
"""
from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any, Iterator

from connectors.conta_azul.extractors.base import Extractor
from connectors.conta_azul.schemas import EventoFinanceiro


class ContasPagarExtractor(Extractor):
    NAME = "contas_a_pagar"
    SCHEMA = EventoFinanceiro
    INCREMENTAL_FIELD = "data_alteracao"
    INCREMENTAL_PARAM = "data_alteracao_de"

    DDL = """
    CREATE TABLE IF NOT EXISTS "{schema}"."conta_azul__contas_a_pagar" (
        id                UUID PRIMARY KEY,
        status            TEXT,
        status_traduzido  TEXT,
        total             NUMERIC(18, 4),
        nao_pago          NUMERIC(18, 4),
        pago              NUMERIC(18, 4),
        descricao         TEXT,
        data_vencimento   TIMESTAMPTZ,
        data_competencia  TIMESTAMPTZ,
        data_criacao      TIMESTAMPTZ,
        data_alteracao    TIMESTAMPTZ,
        categorias        JSONB,
        centros_de_custo  JSONB,
        fornecedor        JSONB,
        renegociacao      JSONB,
        raw               JSONB,
        loaded_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """

    UPSERT_SQL = """
    INSERT INTO "{schema}"."conta_azul__contas_a_pagar"
        (id, status, status_traduzido, total, nao_pago, pago, descricao,
         data_vencimento, data_competencia, data_criacao, data_alteracao,
         categorias, centros_de_custo, fornecedor, renegociacao, raw)
    VALUES
        (%(id)s, %(status)s, %(status_traduzido)s, %(total)s, %(nao_pago)s,
         %(pago)s, %(descricao)s, %(data_vencimento)s, %(data_competencia)s,
         %(data_criacao)s, %(data_alteracao)s,
         %(categorias)s::jsonb, %(centros_de_custo)s::jsonb,
         %(fornecedor)s::jsonb, %(renegociacao)s::jsonb, %(raw)s::jsonb)
    ON CONFLICT (id) DO UPDATE SET
        status           = EXCLUDED.status,
        status_traduzido = EXCLUDED.status_traduzido,
        total            = EXCLUDED.total,
        nao_pago         = EXCLUDED.nao_pago,
        pago             = EXCLUDED.pago,
        descricao        = EXCLUDED.descricao,
        data_vencimento  = EXCLUDED.data_vencimento,
        data_competencia = EXCLUDED.data_competencia,
        data_alteracao   = EXCLUDED.data_alteracao,
        categorias       = EXCLUDED.categorias,
        centros_de_custo = EXCLUDED.centros_de_custo,
        fornecedor       = EXCLUDED.fornecedor,
        renegociacao     = EXCLUDED.renegociacao,
        raw              = EXCLUDED.raw,
        loaded_at        = NOW()
    """

    def fetch(self, client: Any, **kwargs: Any) -> Iterator[EventoFinanceiro]:
        # API exige data_alteracao_de em formato ISO 8601 datetime
        # (YYYY-MM-DDTHH:MM:SS) sem timezone.
        today = date.today()
        five_years_ago = today - timedelta(days=5 * 365)
        extra_params: dict[str, Any] = {
            "data_vencimento_de": five_years_ago.isoformat(),
            "data_vencimento_ate": today.isoformat(),
        }
        watermark = kwargs.get("watermark")
        if watermark and watermark.get("value"):
            wm_dt = watermark["value"].split(".")[0].split("+")[0].rstrip("Z")
            extra_params["data_alteracao_de"] = wm_dt
            extra_params["data_alteracao_ate"] = f"{today.isoformat()}T23:59:59"

        yield from client.paginate_all(
            "/financeiro/eventos-financeiros/contas-a-pagar/buscar",
            EventoFinanceiro,
            extra_params=extra_params,
        )

    def serialize_for_upsert(self, item: EventoFinanceiro) -> dict[str, Any]:
        raw_dict = item.model_dump(mode="json")
        return {
            "id": str(item.id),
            "status": item.status,
            "status_traduzido": item.status_traduzido,
            "total": item.total,
            "nao_pago": item.nao_pago,
            "pago": item.pago,
            "descricao": item.descricao,
            "data_vencimento": item.data_vencimento,
            "data_competencia": item.data_competencia,
            "data_criacao": item.data_criacao,
            "data_alteracao": item.data_alteracao,
            "categorias": json.dumps(item.categorias) if item.categorias else None,
            "centros_de_custo": json.dumps(item.centros_de_custo) if item.centros_de_custo else None,
            "fornecedor": json.dumps(item.fornecedor) if item.fornecedor else None,
            "renegociacao": json.dumps(item.renegociacao) if item.renegociacao else None,
            "raw": json.dumps(raw_dict, default=str),
        }

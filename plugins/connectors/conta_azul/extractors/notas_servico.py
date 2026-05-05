"""Extractor de notas fiscais de servico (NFS-e).

Endpoint API:  GET /v1/notas-fiscais-servico
Tabela raw:    <schema>.conta_azul__notas_servico

Volume HIGH em prod (cresce todo dia). Janela rolante de 15 dias por
data_competencia (limite imposto pela API). Multiplas chamadas pra
cobrir periodo maior — futuro otimizar com janelas paralelas.

Tenant Luminea retorna 0 — schema baseado em padrao da API. extra='allow'
captura campos novos. Coluna `raw` JSONB guarda objeto cru pra debug.
"""
from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any, Iterator

from connectors.conta_azul.extractors.base import Extractor
from connectors.conta_azul.schemas import NotaServico


class NotasServicoExtractor(Extractor):
    NAME = "notas_servico"
    SCHEMA = NotaServico

    DDL = """
    CREATE TABLE IF NOT EXISTS "{schema}"."conta_azul__notas_servico" (
        id                UUID PRIMARY KEY,
        numero            TEXT,
        status            TEXT,
        valor             NUMERIC(18, 4),
        data_emissao      TIMESTAMPTZ,
        data_competencia  TIMESTAMPTZ,
        cliente_id        UUID,
        raw               JSONB,
        loaded_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """

    UPSERT_SQL = """
    INSERT INTO "{schema}"."conta_azul__notas_servico"
        (id, numero, status, valor, data_emissao, data_competencia,
         cliente_id, raw)
    VALUES
        (%(id)s, %(numero)s, %(status)s, %(valor)s, %(data_emissao)s,
         %(data_competencia)s, %(cliente_id)s, %(raw)s::jsonb)
    ON CONFLICT (id) DO UPDATE SET
        numero           = EXCLUDED.numero,
        status           = EXCLUDED.status,
        valor            = EXCLUDED.valor,
        data_emissao     = EXCLUDED.data_emissao,
        data_competencia = EXCLUDED.data_competencia,
        cliente_id       = EXCLUDED.cliente_id,
        raw              = EXCLUDED.raw,
        loaded_at        = NOW()
    """

    def fetch(self, client: Any, **kwargs: Any) -> Iterator[NotaServico]:
        # API impoe janela max de 15 dias por chamada. Cobre 1 ano (~24
        # janelas) iterando. Conservador — em prod com volume real,
        # paralelizar e reduzir alcance via watermark.
        today = date.today()
        one_year_ago = today - timedelta(days=365)

        cursor = one_year_ago
        while cursor < today:
            window_end = min(cursor + timedelta(days=15), today)
            yield from client.paginate_all(
                "/notas-fiscais-servico",
                NotaServico,
                extra_params={
                    "data_competencia_de": cursor.isoformat(),
                    "data_competencia_ate": window_end.isoformat(),
                },
            )
            cursor = window_end + timedelta(days=1)

    def serialize_for_upsert(self, item: NotaServico) -> dict[str, Any]:
        raw_dict = item.model_dump(mode="json")
        return {
            "id": str(item.id),
            "numero": item.numero,
            "status": item.status,
            "valor": item.valor,
            "data_emissao": item.data_emissao,
            "data_competencia": item.data_competencia,
            "cliente_id": str(item.cliente_id) if item.cliente_id else None,
            "raw": json.dumps(raw_dict, default=str),
        }

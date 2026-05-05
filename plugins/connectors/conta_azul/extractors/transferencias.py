"""Extractor de transferencias entre contas financeiras.

Endpoint API:  GET /v1/financeiro/transferencias
Tabela raw:    <schema>.conta_azul__transferencias

Granularidade: 1 row por transferencia (movimento interno entre contas).
Doc exige data_inicio + data_fim — janela rolante 5 anos.
Sem filtro data_alteracao_de na doc — window only.
"""
from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any, Iterator

from connectors.conta_azul.extractors.base import Extractor
from connectors.conta_azul.schemas import Transferencia


class TransferenciasExtractor(Extractor):
    NAME = "transferencias"
    SCHEMA = Transferencia

    DDL = """
    CREATE TABLE IF NOT EXISTS "{schema}"."conta_azul__transferencias" (
        id                  UUID PRIMARY KEY,
        data_transferencia  TIMESTAMPTZ,
        valor               NUMERIC(18, 4),
        descricao           TEXT,
        conta_origem_id     UUID,
        conta_destino_id    UUID,
        conta_origem        JSONB,
        conta_destino       JSONB,
        data_criacao        TIMESTAMPTZ,
        data_alteracao      TIMESTAMPTZ,
        raw                 JSONB,
        loaded_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """

    UPSERT_SQL = """
    INSERT INTO "{schema}"."conta_azul__transferencias"
        (id, data_transferencia, valor, descricao,
         conta_origem_id, conta_destino_id,
         conta_origem, conta_destino,
         data_criacao, data_alteracao, raw)
    VALUES
        (%(id)s, %(data_transferencia)s, %(valor)s, %(descricao)s,
         %(conta_origem_id)s, %(conta_destino_id)s,
         %(conta_origem)s::jsonb, %(conta_destino)s::jsonb,
         %(data_criacao)s, %(data_alteracao)s, %(raw)s::jsonb)
    ON CONFLICT (id) DO UPDATE SET
        data_transferencia = EXCLUDED.data_transferencia,
        valor              = EXCLUDED.valor,
        descricao          = EXCLUDED.descricao,
        conta_origem_id    = EXCLUDED.conta_origem_id,
        conta_destino_id   = EXCLUDED.conta_destino_id,
        conta_origem       = EXCLUDED.conta_origem,
        conta_destino      = EXCLUDED.conta_destino,
        data_alteracao     = EXCLUDED.data_alteracao,
        raw                = EXCLUDED.raw,
        loaded_at          = NOW()
    """

    def fetch(self, client: Any, **kwargs: Any) -> Iterator[Transferencia]:
        # API enforce: data_inicio/fim diff max 1 ano. Janela rolante 5 anos
        # em chunks de 365d cobre historico completo.
        today = date.today()
        five_years_ago = today - timedelta(days=5 * 365)

        cursor = five_years_ago
        while cursor < today:
            window_end = min(cursor + timedelta(days=365), today)
            yield from client.paginate_all(
                "/financeiro/transferencias", Transferencia,
                extra_params={
                    "data_inicio": cursor.isoformat(),
                    "data_fim": window_end.isoformat(),
                },
            )
            cursor = window_end + timedelta(days=1)

    def serialize_for_upsert(self, item: Transferencia) -> dict[str, Any]:
        # Quando contas vem aninhadas, extrair id pra escalar.
        origem_id = item.conta_origem_id
        destino_id = item.conta_destino_id
        if not origem_id and item.conta_origem and "id" in item.conta_origem:
            origem_id = item.conta_origem["id"]
        if not destino_id and item.conta_destino and "id" in item.conta_destino:
            destino_id = item.conta_destino["id"]
        return {
            "id": str(item.id),
            "data_transferencia": item.data_transferencia,
            "valor": item.valor,
            "descricao": item.descricao,
            "conta_origem_id": str(origem_id) if origem_id else None,
            "conta_destino_id": str(destino_id) if destino_id else None,
            "conta_origem": json.dumps(item.conta_origem) if item.conta_origem else None,
            "conta_destino": json.dumps(item.conta_destino) if item.conta_destino else None,
            "data_criacao": item.data_criacao,
            "data_alteracao": item.data_alteracao,
            "raw": json.dumps(item.model_dump(mode="json"), default=str),
        }

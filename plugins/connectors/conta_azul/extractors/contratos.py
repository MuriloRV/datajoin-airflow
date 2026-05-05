"""Extractor de contratos.

Endpoint API:  GET /v1/contratos?data_inicio=YYYY-MM-DD&data_fim=YYYY-MM-DD
Tabela raw:    <schema>.conta_azul__contratos

Volume medio. Schema TENTATIVO — Luminea retorna 0 contratos, sem dado real
pra validar shape exato. extra='allow' protege de campos novos.

Janela: 5 anos retroativos ate hoje. Conservador pra captura inicial.
Quando aparecer dado real, pode-se ajustar pra incremental por
data_alteracao (se a API suportar).
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Iterator

from connectors.conta_azul.extractors.base import Extractor
from connectors.conta_azul.schemas import Contrato


class ContratosExtractor(Extractor):
    NAME = "contratos"
    SCHEMA = Contrato

    DDL = """
    CREATE TABLE IF NOT EXISTS "{schema}"."conta_azul__contratos" (
        id              UUID PRIMARY KEY,
        numero          TEXT,
        descricao       TEXT,
        cliente_id      UUID,
        valor           NUMERIC(18, 4),
        data_inicio     TIMESTAMPTZ,
        data_fim        TIMESTAMPTZ,
        status          TEXT,
        data_criacao    TIMESTAMPTZ,
        data_alteracao  TIMESTAMPTZ,
        raw             JSONB,
        loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """

    # `raw` JSONB guarda o objeto completo — protege contra mudanca de
    # schema (campos extras nao identificados ainda).
    UPSERT_SQL = """
    INSERT INTO "{schema}"."conta_azul__contratos"
        (id, numero, descricao, cliente_id, valor, data_inicio, data_fim,
         status, data_criacao, data_alteracao, raw)
    VALUES
        (%(id)s, %(numero)s, %(descricao)s, %(cliente_id)s, %(valor)s,
         %(data_inicio)s, %(data_fim)s, %(status)s, %(data_criacao)s,
         %(data_alteracao)s, %(raw)s::jsonb)
    ON CONFLICT (id) DO UPDATE SET
        numero         = EXCLUDED.numero,
        descricao      = EXCLUDED.descricao,
        cliente_id     = EXCLUDED.cliente_id,
        valor          = EXCLUDED.valor,
        data_inicio    = EXCLUDED.data_inicio,
        data_fim       = EXCLUDED.data_fim,
        status         = EXCLUDED.status,
        data_alteracao = EXCLUDED.data_alteracao,
        raw            = EXCLUDED.raw,
        loaded_at      = NOW()
    """

    def fetch(self, client: Any, **kwargs: Any) -> Iterator[Contrato]:
        # Janela rolante de 5 anos retroativos. Watermark futuro pode
        # encurtar isso, mas por enquanto full window.
        today = date.today()
        five_years_ago = today - timedelta(days=5 * 365)
        page = 1
        page_size = 100
        seen = 0
        while True:
            batch = client.list_contratos(
                data_inicio=five_years_ago.isoformat(),
                data_fim=today.isoformat(),
                page=page,
                page_size=page_size,
            )
            if not batch:
                break
            yield from batch
            seen += len(batch)
            if len(batch) < page_size:
                break
            page += 1

    def serialize_for_upsert(self, item: Contrato) -> dict[str, Any]:
        import json
        # Dump pydantic completo pra coluna `raw` — preserva campos
        # extras nao mapeados na coluna escalar.
        raw_dict = item.model_dump(mode="json")
        return {
            "id": str(item.id),
            "numero": item.numero,
            "descricao": item.descricao,
            "cliente_id": str(item.cliente_id) if item.cliente_id else None,
            "valor": item.valor,
            "data_inicio": item.data_inicio,
            "data_fim": item.data_fim,
            "status": item.status,
            "data_criacao": item.data_criacao,
            "data_alteracao": item.data_alteracao,
            "raw": json.dumps(raw_dict, default=str),
        }

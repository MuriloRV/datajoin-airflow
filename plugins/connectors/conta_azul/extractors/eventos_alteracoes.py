"""Extractor de IDs de eventos financeiros alterados (CDC oficial).

Endpoint API: GET /v1/financeiro/eventos-financeiros/alteracoes
Tabela raw:   <schema>.conta_azul__eventos_alteracoes

Uso analytics: tabela auxiliar de Change Data Capture. Lista IDs de
eventos financeiros (contas_a_receber/pagar) que mudaram num intervalo
de tempo. Complementa o filtro `data_alteracao_de` do /buscar — captura
mudancas em eventos cuja `data_vencimento` caiu fora da janela de 5 anos
do extractor principal.

Nao substitui contas_a_receber/pagar — e' um indicador. Pode ser usado
em runs futuras pra orientar buscas de detalhe (parcelas/baixas) so dos
eventos que de fato mudaram.

Watermark: max(data_alteracao). Janela default: 30 dias retroativos no
1o run; depois sobe pelo watermark.
"""
from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Iterator

from connectors.conta_azul.extractors.base import Extractor
from connectors.conta_azul.schemas import EventoAlteracao


log = logging.getLogger(__name__)


class EventosAlteracoesExtractor(Extractor):
    NAME = "eventos_alteracoes"
    SCHEMA = EventoAlteracao
    INCREMENTAL_FIELD = "data_alteracao"
    INCREMENTAL_PARAM = "data_alteracao_de"

    DDL = """
    CREATE TABLE IF NOT EXISTS "{schema}"."conta_azul__eventos_alteracoes" (
        id              UUID PRIMARY KEY,
        data_alteracao  TIMESTAMPTZ,
        tipo_evento     TEXT,
        raw             JSONB,
        loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """

    UPSERT_SQL = """
    INSERT INTO "{schema}"."conta_azul__eventos_alteracoes"
        (id, data_alteracao, tipo_evento, raw)
    VALUES
        (%(id)s, %(data_alteracao)s, %(tipo_evento)s, %(raw)s::jsonb)
    ON CONFLICT (id) DO UPDATE SET
        data_alteracao = EXCLUDED.data_alteracao,
        tipo_evento    = EXCLUDED.tipo_evento,
        raw            = EXCLUDED.raw,
        loaded_at      = NOW()
    """

    def fetch(self, client: Any, **kwargs: Any) -> Iterator[EventoAlteracao]:
        watermark = kwargs.get("watermark")
        # Janela rolante: 30 dias retroativos no 1o run; watermark depois.
        ate = datetime.now(timezone.utc).replace(microsecond=0)
        if watermark and watermark.get("value"):
            wm_str = watermark["value"].split(".")[0].split("+")[0].rstrip("Z")
            de_str = wm_str
        else:
            de = ate - timedelta(days=30)
            de_str = de.replace(tzinfo=None).isoformat(timespec="seconds")

        ate_str = ate.replace(tzinfo=None).isoformat(timespec="seconds")
        log.info(
            "EventosAlteracoes: janela %s -> %s", de_str, ate_str
        )
        yield from client.paginate_all(
            "/financeiro/eventos-financeiros/alteracoes",
            EventoAlteracao,
            extra_params={
                "data_alteracao_de": de_str,
                "data_alteracao_ate": ate_str,
            },
        )

    def serialize_for_upsert(self, item: EventoAlteracao) -> dict[str, Any]:
        raw_dict = item.model_dump(mode="json")
        return {
            "id": str(item.id),
            "data_alteracao": item.data_alteracao,
            "tipo_evento": item.tipo_evento,
            "raw": json.dumps(raw_dict, default=str),
        }

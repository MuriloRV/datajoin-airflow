"""Extractor de snapshot diario de saldo por conta financeira.

Endpoint API: GET /v1/conta-financeira/{id}/saldo-atual
Tabela raw:   <schema>.conta_azul__saldo_atual

Granularidade: 1 row = 1 conta + 1 data (snapshot diario via run da DAG).
PK = UUID v5 hash de (conta_financeira_id, snapshot_date) — UPSERT
idempotente intra-dia (rerodar a DAG no mesmo dia atualiza o saldo).
Append-only entre dias (cada dia gera nova PK -> historico).

Padrao N+1 lendo raw.contas_financeiras. Sem watermark — full snapshot
a cada run.

Critico pra: fluxo de caixa diario, DRE de regime de caixa, alertas de
saldo negativo, conciliacao bancaria.
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, timezone
from typing import Any, ClassVar, Iterator
from uuid import NAMESPACE_OID, uuid5

from connectors.conta_azul.extractors.base import Extractor
from connectors.conta_azul.schemas import SaldoAtual


log = logging.getLogger(__name__)


def _snapshot_id(conta_id: str, snapshot_date: date) -> str:
    """UUID v5 estavel a partir de (conta, dia). UPSERT idempotente intra-dia."""
    key = f"saldo_atual|{conta_id}|{snapshot_date.isoformat()}"
    return str(uuid5(NAMESPACE_OID, key))


class SaldoAtualExtractor(Extractor):
    NAME = "saldo_atual"
    SCHEMA = SaldoAtual

    REQUIRES_DW_READ: ClassVar[bool] = True

    DDL = """
    CREATE TABLE IF NOT EXISTS "{schema}"."conta_azul__saldo_atual" (
        id                    UUID PRIMARY KEY,
        conta_financeira_id   UUID,
        saldo_atual           NUMERIC(18, 4),
        saldo                 NUMERIC(18, 4),
        snapshot_date         DATE,
        data_snapshot         TIMESTAMPTZ,
        raw                   JSONB,
        loaded_at             TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """

    UPSERT_SQL = """
    INSERT INTO "{schema}"."conta_azul__saldo_atual"
        (id, conta_financeira_id, saldo_atual, saldo, snapshot_date,
         data_snapshot, raw)
    VALUES
        (%(id)s, %(conta_financeira_id)s, %(saldo_atual)s, %(saldo)s,
         %(snapshot_date)s, %(data_snapshot)s, %(raw)s::jsonb)
    ON CONFLICT (id) DO UPDATE SET
        saldo_atual    = EXCLUDED.saldo_atual,
        saldo          = EXCLUDED.saldo,
        data_snapshot  = EXCLUDED.data_snapshot,
        raw            = EXCLUDED.raw,
        loaded_at      = NOW()
    """

    def fetch(self, client: Any, **kwargs: Any) -> Iterator[SaldoAtual]:
        dw_conn = kwargs.get("dw_conn")
        schema_raw = kwargs.get("schema_raw")

        if dw_conn is None or schema_raw is None:
            log.error("SaldoAtualExtractor: faltando dw_conn ou schema_raw")
            return

        with dw_conn.cursor() as cur:
            cur.execute(
                f'SELECT id::text FROM "{schema_raw}".conta_azul__contas_financeiras '
                f'WHERE COALESCE(ativo, true) = true'
            )
            conta_ids = [row[0] for row in cur.fetchall()]

        now_utc = datetime.now(timezone.utc).replace(microsecond=0)
        snapshot_date = now_utc.date()
        log.info(
            "SaldoAtual: snapshotting %d contas em %s",
            len(conta_ids), snapshot_date,
        )
        for conta_id in conta_ids:
            saldo_raw = client.get_saldo_atual(conta_id)
            if saldo_raw is None:
                continue
            saldo_raw["conta_financeira_id"] = conta_id
            saldo_raw["data_snapshot"] = now_utc.isoformat()
            try:
                saldo = SaldoAtual.model_validate(saldo_raw)
            except Exception:
                log.exception(
                    "SaldoAtual: falha parseando saldo da conta %s — pulando",
                    conta_id,
                )
                continue
            # Stash snapshot_date pra serializacao (nao esta no schema).
            saldo._snapshot_date = snapshot_date  # type: ignore[attr-defined]
            yield saldo

    def serialize_for_upsert(self, item: SaldoAtual) -> dict[str, Any]:
        raw_dict = item.model_dump(mode="json")
        snapshot_date: date = getattr(item, "_snapshot_date", date.today())
        item_id = _snapshot_id(
            str(item.conta_financeira_id) if item.conta_financeira_id else "unknown",
            snapshot_date,
        )
        return {
            "id": item_id,
            "conta_financeira_id": (
                str(item.conta_financeira_id) if item.conta_financeira_id else None
            ),
            "saldo_atual": item.saldo_atual,
            "saldo": item.saldo,
            "snapshot_date": snapshot_date,
            "data_snapshot": item.data_snapshot,
            "raw": json.dumps(raw_dict, default=str),
        }

"""Extractor de cobrancas (boletos / PIX) emitidas em contas a receber.

Endpoint API: GET /v1/financeiro/eventos-financeiros/contas-a-receber/cobranca/{id_cobranca}
Tabela raw:   <schema>.conta_azul__cobrancas

Granularidade: 1 row = 1 cobranca. Status do funil: AGUARDANDO_CONFIRMACAO,
REGISTRADO, QUITADO, CANCELADO, INVALIDO, EXPIRADO.

Descoberta de IDs: a doc da Conta Azul nao expoe endpoint de listagem
de cobrancas. IDs costumam aparecer dentro do payload de raw.parcelas_detalhe
(campo `cobranca` ou `cobrancas`) ou raw.contas_a_receber.raw->cobrancas.
Este extractor faz scan defensivo em ambas as tabelas raw, coleta IDs
unicos, e busca o detalhe de cada um.

Watermark: max(data_alteracao). Conservador — pode reprocessar cobrancas
inalteradas em parcelas mexidas, mas o UPSERT por id absorve.
"""
from __future__ import annotations

import json
import logging
from typing import Any, ClassVar, Iterator

from connectors.conta_azul.extractors.base import Extractor
from connectors.conta_azul.schemas import Cobranca


log = logging.getLogger(__name__)


def _scan_cobranca_ids(payload: Any) -> Iterator[str]:
    """Caminha o JSON procurando objetos com chave `cobranca`/`cobrancas`
    contendo `id` UUID-like. Defensivo contra variacoes de shape entre
    tenants — a API do Conta Azul nao documenta o path exato.
    """
    if isinstance(payload, dict):
        for key, value in payload.items():
            if key == "cobranca" and isinstance(value, dict) and value.get("id"):
                yield str(value["id"])
            elif key == "cobrancas" and isinstance(value, list):
                for c in value:
                    if isinstance(c, dict) and c.get("id"):
                        yield str(c["id"])
            elif key == "id_cobranca" and value:
                yield str(value)
            else:
                yield from _scan_cobranca_ids(value)
    elif isinstance(payload, list):
        for item in payload:
            yield from _scan_cobranca_ids(item)


class CobrancasExtractor(Extractor):
    NAME = "cobrancas"
    SCHEMA = Cobranca
    INCREMENTAL_FIELD = "data_alteracao"
    INCREMENTAL_PARAM = None

    REQUIRES_DW_READ: ClassVar[bool] = True

    DDL = """
    CREATE TABLE IF NOT EXISTS "{schema}"."conta_azul__cobrancas" (
        id                      UUID PRIMARY KEY,
        parcela_id              UUID,
        evento_financeiro_id    UUID,
        status                  TEXT,
        url                     TEXT,
        valor                   NUMERIC(18, 4),
        tipo                    TEXT,
        data_vencimento         TIMESTAMPTZ,
        data_emissao            TIMESTAMPTZ,
        data_alteracao          TIMESTAMPTZ,
        raw                     JSONB,
        loaded_at               TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """

    UPSERT_SQL = """
    INSERT INTO "{schema}"."conta_azul__cobrancas"
        (id, parcela_id, evento_financeiro_id, status, url, valor, tipo,
         data_vencimento, data_emissao, data_alteracao, raw)
    VALUES
        (%(id)s, %(parcela_id)s, %(evento_financeiro_id)s, %(status)s,
         %(url)s, %(valor)s, %(tipo)s, %(data_vencimento)s,
         %(data_emissao)s, %(data_alteracao)s, %(raw)s::jsonb)
    ON CONFLICT (id) DO UPDATE SET
        parcela_id           = EXCLUDED.parcela_id,
        evento_financeiro_id = EXCLUDED.evento_financeiro_id,
        status               = EXCLUDED.status,
        url                  = EXCLUDED.url,
        valor                = EXCLUDED.valor,
        tipo                 = EXCLUDED.tipo,
        data_vencimento      = EXCLUDED.data_vencimento,
        data_emissao         = EXCLUDED.data_emissao,
        data_alteracao       = EXCLUDED.data_alteracao,
        raw                  = EXCLUDED.raw,
        loaded_at            = NOW()
    """

    def fetch(self, client: Any, **kwargs: Any) -> Iterator[Cobranca]:
        dw_conn = kwargs.get("dw_conn")
        schema_raw = kwargs.get("schema_raw")

        self._latest_data_alt = None

        if dw_conn is None or schema_raw is None:
            log.error("CobrancasExtractor: faltando dw_conn ou schema_raw")
            return

        # Coleta IDs candidatos varrendo o JSONB de parcelas_detalhe + contas_a_receber.
        cobranca_ids: set[str] = set()
        with dw_conn.cursor() as cur:
            for source_table in (
                "conta_azul__parcelas_detalhe",
                "conta_azul__contas_a_receber",
            ):
                # tabela pode ainda nao existir no 1o run da DAG — best-effort.
                try:
                    cur.execute(
                        f'SELECT raw FROM "{schema_raw}"."{source_table}" '
                        f'WHERE raw IS NOT NULL'
                    )
                    for (raw_json,) in cur:
                        if raw_json is None:
                            continue
                        # raw vem dict (jsonb -> psycopg auto-decode) ou str dependendo do driver.
                        payload = raw_json if isinstance(raw_json, (dict, list)) else json.loads(raw_json)
                        for cid in _scan_cobranca_ids(payload):
                            cobranca_ids.add(cid)
                except Exception as e:  # noqa: BLE001
                    log.warning(
                        "Cobrancas: scan de %s falhou (%s) — pulando essa fonte",
                        source_table, e,
                    )
                    continue

        log.info(
            "Cobrancas: %d IDs descobertos pra buscar detalhe", len(cobranca_ids)
        )
        for cid in cobranca_ids:
            cobranca_raw = client.get_cobranca(cid)
            if cobranca_raw is None:
                continue
            cobranca_raw.setdefault("id", cid)
            try:
                cobranca = Cobranca.model_validate(cobranca_raw)
            except Exception:
                log.exception(
                    "Cobrancas: falha parseando cobranca %s — pulando", cid
                )
                continue
            if cobranca.data_alteracao is not None and (
                self._latest_data_alt is None
                or cobranca.data_alteracao > self._latest_data_alt
            ):
                self._latest_data_alt = cobranca.data_alteracao
            yield cobranca

    def watermark_value(self, item: Cobranca) -> Any:
        return getattr(self, "_latest_data_alt", None)

    def serialize_for_upsert(self, item: Cobranca) -> dict[str, Any]:
        raw_dict = item.model_dump(mode="json")
        return {
            "id": str(item.id),
            "parcela_id": str(item.parcela_id) if item.parcela_id else None,
            "evento_financeiro_id": (
                str(item.evento_financeiro_id) if item.evento_financeiro_id else None
            ),
            "status": item.status,
            "url": item.url,
            "valor": item.valor,
            "tipo": item.tipo,
            "data_vencimento": item.data_vencimento,
            "data_emissao": item.data_emissao,
            "data_alteracao": item.data_alteracao,
            "raw": json.dumps(raw_dict, default=str),
        }

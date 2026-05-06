"""Extractor de itens de NFe (line items extraidos do detalhe da nota).

Endpoint API: GET /v1/notas-fiscais/{chave_acesso}
Tabela raw:   <schema>.conta_azul__notas_fiscais_itens

Granularidade: 1 row = 1 linha de produto na nota fiscal.

A doc nao expoe endpoint dedicado de itens de NFe — vem aninhado dentro
do detalhe da nota (campo `itens` no payload). Este extractor faz N+1
sobre raw.notas_fiscais.chave_acesso, busca o detalhe completo, e
desempacota cada item em uma row.

Replace strategy: como em vendas_itens, deleta itens antigos da NF
antes do batch de upsert da mesma chave.
"""
from __future__ import annotations

import json
import logging
from typing import Any, ClassVar, Iterator
from uuid import NAMESPACE_OID, uuid5

from connectors.conta_azul.extractors.base import Extractor
from connectors.conta_azul.schemas import NotaFiscalItem


log = logging.getLogger(__name__)


def _deterministic_nf_item_id(chave: str, numero_item: int) -> str:
    """UUID v5 estavel a partir de (chave_acesso, numero_item) — itens de
    NFe nem sempre tem PK propria; numero_item dentro da chave e' unique."""
    key = f"nf_item|{chave}|{numero_item}"
    return str(uuid5(NAMESPACE_OID, key))


class NotasFiscaisItensExtractor(Extractor):
    NAME = "notas_fiscais_itens"
    SCHEMA = NotaFiscalItem

    REQUIRES_DW_READ: ClassVar[bool] = True

    DDL = """
    CREATE TABLE IF NOT EXISTS "{schema}"."conta_azul__notas_fiscais_itens" (
        id              TEXT PRIMARY KEY,
        nota_fiscal_id  UUID,
        chave_acesso    TEXT,
        numero_item     INTEGER,
        produto_id      UUID,
        codigo          TEXT,
        descricao       TEXT,
        ncm             TEXT,
        cfop            TEXT,
        quantidade      NUMERIC(18, 4),
        valor_unitario  NUMERIC(18, 4),
        valor_total     NUMERIC(18, 4),
        valor_icms      NUMERIC(18, 4),
        valor_ipi       NUMERIC(18, 4),
        valor_pis       NUMERIC(18, 4),
        valor_cofins    NUMERIC(18, 4),
        raw             JSONB,
        loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """

    UPSERT_SQL = """
    INSERT INTO "{schema}"."conta_azul__notas_fiscais_itens"
        (id, nota_fiscal_id, chave_acesso, numero_item, produto_id, codigo,
         descricao, ncm, cfop, quantidade, valor_unitario, valor_total,
         valor_icms, valor_ipi, valor_pis, valor_cofins, raw)
    VALUES
        (%(id)s, %(nota_fiscal_id)s, %(chave_acesso)s, %(numero_item)s,
         %(produto_id)s, %(codigo)s, %(descricao)s, %(ncm)s, %(cfop)s,
         %(quantidade)s, %(valor_unitario)s, %(valor_total)s, %(valor_icms)s,
         %(valor_ipi)s, %(valor_pis)s, %(valor_cofins)s, %(raw)s::jsonb)
    ON CONFLICT (id) DO UPDATE SET
        nota_fiscal_id = EXCLUDED.nota_fiscal_id,
        chave_acesso   = EXCLUDED.chave_acesso,
        numero_item    = EXCLUDED.numero_item,
        produto_id     = EXCLUDED.produto_id,
        codigo         = EXCLUDED.codigo,
        descricao      = EXCLUDED.descricao,
        ncm            = EXCLUDED.ncm,
        cfop           = EXCLUDED.cfop,
        quantidade     = EXCLUDED.quantidade,
        valor_unitario = EXCLUDED.valor_unitario,
        valor_total    = EXCLUDED.valor_total,
        valor_icms     = EXCLUDED.valor_icms,
        valor_ipi      = EXCLUDED.valor_ipi,
        valor_pis      = EXCLUDED.valor_pis,
        valor_cofins   = EXCLUDED.valor_cofins,
        raw            = EXCLUDED.raw,
        loaded_at      = NOW()
    """

    def fetch(self, client: Any, **kwargs: Any) -> Iterator[NotaFiscalItem]:
        dw_conn = kwargs.get("dw_conn")
        schema_raw = kwargs.get("schema_raw")

        if dw_conn is None or schema_raw is None:
            log.error("NotasFiscaisItensExtractor: faltando dw_conn ou schema_raw")
            return

        with dw_conn.cursor() as cur:
            cur.execute(
                f'SELECT id::text, chave_acesso FROM "{schema_raw}".conta_azul__notas_fiscais '
                f'WHERE chave_acesso IS NOT NULL '
                f'ORDER BY data_emissao DESC NULLS LAST'
            )
            nf_rows = cur.fetchall()

        log.info(
            "NotasFiscaisItens: %d notas pra extrair itens", len(nf_rows)
        )
        for nf_id, chave in nf_rows:
            detalhe_raw = client.get_nota_fiscal_detalhe(chave)
            if not detalhe_raw:
                continue
            # Itens podem estar em `itens`, `produtos`, ou aninhados em outra chave.
            itens_raw = (
                detalhe_raw.get("itens")
                or detalhe_raw.get("produtos")
                or []
            )
            if not itens_raw:
                continue
            with dw_conn.cursor() as cur:
                cur.execute(
                    f'DELETE FROM "{schema_raw}".conta_azul__notas_fiscais_itens '
                    f'WHERE chave_acesso = %s',
                    (chave,),
                )
            for idx, item_raw in enumerate(itens_raw, start=1):
                if not isinstance(item_raw, dict):
                    continue
                numero_item = item_raw.get("numero_item") or item_raw.get("numero") or idx
                item_raw["nota_fiscal_id"] = nf_id
                item_raw.setdefault("chave_acesso", chave)
                item_raw["numero_item"] = numero_item
                if not item_raw.get("id"):
                    item_raw["id"] = _deterministic_nf_item_id(chave, int(numero_item))
                try:
                    item = NotaFiscalItem.model_validate(item_raw)
                except Exception:
                    log.exception(
                        "NotasFiscaisItens: falha parseando item da NFe %s — pulando",
                        chave,
                    )
                    continue
                yield item

    def serialize_for_upsert(self, item: NotaFiscalItem) -> dict[str, Any]:
        raw_dict = item.model_dump(mode="json")
        return {
            "id": str(item.id),
            "nota_fiscal_id": (
                str(item.nota_fiscal_id) if item.nota_fiscal_id else None
            ),
            "chave_acesso": item.chave_acesso,
            "numero_item": item.numero_item,
            "produto_id": str(item.produto_id) if item.produto_id else None,
            "codigo": item.codigo,
            "descricao": item.descricao,
            "ncm": item.ncm,
            "cfop": item.cfop,
            "quantidade": item.quantidade,
            "valor_unitario": item.valor_unitario,
            "valor_total": item.valor_total,
            "valor_icms": item.valor_icms,
            "valor_ipi": item.valor_ipi,
            "valor_pis": item.valor_pis,
            "valor_cofins": item.valor_cofins,
            "raw": json.dumps(raw_dict, default=str),
        }

"""Extractor de itens de vendas (line items / N+1 a partir de raw.vendas).

Endpoint API: GET /v1/vendas/{id}/itens  (fallback /v1/venda/{id}/itens)
Tabela raw:   <schema>.conta_azul__vendas_itens

Granularidade: 1 row = 1 linha de produto/servico vendido.

Critico pra analytics: o endpoint /venda/busca SO retorna o agregado
(total da venda, tipo). Sem esse extractor nao da pra calcular mix de
produtos, ticket medio por SKU, margem por item, top vendedores por
categoria etc.

Padrao N+1 com watermark sobre raw.vendas.data_alteracao — uma venda
alterada pode ter linhas alteradas/novas/removidas. Replace strategy:
deleta os itens antigos da venda antes de inserir os novos (evita
linhas orfas se a API removeu um item). Implementado dentro do fetch
chamando DELETE WHERE venda_id=... antes do batch de UPSERT.

NOTA: como o loader generico nao suporta DELETE pre-fetch, gravamos
a delecao por venda no proprio fetch (executando via dw_conn) antes
de yield. Isso desvia ligeiramente do contract base mas e' a forma
correta de evitar drift de items.
"""
from __future__ import annotations

import json
import logging
from typing import Any, ClassVar, Iterator
from uuid import NAMESPACE_OID, uuid5

from connectors.conta_azul.extractors.base import Extractor
from connectors.conta_azul.schemas import VendaItem


log = logging.getLogger(__name__)


def _deterministic_item_id(venda_id: str, numero_item: int | None, codigo: str | None) -> str:
    """UUID v5 estavel quando a API nao traz `id` por linha.

    Caso muito comum em itens — algumas APIs nao expoem PK por linha.
    Sem id, sem UPSERT idempotente. Usamos hash de (venda_id, numero_item, codigo)
    como fallback determinista.
    """
    key = f"venda_item|{venda_id}|{numero_item or 0}|{codigo or ''}"
    return str(uuid5(NAMESPACE_OID, key))


class VendasItensExtractor(Extractor):
    NAME = "vendas_itens"
    SCHEMA = VendaItem
    INCREMENTAL_FIELD = None  # propagado de raw.vendas, sem watermark proprio
    INCREMENTAL_PARAM = None

    REQUIRES_DW_READ: ClassVar[bool] = True

    DDL = """
    CREATE TABLE IF NOT EXISTS "{schema}"."conta_azul__vendas_itens" (
        id              UUID PRIMARY KEY,
        venda_id        UUID,
        produto_id      UUID,
        servico_id      UUID,
        descricao       TEXT,
        codigo          TEXT,
        quantidade      NUMERIC(18, 4),
        valor_unitario  NUMERIC(18, 4),
        desconto        NUMERIC(18, 4),
        valor_total     NUMERIC(18, 4),
        cfop            TEXT,
        ncm             TEXT,
        unidade         TEXT,
        raw             JSONB,
        loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """

    UPSERT_SQL = """
    INSERT INTO "{schema}"."conta_azul__vendas_itens"
        (id, venda_id, produto_id, servico_id, descricao, codigo,
         quantidade, valor_unitario, desconto, valor_total, cfop, ncm,
         unidade, raw)
    VALUES
        (%(id)s, %(venda_id)s, %(produto_id)s, %(servico_id)s,
         %(descricao)s, %(codigo)s, %(quantidade)s, %(valor_unitario)s,
         %(desconto)s, %(valor_total)s, %(cfop)s, %(ncm)s, %(unidade)s,
         %(raw)s::jsonb)
    ON CONFLICT (id) DO UPDATE SET
        venda_id       = EXCLUDED.venda_id,
        produto_id     = EXCLUDED.produto_id,
        servico_id     = EXCLUDED.servico_id,
        descricao      = EXCLUDED.descricao,
        codigo         = EXCLUDED.codigo,
        quantidade     = EXCLUDED.quantidade,
        valor_unitario = EXCLUDED.valor_unitario,
        desconto       = EXCLUDED.desconto,
        valor_total    = EXCLUDED.valor_total,
        cfop           = EXCLUDED.cfop,
        ncm            = EXCLUDED.ncm,
        unidade        = EXCLUDED.unidade,
        raw            = EXCLUDED.raw,
        loaded_at      = NOW()
    """

    def fetch(self, client: Any, **kwargs: Any) -> Iterator[VendaItem]:
        dw_conn = kwargs.get("dw_conn")
        schema_raw = kwargs.get("schema_raw")

        if dw_conn is None or schema_raw is None:
            log.error("VendasItensExtractor: faltando dw_conn ou schema_raw")
            return

        # Sem watermark proprio: cada run reprocessa todos itens das vendas
        # alteradas RECENTEMENTE. Conservador (recalcula linhas inalteradas
        # de vendas que so mudaram metadata) mas garante drift detection
        # quando a API remove itens. Ajustar para watermark se volume crescer.
        with dw_conn.cursor() as cur:
            cur.execute(
                f'SELECT id::text FROM "{schema_raw}".conta_azul__vendas '
                f'ORDER BY data_alteracao DESC NULLS LAST'
            )
            venda_ids = [row[0] for row in cur.fetchall()]

        log.info("VendasItens: iterando itens de %d vendas", len(venda_ids))
        for vid in venda_ids:
            itens_raw = client.list_venda_itens(vid)
            if not itens_raw:
                continue
            # Replace strategy: deleta itens antigos da venda antes do upsert.
            # Garante que linhas removidas no Conta Azul sumam do raw.
            with dw_conn.cursor() as cur:
                cur.execute(
                    f'DELETE FROM "{schema_raw}".conta_azul__vendas_itens '
                    f'WHERE venda_id = %s',
                    (vid,),
                )
            for idx, item_raw in enumerate(itens_raw, start=1):
                if not isinstance(item_raw, dict):
                    continue
                item_raw.setdefault("venda_id", vid)
                # Garante PK: usa id da API quando vier, ou hash deterministico.
                if not item_raw.get("id"):
                    item_raw["id"] = _deterministic_item_id(
                        vid,
                        item_raw.get("numero_item") or idx,
                        item_raw.get("codigo"),
                    )
                try:
                    item = VendaItem.model_validate(item_raw)
                except Exception:
                    log.exception(
                        "VendasItens: falha parseando item de venda %s — pulando",
                        vid,
                    )
                    continue
                yield item

    def serialize_for_upsert(self, item: VendaItem) -> dict[str, Any]:
        raw_dict = item.model_dump(mode="json")
        return {
            "id": str(item.id),
            "venda_id": str(item.venda_id) if item.venda_id else None,
            "produto_id": str(item.produto_id) if item.produto_id else None,
            "servico_id": str(item.servico_id) if item.servico_id else None,
            "descricao": item.descricao,
            "codigo": item.codigo,
            "quantidade": item.quantidade,
            "valor_unitario": item.valor_unitario,
            "desconto": item.desconto,
            "valor_total": item.valor_total,
            "cfop": item.cfop,
            "ncm": item.ncm,
            "unidade": item.unidade,
            "raw": json.dumps(raw_dict, default=str),
        }

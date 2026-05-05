"""Extractor de vendas (orcamentos, pedidos, vendas, notas fiscais).

Endpoint API:  GET /v1/venda/busca
Tabela raw:    <schema>.conta_azul__vendas

Endpoint correto (confirmado pela doc oficial):
    /v1/venda                 -> 405 (so /busca aceita GET)
    /v1/venda/busca           -> OK, envelope `itens`

Resposta tem totalizadores no nivel raiz (totais, quantidades, total_itens)
fora do envelope `itens` — paginate_all reconhece `total_itens` pra parada
inteligente.

Schema TENTATIVO — campos confirmados pela doc oficial sao os filtros
(numero, situacao, tipo, origem, ids_clientes/vendedores). Forma do item
nao foi documentada — extra='allow' + raw jsonb preservam o que falta.

Incremental: data_alteracao_de + data_alteracao_ate (ISO 8601, sem
microsegundos, sem timezone — endpoint documenta GMT-3, mantemos UTC
como nas outras entidades. Idempotente: UPSERT por id, watermark grava
max(data_alteracao) real visto).
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Iterator

from connectors.conta_azul.extractors.base import Extractor
from connectors.conta_azul.schemas import Venda


class VendasExtractor(Extractor):
    NAME = "vendas"
    SCHEMA = Venda
    INCREMENTAL_FIELD = "data_alteracao"
    INCREMENTAL_PARAM = "data_alteracao_de"

    # DDL alinhada com shape REAL confirmado via probe (mai/2026):
    #   - data: date da venda (campo `data` na API, nao `data_venda` que vem null)
    #   - criado_em: datetime de criacao (campo `criado_em`, nao `data_criacao`)
    #   - tipo_item: enum PRODUCT/SERVICE (campo `itens` na API — NOMENCLATURA
    #     enganosa, nao e' lista de items mas tipo de venda)
    #   - condicao_pagamento, versao: campos extras vistos no probe
    DDL = """
    CREATE TABLE IF NOT EXISTS "{schema}"."conta_azul__vendas" (
        id                  UUID PRIMARY KEY,
        numero              BIGINT,
        situacao            TEXT,
        tipo                TEXT,
        tipo_item           TEXT,
        origem              TEXT,
        pendente            BOOLEAN,
        condicao_pagamento  BOOLEAN,
        versao              INTEGER,
        total               NUMERIC(18, 4),
        desconto            NUMERIC(18, 4),
        data_venda          DATE,
        criado_em           TIMESTAMPTZ,
        data_alteracao      TIMESTAMPTZ,
        observacoes         TEXT,
        cliente             JSONB,
        vendedor            JSONB,
        natureza_operacao   JSONB,
        categoria           JSONB,
        pagamento           JSONB,
        id_legado           BIGINT,
        id_legado_cliente   BIGINT,
        id_legado_dono      BIGINT,
        raw                 JSONB,
        loaded_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """

    UPSERT_SQL = """
    INSERT INTO "{schema}"."conta_azul__vendas"
        (id, numero, situacao, tipo, tipo_item, origem, pendente,
         condicao_pagamento, versao, total, desconto, data_venda,
         criado_em, data_alteracao, observacoes,
         cliente, vendedor, natureza_operacao, categoria, pagamento,
         id_legado, id_legado_cliente, id_legado_dono, raw)
    VALUES
        (%(id)s, %(numero)s, %(situacao)s, %(tipo)s, %(tipo_item)s,
         %(origem)s, %(pendente)s, %(condicao_pagamento)s, %(versao)s,
         %(total)s, %(desconto)s, %(data_venda)s,
         %(criado_em)s, %(data_alteracao)s, %(observacoes)s,
         %(cliente)s::jsonb, %(vendedor)s::jsonb, %(natureza_operacao)s::jsonb,
         %(categoria)s::jsonb, %(pagamento)s::jsonb,
         %(id_legado)s, %(id_legado_cliente)s, %(id_legado_dono)s,
         %(raw)s::jsonb)
    ON CONFLICT (id) DO UPDATE SET
        numero             = EXCLUDED.numero,
        situacao           = EXCLUDED.situacao,
        tipo               = EXCLUDED.tipo,
        tipo_item          = EXCLUDED.tipo_item,
        origem             = EXCLUDED.origem,
        pendente           = EXCLUDED.pendente,
        condicao_pagamento = EXCLUDED.condicao_pagamento,
        versao             = EXCLUDED.versao,
        total              = EXCLUDED.total,
        desconto           = EXCLUDED.desconto,
        data_venda         = EXCLUDED.data_venda,
        data_alteracao     = EXCLUDED.data_alteracao,
        observacoes        = EXCLUDED.observacoes,
        cliente            = EXCLUDED.cliente,
        vendedor           = EXCLUDED.vendedor,
        natureza_operacao  = EXCLUDED.natureza_operacao,
        categoria          = EXCLUDED.categoria,
        pagamento          = EXCLUDED.pagamento,
        id_legado          = EXCLUDED.id_legado,
        id_legado_cliente  = EXCLUDED.id_legado_cliente,
        id_legado_dono     = EXCLUDED.id_legado_dono,
        raw                = EXCLUDED.raw,
        loaded_at          = NOW()
    """

    def fetch(self, client: Any, **kwargs: Any) -> Iterator[Venda]:
        """Pagina /v1/venda/busca.

        Filtro temporal incremental: data_alteracao_de + data_alteracao_ate.
        Formato ISO 8601 SEM microsegundos e SEM timezone — mesmo padrao
        de pessoas (testado em produdacao).

        Sem watermark: full refresh. data_alteracao_ate=now garante que a
        primeira run pegue tudo.
        """
        watermark = kwargs.get("watermark")
        extra_params: dict[str, Any] | None = None
        if watermark and watermark.get("value"):
            wm = watermark["value"].split(".")[0].split("+")[0].rstrip("Z")
            extra_params = {
                "data_alteracao_de": wm,
                "data_alteracao_ate": datetime.utcnow().replace(
                    microsecond=0
                ).isoformat(),
            }
        yield from client.paginate_all(
            "/venda/busca", Venda,
            extra_params=extra_params,
        )

    def serialize_for_upsert(self, item: Venda) -> dict[str, Any]:
        raw_dict = item.model_dump(mode="json", by_alias=True)
        return {
            "id": str(item.id),
            "numero": item.numero,
            "situacao": item.situacao,
            "tipo": item.tipo,
            "tipo_item": item.tipo_item,
            "origem": item.origem,
            "pendente": item.pendente,
            "condicao_pagamento": item.condicao_pagamento,
            "versao": item.versao,
            "total": item.total,
            "desconto": item.desconto,
            # Campo `data` da API (date) — armazenado em data_venda no DW
            # pra clareza analitica.
            "data_venda": item.data,
            "criado_em": item.criado_em,
            "data_alteracao": item.data_alteracao,
            "observacoes": item.observacoes,
            "cliente": json.dumps(item.cliente) if item.cliente else None,
            "vendedor": json.dumps(item.vendedor) if item.vendedor else None,
            "natureza_operacao": (
                json.dumps(item.natureza_operacao) if item.natureza_operacao else None
            ),
            "categoria": json.dumps(item.categoria) if item.categoria else None,
            "pagamento": json.dumps(item.pagamento) if item.pagamento else None,
            "id_legado": item.id_legado,
            "id_legado_cliente": item.id_legado_cliente,
            "id_legado_dono": item.id_legado_dono,
            "raw": json.dumps(raw_dict, default=str),
        }

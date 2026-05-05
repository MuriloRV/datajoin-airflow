"""Extractor de notas fiscais de produto (NFe).

Endpoint API:  GET /v1/notas-fiscais
Tabela raw:    <schema>.conta_azul__notas_fiscais

Doc oficial:
  - Filtros obrigatorios: data_inicial, data_final (YYYY-MM-DD).
  - Page size enum: 10 | 20 | 50 | 100.
  - Envelope: {itens, paginacao: {pagina_atual, tamanho_pagina,
    total_itens, total_paginas}} — nested totalizer (paginate_all
    detecta ambos formatos).
  - Hoje so retorna NFe com status EMITIDA e CORRIGIDA_SUCESSO
    (outros status estao em construcao na API).

Diferenca vs notas_servico:
  - notas_servico = NFS-e (servico), endpoint /v1/notas-fiscais-servico,
    janela MAX 15 dias documentada, todos status visiveis.
  - notas_fiscais = NFe (produto), endpoint /v1/notas-fiscais, sem
    limite de janela documentado, so EMITIDA + CORRIGIDA_SUCESSO.

Janela: rolante de 1 ano em chunks de 30 dias — defensivo (a doc nao
expoe limite de range, mas chunks evitam timeouts em volumes altos).
Idempotente: UPSERT por id.
"""
from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any, Iterator

from connectors.conta_azul.extractors.base import Extractor
from connectors.conta_azul.schemas import NotaFiscal


class NotasFiscaisExtractor(Extractor):
    NAME = "notas_fiscais"
    SCHEMA = NotaFiscal

    DDL = """
    CREATE TABLE IF NOT EXISTS "{schema}"."conta_azul__notas_fiscais" (
        id                 UUID PRIMARY KEY,
        numero             TEXT,
        status             TEXT,
        valor              NUMERIC(18, 4),
        data_emissao       TIMESTAMPTZ,
        chave_acesso       TEXT,
        serie              TEXT,
        venda_id           UUID,
        cliente_id         UUID,
        documento_tomador  TEXT,
        raw                JSONB,
        loaded_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """

    UPSERT_SQL = """
    INSERT INTO "{schema}"."conta_azul__notas_fiscais"
        (id, numero, status, valor, data_emissao, chave_acesso, serie,
         venda_id, cliente_id, documento_tomador, raw)
    VALUES
        (%(id)s, %(numero)s, %(status)s, %(valor)s, %(data_emissao)s,
         %(chave_acesso)s, %(serie)s, %(venda_id)s, %(cliente_id)s,
         %(documento_tomador)s, %(raw)s::jsonb)
    ON CONFLICT (id) DO UPDATE SET
        numero            = EXCLUDED.numero,
        status            = EXCLUDED.status,
        valor             = EXCLUDED.valor,
        data_emissao      = EXCLUDED.data_emissao,
        chave_acesso      = EXCLUDED.chave_acesso,
        serie             = EXCLUDED.serie,
        venda_id          = EXCLUDED.venda_id,
        cliente_id        = EXCLUDED.cliente_id,
        documento_tomador = EXCLUDED.documento_tomador,
        raw               = EXCLUDED.raw,
        loaded_at         = NOW()
    """

    def fetch(self, client: Any, **kwargs: Any) -> Iterator[NotaFiscal]:
        # Janela rolante 1 ano em chunks de 15 dias.
        # API enforce 15 dias maximo por chamada (msg explicita: "O periodo
        # entre data_competencia_de e data_competencia_ate nao pode ser
        # maior que 15 dias") — mesma regra do /notas-fiscais-servico.
        today = date.today()
        one_year_ago = today - timedelta(days=365)

        cursor = one_year_ago
        while cursor < today:
            window_end = min(cursor + timedelta(days=15), today)
            yield from client.paginate_all(
                "/notas-fiscais",
                NotaFiscal,
                page_size=100,
                extra_params={
                    "data_inicial": cursor.isoformat(),
                    "data_final": window_end.isoformat(),
                },
            )
            cursor = window_end + timedelta(days=1)

    def serialize_for_upsert(self, item: NotaFiscal) -> dict[str, Any]:
        raw_dict = item.model_dump(mode="json")
        return {
            "id": str(item.id),
            "numero": item.numero,
            "status": item.status,
            "valor": item.valor,
            "data_emissao": item.data_emissao,
            "chave_acesso": item.chave_acesso,
            "serie": item.serie,
            "venda_id": str(item.venda_id) if item.venda_id else None,
            "cliente_id": str(item.cliente_id) if item.cliente_id else None,
            "documento_tomador": item.documento_tomador,
            "raw": json.dumps(raw_dict, default=str),
        }

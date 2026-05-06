"""Extractor de detalhe de servicos (N+1 a partir de raw.servicos).

Endpoint API: GET /v1/servicos/{id}  (fallback /v1/servico/{id})
Tabela raw:   <schema>.conta_azul__servicos_detalhe

Adiciona tributacao (ISS/PIS/COFINS), codigo de servico municipal,
observacoes fiscais — campos AUSENTES do agregado de /v1/servicos.

Observacao: o extractor `servicos` usa /v1/servico (singular). Para o
detalhe a doc oficial expoe /v1/servicos/{id} mas o singular costuma
ser aceito tambem — o client tenta plural primeiro com fallback.

Sem watermark de data_alteracao no schema base de Servico (api nao
expoe consistentemente). Usamos full refresh — volume baixo.
"""
from __future__ import annotations

import json
import logging
from typing import Any, ClassVar, Iterator

from connectors.conta_azul.extractors.base import Extractor
from connectors.conta_azul.schemas import ServicoDetalhe


log = logging.getLogger(__name__)


class ServicosDetalheExtractor(Extractor):
    NAME = "servicos_detalhe"
    SCHEMA = ServicoDetalhe

    REQUIRES_DW_READ: ClassVar[bool] = True

    DDL = """
    CREATE TABLE IF NOT EXISTS "{schema}"."conta_azul__servicos_detalhe" (
        id                       UUID PRIMARY KEY,
        nome                     TEXT,
        codigo                   TEXT,
        descricao                TEXT,
        preco                    NUMERIC(18, 4),
        custo                    NUMERIC(18, 4),
        aliquota_iss             NUMERIC(18, 4),
        codigo_servico_municipal TEXT,
        tributacao               JSONB,
        observacao               TEXT,
        status                   TEXT,
        tipo_servico             TEXT,
        data_criacao             TIMESTAMPTZ,
        data_alteracao           TIMESTAMPTZ,
        raw                      JSONB,
        loaded_at                TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """

    UPSERT_SQL = """
    INSERT INTO "{schema}"."conta_azul__servicos_detalhe"
        (id, nome, codigo, descricao, preco, custo, aliquota_iss,
         codigo_servico_municipal, tributacao, observacao, status,
         tipo_servico, data_criacao, data_alteracao, raw)
    VALUES
        (%(id)s, %(nome)s, %(codigo)s, %(descricao)s, %(preco)s,
         %(custo)s, %(aliquota_iss)s, %(codigo_servico_municipal)s,
         %(tributacao)s::jsonb, %(observacao)s, %(status)s,
         %(tipo_servico)s, %(data_criacao)s, %(data_alteracao)s,
         %(raw)s::jsonb)
    ON CONFLICT (id) DO UPDATE SET
        nome                     = EXCLUDED.nome,
        codigo                   = EXCLUDED.codigo,
        descricao                = EXCLUDED.descricao,
        preco                    = EXCLUDED.preco,
        custo                    = EXCLUDED.custo,
        aliquota_iss             = EXCLUDED.aliquota_iss,
        codigo_servico_municipal = EXCLUDED.codigo_servico_municipal,
        tributacao               = EXCLUDED.tributacao,
        observacao               = EXCLUDED.observacao,
        status                   = EXCLUDED.status,
        tipo_servico             = EXCLUDED.tipo_servico,
        data_alteracao           = EXCLUDED.data_alteracao,
        raw                      = EXCLUDED.raw,
        loaded_at                = NOW()
    """

    def fetch(self, client: Any, **kwargs: Any) -> Iterator[ServicoDetalhe]:
        dw_conn = kwargs.get("dw_conn")
        schema_raw = kwargs.get("schema_raw")

        if dw_conn is None or schema_raw is None:
            log.error("ServicosDetalheExtractor: faltando dw_conn ou schema_raw")
            return

        with dw_conn.cursor() as cur:
            cur.execute(
                f'SELECT id::text FROM "{schema_raw}".conta_azul__servicos'
            )
            servico_ids = [row[0] for row in cur.fetchall()]

        log.info(
            "ServicosDetalhe: %d servicos pra buscar detalhe", len(servico_ids)
        )
        for sid in servico_ids:
            detalhe_raw = client.get_servico_detalhe(sid)
            if detalhe_raw is None:
                continue
            detalhe_raw.setdefault("id", sid)
            try:
                detalhe = ServicoDetalhe.model_validate(detalhe_raw)
            except Exception:
                log.exception(
                    "ServicosDetalhe: falha parseando servico %s — pulando", sid
                )
                continue
            yield detalhe

    def serialize_for_upsert(self, item: ServicoDetalhe) -> dict[str, Any]:
        raw_dict = item.model_dump(mode="json")
        return {
            "id": str(item.id),
            "nome": item.nome,
            "codigo": item.codigo,
            "descricao": item.descricao,
            "preco": item.preco,
            "custo": item.custo,
            "aliquota_iss": item.aliquota_iss,
            "codigo_servico_municipal": item.codigo_servico_municipal,
            "tributacao": json.dumps(item.tributacao) if item.tributacao else None,
            "observacao": item.observacao,
            "status": item.status,
            "tipo_servico": item.tipo_servico,
            "data_criacao": item.data_criacao,
            "data_alteracao": item.data_alteracao,
            "raw": json.dumps(raw_dict, default=str),
        }

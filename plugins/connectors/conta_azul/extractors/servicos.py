"""Extractor de servicos.

Endpoint API:  GET /v1/servico (singular)
Tabela raw:    <schema>.conta_azul__servicos (plural — convencao do raw)

Volume medio. Sem nested.
"""
from __future__ import annotations

from typing import Any, Iterator

from connectors.conta_azul.extractors.base import Extractor
from connectors.conta_azul.schemas import Servico


class ServicosExtractor(Extractor):
    NAME = "servicos"
    SCHEMA = Servico

    DDL = """
    CREATE TABLE IF NOT EXISTS "{schema}"."conta_azul__servicos" (
        id            UUID PRIMARY KEY,
        id_servico    BIGINT,
        nome          TEXT,
        codigo        TEXT,
        descricao     TEXT,
        preco         NUMERIC(18, 4),
        custo         NUMERIC(18, 4),
        status        TEXT,
        tipo_servico  TEXT,
        loaded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """

    UPSERT_SQL = """
    INSERT INTO "{schema}"."conta_azul__servicos"
        (id, id_servico, nome, codigo, descricao, preco, custo, status, tipo_servico)
    VALUES
        (%(id)s, %(id_servico)s, %(nome)s, %(codigo)s, %(descricao)s,
         %(preco)s, %(custo)s, %(status)s, %(tipo_servico)s)
    ON CONFLICT (id) DO UPDATE SET
        id_servico   = EXCLUDED.id_servico,
        nome         = EXCLUDED.nome,
        codigo       = EXCLUDED.codigo,
        descricao    = EXCLUDED.descricao,
        preco        = EXCLUDED.preco,
        custo        = EXCLUDED.custo,
        status       = EXCLUDED.status,
        tipo_servico = EXCLUDED.tipo_servico,
        loaded_at    = NOW()
    """

    def fetch(self, client: Any, **kwargs: Any) -> Iterator[Servico]:
        # /servico ignora o param `pagina` (bug da API). paginate_all
        # detecta via itens_totais e para. Sem incremental.
        yield from client.paginate_all("/servico", Servico)

    def serialize_for_upsert(self, item: Servico) -> dict[str, Any]:
        return {
            "id": str(item.id),
            "id_servico": item.id_servico,
            "nome": item.nome,
            "codigo": item.codigo,
            "descricao": item.descricao,
            "preco": item.preco,
            "custo": item.custo,
            "status": item.status,
            "tipo_servico": item.tipo_servico,
        }

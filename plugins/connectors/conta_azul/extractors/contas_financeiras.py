"""Extractor de contas financeiras (bancos/caixas/cartoes).

Endpoint API:  GET /v1/conta-financeira
Tabela raw:    <schema>.conta_azul__contas_financeiras

Volume baixo (dimensao). Sem nested.
"""
from __future__ import annotations

from typing import Any, Iterator

from connectors.conta_azul.extractors.base import Extractor
from connectors.conta_azul.schemas import ContaFinanceira


class ContasFinanceirasExtractor(Extractor):
    NAME = "contas_financeiras"
    SCHEMA = ContaFinanceira

    DDL = """
    CREATE TABLE IF NOT EXISTS "{schema}"."conta_azul__contas_financeiras" (
        id                              UUID PRIMARY KEY,
        nome                            TEXT NOT NULL,
        tipo                            TEXT,
        banco                           TEXT,
        codigo_banco                    INTEGER,
        agencia                         TEXT,
        numero                          TEXT,
        ativo                           BOOLEAN,
        conta_padrao                    BOOLEAN,
        possui_config_boleto_bancario   BOOLEAN,
        loaded_at                       TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """

    UPSERT_SQL = """
    INSERT INTO "{schema}"."conta_azul__contas_financeiras"
        (id, nome, tipo, banco, codigo_banco, agencia, numero, ativo,
         conta_padrao, possui_config_boleto_bancario)
    VALUES
        (%(id)s, %(nome)s, %(tipo)s, %(banco)s, %(codigo_banco)s,
         %(agencia)s, %(numero)s, %(ativo)s, %(conta_padrao)s,
         %(possui_config_boleto_bancario)s)
    ON CONFLICT (id) DO UPDATE SET
        nome                          = EXCLUDED.nome,
        tipo                          = EXCLUDED.tipo,
        banco                         = EXCLUDED.banco,
        codigo_banco                  = EXCLUDED.codigo_banco,
        agencia                       = EXCLUDED.agencia,
        numero                        = EXCLUDED.numero,
        ativo                         = EXCLUDED.ativo,
        conta_padrao                  = EXCLUDED.conta_padrao,
        possui_config_boleto_bancario = EXCLUDED.possui_config_boleto_bancario,
        loaded_at                     = NOW()
    """

    def fetch(self, client: Any, **kwargs: Any) -> Iterator[ContaFinanceira]:
        yield from client.paginate_all("/conta-financeira", ContaFinanceira)

    def serialize_for_upsert(self, item: ContaFinanceira) -> dict[str, Any]:
        return {
            "id": str(item.id),
            "nome": item.nome,
            "tipo": item.tipo,
            "banco": item.banco,
            "codigo_banco": item.codigo_banco,
            "agencia": item.agencia,
            "numero": item.numero,
            "ativo": item.ativo,
            "conta_padrao": item.conta_padrao,
            "possui_config_boleto_bancario": item.possui_config_boleto_bancario,
        }

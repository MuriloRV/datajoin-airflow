"""Extractor da conta conectada (info do tenant).

Endpoint API:  GET /v1/pessoas/conta-conectada
Tabela raw:    <schema>.conta_azul__conta_conectada

Padrao DIFERENTE: response e' um objeto unico (sem envelope `itens`),
sem paginacao. PK = documento (CNPJ) — natural key.

Volume: 1 row por tenant. Atualiza poucas vezes por ano. Full refresh.
"""
from __future__ import annotations

import json
import logging
from typing import Any, Iterator

from connectors.conta_azul.extractors.base import Extractor
from connectors.conta_azul.schemas import ContaConectada


log = logging.getLogger(__name__)


class ContaConectadaExtractor(Extractor):
    NAME = "conta_conectada"
    SCHEMA = ContaConectada

    # PK synthetic — sempre 1 row por schema_raw (1 tenant). Documento
    # pode vir vazio em contas dev/test (visto Luminea), entao nao serve
    # como PK. Constant=1 + check constraint garante singleton.
    DDL = """
    CREATE TABLE IF NOT EXISTS "{schema}"."conta_azul__conta_conectada" (
        pk              INTEGER PRIMARY KEY DEFAULT 1 CHECK (pk = 1),
        documento       TEXT,
        razao_social    TEXT,
        nome_fantasia   TEXT,
        email           TEXT,
        data_fundacao   TEXT,
        raw             JSONB,
        loaded_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """

    UPSERT_SQL = """
    INSERT INTO "{schema}"."conta_azul__conta_conectada"
        (pk, documento, razao_social, nome_fantasia, email, data_fundacao, raw)
    VALUES
        (1, %(documento)s, %(razao_social)s, %(nome_fantasia)s, %(email)s,
         %(data_fundacao)s, %(raw)s::jsonb)
    ON CONFLICT (pk) DO UPDATE SET
        documento     = EXCLUDED.documento,
        razao_social  = EXCLUDED.razao_social,
        nome_fantasia = EXCLUDED.nome_fantasia,
        email         = EXCLUDED.email,
        data_fundacao = EXCLUDED.data_fundacao,
        raw           = EXCLUDED.raw,
        loaded_at     = NOW()
    """

    def fetch(self, client: Any, **kwargs: Any) -> Iterator[ContaConectada]:
        if client._mock:
            return
        # Single-object endpoint — usa client.get_single (helper publico).
        # Erros HTTP / validation re-lancam pra task falhar e Airflow
        # retry — consistente com os outros extractors. Engolir errors
        # silenciosamente esconde problemas reais (ja causou bug em prod).
        data = client.get_single("/pessoas/conta-conectada")
        if data is None:
            return
        if not isinstance(data, dict):
            raise TypeError(
                f"conta_conectada: esperava dict, recebeu "
                f"{type(data).__name__}: {str(data)[:200]}"
            )
        try:
            yield ContaConectada.model_validate(data)
        except Exception:
            log.error(
                "conta_conectada: schema validation falhou — keys=%s, sample=%s",
                list(data.keys()),
                json.dumps(data, ensure_ascii=False, default=str)[:1500],
            )
            raise

    def serialize_for_upsert(self, item: ContaConectada) -> dict[str, Any]:
        return {
            "documento": item.documento,
            "razao_social": item.razao_social,
            "nome_fantasia": item.nome_fantasia,
            "email": item.email,
            "data_fundacao": item.data_fundacao,
            "raw": json.dumps(item.model_dump(mode="json"), default=str),
        }
